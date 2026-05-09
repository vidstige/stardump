type Vec3 = [number, number, number];

type NodeTable = {
  nodeCount:         number;
  halfExtentPc:      number;
  depth:             number;
  childMask:         Uint8Array;
  firstChild:        Uint32Array;
  pointFirst:        Uint32Array;
  pointCount:        Uint32Array;
  subtreePointCount: Uint32Array;
  pointsOffset:      number;
};

type WantedNode = { nodeIdx: number; priority: number };

type InitMsg = { type: 'init'; nodeTable: NodeTable; apiRoot: string; dataset: string };
type ViewMsg = { type: 'view'; eye: Vec3; pixelsPerRadian: number; pixelThreshold: number };

export type DrawRange   = { chunkId: number; byteOffset: number; count: number };
export type FrameMsg    = { type: 'frame';    draws: DrawRange[] };
export type ChunkMsg    = { type: 'chunk';    chunkId: number; data: Float32Array };
export type ProgressMsg = { type: 'progress'; cached: number; inFlight: number; total: number };
export type LodWorkerMsg = FrameMsg | ChunkMsg | ProgressMsg;

const MAX_CONCURRENT_FETCHES = 16;
const MAX_BATCH_BYTES        = 1024 * 1024;
const MAX_CHUNK_POINTS       = 65536;
const MERGE_FOOTPRINT_FACTOR = 8;
const CAMERA_MOVE_FACTOR     = 0.02; // re-walk when eye moves > 2% of scene half-extent
const CAMERA_ZOOM_FACTOR     = 1.05; // re-walk when zoom changes by 5%

let nodeTable: NodeTable | null = null;
let nodePointCache  = new Map<number, Float32Array>();
let pendingFetches  = new Set<number>();
let pendingRequests = 0;
let wantedNodes: WantedNode[] = [];
let lodCachedCount = 0;
let sentChunks = new Map<number, number>(); // chunkId -> sum-of-nodeIdx token

let apiRoot = '';
let dataset = '';

let latestEye: Vec3       = [0, 0, 0];
let latestPixelsPerRadian = 1;
let latestPixelThreshold  = 8;

let lastWalkEye: Vec3         = [Infinity, Infinity, Infinity];
let lastWalkPixelsPerRadian   = 0;
let lastWalkPixelThreshold    = 0;

self.addEventListener('message', (e: MessageEvent<InitMsg | ViewMsg>) => {
  const msg = e.data;
  if (msg.type === 'init') {
    nodeTable             = msg.nodeTable;
    apiRoot               = msg.apiRoot;
    dataset               = msg.dataset;
    nodePointCache        = new Map();
    pendingFetches        = new Set();
    pendingRequests       = 0;
    wantedNodes           = [];
    sentChunks            = new Map();
    lastWalkEye           = [Infinity, Infinity, Infinity];
    lastWalkPixelsPerRadian = 0;
    lastWalkPixelThreshold  = 0;
  } else {
    latestEye             = msg.eye;
    latestPixelsPerRadian = msg.pixelsPerRadian;
    latestPixelThreshold  = msg.pixelThreshold;
    if (cameraMovedEnough()) runLodUpdate();
  }
});

function cameraMovedEnough(): boolean {
  if (!nodeTable) return false;
  if (latestPixelThreshold !== lastWalkPixelThreshold) return true;
  const dx = latestEye[0] - lastWalkEye[0];
  const dy = latestEye[1] - lastWalkEye[1];
  const dz = latestEye[2] - lastWalkEye[2];
  const moveSq = dx*dx + dy*dy + dz*dz;
  const threshold = nodeTable.halfExtentPc * CAMERA_MOVE_FACTOR;
  if (moveSq > threshold * threshold) return true;
  if (lastWalkPixelsPerRadian === 0) return true;
  const ratio = latestPixelsPerRadian / lastWalkPixelsPerRadian;
  return ratio > CAMERA_ZOOM_FACTOR || ratio < 1 / CAMERA_ZOOM_FACTOR;
}

// Iterative subtree scan: collects cached LOD-cut nodes and counts total expected.
// Using an explicit stack avoids deep call-stack recursion for large trees.
function collectSubtreeNodes(
  nt: NodeTable,
  eye: Vec3,
  pixelsPerRadian: number,
  pixelThreshold: number,
  rootIdx: number,
  rootMinX: number, rootMinY: number, rootMinZ: number,
  rootMaxX: number, rootMaxY: number, rootMaxZ: number,
): { cached: number[]; total: number } {
  const cached: number[] = [];
  let total = 0;
  const stack: [number, number, number, number, number, number, number][] = [
    [rootIdx, rootMinX, rootMinY, rootMinZ, rootMaxX, rootMaxY, rootMaxZ],
  ];
  while (stack.length > 0) {
    const [nodeIdx, minX, minY, minZ, maxX, maxY, maxZ] = stack.pop()!;
    const cm     = nt.childMask[nodeIdx];
    const pCount = nt.pointCount[nodeIdx];
    const cx = (minX + maxX) * 0.5, cy = (minY + maxY) * 0.5, cz = (minZ + maxZ) * 0.5;
    const half = (maxX - minX) * 0.5;
    const dx = cx - eye[0], dy = cy - eye[1], dz = cz - eye[2];
    const dist = Math.max(Math.sqrt(dx*dx + dy*dy + dz*dz), half);
    const footprintPx = (half / dist) * pixelsPerRadian;
    if (cm === 0 || (footprintPx < pixelThreshold && pCount > 0)) {
      if (pCount > 0) {
        total++;
        if (nodePointCache.has(nodeIdx)) cached.push(nodeIdx);
      }
      continue;
    }
    const mx = cx, my = cy, mz = cz;
    let childIdx = nt.firstChild[nodeIdx];
    for (let c = 0; c < 8; c++) {
      if ((cm & (1 << c)) === 0) continue;
      stack.push([
        childIdx,
        (c & 1) === 0 ? minX : mx, (c & 2) === 0 ? minY : my, (c & 4) === 0 ? minZ : mz,
        (c & 1) === 0 ? mx : maxX, (c & 2) === 0 ? my : maxY, (c & 4) === 0 ? mz : maxZ,
      ]);
      childIdx++;
    }
  }
  return { cached, total };
}

function emitFrame(): void {
  if (!nodeTable) return;
  const nt = nodeTable;
  const draws: DrawRange[] = [];
  const activeChunks = new Set<number>();
  const mergeThreshold = latestPixelThreshold * MERGE_FOOTPRINT_FACTOR;

  function emitChunk(chunkId: number, nodes: number[]): void {
    if (nodes.length === 0) return;
    const totalPoints = nodes.reduce((s, n) => s + nt.pointCount[n], 0);
    if (totalPoints === 0) return;
    activeChunks.add(chunkId);
    const token = nodes.reduce((s, n) => s + n, 0);
    if (sentChunks.get(chunkId) !== token) {
      const packed = new Float32Array(totalPoints * 5);
      let off = 0;
      for (const n of nodes) {
        packed.set(nodePointCache.get(n)!, off);
        off += nt.pointCount[n] * 5;
      }
      self.postMessage({ type: 'chunk', chunkId, data: packed } as ChunkMsg);
      sentChunks.set(chunkId, token);
    }
    draws.push({ chunkId, byteOffset: 0, count: totalPoints });
  }

  // Returns the cached LOD-cut nodes for this subtree when fully loaded, null otherwise.
  // Recursion is bounded to nodes above mergeThreshold (a few levels at most).
  function walk(
    nodeIdx: number,
    minX: number, minY: number, minZ: number,
    maxX: number, maxY: number, maxZ: number,
  ): number[] | null {
    const cm     = nt.childMask[nodeIdx];
    const pCount = nt.pointCount[nodeIdx];
    const cx = (minX + maxX) * 0.5, cy = (minY + maxY) * 0.5, cz = (minZ + maxZ) * 0.5;
    const half = (maxX - minX) * 0.5;
    const dx = cx - latestEye[0], dy = cy - latestEye[1], dz = cz - latestEye[2];
    const dist = Math.max(Math.sqrt(dx*dx + dy*dy + dz*dz), half);
    const footprintPx = (half / dist) * latestPixelsPerRadian;

    // LOD cut: node is fine-grained enough — use its own sample data
    if (cm === 0 || (footprintPx < latestPixelThreshold && pCount > 0)) {
      if (pCount === 0) return [];
      return nodePointCache.has(nodeIdx) ? [nodeIdx] : null;
    }

    // Merge boundary: pack everything in this subtree into one chunk.
    // If fully loaded, return the list so an ancestor can merge further.
    if (footprintPx < mergeThreshold) {
      const { cached, total } = collectSubtreeNodes(
        nt, latestEye, latestPixelsPerRadian, latestPixelThreshold,
        nodeIdx, minX, minY, minZ, maxX, maxY, maxZ,
      );
      if (cached.length === total) return cached;
      emitChunk(nodeIdx, cached);
      return null;
    }

    // Above merge threshold: recurse and merge only when all children are complete
    const childResults: (number[] | null)[] = [];
    const childIndices: number[] = [];
    const mx = cx, my = cy, mz = cz;
    let childIdx = nt.firstChild[nodeIdx];
    let allComplete = true;
    for (let c = 0; c < 8; c++) {
      if ((cm & (1 << c)) === 0) continue;
      const result = walk(
        childIdx,
        (c & 1) === 0 ? minX : mx, (c & 2) === 0 ? minY : my, (c & 4) === 0 ? minZ : mz,
        (c & 1) === 0 ? mx : maxX, (c & 2) === 0 ? my : maxY, (c & 4) === 0 ? mz : maxZ,
      );
      childResults.push(result);
      childIndices.push(childIdx);
      if (result === null) allComplete = false;
      childIdx++;
    }
    if (allComplete) {
      const merged: number[] = [];
      for (const r of childResults) if (r) merged.push(...r);
      const totalPts = merged.reduce((s, n) => s + nt.pointCount[n], 0);
      if (totalPts > MAX_CHUNK_POINTS) {
        emitChunk(nodeIdx, merged);
        return null;
      }
      return merged;
    }
    for (let i = 0; i < childResults.length; i++) {
      const r = childResults[i];
      if (r !== null) emitChunk(childIndices[i], r);
    }
    return null;
  }

  const e = nt.halfExtentPc;
  if (nt.nodeCount > 0) {
    const rootResult = walk(0, -e, -e, -e, e, e, e);
    if (rootResult !== null) emitChunk(0, rootResult);
  }

  for (const chunkId of sentChunks.keys()) {
    if (!activeChunks.has(chunkId)) sentChunks.delete(chunkId);
  }

  self.postMessage({ type: 'frame', draws } as FrameMsg);
}

function runLodUpdate(): void {
  lastWalkEye           = [...latestEye] as Vec3;
  lastWalkPixelsPerRadian = latestPixelsPerRadian;
  lastWalkPixelThreshold  = latestPixelThreshold;

  emitFrame();
  rebuildWanted();
}

function evictCache(): void {
  const wantedSet = new Set(wantedNodes.map(w => w.nodeIdx));
  for (const nodeIdx of nodePointCache.keys()) {
    if (!wantedSet.has(nodeIdx)) nodePointCache.delete(nodeIdx);
  }
}

function rebuildWanted(): void {
  if (!nodeTable) return;
  const { wanted, cachedCount } = collectWanted(nodeTable, latestEye, latestPixelsPerRadian, latestPixelThreshold);
  wantedNodes    = wanted;
  lodCachedCount = cachedCount;
  evictCache();
  scheduleFetches();
  postProgress();
}

function collectWanted(
  nt: NodeTable,
  eye: Vec3,
  pixelsPerRadian: number,
  pixelThreshold: number,
): { wanted: WantedNode[]; cachedCount: number } {
  const wanted: WantedNode[] = [];
  let cachedCount = 0;

  function walk(
    nodeIdx: number,
    minX: number, minY: number, minZ: number,
    maxX: number, maxY: number, maxZ: number,
  ): void {
    const cm     = nt.childMask[nodeIdx];
    const pCount = nt.pointCount[nodeIdx];

    const cx = (minX + maxX) * 0.5, cy = (minY + maxY) * 0.5, cz = (minZ + maxZ) * 0.5;
    const half = (maxX - minX) * 0.5;
    const dx = cx - eye[0], dy = cy - eye[1], dz = cz - eye[2];
    const dist = Math.max(Math.sqrt(dx*dx + dy*dy + dz*dz), half);
    const footprintPx = (half / dist) * pixelsPerRadian;

    if (cm === 0 || (footprintPx < pixelThreshold && pCount > 0)) {
      if (pCount > 0) {
        wanted.push({ nodeIdx, priority: footprintPx });
        if (nodePointCache.has(nodeIdx)) cachedCount++;
      }
      return;
    }

    if (pCount > 0) {
      wanted.push({ nodeIdx, priority: footprintPx });
      if (nodePointCache.has(nodeIdx)) cachedCount++;
    }

    const mx = cx, my = cy, mz = cz;
    let childIdx = nt.firstChild[nodeIdx];
    for (let c = 0; c < 8; c++) {
      if ((cm & (1 << c)) === 0) continue;
      walk(childIdx,
        (c & 1) === 0 ? minX : mx, (c & 2) === 0 ? minY : my, (c & 4) === 0 ? minZ : mz,
        (c & 1) === 0 ? mx : maxX, (c & 2) === 0 ? my : maxY, (c & 4) === 0 ? mz : maxZ,
      );
      childIdx++;
    }
  }

  const e = nt.halfExtentPc;
  if (nt.nodeCount > 0) walk(0, -e, -e, -e, e, e, e);
  return { wanted, cachedCount };
}

function scheduleFetches(): void {
  if (!nodeTable) return;
  const nt = nodeTable;
  const slots = MAX_CONCURRENT_FETCHES - pendingRequests;
  if (slots <= 0) return;

  const candidates = wantedNodes
    .filter(w => !pendingFetches.has(w.nodeIdx) && !nodePointCache.has(w.nodeIdx))
    .sort((a, b) => nt.pointFirst[a.nodeIdx] - nt.pointFirst[b.nodeIdx]);

  const batches: { nodes: number[]; priority: number; bytes: number }[] = [];
  for (const w of candidates) {
    const nodeBytes = nt.pointCount[w.nodeIdx] * 20;
    if (batches.length > 0) {
      const last     = batches[batches.length - 1];
      const lastNode = last.nodes[last.nodes.length - 1];
      if (
        nt.pointFirst[w.nodeIdx] === nt.pointFirst[lastNode] + nt.pointCount[lastNode] &&
        last.bytes + nodeBytes <= MAX_BATCH_BYTES
      ) {
        last.nodes.push(w.nodeIdx);
        last.bytes += nodeBytes;
        last.priority = Math.max(last.priority, w.priority);
        continue;
      }
    }
    batches.push({ nodes: [w.nodeIdx], priority: w.priority, bytes: nodeBytes });
  }

  batches.sort((a, b) => b.priority - a.priority);
  for (let i = 0; i < batches.length && i < slots; i++) {
    void fetchBatch(batches[i].nodes);
  }
}

async function fetchRange(url: string, start: number, end: number): Promise<ArrayBuffer> {
  const resp = await fetch(url, { headers: { Range: `bytes=${start}-${end}` } });
  if (resp.status !== 206 && resp.status !== 200) {
    throw new Error(`fetch range ${start}-${end} failed: ${resp.status}`);
  }
  return resp.arrayBuffer();
}

async function fetchBatch(nodes: number[]): Promise<void> {
  const nt = nodeTable!;
  for (const nodeIdx of nodes) pendingFetches.add(nodeIdx);
  pendingRequests++;
  try {
    const url       = `${apiRoot}/datasets/${dataset}/starcloud.bin`;
    const firstNode = nodes[0];
    const lastNode  = nodes[nodes.length - 1];
    const start     = nt.pointsOffset + nt.pointFirst[firstNode] * 20;
    const end       = nt.pointsOffset + (nt.pointFirst[lastNode] + nt.pointCount[lastNode]) * 20 - 1;
    const buf       = await fetchRange(url, start, end);
    if (nodeTable === nt) {
      for (const nodeIdx of nodes) {
        const off = (nt.pointFirst[nodeIdx] - nt.pointFirst[firstNode]) * 20;
        nodePointCache.set(nodeIdx, new Float32Array(buf.slice(off, off + nt.pointCount[nodeIdx] * 20)));
      }
      emitFrame();
    }
  } finally {
    pendingRequests--;
    for (const nodeIdx of nodes) pendingFetches.delete(nodeIdx);
  }
  scheduleFetches();
  postProgress();
}

function postProgress(): void {
  const nowCached = wantedNodes.filter(w => nodePointCache.has(w.nodeIdx)).length;
  self.postMessage({
    type: 'progress',
    cached: lodCachedCount + nowCached,
    inFlight: pendingFetches.size,
    total: lodCachedCount + wantedNodes.length,
  } as ProgressMsg);
}

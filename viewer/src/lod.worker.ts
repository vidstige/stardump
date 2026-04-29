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

export type FrameMsg    = { type: 'frame';    data: Float32Array; count: number };
export type ProgressMsg = { type: 'progress'; loaded: number; total: number; pending: number };
export type LodWorkerMsg = FrameMsg | ProgressMsg;

const MAX_CONCURRENT_FETCHES = 16;
const MAX_BATCH_BYTES        = 128 * 1024;
const LOD_THROTTLE_MS        = 1000;

let nodeTable: NodeTable | null = null;
let nodePointCache  = new Map<number, Float32Array>();
let pendingFetches  = new Set<number>();
let pendingRequests = 0;
let wantedNodes: WantedNode[] = [];
let leafSet = new Set<number>();

let apiRoot = '';
let dataset = '';

let latestEye: Vec3       = [0, 0, 0];
let latestPixelsPerRadian = 1;
let latestPixelThreshold  = 8;

let lodDirty  = false;
let lastLodAt = -LOD_THROTTLE_MS;
let lodTimer: ReturnType<typeof setTimeout> | null = null;

self.addEventListener('message', (e: MessageEvent<InitMsg | ViewMsg>) => {
  const msg = e.data;
  if (msg.type === 'init') {
    nodeTable       = msg.nodeTable;
    apiRoot         = msg.apiRoot;
    dataset         = msg.dataset;
    nodePointCache  = new Map();
    pendingFetches  = new Set();
    pendingRequests = 0;
    wantedNodes     = [];
    leafSet         = buildLeafSet(msg.nodeTable);
    lastLodAt       = -LOD_THROTTLE_MS;
    lodDirty        = true;
  } else {
    latestEye             = msg.eye;
    latestPixelsPerRadian = msg.pixelsPerRadian;
    latestPixelThreshold  = msg.pixelThreshold;
    lodDirty = true;
    maybeRunLod();
  }
});

function maybeRunLod(): void {
  if (!lodDirty || !nodeTable) return;
  const now = performance.now();
  const remaining = LOD_THROTTLE_MS - (now - lastLodAt);
  if (remaining <= 0) {
    runLodUpdate();
    return;
  }
  if (lodTimer === null) {
    lodTimer = setTimeout(() => { lodTimer = null; maybeRunLod(); }, remaining);
  }
}

function runLodUpdate(): void {
  const nt = nodeTable!;
  lastLodAt = performance.now();
  lodDirty  = false;

  const ranges = collectRanges(nt, latestEye, latestPixelsPerRadian, latestPixelThreshold);
  let totalCount = 0;
  for (const r of ranges) totalCount += r.count;
  const packed = new Float32Array(totalCount * 5);
  let offset = 0;
  for (const r of ranges) {
    packed.set(nodePointCache.get(r.nodeIdx)!, offset * 5);
    offset += r.count;
  }
  self.postMessage({ type: 'frame', data: packed, count: offset } as FrameMsg, [packed.buffer]);

  rebuildWanted();
}

function rebuildWanted(): void {
  if (!nodeTable) return;
  wantedNodes = collectWanted(nodeTable, latestEye, latestPixelsPerRadian, latestPixelThreshold);
  scheduleFetches();
}

function collectRanges(
  nt: NodeTable,
  eye: Vec3,
  pixelsPerRadian: number,
  pixelThreshold: number,
): { nodeIdx: number; count: number }[] {
  const ranges: { nodeIdx: number; count: number }[] = [];

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
      if (nodePointCache.has(nodeIdx) && pCount > 0) ranges.push({ nodeIdx, count: pCount });
      return;
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
  return ranges;
}

function collectWanted(
  nt: NodeTable,
  eye: Vec3,
  pixelsPerRadian: number,
  pixelThreshold: number,
): WantedNode[] {
  const wanted: WantedNode[] = [];

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
      if (!nodePointCache.has(nodeIdx) && pCount > 0) {
        wanted.push({ nodeIdx, priority: footprintPx });
      }
      return;
    }

    if (!nodePointCache.has(nodeIdx) && pCount > 0) {
      wanted.push({ nodeIdx, priority: footprintPx });
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
  return wanted;
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
      postProgress();
      lodDirty = true;
      maybeRunLod();
    }
  } finally {
    pendingRequests--;
    for (const nodeIdx of nodes) pendingFetches.delete(nodeIdx);
  }
  rebuildWanted();
}

function postProgress(): void {
  let loaded = 0;
  for (const nodeIdx of leafSet) {
    if (nodePointCache.has(nodeIdx)) loaded++;
  }
  self.postMessage({
    type: 'progress', loaded, total: leafSet.size, pending: pendingRequests,
  } as ProgressMsg);
}

function buildLeafSet(nt: NodeTable): Set<number> {
  const leaves = new Set<number>();
  function walk(nodeIdx: number): void {
    if (nt.childMask[nodeIdx] === 0) { leaves.add(nodeIdx); return; }
    let childIdx = nt.firstChild[nodeIdx];
    for (let c = 0; c < 8; c++) {
      if ((nt.childMask[nodeIdx] & (1 << c)) === 0) continue;
      walk(childIdx);
      childIdx++;
    }
  }
  if (nt.nodeCount > 0) walk(0);
  return leaves;
}

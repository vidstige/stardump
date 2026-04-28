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

type FetchCandidate = { nodeIdx: number; priority: number; subtree: boolean };

type InitMsg = { type: 'init'; nodeTable: NodeTable; apiRoot: string; dataset: string };
type ViewMsg = { type: 'view'; eye: Vec3; pixelsPerRadian: number; pixelThreshold: number };

export type FrameMsg    = { type: 'frame';    data: Float32Array; count: number };
export type ProgressMsg = { type: 'progress'; loaded: number; total: number; pending: number };
export type LodWorkerMsg = FrameMsg | ProgressMsg;

const MAX_CONCURRENT_FETCHES = 16;
const MAX_SUBTREE_BYTES      = 32 * 1024;
const LOD_THROTTLE_MS        = 1000;
const FETCH_THROTTLE_MS      = 200;

let nodeTable: NodeTable | null = null;
let nodePointCache  = new Map<number, Float32Array>();
let pendingFetches  = new Set<number>();
let pendingHttpRequests = 0;
let fetchQueue: FetchCandidate[] = [];
let leafSet = new Set<number>();

let apiRoot = '';
let dataset = '';

let latestEye: Vec3           = [0, 0, 0];
let latestPixelsPerRadian     = 1;
let latestPixelThreshold      = 8;

let lodDirty  = false;
let lastLodAt = -LOD_THROTTLE_MS;
let lodTimer: ReturnType<typeof setTimeout> | null = null;

self.addEventListener('message', (e: MessageEvent<InitMsg | ViewMsg>) => {
  const msg = e.data;
  if (msg.type === 'init') {
    nodeTable           = msg.nodeTable;
    apiRoot             = msg.apiRoot;
    dataset             = msg.dataset;
    nodePointCache      = new Map();
    pendingFetches      = new Set();
    pendingHttpRequests = 0;
    fetchQueue          = [];
    leafSet             = buildLeafSet(msg.nodeTable);
    lastLodAt           = -LOD_THROTTLE_MS;
    lodDirty            = true;
  } else {
    latestEye             = msg.eye;
    latestPixelsPerRadian = msg.pixelsPerRadian;
    latestPixelThreshold  = msg.pixelThreshold;
    lodDirty = true;
    maybeRunLod();
  }
});

setInterval(scheduleFetches, FETCH_THROTTLE_MS);

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

  const slots = Math.max(MAX_CONCURRENT_FETCHES - pendingHttpRequests, 0);
  const candidates: FetchCandidate[] = [];
  const ranges = rebuildLod(nt, latestEye, latestPixelsPerRadian, latestPixelThreshold, candidates, slots);

  let totalCount = 0;
  for (const r of ranges) {
    if (nodePointCache.has(r.nodeIdx)) totalCount += r.count;
  }
  const packed = new Float32Array(totalCount * 5);
  let offset = 0;
  for (const r of ranges) {
    const pts = nodePointCache.get(r.nodeIdx);
    if (!pts) continue;
    packed.set(pts, offset * 5);
    offset += r.count;
  }

  self.postMessage({ type: 'frame', data: packed, count: offset } as FrameMsg, [packed.buffer]);
  fetchQueue = candidates;
}

function rebuildLod(
  nt: NodeTable,
  eye: Vec3,
  pixelsPerRadian: number,
  pixelThreshold: number,
  fetchCandidates: FetchCandidate[],
  maxFetchCandidates: number,
): { nodeIdx: number; count: number }[] {
  const ranges: { nodeIdx: number; count: number }[] = [];

  let heapMin = -Infinity;
  let heapMinIdx = 0;

  function pushCandidate(nodeIdx: number, priority: number, subtree: boolean): void {
    if (fetchCandidates.length < maxFetchCandidates) {
      fetchCandidates.push({ nodeIdx, priority, subtree });
      if (fetchCandidates.length === maxFetchCandidates) {
        heapMin = fetchCandidates[0].priority; heapMinIdx = 0;
        for (let i = 1; i < fetchCandidates.length; i++) {
          if (fetchCandidates[i].priority < heapMin) { heapMin = fetchCandidates[i].priority; heapMinIdx = i; }
        }
      }
    } else if (priority > heapMin) {
      fetchCandidates[heapMinIdx] = { nodeIdx, priority, subtree };
      heapMin = fetchCandidates[0].priority; heapMinIdx = 0;
      for (let i = 1; i < fetchCandidates.length; i++) {
        if (fetchCandidates[i].priority < heapMin) { heapMin = fetchCandidates[i].priority; heapMinIdx = i; }
      }
    }
  }

  function walk(
    nodeIdx: number,
    minX: number, minY: number, minZ: number,
    maxX: number, maxY: number, maxZ: number,
  ): void {
    const cm     = nt.childMask[nodeIdx];
    const pCount = nt.pointCount[nodeIdx];

    const cx = (minX + maxX) * 0.5;
    const cy = (minY + maxY) * 0.5;
    const cz = (minZ + maxZ) * 0.5;
    const half = (maxX - minX) * 0.5;
    const dx = cx - eye[0], dy = cy - eye[1], dz = cz - eye[2];
    const dist = Math.max(Math.sqrt(dx*dx + dy*dy + dz*dz), half);
    const footprintPx = (half / dist) * pixelsPerRadian;

    const atCut = cm === 0 || (footprintPx < pixelThreshold && pCount > 0);

    if (atCut) {
      if (pCount > 0) ranges.push({ nodeIdx, count: pCount });
      if (!nodePointCache.has(nodeIdx) && !pendingFetches.has(nodeIdx) && pCount > 0) {
        const subtree = nt.subtreePointCount[nodeIdx] * 20 <= MAX_SUBTREE_BYTES;
        pushCandidate(nodeIdx, footprintPx, subtree);
      }
      return;
    }

    if (!nodePointCache.has(nodeIdx) && !pendingFetches.has(nodeIdx) && pCount > 0) {
      pushCandidate(nodeIdx, footprintPx, false);
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

function scheduleFetches(): void {
  if (!nodeTable || fetchQueue.length === 0) return;
  const slots = MAX_CONCURRENT_FETCHES - pendingHttpRequests;
  if (slots <= 0) return;
  fetchQueue.sort((a, b) => b.priority - a.priority);
  for (let i = 0; i < Math.min(slots, fetchQueue.length); i++) {
    const { nodeIdx, subtree } = fetchQueue[i];
    if (subtree) void fetchSubtreePoints(nodeIdx);
    else         void fetchNodePoints(nodeIdx);
  }
  fetchQueue = [];
}

async function fetchRange(url: string, start: number, end: number): Promise<ArrayBuffer> {
  const resp = await fetch(url, { headers: { Range: `bytes=${start}-${end}` } });
  if (resp.status !== 206 && resp.status !== 200) {
    throw new Error(`fetch range ${start}-${end} failed: ${resp.status}`);
  }
  return resp.arrayBuffer();
}

async function fetchNodePoints(nodeIdx: number): Promise<void> {
  const nt = nodeTable!;
  if (pendingFetches.has(nodeIdx) || nodePointCache.has(nodeIdx)) return;
  pendingFetches.add(nodeIdx);
  pendingHttpRequests++;
  try {
    const url   = `${apiRoot}/datasets/${dataset}/starcloud.bin`;
    const start = nt.pointsOffset + nt.pointFirst[nodeIdx] * 20;
    const end   = start + nt.pointCount[nodeIdx] * 20 - 1;
    const buf   = await fetchRange(url, start, end);
    if (nodeTable === nt) {
      nodePointCache.set(nodeIdx, new Float32Array(buf));
      postProgress();
      lodDirty = true;
      maybeRunLod();
    }
  } finally {
    pendingHttpRequests--;
    pendingFetches.delete(nodeIdx);
  }
}

async function fetchSubtreePoints(nodeIdx: number): Promise<void> {
  const nt = nodeTable!;
  const toFetch: number[] = [];
  function collect(idx: number): void {
    if (pendingFetches.has(idx) || nodePointCache.has(idx)) return;
    toFetch.push(idx);
    pendingFetches.add(idx);
    const cm = nt.childMask[idx];
    let childIdx = nt.firstChild[idx];
    for (let c = 0; c < 8; c++) {
      if ((cm & (1 << c)) === 0) continue;
      collect(childIdx);
      childIdx++;
    }
  }
  collect(nodeIdx);
  if (toFetch.length === 0) return;

  pendingHttpRequests++;
  try {
    const url       = `${apiRoot}/datasets/${dataset}/starcloud.bin`;
    const totalPts  = nt.subtreePointCount[nodeIdx];
    const byteStart = nt.pointsOffset + nt.pointFirst[nodeIdx] * 20;
    const byteEnd   = byteStart + totalPts * 20 - 1;
    const buf       = await fetchRange(url, byteStart, byteEnd);
    if (nodeTable !== nt) return;
    const data = new Float32Array(buf);
    const base = nt.pointFirst[nodeIdx];
    for (const idx of toFetch) {
      if (nt.pointCount[idx] === 0) continue;
      const off = (nt.pointFirst[idx] - base) * 5;
      nodePointCache.set(idx, data.subarray(off, off + nt.pointCount[idx] * 5));
    }
    postProgress();
    lodDirty = true;
    maybeRunLod();
  } finally {
    pendingHttpRequests--;
    for (const idx of toFetch) pendingFetches.delete(idx);
  }
}

function postProgress(): void {
  let loaded = 0;
  for (const nodeIdx of leafSet) {
    if (nodePointCache.has(nodeIdx)) loaded++;
  }
  self.postMessage({
    type: 'progress', loaded, total: leafSet.size, pending: pendingFetches.size,
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

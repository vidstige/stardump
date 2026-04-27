import createRegl from "regl";

type Mat4 = Float32Array;
type Vec3 = [number, number, number];
type Quaternion = [number, number, number, number];

type Camera = {
  position: Vec3;
  orientation: Quaternion;
  fovY: number;
  near: number;
  far: number;
};

type FrustumParams = {
  aspect: number;
  near: number;
  far: number;
  fovy: number;
};

type Bounds = { min: Vec3; max: Vec3 };

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

declare global {
  interface Window {
    starDump?: {
      getCamera: () => Camera;
    };
  }
}

type SceneState = { count: number };

const searchParams = new URLSearchParams(window.location.search);
const REMOTE_API = "https://star-dump-query-api-494247280614.europe-west1.run.app";
const LOCAL_API  = "http://127.0.0.1:3000";
const API_URLS   = [REMOTE_API, LOCAL_API];
const isLocal = window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1";
let API_ROOT = searchParams.get("api") ?? (isLocal ? LOCAL_API : REMOTE_API);
const DATASET_OVERRIDE = searchParams.get("dataset");
let pixelThreshold = 8;
const LOD_THROTTLE_MS = 100;
const MAX_CONCURRENT_FETCHES = 8;
const MAX_SUBTREE_BYTES = 512 * 1024;

function add(a: Vec3, b: Vec3): Vec3 {
  return [a[0] + b[0], a[1] + b[1], a[2] + b[2]];
}

function subtract(a: Vec3, b: Vec3): Vec3 {
  return [a[0] - b[0], a[1] - b[1], a[2] - b[2]];
}

function scale(v: Vec3, amount: number): Vec3 {
  return [v[0] * amount, v[1] * amount, v[2] * amount];
}

function cross(a: Vec3, b: Vec3): Vec3 {
  return [
    a[1] * b[2] - a[2] * b[1],
    a[2] * b[0] - a[0] * b[2],
    a[0] * b[1] - a[1] * b[0],
  ];
}

function normalize(v: Vec3): Vec3 {
  const length = Math.hypot(v[0], v[1], v[2]) || 1;
  return [v[0] / length, v[1] / length, v[2] / length];
}

function normalizeQuaternion(q: Quaternion): Quaternion {
  const length = Math.hypot(q[0], q[1], q[2], q[3]) || 1;
  return [q[0] / length, q[1] / length, q[2] / length, q[3] / length];
}

function multiplyQuaternion(a: Quaternion, b: Quaternion): Quaternion {
  return [
    a[3]*b[0] + a[0]*b[3] + a[1]*b[2] - a[2]*b[1],
    a[3]*b[1] - a[0]*b[2] + a[1]*b[3] + a[2]*b[0],
    a[3]*b[2] + a[0]*b[1] - a[1]*b[0] + a[2]*b[3],
    a[3]*b[3] - a[0]*b[0] - a[1]*b[1] - a[2]*b[2],
  ];
}

function quaternionFromAxisAngle(axis: Vec3, angle: number): Quaternion {
  const s = Math.sin(angle / 2);
  return [axis[0]*s, axis[1]*s, axis[2]*s, Math.cos(angle / 2)];
}

function projectionMatrix(frustum: FrustumParams): Mat4 {
  const f = 1 / Math.tan(frustum.fovy / 2);
  const nf = 1 / (frustum.near - frustum.far);
  return new Float32Array([
    f / frustum.aspect, 0, 0, 0,
    0, f, 0, 0,
    0, 0, (frustum.far + frustum.near) * nf, -1,
    0, 0, 2 * frustum.far * frustum.near * nf, 0,
  ]);
}

function lookAt(eye: Vec3, center: Vec3, up: Vec3): Mat4 {
  const z = normalize(subtract(eye, center));
  const x = normalize(cross(up, z));
  const y = cross(z, x);

  return new Float32Array([
    x[0], y[0], z[0], 0,
    x[1], y[1], z[1], 0,
    x[2], y[2], z[2], 0,
    -(x[0] * eye[0] + x[1] * eye[1] + x[2] * eye[2]),
    -(y[0] * eye[0] + y[1] * eye[1] + y[2] * eye[2]),
    -(z[0] * eye[0] + z[1] * eye[1] + z[2] * eye[2]),
    1,
  ]);
}

function rotateVector(q: Quaternion, v: Vec3): Vec3 {
  const qv: Vec3 = [q[0], q[1], q[2]];
  const uv = cross(qv, v);
  const uuv = cross(qv, uv);
  return add(v, add(scale(uv, 2 * q[3]), scale(uuv, 2)));
}

function cameraBasis(camera: Camera): { forward: Vec3; right: Vec3; up: Vec3 } {
  return {
    forward: rotateVector(camera.orientation, [0, 0, -1]),
    right:   rotateVector(camera.orientation, [1, 0, 0]),
    up:      rotateVector(camera.orientation, [0, 1, 0]),
  };
}

function viewMatrix(position: Vec3, orientation: Quaternion): Mat4 {
  const forward = rotateVector(orientation, [0, 0, -1]);
  const up      = rotateVector(orientation, [0, 1, 0]);
  return lookAt(position, add(position, forward), up);
}


function boundsCenterAndHalf(b: Bounds): { cx: number; cy: number; cz: number; half: number } {
  const cx = (b.min[0] + b.max[0]) * 0.5;
  const cy = (b.min[1] + b.max[1]) * 0.5;
  const cz = (b.min[2] + b.max[2]) * 0.5;
  const half = (b.max[0] - b.min[0]) * 0.5;
  return { cx, cy, cz, half };
}

function childBounds(parent: Bounds, child: number): Bounds {
  const mx = (parent.min[0] + parent.max[0]) * 0.5;
  const my = (parent.min[1] + parent.max[1]) * 0.5;
  const mz = (parent.min[2] + parent.max[2]) * 0.5;
  return {
    min: [
      (child & 1) === 0 ? parent.min[0] : mx,
      (child & 2) === 0 ? parent.min[1] : my,
      (child & 4) === 0 ? parent.min[2] : mz,
    ],
    max: [
      (child & 1) === 0 ? mx : parent.max[0],
      (child & 2) === 0 ? my : parent.max[1],
      (child & 4) === 0 ? mz : parent.max[2],
    ],
  };
}


function collectCut(
  nt: NodeTable,
  rootBounds: Bounds,
  eye: Vec3,
  pixelsPerRadian: number,
  pixelThreshold: number,
): { nodeIdx: number; count: number; depth: number }[] {
  const out: { nodeIdx: number; count: number; depth: number }[] = [];

  function walk(nodeIdx: number, bounds: Bounds, depth: number): void {
    const cm     = nt.childMask[nodeIdx];
    const pCount = nt.pointCount[nodeIdx];

    if (cm === 0) {
      if (pCount > 0) out.push({ nodeIdx, count: pCount, depth });
      return;
    }

    const { cx, cy, cz, half } = boundsCenterAndHalf(bounds);
    const dx = cx - eye[0], dy = cy - eye[1], dz = cz - eye[2];
    const dist = Math.max(Math.hypot(dx, dy, dz), half);
    const footprintPx = (half / dist) * pixelsPerRadian;

    if (footprintPx < pixelThreshold && pCount > 0) {
      out.push({ nodeIdx, count: pCount, depth });
      return;
    }

    let childIdx = nt.firstChild[nodeIdx];
    for (let c = 0; c < 8; c++) {
      if ((cm & (1 << c)) === 0) continue;
      walk(childIdx, childBounds(bounds, c), depth + 1);
      childIdx++;
    }
  }

  if (nt.nodeCount > 0) walk(0, rootBounds, 0);
  return out;
}

async function fetchRange(url: string, start: number, end: number): Promise<ArrayBuffer> {
  const resp = await fetch(url, { headers: { Range: `bytes=${start}-${end}` } });
  if (resp.status !== 206 && resp.status !== 200) {
    throw new Error(`fetch range ${start}-${end} failed: ${resp.status}`);
  }
  return resp.arrayBuffer();
}

async function fetchDatasetNames(): Promise<string[]> {
  const response = await fetch(`${API_ROOT}/indices`);
  if (!response.ok) {
    throw new Error(`failed to list datasets: ${response.status}`);
  }
  const names = (await response.text())
    .split("\n")
    .map((name) => name.trim())
    .filter(Boolean);
  if (names.length === 0) {
    throw new Error("no datasets found");
  }
  return names;
}

const app = document.querySelector<HTMLDivElement>("#app");
if (!app) {
  throw new Error("missing #app");
}
const statusElement         = document.querySelector<HTMLParagraphElement>("#status");
const apiSelectElement      = document.querySelector<HTMLSelectElement>("#api-select");
const datasetSelectElement  = document.querySelector<HTMLSelectElement>("#dataset-select");
const lodBarElement         = document.querySelector<HTMLElement>("#lod-fill");
const queryCountElement     = document.querySelector<HTMLElement>("#query-count");
const coordinatesElement    = document.querySelector<HTMLElement>("#coordinates");
const farSliderElement      = document.querySelector<HTMLInputElement>("#far-slider");
const farValueElement       = document.querySelector<HTMLElement>("#far-value");
const exposureSliderElement        = document.querySelector<HTMLInputElement>("#exposure-slider");
const exposureValueElement         = document.querySelector<HTMLElement>("#exposure-value");
const sizeScaleSliderElement       = document.querySelector<HTMLInputElement>("#size-scale-slider");
const sizeScaleValueElement        = document.querySelector<HTMLElement>("#size-scale-value");
const maxRadiusSliderElement       = document.querySelector<HTMLInputElement>("#max-radius-slider");
const maxRadiusValueElement        = document.querySelector<HTMLElement>("#max-radius-value");
const pixelThresholdSliderElement  = document.querySelector<HTMLInputElement>("#pixel-threshold-slider");
const pixelThresholdValueElement   = document.querySelector<HTMLElement>("#pixel-threshold-value");
if (!statusElement || !apiSelectElement || !datasetSelectElement ||
    !lodBarElement || !coordinatesElement ||
    !farSliderElement || !farValueElement || !exposureSliderElement || !exposureValueElement ||
    !sizeScaleSliderElement || !sizeScaleValueElement ||
    !maxRadiusSliderElement || !maxRadiusValueElement ||
    !pixelThresholdSliderElement || !pixelThresholdValueElement) {
  throw new Error("missing hud elements");
}
const hudStatus         = statusElement;
const apiSelect         = apiSelectElement;
const datasetSelect     = datasetSelectElement;
const hudLodBar         = lodBarElement;
const hudQueryCount     = queryCountElement;
const hudCoordinates    = coordinatesElement;
const hudFarSlider           = farSliderElement;
const hudFarValue            = farValueElement;
const hudExposureSlider      = exposureSliderElement;
const hudExposureValue       = exposureValueElement;
const hudSizeScaleSlider     = sizeScaleSliderElement;
const hudSizeScaleValue      = sizeScaleValueElement;
const hudMaxRadiusSlider     = maxRadiusSliderElement;
const hudMaxRadiusValue      = maxRadiusValueElement;
const hudPixelThresholdSlider = pixelThresholdSliderElement;
const hudPixelThresholdValue  = pixelThresholdValueElement;
const hudFps = document.querySelector<HTMLElement>("#fps")!

const canvas = document.createElement("canvas");
app.prepend(canvas);
canvas.width  = window.innerWidth;
canvas.height = window.innerHeight;

const regl = createRegl({
  canvas,
  attributes: { antialias: false, alpha: false },
  extensions: ["OES_texture_half_float", "EXT_color_buffer_half_float"],
});

const starBuffer = regl.buffer({ usage: "dynamic", type: "float", length: 0 });
const scene: SceneState = { count: 0 };
let starDataBuffer = new Float32Array(0);

// Offscreen float HDR accumulation buffer + full-screen tone-map pass
const hdrBuffer = regl.framebuffer({
  width:       canvas.width,
  height:      canvas.height,
  colorFormat: "rgba",
  colorType:   "half float",
  depth:       false,
});

const quadBuffer = regl.buffer(new Float32Array([
  -1, -1,  1, -1,  1,  1,
  -1, -1,  1,  1, -1,  1,
]));

const toneMap = regl({
  vert: `
    precision highp float;
    attribute vec2 position;
    varying vec2 vUv;
    void main() {
      vUv = position * 0.5 + 0.5;
      gl_Position = vec4(position, 0.0, 1.0);
    }
  `,
  frag: `
    precision highp float;
    uniform sampler2D uHdr;
    varying vec2 vUv;
    void main() {
      vec3 hdr = texture2D(uHdr, vUv).rgb;
      gl_FragColor = vec4(pow(hdr / (1.0 + hdr), vec3(1.0 / 2.2)), 1.0);
    }
  `,
  attributes: { position: { buffer: quadBuffer, size: 2 } },
  uniforms:   { uHdr: () => (hdrBuffer as any).color[0] },
  count: 6,
  depth: { enable: false },
});

const camera: Camera = {
  position:    [0, 0, 0],
  orientation: [0, 0, 0, 1],
  fovY:  Math.PI / 3,
  near:  0.1,
  far:   500,
};

const keyState = new Set<string>();
let previousTime = 0;
let smoothFps = 0;
let datasetName: string | null = null;
let datasetNames: string[] | null = null;
let exposure   = 500.0;
let sizeScale  = 1.0;
let maxRadius  = 1.0;

let nodeTable: NodeTable | null = null;
let nodePointCache = new Map<number, Float32Array>();
let pendingFetches = new Set<number>();
let pendingHttpRequests = 0;
let lodDirty = true;
let lastLodAt = -LOD_THROTTLE_MS;

window.starDump = {
  getCamera: () => camera,
};

function computeSubtreePointCounts(nt: Omit<NodeTable, "subtreePointCount">): Uint32Array {
  const counts = new Uint32Array(nt.nodeCount);
  function walk(nodeIdx: number): number {
    let total = nt.pointCount[nodeIdx];
    const cm = nt.childMask[nodeIdx];
    let childIdx = nt.firstChild[nodeIdx];
    for (let c = 0; c < 8; c++) {
      if ((cm & (1 << c)) === 0) continue;
      total += walk(childIdx);
      childIdx++;
    }
    counts[nodeIdx] = total;
    return total;
  }
  if (nt.nodeCount > 0) walk(0);
  return counts;
}

async function fetchNodeTable(apiRoot: string, dataset: string): Promise<NodeTable> {
  const url = `${apiRoot}/datasets/${dataset}/starcloud.bin`;
  hudStatus.textContent = "Fetching node table…";

  const headerBuf = await fetchRange(url, 0, 31);
  if (headerBuf.byteLength < 32) throw new Error("starcloud header too short");
  const hv = new DataView(headerBuf);

  const magic = String.fromCharCode(
    hv.getUint8(0), hv.getUint8(1), hv.getUint8(2), hv.getUint8(3),
    hv.getUint8(4), hv.getUint8(5), hv.getUint8(6), hv.getUint8(7),
  );
  if (magic !== "STRCLD\0\0") throw new Error(`bad starcloud magic`);
  const version = hv.getUint16(8, true);
  if (version !== 1) throw new Error(`unsupported starcloud version ${version}`);
  const depth        = hv.getUint8(10);
  const halfExtentPc = hv.getFloat32(12, true);
  const nodeCount    = hv.getUint32(16, true);

  const nodesEnd = 32 + nodeCount * 20;
  const nodesBuf = await fetchRange(url, 32, nodesEnd - 1);

  const nv = new DataView(nodesBuf);
  const childMask     = new Uint8Array(nodeCount);
  const firstChild    = new Uint32Array(nodeCount);
  const pointFirst    = new Uint32Array(nodeCount);
  const pointCountArr = new Uint32Array(nodeCount);
  for (let i = 0; i < nodeCount; i++) {
    const off = i * 20;
    childMask[i]      = nv.getUint8(off);
    firstChild[i]     = nv.getUint32(off + 4,  true);
    pointFirst[i]     = nv.getUint32(off + 8,  true);
    pointCountArr[i]  = nv.getUint32(off + 12, true);
  }

  hudStatus.textContent = "";
  const nt: Omit<NodeTable, "subtreePointCount"> = {
    nodeCount, halfExtentPc, depth,
    childMask, firstChild, pointFirst,
    pointCount: pointCountArr,
    pointsOffset: nodesEnd,
  };
  return { ...nt, subtreePointCount: computeSubtreePointCounts(nt) };
}

async function fetchNodePoints(
  apiRoot: string,
  dataset: string,
  nt: NodeTable,
  nodeIdx: number,
): Promise<void> {
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
      lodDirty = true;
      updateLodBar();
    }
  } finally {
    pendingHttpRequests--;
    pendingFetches.delete(nodeIdx);
  }
}

async function fetchSubtreePoints(
  apiRoot: string,
  dataset: string,
  nt: NodeTable,
  nodeIdx: number,
): Promise<void> {
  // Collect all uncached, non-pending nodes in the subtree and mark them pending.
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
    const url        = `${apiRoot}/datasets/${dataset}/starcloud.bin`;
    const totalPts   = nt.subtreePointCount[nodeIdx];
    const byteStart  = nt.pointsOffset + nt.pointFirst[nodeIdx] * 20;
    const byteEnd    = byteStart + totalPts * 20 - 1;
    const buf        = await fetchRange(url, byteStart, byteEnd);
    if (nodeTable !== nt) return;
    const data = new Float32Array(buf);
    const base = nt.pointFirst[nodeIdx];
    for (const idx of toFetch) {
      if (nt.pointCount[idx] === 0) continue;
      const off = (nt.pointFirst[idx] - base) * 5;
      nodePointCache.set(idx, data.subarray(off, off + nt.pointCount[idx] * 5));
    }
    lodDirty = true;
    updateLodBar();
  } finally {
    pendingHttpRequests--;
    for (const idx of toFetch) pendingFetches.delete(idx);
  }
}

function updateBuffers(data: Float32Array, count: number): void {
  starBuffer(data);
  scene.count = count;
}

type LeafInfo = { nodeIdx: number; count: number };
let allLeaves: LeafInfo[] = [];

function buildLeafList(nt: NodeTable): LeafInfo[] {
  const leaves: LeafInfo[] = [];
  function walk(nodeIdx: number): void {
    if (nt.childMask[nodeIdx] === 0) {
      leaves.push({ nodeIdx, count: nt.pointCount[nodeIdx] });
      return;
    }
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

function updateLodBar(): void {
  if (allLeaves.length === 0) return;
  const loaded = allLeaves.filter(l => nodePointCache.has(l.nodeIdx)).length;
  hudLodBar.style.width = `${(loaded / allLeaves.length * 100).toFixed(1)}%`;
}

function collectAndUploadStars(
  nt: NodeTable,
  eye: Vec3,
  pixelsPerRadian: number,
): void {
  const rootBounds: Bounds = {
    min: [-nt.halfExtentPc, -nt.halfExtentPc, -nt.halfExtentPc],
    max: [ nt.halfExtentPc,  nt.halfExtentPc,  nt.halfExtentPc],
  };

  const ranges = collectCut(nt, rootBounds, eye, pixelsPerRadian, pixelThreshold);

  let totalCount = 0;
  for (const r of ranges) {
    if (nodePointCache.has(r.nodeIdx)) totalCount += r.count;
  }

  if (starDataBuffer.length < totalCount * 5) {
    starDataBuffer = new Float32Array(totalCount * 5);
  }

  let out = 0;
  for (const r of ranges) {
    const pts = nodePointCache.get(r.nodeIdx);
    if (!pts) continue;
    starDataBuffer.set(pts, out * 5);
    out += r.count;
  }

  updateBuffers(starDataBuffer.subarray(0, out * 5), out);
}

function scheduleFetches(nt: NodeTable, eye: Vec3, pixelsPerRadian: number): void {
  const slots = MAX_CONCURRENT_FETCHES - pendingHttpRequests;
  if (slots <= 0) return;

  type Candidate = { nodeIdx: number; priority: number; subtree: boolean };
  const candidates: Candidate[] = [];

  const rootBounds: Bounds = {
    min: [-nt.halfExtentPc, -nt.halfExtentPc, -nt.halfExtentPc],
    max: [ nt.halfExtentPc,  nt.halfExtentPc,  nt.halfExtentPc],
  };

  function walk(nodeIdx: number, bounds: Bounds): void {
    const { cx, cy, cz, half } = boundsCenterAndHalf(bounds);
    const dx = cx - eye[0], dy = cy - eye[1], dz = cz - eye[2];
    const dist = Math.max(Math.hypot(dx, dy, dz), half);
    const footprintPx = (half / dist) * pixelsPerRadian;

    const cached  = nodePointCache.has(nodeIdx);
    const pending = pendingFetches.has(nodeIdx);
    const cm      = nt.childMask[nodeIdx];

    if (!cached && !pending) {
      const subtreeFits = nt.subtreePointCount[nodeIdx] * 20 <= MAX_SUBTREE_BYTES;
      if (footprintPx < pixelThreshold || cm === 0) {
        // Node is small on screen or a leaf: one request covers the whole subtree.
        if (subtreeFits) {
          candidates.push({ nodeIdx, priority: footprintPx, subtree: true });
          return;
        }
        // Subtree too large: fetch own sample and let recursion handle children.
      }
      if (nt.pointCount[nodeIdx] > 0) {
        candidates.push({ nodeIdx, priority: footprintPx, subtree: false });
      }
    }

    if (cm === 0) return;
    let childIdx = nt.firstChild[nodeIdx];
    for (let c = 0; c < 8; c++) {
      if ((cm & (1 << c)) === 0) continue;
      walk(childIdx, childBounds(bounds, c));
      childIdx++;
    }
  }

  if (nt.nodeCount > 0) walk(0, rootBounds);

  // Highest footprintPx first: large/close nodes before small/distant ones.
  candidates.sort((a, b) => b.priority - a.priority);

  const dataset = datasetName ?? "";
  for (let i = 0; i < Math.min(slots, candidates.length); i++) {
    const { nodeIdx, subtree } = candidates[i];
    if (subtree) {
      void fetchSubtreePoints(API_ROOT, dataset, nt, nodeIdx);
    } else {
      void fetchNodePoints(API_ROOT, dataset, nt, nodeIdx);
    }
  }
}

async function ensureDatasetName(): Promise<string> {
  if (datasetName) return datasetName;
  datasetNames = await fetchDatasetNames();
  datasetName = DATASET_OVERRIDE && datasetNames.includes(DATASET_OVERRIDE)
    ? DATASET_OVERRIDE
    : datasetNames[0];
  populateDatasetSelect(datasetNames, datasetName);
  return datasetName;
}

async function loadDataset(): Promise<void> {
  nodeTable = null;
  nodePointCache = new Map();
  pendingFetches = new Set();
  updateBuffers(new Float32Array(0), 0);
  try {
    const name = await ensureDatasetName();
    nodeTable = await fetchNodeTable(API_ROOT, name);
    allLeaves = buildLeafList(nodeTable);
    updateLodBar();
    lodDirty = true;
  } catch (error) {
    hudStatus.textContent = error instanceof Error ? error.message : String(error);
  }
}

for (const url of API_URLS) {
  const option = document.createElement("option");
  option.value = url;
  option.textContent = url.startsWith("http://127") ? "Local (127.0.0.1:3000)" : "Remote (Cloud Run)";
  option.selected = url === API_ROOT;
  apiSelect.append(option);
}

apiSelect.addEventListener("change", () => {
  if (!apiSelect.value || apiSelect.value === API_ROOT) return;
  API_ROOT = apiSelect.value;
  datasetName = null;
  datasetNames = null;
  hudStatus.textContent = `Connecting to ${API_ROOT}…`;
  void loadDataset();
});

hudStatus.textContent = `Connecting to ${API_ROOT}…`;

hudFarSlider.value = String(camera.far);
hudFarValue.textContent = `${camera.far.toFixed(0)} pc`;

hudFarSlider.addEventListener("input", () => {
  camera.far = Number(hudFarSlider.value);
  hudFarValue.textContent = `${camera.far.toFixed(0)} pc`;
  lodDirty = true;
});

const exposureMin = Math.log10(1e-3);
const exposureMax = Math.log10(1e6);
hudExposureSlider.value = String(
  ((Math.log10(exposure) - exposureMin) / (exposureMax - exposureMin)) * 100
);
hudExposureValue.textContent = exposure.toExponential(1);

hudExposureSlider.addEventListener("input", () => {
  const t = Number(hudExposureSlider.value) / 100;
  exposure = Math.pow(10, exposureMin + t * (exposureMax - exposureMin));
  hudExposureValue.textContent = exposure.toExponential(1);
});

hudSizeScaleSlider.value = String(sizeScale);
hudSizeScaleValue.textContent = sizeScale.toFixed(1);
hudSizeScaleSlider.addEventListener("input", () => {
  sizeScale = Number(hudSizeScaleSlider.value);
  hudSizeScaleValue.textContent = sizeScale.toFixed(1);
});

hudMaxRadiusSlider.value = String(maxRadius);
hudMaxRadiusValue.textContent = `${maxRadius.toFixed(1)} px`;
hudMaxRadiusSlider.addEventListener("input", () => {
  maxRadius = Number(hudMaxRadiusSlider.value);
  hudMaxRadiusValue.textContent = `${maxRadius.toFixed(1)} px`;
});

hudPixelThresholdSlider.value = String(pixelThreshold);
hudPixelThresholdValue.textContent = `${pixelThreshold} px`;
hudPixelThresholdSlider.addEventListener("input", () => {
  pixelThreshold = Number(hudPixelThresholdSlider.value);
  hudPixelThresholdValue.textContent = `${pixelThreshold} px`;
  lodDirty = true;
});

function populateDatasetSelect(names: string[], selectedName: string): void {
  datasetSelect.replaceChildren();
  for (const name of names) {
    const option = document.createElement("option");
    option.value = name;
    option.textContent = name;
    option.selected = name === selectedName;
    datasetSelect.append(option);
  }
  datasetSelect.disabled = names.length <= 1;
}

datasetSelect.addEventListener("change", () => {
  if (!datasetSelect.value || datasetSelect.value === datasetName) return;
  datasetName = datasetSelect.value;
  const url = new URL(window.location.href);
  url.searchParams.set("dataset", datasetName);
  window.history.replaceState({}, "", url);
  void loadDataset();
});

function updateCamera(deltaTime: number): void {
  const { forward, right } = cameraBasis(camera);
  const speed = 2 * deltaTime;
  let movement: Vec3 = [0, 0, 0];

  if (keyState.has("KeyW")) movement = add(movement, forward);
  if (keyState.has("KeyS")) movement = subtract(movement, forward);
  if (keyState.has("KeyA")) movement = subtract(movement, right);
  if (keyState.has("KeyD")) movement = add(movement, right);

  if (movement[0] || movement[1] || movement[2]) {
    camera.position = add(camera.position, scale(normalize(movement), speed));
    lodDirty = true;
  }

  const rollSpeed = 0.25 * deltaTime;
  if (keyState.has("KeyQ")) {
    camera.orientation = normalizeQuaternion(multiplyQuaternion(
      camera.orientation, quaternionFromAxisAngle([0, 0, 1], -rollSpeed)
    ));
    lodDirty = true;
  }
  if (keyState.has("KeyE")) {
    camera.orientation = normalizeQuaternion(multiplyQuaternion(
      camera.orientation, quaternionFromAxisAngle([0, 0, 1], rollSpeed)
    ));
    lodDirty = true;
  }
}

canvas.addEventListener("click", () => {
  if (document.pointerLockElement === canvas) {
    document.exitPointerLock();
  } else {
    void canvas.requestPointerLock();
  }
});

function onMouseMove(event: MouseEvent): void {
  const s = 0.0025;
  if (event.movementX !== 0) {
    camera.orientation = normalizeQuaternion(multiplyQuaternion(
      camera.orientation, quaternionFromAxisAngle([0, 1, 0], -event.movementX * s)
    ));
    lodDirty = true;
  }
  if (event.movementY !== 0) {
    camera.orientation = normalizeQuaternion(multiplyQuaternion(
      camera.orientation, quaternionFromAxisAngle([1, 0, 0], -event.movementY * s)
    ));
    lodDirty = true;
  }
}

document.addEventListener("pointerlockchange", () => {
  if (document.pointerLockElement === canvas) {
    document.addEventListener("mousemove", onMouseMove);
  } else {
    document.removeEventListener("mousemove", onMouseMove);
  }
});

window.addEventListener("keydown", (event) => {
  if (event.code.startsWith("Key")) {
    keyState.add(event.code);
  }
  if (["KeyW", "KeyA", "KeyS", "KeyD", "KeyQ", "KeyE"].includes(event.code)) {
    event.preventDefault();
  }
});

window.addEventListener("keyup", (event) => {
  keyState.delete(event.code);
});

const drawStars = regl({
  vert: `
    precision highp float;

    attribute vec3 position;
    attribute float luminosity;
    attribute float bpRp;

    uniform mat4 projection;
    uniform mat4 view;
    uniform vec3 cameraPosition;
    uniform float exposure;
    uniform float uSizeScale;
    uniform float uMaxRadius;

    varying vec3 vColor;
    varying float vBrightness;
    varying float vGaussCoeff;

    vec3 bpRpToColor(float t) {
      vec3 blue   = vec3(0.6, 0.7, 1.0);
      vec3 white  = vec3(1.0, 0.95, 0.9);
      vec3 yellow = vec3(1.0, 0.85, 0.4);
      vec3 red    = vec3(1.0, 0.3,  0.1);
      if (t < 0.33) return mix(blue,   white,  t / 0.33);
      if (t < 0.66) return mix(white,  yellow, (t - 0.33) / 0.33);
                    return mix(yellow, red,    (t - 0.66) / 0.34);
    }

    void main() {
      gl_Position = projection * view * vec4(position, 1.0);
      float dist = length(position - cameraPosition);
      float flux = luminosity / max(dist * dist, 0.01);
      float brightness = flux * exposure;
      float t = clamp((bpRp + 0.5) / 3.5, 0.0, 1.0);
      vColor = (bpRp != bpRp) ? vec3(1.0) : bpRpToColor(t);

      // Match render-fast.ts: radius scales with brightness, clamped to [0.8, uMaxRadius]
      float rPx = clamp(brightness * uSizeScale, 0.8, uMaxRadius);
      float spriteSizePx = ceil(rPx) * 2.0 + 1.0;
      gl_PointSize = spriteSizePx;
      vBrightness = brightness;
      // Convert Gaussian coeff from pixel² to gl_PointCoord² space
      vGaussCoeff = 4.0 * spriteSizePx * spriteSizePx / (rPx * rPx);
    }
  `,
  frag: `
    precision highp float;

    varying vec3 vColor;
    varying float vBrightness;
    varying float vGaussCoeff;

    void main() {
      vec2 d = gl_PointCoord - 0.5;
      float r2 = dot(d, d);
      float val = vBrightness * exp(-r2 * vGaussCoeff);
      if (val < 1e-6) discard;
      gl_FragColor = vec4(vColor * val, 1.0);
    }
  `,
  attributes: {
    position:   { buffer: starBuffer, size: 3, stride: 20, offset:  0 },
    luminosity: { buffer: starBuffer, size: 1, stride: 20, offset: 12 },
    bpRp:       { buffer: starBuffer, size: 1, stride: 20, offset: 16 },
  },
  uniforms: {
    projection:     () => projectionMatrix({ fovy: camera.fovY, aspect: canvas.width / Math.max(canvas.height, 1), near: camera.near, far: camera.far }),
    view:           () => viewMatrix(camera.position, camera.orientation),
    cameraPosition: () => camera.position,
    exposure:       () => exposure,
    uSizeScale:     () => sizeScale,
    uMaxRadius:     () => maxRadius,
  },
  primitive: "points",
  count: () => scene.count,
  blend: {
    enable: true,
    func: { src: "one", dst: "one" },
  },
  depth: { enable: false },
});

const renderToHdr = regl({ framebuffer: hdrBuffer });

regl.frame(({ time }) => {
  const deltaTime = previousTime === 0 ? 0 : time - previousTime;
  previousTime = time;
  if (deltaTime > 0) {
    smoothFps = smoothFps === 0 ? 1 / deltaTime : smoothFps * 0.9 + (1 / deltaTime) * 0.1;
    hudFps.textContent = `${smoothFps.toFixed(0)} fps`;
  }
  updateCamera(deltaTime);

  const [cx, cy, cz] = camera.position;
  hudCoordinates.textContent = `${cx.toFixed(2)}, ${cy.toFixed(2)}, ${cz.toFixed(2)}`;
  if (hudQueryCount) hudQueryCount.textContent = String(pendingFetches.size);

  if (nodeTable && lodDirty) {
    const now = performance.now();
    if (now - lastLodAt >= LOD_THROTTLE_MS) {
      lastLodAt = now;
      lodDirty  = false;
      const pixelsPerRadian = canvas.height / Math.max(camera.fovY, 1e-6);
      collectAndUploadStars(nodeTable, camera.position, pixelsPerRadian);
      scheduleFetches(nodeTable, camera.position, pixelsPerRadian);
    }
  }

  // Resize canvas + HDR buffer to match display
  const w = canvas.clientWidth  | 0;
  const h = canvas.clientHeight | 0;
  if (w > 0 && h > 0 && (canvas.width !== w || canvas.height !== h)) {
    canvas.width  = w;
    canvas.height = h;
    hdrBuffer.resize(w, h);
    lodDirty = true;
  }

  // Pass 1: accumulate stars into float HDR buffer (linear, no tone map)
  renderToHdr(() => {
    regl.clear({ color: [0, 0, 0, 1] });
    drawStars();
  });

  // Pass 2: Reinhardt + gamma tone-map HDR → 8-bit screen
  regl.clear({ color: [0, 0, 0, 1] });
  toneMap();
});

void loadDataset();

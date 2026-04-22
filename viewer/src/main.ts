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
type Plane  = { nx: number; ny: number; nz: number; d: number };

type NodeTable = {
  nodeCount:    number;
  halfExtentPc: number;
  depth:        number;
  childMask:    Uint8Array;
  firstChild:   Uint32Array;
  pointFirst:   Uint32Array;
  pointCount:   Uint32Array;
  pointsOffset: number;
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
let API_ROOT = searchParams.get("api") ?? REMOTE_API;
const DATASET_OVERRIDE = searchParams.get("dataset");
let pixelThreshold = 8;
const LOD_THROTTLE_MS = 100;
const MAX_CONCURRENT_FETCHES = 8;

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

// Frustum helpers ported from render-fast.ts

function planeFromPointNormal(
  px: number, py: number, pz: number,
  nx: number, ny: number, nz: number,
): Plane {
  const len = Math.hypot(nx, ny, nz) || 1;
  const ux = nx / len, uy = ny / len, uz = nz / len;
  return { nx: ux, ny: uy, nz: uz, d: -(ux * px + uy * py + uz * pz) };
}

function boundsMaxCorner(b: Bounds, n: Vec3): Vec3 {
  return [
    n[0] >= 0 ? b.max[0] : b.min[0],
    n[1] >= 0 ? b.max[1] : b.min[1],
    n[2] >= 0 ? b.max[2] : b.min[2],
  ];
}

function frustumIntersectsBounds(planes: Plane[], b: Bounds): boolean {
  for (const p of planes) {
    const c = boundsMaxCorner(b, [p.nx, p.ny, p.nz]);
    if (p.nx * c[0] + p.ny * c[1] + p.nz * c[2] + p.d < 0) return false;
  }
  return true;
}

function buildFrustumPlanes(
  eye: Vec3,
  forward: Vec3,
  right: Vec3,
  up: Vec3,
  near: number,
  far: number,
  fovy: number,
  aspect: number,
): Plane[] {
  const nearCenter: Vec3 = [
    eye[0] + forward[0] * near,
    eye[1] + forward[1] * near,
    eye[2] + forward[2] * near,
  ];
  const farCenter: Vec3 = [
    eye[0] + forward[0] * far,
    eye[1] + forward[1] * far,
    eye[2] + forward[2] * far,
  ];
  const tanHalf = Math.tan(fovy * 0.5);
  const hNear = near * tanHalf;
  const wNear = hNear * aspect;
  const leftNormal: Vec3 = [
    right[0] * near + forward[0] * wNear,
    right[1] * near + forward[1] * wNear,
    right[2] * near + forward[2] * wNear,
  ];
  const rightNormal: Vec3 = [
    -right[0] * near + forward[0] * wNear,
    -right[1] * near + forward[1] * wNear,
    -right[2] * near + forward[2] * wNear,
  ];
  const bottomNormal: Vec3 = [
    up[0] * near + forward[0] * hNear,
    up[1] * near + forward[1] * hNear,
    up[2] * near + forward[2] * hNear,
  ];
  const topNormal: Vec3 = [
    -up[0] * near + forward[0] * hNear,
    -up[1] * near + forward[1] * hNear,
    -up[2] * near + forward[2] * hNear,
  ];
  return [
    planeFromPointNormal(nearCenter[0], nearCenter[1], nearCenter[2],  forward[0],      forward[1],      forward[2]),
    planeFromPointNormal(farCenter[0],  farCenter[1],  farCenter[2],  -forward[0],     -forward[1],     -forward[2]),
    planeFromPointNormal(eye[0],        eye[1],        eye[2],         leftNormal[0],   leftNormal[1],   leftNormal[2]),
    planeFromPointNormal(eye[0],        eye[1],        eye[2],         rightNormal[0],  rightNormal[1],  rightNormal[2]),
    planeFromPointNormal(eye[0],        eye[1],        eye[2],         bottomNormal[0], bottomNormal[1], bottomNormal[2]),
    planeFromPointNormal(eye[0],        eye[1],        eye[2],         topNormal[0],    topNormal[1],    topNormal[2]),
  ];
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

function pointInFrustum(planes: Plane[], px: number, py: number, pz: number): boolean {
  for (const p of planes) {
    if (p.nx * px + p.ny * py + p.nz * pz + p.d < 0) return false;
  }
  return true;
}

function collectCut(
  nt: NodeTable,
  rootBounds: Bounds,
  eye: Vec3,
  planes: Plane[],
  pixelsPerRadian: number,
  pixelThreshold: number,
): { nodeIdx: number; count: number }[] {
  const out: { nodeIdx: number; count: number }[] = [];

  function walk(nodeIdx: number, bounds: Bounds): void {
    if (!frustumIntersectsBounds(planes, bounds)) return;
    const cm     = nt.childMask[nodeIdx];
    const pCount = nt.pointCount[nodeIdx];

    if (cm === 0) {
      if (pCount > 0) out.push({ nodeIdx, count: pCount });
      return;
    }

    const { cx, cy, cz, half } = boundsCenterAndHalf(bounds);
    const dx = cx - eye[0], dy = cy - eye[1], dz = cz - eye[2];
    const dist = Math.max(Math.hypot(dx, dy, dz), half);
    const footprintPx = (half / dist) * pixelsPerRadian;

    if (footprintPx < pixelThreshold && pCount > 0) {
      out.push({ nodeIdx, count: pCount });
      return;
    }

    let childIdx = nt.firstChild[nodeIdx];
    for (let c = 0; c < 8; c++) {
      if ((cm & (1 << c)) === 0) continue;
      walk(childIdx, childBounds(bounds, c));
      childIdx++;
    }
  }

  if (nt.nodeCount > 0) walk(0, rootBounds);
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
const unitsCountElement     = document.querySelector<HTMLElement>("#units-count");
const queryCountElement     = document.querySelector<HTMLElement>("#query-count");
const coordinatesElement    = document.querySelector<HTMLElement>("#coordinates");
const farSliderElement      = document.querySelector<HTMLInputElement>("#far-slider");
const farValueElement       = document.querySelector<HTMLElement>("#far-value");
const exposureSliderElement        = document.querySelector<HTMLInputElement>("#exposure-slider");
const exposureValueElement         = document.querySelector<HTMLElement>("#exposure-value");
const pixelThresholdSliderElement  = document.querySelector<HTMLInputElement>("#pixel-threshold-slider");
const pixelThresholdValueElement   = document.querySelector<HTMLElement>("#pixel-threshold-value");
if (!statusElement || !apiSelectElement || !datasetSelectElement ||
    !unitsCountElement || !coordinatesElement ||
    !farSliderElement || !farValueElement || !exposureSliderElement || !exposureValueElement ||
    !pixelThresholdSliderElement || !pixelThresholdValueElement) {
  throw new Error("missing hud elements");
}
const hudStatus         = statusElement;
const apiSelect         = apiSelectElement;
const datasetSelect     = datasetSelectElement;
const hudUnitsCount     = unitsCountElement;
const hudQueryCount     = queryCountElement;
const hudCoordinates    = coordinatesElement;
const hudFarSlider           = farSliderElement;
const hudFarValue            = farValueElement;
const hudExposureSlider      = exposureSliderElement;
const hudExposureValue       = exposureValueElement;
const hudPixelThresholdSlider = pixelThresholdSliderElement;
const hudPixelThresholdValue  = pixelThresholdValueElement;

const canvas = document.createElement("canvas");
app.prepend(canvas);
canvas.width  = window.innerWidth;
canvas.height = window.innerHeight;

const regl = createRegl({
  canvas,
  attributes: { antialias: false, alpha: false },
  extensions: ["OES_texture_float", "WEBGL_color_buffer_float"],
});

const positionBuffer  = regl.buffer({ usage: "dynamic", type: "float", length: 0 });
const luminosityBuffer = regl.buffer({ usage: "dynamic", type: "float", length: 0 });
const bpRpBuffer      = regl.buffer({ usage: "dynamic", type: "float", length: 0 });
const scene: SceneState = { count: 0 };

// Offscreen float HDR accumulation buffer + full-screen tone-map pass
const hdrBuffer = regl.framebuffer({
  width:       canvas.width,
  height:      canvas.height,
  colorFormat: "rgba",
  colorType:   "float",
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
let datasetName: string | null = null;
let datasetNames: string[] | null = null;
let exposure = 500.0;

let nodeTable: NodeTable | null = null;
let nodePointCache = new Map<number, Float32Array>();
let pendingFetches = new Set<number>();
let lodDirty = true;
let lastLodAt = -LOD_THROTTLE_MS;

window.starDump = {
  getCamera: () => camera,
};

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
  return {
    nodeCount, halfExtentPc, depth,
    childMask, firstChild, pointFirst,
    pointCount: pointCountArr,
    pointsOffset: nodesEnd,
  };
}

async function fetchNodePoints(
  apiRoot: string,
  dataset: string,
  nt: NodeTable,
  nodeIdx: number,
): Promise<void> {
  if (pendingFetches.has(nodeIdx) || nodePointCache.has(nodeIdx)) return;
  pendingFetches.add(nodeIdx);
  try {
    const url   = `${apiRoot}/datasets/${dataset}/starcloud.bin`;
    const start = nt.pointsOffset + nt.pointFirst[nodeIdx] * 20;
    const end   = start + nt.pointCount[nodeIdx] * 20 - 1;
    const buf   = await fetchRange(url, start, end);
    // Discard if dataset changed while in-flight.
    if (nodeTable === nt) {
      nodePointCache.set(nodeIdx, new Float32Array(buf));
      lodDirty = true;
    }
  } finally {
    pendingFetches.delete(nodeIdx);
  }
}

function updateBuffers(
  positions: Float32Array,
  luminosities: Float32Array,
  bpRps: Float32Array,
  count: number,
): void {
  positionBuffer(positions);
  luminosityBuffer(luminosities);
  bpRpBuffer(bpRps);
  scene.count = count;
}

function collectAndUploadStars(
  nt: NodeTable,
  eye: Vec3,
  planes: Plane[],
  pixelsPerRadian: number,
  currentDataset: string,
): number {
  const rootBounds: Bounds = {
    min: [-nt.halfExtentPc, -nt.halfExtentPc, -nt.halfExtentPc],
    max: [ nt.halfExtentPc,  nt.halfExtentPc,  nt.halfExtentPc],
  };
  const ranges = collectCut(nt, rootBounds, eye, planes, pixelsPerRadian, pixelThreshold);

  // Count cached points for allocation.
  let totalCount = 0;
  for (const r of ranges) {
    if (nodePointCache.has(r.nodeIdx)) totalCount += r.count;
  }

  const positions   = new Float32Array(totalCount * 3);
  const luminosities = new Float32Array(totalCount);
  const bpRps       = new Float32Array(totalCount);
  let out = 0;

  let queued = 0;
  for (const r of ranges) {
    const pts = nodePointCache.get(r.nodeIdx);
    if (!pts) {
      if (queued < MAX_CONCURRENT_FETCHES && !pendingFetches.has(r.nodeIdx)) {
        queued++;
        void fetchNodePoints(API_ROOT, currentDataset, nt, r.nodeIdx);
      }
      continue;
    }
    for (let i = 0; i < r.count; i++) {
      const b = i * 5;
      const px = pts[b], py = pts[b + 1], pz = pts[b + 2];
      if (!pointInFrustum(planes, px, py, pz)) continue;
      positions[out * 3]     = px;
      positions[out * 3 + 1] = py;
      positions[out * 3 + 2] = pz;
      luminosities[out]      = pts[b + 3];
      bpRps[out]             = pts[b + 4];
      out++;
    }
  }

  updateBuffers(
    positions.subarray(0, out * 3),
    luminosities.subarray(0, out),
    bpRps.subarray(0, out),
    out,
  );
  return out;
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
  updateBuffers(new Float32Array(0), new Float32Array(0), new Float32Array(0), 0);
  try {
    const name = await ensureDatasetName();
    nodeTable = await fetchNodeTable(API_ROOT, name);
    hudUnitsCount.textContent = `${nodeTable.nodeCount} nodes`;
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

  const rollSpeed = 1.5 * deltaTime;
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

let cameraActive = false;

canvas.addEventListener("click", () => {
  if (cameraActive) {
    document.exitPointerLock();
  } else {
    void canvas.requestPointerLock();
  }
});

document.addEventListener("pointerlockchange", () => {
  cameraActive = document.pointerLockElement === canvas;
});

document.addEventListener("mousemove", (event) => {
  if (!cameraActive) return;
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

      // Match render-fast.ts: radius scales with brightness, clamped 0.8–8 px
      float rPx = clamp(brightness * 2.0, 0.8, 8.0);
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
    position:   { buffer: positionBuffer,   size: 3 },
    luminosity: { buffer: luminosityBuffer, size: 1 },
    bpRp:       { buffer: bpRpBuffer,       size: 1 },
  },
  uniforms: {
    projection:     () => projectionMatrix({ fovy: camera.fovY, aspect: canvas.width / Math.max(canvas.height, 1), near: camera.near, far: camera.far }),
    view:           () => viewMatrix(camera.position, camera.orientation),
    cameraPosition: () => camera.position,
    exposure:       () => exposure,
  },
  primitive: "points",
  count: () => scene.count,
  blend: {
    enable: true,
    func: { src: "one", dst: "one" },
  },
  depth: { enable: false },
});

regl.frame(({ time }) => {
  const deltaTime = previousTime === 0 ? 0 : time - previousTime;
  previousTime = time;
  updateCamera(deltaTime);

  const [cx, cy, cz] = camera.position;
  hudCoordinates.textContent = `${cx.toFixed(2)}, ${cy.toFixed(2)}, ${cz.toFixed(2)}`;
  if (hudQueryCount) hudQueryCount.textContent = String(pendingFetches.size);

  if (nodeTable && lodDirty) {
    const now = performance.now();
    if (now - lastLodAt >= LOD_THROTTLE_MS) {
      lastLodAt = now;
      lodDirty  = false;
      const aspect = canvas.width / Math.max(canvas.height, 1);
      const { forward, right, up } = cameraBasis(camera);
      const pixelsPerRadian = canvas.height / Math.max(camera.fovY, 1e-6);
      const planes = buildFrustumPlanes(
        camera.position, forward, right, up,
        camera.near, camera.far, camera.fovY, aspect,
      );
      const count = collectAndUploadStars(
        nodeTable, camera.position, planes, pixelsPerRadian,
        datasetName ?? "",
      );
      hudUnitsCount.textContent = String(count);
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
  regl({ framebuffer: hdrBuffer })(() => {
    regl.clear({ color: [0, 0, 0, 1] });
    drawStars();
  });

  // Pass 2: Reinhardt + gamma tone-map HDR → 8-bit screen
  regl.clear({ color: [0, 0, 0, 1] });
  toneMap();
});

void loadDataset();

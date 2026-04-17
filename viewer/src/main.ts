import createRegl from "regl";

type Mat4 = Float32Array;
type Vec3 = [number, number, number];
type Quaternion = [number, number, number, number];

type Camera = {
  position: Vec3;
  yaw: number;
  pitch: number;
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

type QueryFrustum = FrustumParams & {
  x: number;
  y: number;
  z: number;
  qx: number;
  qy: number;
  qz: number;
  qw: number;
};

declare global {
  interface Window {
    gaiaViewer?: {
      getFrustum: () => QueryFrustum;
    };
  }
}

type CsvStar = {
  x: number;
  y: number;
  z: number;
  sourceId: string;
};

type SceneState = {
  count: number;
};

const searchParams = new URLSearchParams(window.location.search);
const API_ROOT = searchParams.get("api")
  ?? "https://star-dump-query-api-494247280614.europe-west1.run.app";
const DATASET_OVERRIDE = searchParams.get("dataset");
const QUERY_LIMIT = 2000;
const QUERY_INTERVAL_MS = 250;
const LOAD_TIME_BUFFER_SIZE = 8;
const DEFAULT_LOAD_TIME_MS = 300;
const QUERY_FOV_EXPANSION = 1.3;
const STAR_POOL_MAX_SIZE = 20000;

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

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
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

function lookAt(
  eye: Vec3,
  center: Vec3,
  up: Vec3,
): Mat4 {
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

function cameraForward(camera: Camera): Vec3 {
  return normalize([
    Math.cos(camera.pitch) * Math.sin(camera.yaw),
    Math.sin(camera.pitch),
    -Math.cos(camera.pitch) * Math.cos(camera.yaw),
  ]);
}

function cameraBasis(camera: Camera): { forward: Vec3; right: Vec3; up: Vec3 } {
  const forward = cameraForward(camera);
  const right = normalize(cross(forward, [0, 1, 0]));
  const up = normalize(cross(right, forward));
  return { forward, right, up };
}

function quaternionFromBasis(right: Vec3, up: Vec3, back: Vec3): Quaternion {
  const m00 = right[0];
  const m01 = up[0];
  const m02 = back[0];
  const m10 = right[1];
  const m11 = up[1];
  const m12 = back[1];
  const m20 = right[2];
  const m21 = up[2];
  const m22 = back[2];
  const trace = m00 + m11 + m22;

  if (trace > 0) {
    const s = Math.sqrt(trace + 1) * 2;
    return normalizeQuaternion([
      (m21 - m12) / s,
      (m02 - m20) / s,
      (m10 - m01) / s,
      0.25 * s,
    ]);
  }
  if (m00 > m11 && m00 > m22) {
    const s = Math.sqrt(1 + m00 - m11 - m22) * 2;
    return normalizeQuaternion([
      0.25 * s,
      (m01 + m10) / s,
      (m02 + m20) / s,
      (m21 - m12) / s,
    ]);
  }
  if (m11 > m22) {
    const s = Math.sqrt(1 + m11 - m00 - m22) * 2;
    return normalizeQuaternion([
      (m01 + m10) / s,
      0.25 * s,
      (m12 + m21) / s,
      (m02 - m20) / s,
    ]);
  }

  const s = Math.sqrt(1 + m22 - m00 - m11) * 2;
  return normalizeQuaternion([
    (m02 + m20) / s,
    (m12 + m21) / s,
    0.25 * s,
    (m10 - m01) / s,
  ]);
}

function rotateVector(q: Quaternion, v: Vec3): Vec3 {
  const qv: Vec3 = [q[0], q[1], q[2]];
  const uv = cross(qv, v);
  const uuv = cross(qv, uv);
  return add(v, add(scale(uv, 2 * q[3]), scale(uuv, 2)));
}

function cameraQuaternion(camera: Camera): Quaternion {
  const { right, up, forward } = cameraBasis(camera);
  return quaternionFromBasis(right, up, scale(forward, -1));
}

function createFrustum(camera: Camera, aspect: number): QueryFrustum {
  const orientation = cameraQuaternion(camera);
  return {
    x: camera.position[0],
    y: camera.position[1],
    z: camera.position[2],
    qx: orientation[0],
    qy: orientation[1],
    qz: orientation[2],
    qw: orientation[3],
    aspect,
    near: camera.near,
    far: camera.far,
    fovy: camera.fovY,
  };
}

function viewMatrix(frustum: QueryFrustum): Mat4 {
  const orientation: Quaternion = [frustum.qx, frustum.qy, frustum.qz, frustum.qw];
  const position: Vec3 = [frustum.x, frustum.y, frustum.z];
  const forward = rotateVector(orientation, [0, 0, -1]);
  const up = rotateVector(orientation, [0, 1, 0]);
  return lookAt(position, add(position, forward), up);
}

function hashString(value: string): number {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function starColor(sourceId: string): Vec3 {
  const hash = hashString(sourceId);
  const r = hash & 0xff;
  const g = (hash >>> 8) & 0xff;
  const b = (hash >>> 16) & 0xff;
  return [
    0.72 + r / 255 * 0.2,
    0.78 + g / 255 * 0.16,
    0.82 + b / 255 * 0.14,
  ];
}

function starSize(sourceId: string): number {
  const hash = hashString(sourceId);
  return 0.7 + ((hash >>> 16) & 0xff) / 255 * 0.9;
}

function parseStars(csvText: string): CsvStar[] {
  const lines = csvText.trim().split("\n");
  if (lines.length <= 1) {
    return [];
  }

  const stars: CsvStar[] = [];
  for (const line of lines.slice(1)) {
    if (!line) {
      continue;
    }
    const [x, y, z, sourceId] = line.split(",");
    if (!x || !y || !z || !sourceId) {
      continue;
    }
    stars.push({
      x: Number(x),
      y: Number(y),
      z: Number(z),
      sourceId,
    });
  }
  return stars;
}

function frustumKey(frustum: QueryFrustum): string {
  return [
    frustum.x.toFixed(2),
    frustum.y.toFixed(2),
    frustum.z.toFixed(2),
    frustum.qx.toFixed(3),
    frustum.qy.toFixed(3),
    frustum.qz.toFixed(3),
    frustum.qw.toFixed(3),
  ].join(":");
}

function frustumUrl(datasetName: string, frustum: QueryFrustum): string {
  const params = new URLSearchParams({
    x: frustum.x.toString(),
    y: frustum.y.toString(),
    z: frustum.z.toString(),
    qx: frustum.qx.toString(),
    qy: frustum.qy.toString(),
    qz: frustum.qz.toString(),
    qw: frustum.qw.toString(),
    near: frustum.near.toString(),
    far: frustum.far.toString(),
    fovy: frustum.fovy.toString(),
    aspect: frustum.aspect.toString(),
    limit: QUERY_LIMIT.toString(),
  });
  return `${API_ROOT}/query/${datasetName}/frustum?${params.toString()}`;
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
const statusElement = document.querySelector<HTMLParagraphElement>("#status");
const detailsElement = document.querySelector<HTMLParagraphElement>("#details");
const apiRootElement = document.querySelector<HTMLElement>("#api-root");
const datasetSelectElement = document.querySelector<HTMLSelectElement>("#dataset-select");
if (!statusElement || !detailsElement || !apiRootElement || !datasetSelectElement) {
  throw new Error("missing hud elements");
}
const hudStatus = statusElement;
const hudDetails = detailsElement;
const hudApiRoot = apiRootElement;
const datasetSelect = datasetSelectElement;

const canvas = document.createElement("canvas");
app.prepend(canvas);

const regl = createRegl({
  canvas,
  attributes: {
    antialias: true,
    alpha: false,
  },
});

const positionBuffer = regl.buffer({ usage: "dynamic", type: "float", length: 0 });
const colorBuffer = regl.buffer({ usage: "dynamic", type: "float", length: 0 });
const sizeBuffer = regl.buffer({ usage: "dynamic", type: "float", length: 0 });
const scene: SceneState = { count: 0 };

const camera: Camera = {
  position: [0, 0, 0],
  yaw: 0,
  pitch: 0,
  fovY: Math.PI / 3,
  near: 0.25,
  far: 50,
};

const keyState = new Set<string>();
let currentFrustum = createFrustum(camera, 1);
let previousTime = 0;
let datasetName: string | null = null;
let datasetNames: string[] | null = null;
let lastQueryAt = -QUERY_INTERVAL_MS;
let lastQueryKey = "";
let activeRequest = 0;
const starPool = new Map<string, CsvStar>();
const loadTimeBuffer: number[] = [];
let prevYaw = 0;
let prevPitch = 0;
let angularVelocity = { yaw: 0, pitch: 0 };

window.gaiaViewer = {
  getFrustum: () => currentFrustum,
};

hudStatus.textContent = `Connecting to ${API_ROOT}...`;
hudDetails.textContent = "Click to capture the mouse, move with WASD, look with the mouse.";
hudApiRoot.textContent = API_ROOT;

function updateHudDetails(): void {
  if (!datasetName) {
    hudDetails.textContent = `Dataset pending, far ${camera.far.toFixed(0)} pc, limit ${QUERY_LIMIT}.`;
    return;
  }
  hudDetails.textContent = `Dataset ${datasetName}, far ${camera.far.toFixed(0)} pc, limit ${QUERY_LIMIT}.`;
}

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

function rebuildBuffers(): void {
  const stars = [...starPool.values()];
  const positions = new Float32Array(stars.length * 3);
  const colors = new Float32Array(stars.length * 3);
  const sizes = new Float32Array(stars.length);

  stars.forEach((star, index) => {
    const color = starColor(star.sourceId);
    positions[index * 3] = star.x;
    positions[index * 3 + 1] = star.y;
    positions[index * 3 + 2] = star.z;
    colors[index * 3] = color[0];
    colors[index * 3 + 1] = color[1];
    colors[index * 3 + 2] = color[2];
    sizes[index] = starSize(star.sourceId);
  });

  positionBuffer(positions);
  colorBuffer(colors);
  sizeBuffer(sizes);
  scene.count = stars.length;
}

function mergeIntoPool(incoming: CsvStar[]): void {
  for (const star of incoming) {
    // Delete then re-insert to move to end of Map (most recently seen = LRU tail)
    starPool.delete(star.sourceId);
    starPool.set(star.sourceId, star);
  }
  // Evict oldest entries from the front of the Map until under the cap
  while (starPool.size > STAR_POOL_MAX_SIZE) {
    starPool.delete(starPool.keys().next().value!);
  }
  rebuildBuffers();
}

async function ensureDatasetName(): Promise<string> {
  if (datasetName) {
    return datasetName;
  }

  datasetNames = await fetchDatasetNames();
  datasetName = DATASET_OVERRIDE && datasetNames.includes(DATASET_OVERRIDE)
    ? DATASET_OVERRIDE
    : datasetNames[0];
  populateDatasetSelect(datasetNames, datasetName);
  updateHudDetails();
  return datasetName;
}

datasetSelect.addEventListener("change", () => {
  if (!datasetSelect.value || datasetSelect.value === datasetName) {
    return;
  }

  datasetName = datasetSelect.value;
  updateHudDetails();
  const url = new URL(window.location.href);
  url.searchParams.set("dataset", datasetName);
  window.history.replaceState({}, "", url);
  starPool.clear();
  rebuildBuffers();
  lastQueryKey = "";
  lastQueryAt = -QUERY_INTERVAL_MS;
  void queryStars(currentFrustum);
});

async function queryStars(frustum: QueryFrustum): Promise<void> {
  const requestId = activeRequest + 1;
  activeRequest = requestId;

  try {
    const name = await ensureDatasetName();
    hudStatus.textContent = `Querying ${name}...`;
    const queryStart = performance.now();
    const response = await fetch(frustumUrl(name, frustum));
    if (!response.ok) {
      throw new Error(`query failed: ${response.status}`);
    }
    const stars = parseStars(await response.text());
    recordLoadTime(performance.now() - queryStart);
    if (activeRequest !== requestId) {
      return;
    }
    mergeIntoPool(stars);
    hudStatus.textContent = `${starPool.size} stars loaded`;
  } catch (error) {
    if (activeRequest !== requestId) {
      return;
    }
    const message = error instanceof Error ? error.message : String(error);
    hudStatus.textContent = `Query failed: ${message}`;
  }
}

function updateCamera(deltaTime: number): void {
  const { forward, right } = cameraBasis(camera);
  const speed = 30 * deltaTime;
  let movement: Vec3 = [0, 0, 0];

  if (keyState.has("KeyW")) {
    movement = add(movement, forward);
  }
  if (keyState.has("KeyS")) {
    movement = subtract(movement, forward);
  }
  if (keyState.has("KeyA")) {
    movement = subtract(movement, right);
  }
  if (keyState.has("KeyD")) {
    movement = add(movement, right);
  }

  if (movement[0] || movement[1] || movement[2]) {
    camera.position = add(camera.position, scale(normalize(movement), speed));
  }
}

function estimatedLoadTime(): number {
  if (loadTimeBuffer.length === 0) return DEFAULT_LOAD_TIME_MS;
  return loadTimeBuffer.reduce((a, b) => a + b, 0) / loadTimeBuffer.length;
}

function recordLoadTime(ms: number): void {
  loadTimeBuffer.push(ms);
  if (loadTimeBuffer.length > LOAD_TIME_BUFFER_SIZE) {
    loadTimeBuffer.shift();
  }
}

function cameraVelocity(): Vec3 {
  const { forward, right } = cameraBasis(camera);
  let movement: Vec3 = [0, 0, 0];
  if (keyState.has("KeyW")) movement = add(movement, forward);
  if (keyState.has("KeyS")) movement = subtract(movement, forward);
  if (keyState.has("KeyA")) movement = subtract(movement, right);
  if (keyState.has("KeyD")) movement = add(movement, right);
  const len = Math.hypot(movement[0], movement[1], movement[2]);
  return len > 0 ? scale(movement, 30 / len) : [0, 0, 0];
}

function createAnticipatedFrustum(aspect: number): QueryFrustum {
  const loadSecs = estimatedLoadTime() / 1000;
  const predictedCamera: Camera = {
    ...camera,
    position: add(camera.position, scale(cameraVelocity(), loadSecs)),
    yaw: camera.yaw + angularVelocity.yaw * loadSecs,
    pitch: clamp(camera.pitch + angularVelocity.pitch * loadSecs, -1.45, 1.45),
    fovY: camera.fovY * QUERY_FOV_EXPANSION,
  };
  return createFrustum(predictedCamera, aspect);
}

canvas.addEventListener("click", () => {
  void canvas.requestPointerLock();
});

document.addEventListener("mousemove", (event) => {
  if (document.pointerLockElement !== canvas) {
    return;
  }

  camera.yaw += event.movementX * 0.0025;
  camera.pitch = clamp(camera.pitch - event.movementY * 0.0025, -1.45, 1.45);
});

window.addEventListener("keydown", (event) => {
  if (event.code.startsWith("Key")) {
    keyState.add(event.code);
  }
  if (["KeyW", "KeyA", "KeyS", "KeyD"].includes(event.code)) {
    event.preventDefault();
  }
});

window.addEventListener("keyup", (event) => {
  keyState.delete(event.code);
});

const drawStars = regl({
  vert: `
    precision mediump float;

    attribute vec3 position;
    attribute vec3 color;
    attribute float size;

    uniform mat4 projection;
    uniform mat4 view;
    uniform float pixelRatio;

    varying vec3 vColor;

    void main() {
      vec4 viewPosition = view * vec4(position, 1.0);
      float depthScale = clamp(40.0 / -viewPosition.z, 0.6, 1.8);
      gl_Position = projection * viewPosition;
      gl_PointSize = size * pixelRatio * depthScale;
      vColor = color;
    }
  `,
  frag: `
    precision mediump float;

    varying vec3 vColor;

    void main() {
      vec2 centered = gl_PointCoord - 0.5;
      float radius = dot(centered, centered);
      if (radius > 0.25) {
        discard;
      }

      float glow = smoothstep(0.25, 0.0, radius);
      gl_FragColor = vec4(vColor * (0.45 + glow * 0.9), glow);
    }
  `,
  attributes: {
    position: { buffer: positionBuffer, size: 3 },
    color: { buffer: colorBuffer, size: 3 },
    size: { buffer: sizeBuffer, size: 1 },
  },
  uniforms: {
    projection: ({ viewportWidth, viewportHeight }) =>
      projectionMatrix({
        aspect: viewportWidth / viewportHeight,
        near: camera.near,
        far: camera.far,
        fovy: camera.fovY,
      }),
    view: ({ viewportWidth, viewportHeight }) => {
      currentFrustum = createFrustum(camera, viewportWidth / viewportHeight);
      return viewMatrix(currentFrustum);
    },
    pixelRatio: () => window.devicePixelRatio || 1,
  },
  primitive: "points",
  count: () => scene.count,
  blend: {
    enable: true,
    func: {
      srcRGB: "src alpha",
      srcAlpha: "one",
      dstRGB: "one",
      dstAlpha: "one minus src alpha",
    },
  },
  depth: {
    enable: false,
  },
});

regl.frame(({ time }) => {
  const deltaTime = previousTime === 0 ? 0 : time - previousTime;
  previousTime = time;

  if (deltaTime > 0) {
    angularVelocity = {
      yaw: (camera.yaw - prevYaw) / deltaTime,
      pitch: (camera.pitch - prevPitch) / deltaTime,
    };
  }
  prevYaw = camera.yaw;
  prevPitch = camera.pitch;

  updateCamera(deltaTime);

  const aspect = canvas.width / Math.max(canvas.height, 1);
  currentFrustum = createFrustum(camera, aspect);
  const key = frustumKey(currentFrustum);
  const now = performance.now();
  if (key !== lastQueryKey && now - lastQueryAt >= QUERY_INTERVAL_MS) {
    lastQueryAt = now;
    lastQueryKey = key;
    void queryStars(createAnticipatedFrustum(aspect));
  }

  regl.clear({
    color: [0.015, 0.025, 0.06, 1],
    depth: 1,
  });

  drawStars();
});

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
    starDump?: {
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
let starPoolMaxSize = 20000;

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
const apiRootElement = document.querySelector<HTMLElement>("#api-root");
const datasetSelectElement = document.querySelector<HTMLSelectElement>("#dataset-select");
const starsCountElement = document.querySelector<HTMLElement>("#stars-count");
const queryCountElement = document.querySelector<HTMLElement>("#query-count");
const coordinatesElement = document.querySelector<HTMLElement>("#coordinates");
const farSliderElement = document.querySelector<HTMLInputElement>("#far-slider");
const farValueElement = document.querySelector<HTMLElement>("#far-value");
const poolSliderElement = document.querySelector<HTMLInputElement>("#pool-slider");
const poolValueElement = document.querySelector<HTMLElement>("#pool-value");
if (!statusElement || !apiRootElement || !datasetSelectElement ||
    !starsCountElement || !queryCountElement || !coordinatesElement ||
    !farSliderElement || !farValueElement || !poolSliderElement || !poolValueElement) {
  throw new Error("missing hud elements");
}
const hudStatus = statusElement;
const hudApiRoot = apiRootElement;
const datasetSelect = datasetSelectElement;
const hudStarsCount = starsCountElement;
const hudQueryCount = queryCountElement;
const hudCoordinates = coordinatesElement;
const hudFarSlider = farSliderElement;
const hudFarValue = farValueElement;
const hudPoolSlider = poolSliderElement;
const hudPoolValue = poolValueElement;

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
let queryEpoch = 0;
let outstandingQueries = 0;
const starPool = new Map<string, CsvStar>();

window.starDump = {
  getFrustum: () => currentFrustum,
};

hudStatus.textContent = `Connecting to ${API_ROOT}...`;
hudApiRoot.textContent = API_ROOT;

hudFarSlider.value = String(camera.far);
hudFarValue.textContent = `${camera.far.toFixed(0)} pc`;

hudFarSlider.addEventListener("input", () => {
  camera.far = Number(hudFarSlider.value);
  hudFarValue.textContent = `${camera.far.toFixed(0)} pc`;
  lastQueryKey = "";
});

hudPoolSlider.value = String(starPoolMaxSize);
hudPoolValue.textContent = `${(starPoolMaxSize / 1000).toFixed(0)}k`;

hudPoolSlider.addEventListener("input", () => {
  starPoolMaxSize = Number(hudPoolSlider.value);
  hudPoolValue.textContent = `${(starPoolMaxSize / 1000).toFixed(0)}k`;
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

function rebuildBuffers(): void {
  const stars = [...starPool.values()];
  const positions = new Float32Array(stars.length * 3);
  stars.forEach((star, index) => {
    positions[index * 3] = star.x;
    positions[index * 3 + 1] = star.y;
    positions[index * 3 + 2] = star.z;
  });

  positionBuffer(positions);
  scene.count = stars.length;
}

function mergeIntoPool(incoming: CsvStar[]): void {
  for (const star of incoming) {
    // Delete then re-insert to move to end of Map (most recently seen = LRU tail)
    starPool.delete(star.sourceId);
    starPool.set(star.sourceId, star);
  }
  // Evict oldest entries from the front of the Map until under the cap
  while (starPool.size > starPoolMaxSize) {
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
  return datasetName;
}

datasetSelect.addEventListener("change", () => {
  if (!datasetSelect.value || datasetSelect.value === datasetName) {
    return;
  }

  datasetName = datasetSelect.value;
  const url = new URL(window.location.href);
  url.searchParams.set("dataset", datasetName);
  window.history.replaceState({}, "", url);
  queryEpoch++;
  starPool.clear();
  rebuildBuffers();
  lastQueryKey = "";
  lastQueryAt = -QUERY_INTERVAL_MS;
  void queryStars(currentFrustum);
});

async function queryStars(frustum: QueryFrustum): Promise<void> {
  const epoch = queryEpoch;
  outstandingQueries++;

  try {
    const name = await ensureDatasetName();
    const response = await fetch(frustumUrl(name, frustum));
    if (!response.ok) {
      throw new Error(`query failed: ${response.status}`);
    }
    const stars = parseStars(await response.text());
    if (epoch !== queryEpoch) {
      return;
    }
    mergeIntoPool(stars);
    hudStatus.textContent = "";
  } catch (error) {
    if (epoch !== queryEpoch) {
      return;
    }
    const message = error instanceof Error ? error.message : String(error);
    hudStatus.textContent = message;
  } finally {
    outstandingQueries--;
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
    precision highp float;

    attribute vec3 position;

    uniform mat4 projection;
    uniform mat4 view;
    uniform float pointSize;

    void main() {
      gl_Position = projection * view * vec4(position, 1.0);
      gl_PointSize = pointSize;
    }
  `,
  frag: `
    precision highp float;

    uniform float pointSize;

    void main() {
      float dist = length(gl_PointCoord - 0.5);
      float alpha = clamp((0.5 - dist) * pointSize, 0.0, 1.0);
      if (alpha < 0.01) discard;
      gl_FragColor = vec4(1.0, 1.0, 1.0, alpha);
    }
  `,
  attributes: {
    position: { buffer: positionBuffer, size: 3 },
  },
  uniforms: {
    projection: () => projectionMatrix(currentFrustum),
    view: () => viewMatrix(currentFrustum),
    pointSize: () => (window.devicePixelRatio || 1) * 1,
  },
  primitive: "points",
  count: () => scene.count,
  blend: {
    enable: true,
    func: { src: "src alpha", dst: "one minus src alpha" },
  },
  depth: { enable: false },
});

regl.frame(({ time }) => {
  const deltaTime = previousTime === 0 ? 0 : time - previousTime;
  previousTime = time;
  updateCamera(deltaTime);

  const aspect = canvas.width / Math.max(canvas.height, 1);
  currentFrustum = createFrustum(camera, aspect);
  const key = frustumKey(currentFrustum);
  const now = performance.now();
  if (key !== lastQueryKey && now - lastQueryAt >= QUERY_INTERVAL_MS) {
    lastQueryAt = now;
    lastQueryKey = key;
    void queryStars(currentFrustum);
  }

  const [cx, cy, cz] = camera.position;
  hudCoordinates.textContent = `${cx.toFixed(2)}, ${cy.toFixed(2)}, ${cz.toFixed(2)}`;
  hudStarsCount.textContent = String(starPool.size);
  hudQueryCount.textContent = String(outstandingQueries);

  regl.clear({
    color: [0.015, 0.025, 0.06, 1],
    depth: 1,
  });

  drawStars();
});

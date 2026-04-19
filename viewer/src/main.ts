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

type SceneState = {
  count: number;
};

const searchParams = new URLSearchParams(window.location.search);
const API_ROOT = searchParams.get("api")
  ?? "https://star-dump-query-api-494247280614.europe-west1.run.app";
const DATASET_OVERRIDE = searchParams.get("dataset");
const QUERY_LIMIT = 20000;
const QUERY_INTERVAL_MS = 250;

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

function createFrustum(camera: Camera, aspect: number): QueryFrustum {
  const orientation = camera.orientation;
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

function lodFrustumUrl(datasetName: string, frustum: QueryFrustum, width: number, height: number): string {
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
    width: width.toString(),
    height: height.toString(),
    limit: QUERY_LIMIT.toString(),
  });
  return `${API_ROOT}/query/${datasetName}/lod-frustum?${params.toString()}`;
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
const unitsCountElement = document.querySelector<HTMLElement>("#units-count");
const queryCountElement = document.querySelector<HTMLElement>("#query-count");
const coordinatesElement = document.querySelector<HTMLElement>("#coordinates");
const farSliderElement = document.querySelector<HTMLInputElement>("#far-slider");
const farValueElement = document.querySelector<HTMLElement>("#far-value");
const exposureSliderElement = document.querySelector<HTMLInputElement>("#exposure-slider");
const exposureValueElement = document.querySelector<HTMLElement>("#exposure-value");
if (!statusElement || !apiRootElement || !datasetSelectElement ||
    !unitsCountElement || !queryCountElement || !coordinatesElement ||
    !farSliderElement || !farValueElement || !exposureSliderElement || !exposureValueElement) {
  throw new Error("missing hud elements");
}
const hudStatus = statusElement;
const hudApiRoot = apiRootElement;
const datasetSelect = datasetSelectElement;
const hudUnitsCount = unitsCountElement;
const hudQueryCount = queryCountElement;
const hudCoordinates = coordinatesElement;
const hudFarSlider = farSliderElement;
const hudFarValue = farValueElement;
const hudExposureSlider = exposureSliderElement;
const hudExposureValue = exposureValueElement;

const canvas = document.createElement("canvas");
app.prepend(canvas);

const regl = createRegl({
  canvas,
  attributes: {
    antialias: false,
    alpha: false,
  },
});

const positionBuffer = regl.buffer({ usage: "dynamic", type: "float", length: 0 });
const luminosityBuffer = regl.buffer({ usage: "dynamic", type: "float", length: 0 });
const bpRpBuffer = regl.buffer({ usage: "dynamic", type: "float", length: 0 });
const scene: SceneState = { count: 0 };

const camera: Camera = {
  position: [0, 0, 0],
  orientation: [0, 0, 0, 1],
  fovY: Math.PI / 3,
  near: 0.1,
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
let exposure = 0.001;

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

const exposureMin = Math.log10(1e-5);
const exposureMax = Math.log10(1e-1);
hudExposureSlider.value = String(
  ((Math.log10(exposure) - exposureMin) / (exposureMax - exposureMin)) * 100
);
hudExposureValue.textContent = exposure.toExponential(1);

hudExposureSlider.addEventListener("input", () => {
  const t = Number(hudExposureSlider.value) / 100;
  exposure = Math.pow(10, exposureMin + t * (exposureMax - exposureMin));
  hudExposureValue.textContent = exposure.toExponential(1);
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

function updateBuffers(positions: Float32Array, luminosities: Float32Array, bpRps: Float32Array, count: number): void {
  positionBuffer(positions);
  luminosityBuffer(luminosities);
  bpRpBuffer(bpRps);
  scene.count = count;
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
  updateBuffers(new Float32Array(0), new Float32Array(0), new Float32Array(0), 0);
  lastQueryKey = "";
  lastQueryAt = -QUERY_INTERVAL_MS;
  void queryStars(currentFrustum);
});

async function queryStars(frustum: QueryFrustum): Promise<void> {
  const epoch = queryEpoch;
  outstandingQueries++;

  try {
    const name = await ensureDatasetName();
    const url = lodFrustumUrl(name, frustum, canvas.width, canvas.height);
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`query failed: ${response.status}`);
    }
    if (epoch !== queryEpoch) {
      return;
    }

    const buf = await response.arrayBuffer();
    const dv = new DataView(buf);
    const count = dv.getUint32(0, true);
    const positions = new Float32Array(count * 3);
    const luminosities = new Float32Array(count);
    const bpRps = new Float32Array(count);
    for (let i = 0; i < count; i++) {
      const base = 4 + i * 20;
      positions[i * 3]     = dv.getFloat32(base,      true);
      positions[i * 3 + 1] = dv.getFloat32(base + 4,  true);
      positions[i * 3 + 2] = dv.getFloat32(base + 8,  true);
      luminosities[i]      = dv.getFloat32(base + 12, true);
      bpRps[i]             = dv.getFloat32(base + 16, true);
    }

    if (epoch !== queryEpoch) {
      return;
    }
    updateBuffers(positions, luminosities, bpRps, count);
    hudUnitsCount.textContent = String(count);
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
  const speed = 2 * deltaTime;
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

  const rollSpeed = 1.5 * deltaTime;
  if (keyState.has("KeyQ")) {
    camera.orientation = normalizeQuaternion(multiplyQuaternion(
      camera.orientation, quaternionFromAxisAngle([0, 0, 1], -rollSpeed)
    ));
  }
  if (keyState.has("KeyE")) {
    camera.orientation = normalizeQuaternion(multiplyQuaternion(
      camera.orientation, quaternionFromAxisAngle([0, 0, 1], rollSpeed)
    ));
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
  }
  if (event.movementY !== 0) {
    camera.orientation = normalizeQuaternion(multiplyQuaternion(
      camera.orientation, quaternionFromAxisAngle([1, 0, 0], -event.movementY * s)
    ));
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
      vBrightness = flux * exposure;
      float t = clamp((bpRp + 0.5) / 3.5, 0.0, 1.0);
      vColor = (bpRp != bpRp) ? vec3(1.0) : bpRpToColor(t);
      gl_PointSize = clamp(sqrt(vBrightness) * 8.0, 1.0, 64.0);
    }
  `,
  frag: `
    precision highp float;

    varying vec3 vColor;
    varying float vBrightness;

    void main() {
      float r = length(gl_PointCoord - 0.5);
      float glow = exp(-r * r / 0.025);
      float alpha = glow * min(vBrightness, 1.0);
      if (alpha < 0.002) discard;
      gl_FragColor = vec4(vColor * vBrightness * glow, alpha);
    }
  `,
  attributes: {
    position:   { buffer: positionBuffer,   size: 3 },
    luminosity: { buffer: luminosityBuffer, size: 1 },
    bpRp:       { buffer: bpRpBuffer,       size: 1 },
  },
  uniforms: {
    projection:     () => projectionMatrix(currentFrustum),
    view:           () => viewMatrix(currentFrustum),
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
  hudQueryCount.textContent = String(outstandingQueries);

  regl.clear({
    color: [0, 0, 0, 1],
    depth: 1,
  });

  drawStars();
});

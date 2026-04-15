import createRegl from "regl";

type Mat4 = Float32Array;
type Vec3 = [number, number, number];
type Quaternion = [number, number, number, number];

type Star = {
  position: Vec3;
  color: Vec3;
  size: number;
};

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

function randomBetween(min: number, max: number): number {
  return min + Math.random() * (max - min);
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

function createStars(count: number): Star[] {
  const stars: Star[] = [];
  for (let i = 0; i < count; i += 1) {
    const radius = Math.pow(Math.random(), 0.45) * 90;
    const theta = randomBetween(0, Math.PI * 2);
    const phi = Math.acos(randomBetween(-1, 1));
    const twinkle = Math.random();
    const hue = randomBetween(0.72, 1.0);

    stars.push({
      position: [
        radius * Math.sin(phi) * Math.cos(theta),
        radius * Math.cos(phi) * 0.7,
        radius * Math.sin(phi) * Math.sin(theta),
      ],
      color: [
        hue,
        randomBetween(0.78, 0.94),
        twinkle > 0.9 ? 0.9 : randomBetween(0.72, 0.98),
      ],
      size: twinkle > 0.97 ? randomBetween(2.2, 3.2) : randomBetween(0.8, 1.8),
    });
  }
  return stars;
}

const app = document.querySelector<HTMLDivElement>("#app");
if (!app) {
  throw new Error("missing #app");
}

const canvas = document.createElement("canvas");
app.prepend(canvas);

const regl = createRegl({
  canvas,
  attributes: {
    antialias: true,
    alpha: false,
  },
});

const stars = createStars(1000);
const positions = stars.map((star) => star.position);
const colors = stars.map((star) => star.color);
const sizes = stars.map((star) => star.size);

const camera: Camera = {
  position: [0, 0, 120],
  yaw: 0,
  pitch: 0,
  fovY: Math.PI / 3,
  near: 0.1,
  far: 300,
};

const keyState = new Set<string>();
let currentFrustum = createFrustum(camera, 1);
let previousTime = 0;

window.gaiaViewer = {
  getFrustum: () => currentFrustum,
};

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
    position: positions,
    color: colors,
    size: sizes,
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
  count: stars.length,
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
  updateCamera(deltaTime);

  regl.clear({
    color: [0.015, 0.025, 0.06, 1],
    depth: 1,
  });

  drawStars();
});

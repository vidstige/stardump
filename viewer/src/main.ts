import createRegl from "regl";

type Mat4 = Float32Array;
type Vec3 = [number, number, number];

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
  fovY: number;
};

type QueryFrustum = FrustumParams & {
  position: Vec3;
  forward: Vec3;
  up: Vec3;
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

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function projectionMatrix(frustum: FrustumParams): Mat4 {
  const f = 1 / Math.tan(frustum.fovY / 2);
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

function createFrustum(camera: Camera, aspect: number): QueryFrustum {
  const { forward, up } = cameraBasis(camera);
  return {
    position: camera.position,
    forward,
    up,
    aspect,
    near: camera.near,
    far: camera.far,
    fovY: camera.fovY,
  };
}

function viewMatrix(frustum: QueryFrustum): Mat4 {
  return lookAt(frustum.position, add(frustum.position, frustum.forward), frustum.up);
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
        fovY: camera.fovY,
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

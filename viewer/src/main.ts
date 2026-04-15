import createRegl from "regl";

type Mat4 = Float32Array;

type Star = {
  position: [number, number, number];
  color: [number, number, number];
  size: number;
};

function subtract(a: [number, number, number], b: [number, number, number]): [number, number, number] {
  return [a[0] - b[0], a[1] - b[1], a[2] - b[2]];
}

function cross(a: [number, number, number], b: [number, number, number]): [number, number, number] {
  return [
    a[1] * b[2] - a[2] * b[1],
    a[2] * b[0] - a[0] * b[2],
    a[0] * b[1] - a[1] * b[0],
  ];
}

function normalize(v: [number, number, number]): [number, number, number] {
  const length = Math.hypot(v[0], v[1], v[2]) || 1;
  return [v[0] / length, v[1] / length, v[2] / length];
}

function perspective(fovy: number, aspect: number, near: number, far: number): Mat4 {
  const f = 1 / Math.tan(fovy / 2);
  const nf = 1 / (near - far);
  return new Float32Array([
    f / aspect, 0, 0, 0,
    0, f, 0, 0,
    0, 0, (far + near) * nf, -1,
    0, 0, 2 * far * near * nf, 0,
  ]);
}

function lookAt(
  eye: [number, number, number],
  center: [number, number, number],
  up: [number, number, number],
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

function createStars(count: number): Star[] {
  const stars: Star[] = [];
  for (let i = 0; i < count; i += 1) {
    const radius = Math.pow(Math.random(), 0.45) * 40;
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
      size: twinkle > 0.97 ? randomBetween(7, 10) : randomBetween(2, 5),
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

const state = {
  distance: 90,
  yaw: 0.5,
  pitch: 0.3,
  dragging: false,
  lastX: 0,
  lastY: 0,
};

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

canvas.addEventListener("pointerdown", (event) => {
  state.dragging = true;
  state.lastX = event.clientX;
  state.lastY = event.clientY;
  canvas.setPointerCapture(event.pointerId);
});

canvas.addEventListener("pointermove", (event) => {
  if (!state.dragging) {
    return;
  }

  const dx = event.clientX - state.lastX;
  const dy = event.clientY - state.lastY;
  state.lastX = event.clientX;
  state.lastY = event.clientY;
  state.yaw += dx * 0.005;
  state.pitch = clamp(state.pitch + dy * 0.005, -1.2, 1.2);
});

function stopDragging(event: PointerEvent): void {
  state.dragging = false;
  if (canvas.hasPointerCapture(event.pointerId)) {
    canvas.releasePointerCapture(event.pointerId);
  }
}

canvas.addEventListener("pointerup", stopDragging);
canvas.addEventListener("pointercancel", stopDragging);

canvas.addEventListener("wheel", (event) => {
  event.preventDefault();
  state.distance = clamp(state.distance + event.deltaY * 0.04, 30, 180);
}, { passive: false });

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
      float depthScale = clamp(120.0 / -viewPosition.z, 0.7, 3.0);
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
      perspective(Math.PI / 4, viewportWidth / viewportHeight, 0.01, 500),
    view: ({ time }) => {
      if (!state.dragging) {
        state.yaw += time === 0 ? 0 : 0.0008;
      }

      const eye: [number, number, number] = [
        Math.cos(state.pitch) * Math.cos(state.yaw) * state.distance,
        Math.sin(state.pitch) * state.distance,
        Math.cos(state.pitch) * Math.sin(state.yaw) * state.distance,
      ];
      return lookAt(eye, [0, 0, 0], [0, 1, 0]);
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

regl.frame(() => {
  regl.clear({
    color: [0.015, 0.025, 0.06, 1],
    depth: 1,
  });

  drawStars();
});

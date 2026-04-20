// Per-star rasterization shared between render-exact.ts and render-fast.ts.
// Mirrors the loop currently in render-exact.ts:141–173. Kept in a separate
// module so the fast renderer can use the exact brightness math without
// modifying the reference path until parity is proven.

export type Vec3 = [number, number, number];

export type Camera = {
  eye: Vec3;
  forward: Vec3;
  right: Vec3;
  up: Vec3;
  width: number;
  height: number;
  tanH: number;
  aspect: number;
};

export type RasterConfig = {
  camera: Camera;
  exposure: number;
};

export type Star = { x: number; y: number; z: number; lum: number; bprp: number };

function dot(a: Vec3, b: Vec3): number {
  return a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
}

export function normalize(v: Vec3): Vec3 {
  const l = Math.hypot(...v) || 1;
  return [v[0] / l, v[1] / l, v[2] / l];
}

export function cross(a: Vec3, b: Vec3): Vec3 {
  return [
    a[1] * b[2] - a[2] * b[1],
    a[2] * b[0] - a[0] * b[2],
    a[0] * b[1] - a[1] * b[0],
  ];
}

export function makeCamera(
  eye: Vec3,
  dir: Vec3,
  up: Vec3,
  fovDeg: number,
  width: number,
  height: number,
): Camera {
  const forward = normalize(dir);
  const right = normalize(cross(forward, up));
  const upOrth = cross(right, forward);
  const fovy = (fovDeg * Math.PI) / 180;
  return {
    eye,
    forward,
    right,
    up: upOrth,
    width,
    height,
    tanH: Math.tan(fovy * 0.5),
    aspect: width / height,
  };
}

export function cameraQuaternion(c: Camera): [number, number, number, number] {
  const r = c.right;
  const u = c.up;
  const f = c.forward;
  const m00 = r[0], m10 = r[1], m20 = r[2];
  const m01 = u[0], m11 = u[1], m21 = u[2];
  const m02 = -f[0], m12 = -f[1], m22 = -f[2];
  const trace = m00 + m11 + m22;
  if (trace > 0) {
    const s = 0.5 / Math.sqrt(trace + 1);
    return [(m21 - m12) * s, (m02 - m20) * s, (m10 - m01) * s, 0.25 / s];
  }
  if (m00 > m11 && m00 > m22) {
    const s = 2 * Math.sqrt(1 + m00 - m11 - m22);
    return [0.25 * s, (m01 + m10) / s, (m02 + m20) / s, (m21 - m12) / s];
  }
  if (m11 > m22) {
    const s = 2 * Math.sqrt(1 + m11 - m00 - m22);
    return [(m01 + m10) / s, 0.25 * s, (m12 + m21) / s, (m02 - m20) / s];
  }
  const s = 2 * Math.sqrt(1 + m22 - m00 - m11);
  return [(m02 + m20) / s, (m12 + m21) / s, 0.25 * s, (m10 - m01) / s];
}

function project(c: Camera, px: number, py: number, pz: number): [number, number, number] | null {
  const rx = px - c.eye[0], ry = py - c.eye[1], rz = pz - c.eye[2];
  const depth = dot([rx, ry, rz], c.forward);
  if (depth <= 0) return null;
  const h = dot([rx, ry, rz], c.right);
  const v = dot([rx, ry, rz], c.up);
  const sx = (h / (depth * c.tanH * c.aspect) * 0.5 + 0.5) * c.width;
  const sy = (1 - (v / (depth * c.tanH) * 0.5 + 0.5)) * c.height;
  return [sx, sy, depth];
}

function bprpToColor(bprp: number): [number, number, number] {
  if (!isFinite(bprp)) return [1, 1, 1];
  const t = Math.max(0, Math.min(1, (bprp + 0.5) / 3.5));
  const lerp = (
    a: [number, number, number],
    b: [number, number, number],
    t: number,
  ): [number, number, number] => [
    a[0] + (b[0] - a[0]) * t,
    a[1] + (b[1] - a[1]) * t,
    a[2] + (b[2] - a[2]) * t,
  ];
  if (t < 0.33) return lerp([0.6, 0.7, 1.0], [1.0, 0.95, 0.9], t / 0.33);
  if (t < 0.66) return lerp([1.0, 0.95, 0.9], [1.0, 0.85, 0.4], (t - 0.33) / 0.33);
  return lerp([1.0, 0.85, 0.4], [1.0, 0.3, 0.1], (t - 0.66) / 0.34);
}

export function rasterize(stars: Iterable<Star>, hdr: Float32Array, cfg: RasterConfig): void {
  const { camera, exposure } = cfg;
  const { width, height } = camera;
  for (const s of stars) {
    if (!(s.lum > 0)) continue;
    const proj = project(camera, s.x, s.y, s.z);
    if (!proj) continue;
    const [sx, sy, dist] = proj;
    const flux = s.lum / Math.max(dist * dist, 0.01);
    const brightness = flux * exposure;
    const [cr, cg, cb] = bprpToColor(s.bprp);

    const rPx = Math.min(Math.max(brightness * 2, 0.8), 8);
    const ir = Math.ceil(rPx);
    for (let dy = -ir; dy <= ir; dy++) {
      for (let dx = -ir; dx <= ir; dx++) {
        const xi = Math.round(sx) + dx, yi = Math.round(sy) + dy;
        if (xi < 0 || xi >= width || yi < 0 || yi >= height) continue;
        const nr = Math.sqrt(dx * dx + dy * dy) / rPx;
        const val = brightness * Math.exp(-nr * nr * 4);
        const idx = (yi * width + xi) * 3;
        hdr[idx] += cr * val;
        hdr[idx + 1] += cg * val;
        hdr[idx + 2] += cb * val;
      }
    }
  }
}

export function tonemapToBytes(hdr: Float32Array, width: number, height: number): Buffer {
  const pixels = Buffer.allocUnsafe(width * height * 3);
  const tm = (v: number) => Math.min(255, Math.round(255 * Math.pow(v / (1 + v), 1 / 2.2)));
  for (let i = 0; i < width * height; i++) {
    pixels[i * 3] = tm(hdr[i * 3]);
    pixels[i * 3 + 1] = tm(hdr[i * 3 + 1]);
    pixels[i * 3 + 2] = tm(hdr[i * 3 + 2]);
  }
  return pixels;
}

export function writePpm(path: string, width: number, height: number, pixels: Buffer): void {
  const fs = require("fs");
  const header = `P6\n${width} ${height}\n255\n`;
  fs.writeFileSync(path, Buffer.concat([Buffer.from(header), pixels]));
}

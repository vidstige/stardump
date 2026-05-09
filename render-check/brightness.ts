import * as fs from "fs";
import { encode as encodePng } from "fast-png";

export type Vec3 = [number, number, number];

export type Camera = {
  eye: Vec3;
  forward: Vec3;
  right: Vec3;
  up: Vec3;
  width: number;
  height: number;
};

export type RasterConfig = {
  camera: Camera;
  projection: Projection;
  exposure: number;
  maxRadius?: number;
};

export type Star = { x: number; y: number; z: number; lum: number; bprp: number };

export type Plane = { nx: number; ny: number; nz: number; d: number };

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

export function makeCamera(eye: Vec3, dir: Vec3, up: Vec3, width: number, height: number): Camera {
  const forward = normalize(dir);
  const right = normalize(cross(forward, up));
  const upOrth = cross(right, forward);
  return { eye, forward, right, up: upOrth, width, height };
}

export function cameraQuaternion(c: Camera): [number, number, number, number] {
  const r = c.right, u = c.up, f = c.forward;
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

function planeFromPointNormal(px: number, py: number, pz: number, nx: number, ny: number, nz: number): Plane {
  const len = Math.hypot(nx, ny, nz) || 1;
  const ux = nx / len, uy = ny / len, uz = nz / len;
  return { nx: ux, ny: uy, nz: uz, d: -(ux * px + uy * py + uz * pz) };
}

export abstract class Projection {
  abstract project(c: Camera, px: number, py: number, pz: number): [number, number, number] | null;
  abstract buildCullingPlanes(camera: Camera, near: number, far: number): Plane[];
  abstract refDist(depth: number): number;
  abstract footprintPx(half: number, dist: number, camera: Camera): number;
}

export class PerspectiveProjection extends Projection {
  readonly tanH: number;
  readonly aspect: number;

  constructor(fovDeg: number, width: number, height: number) {
    super();
    this.tanH = Math.tan((fovDeg * Math.PI) / 360);
    this.aspect = width / height;
  }

  project(c: Camera, px: number, py: number, pz: number): [number, number, number] | null {
    const rx = px - c.eye[0], ry = py - c.eye[1], rz = pz - c.eye[2];
    const depth = dot([rx, ry, rz], c.forward);
    if (depth <= 0) return null;
    const h = dot([rx, ry, rz], c.right);
    const v = dot([rx, ry, rz], c.up);
    const sx = (h / (depth * this.tanH * this.aspect) * 0.5 + 0.5) * c.width;
    const sy = (1 - (v / (depth * this.tanH) * 0.5 + 0.5)) * c.height;
    return [sx, sy, depth];
  }

  refDist(depth: number): number { return depth; }

  footprintPx(half: number, dist: number, camera: Camera): number {
    const pixelsPerRadian = camera.height / (2 * Math.atan(this.tanH));
    return (half / dist) * pixelsPerRadian;
  }

  buildCullingPlanes(camera: Camera, near: number, far: number): Plane[] {
    const { eye, forward, right, up } = camera;
    const ne: Vec3 = [eye[0]+forward[0]*near, eye[1]+forward[1]*near, eye[2]+forward[2]*near];
    const fe: Vec3 = [eye[0]+forward[0]*far,  eye[1]+forward[1]*far,  eye[2]+forward[2]*far];
    const hNear = near * this.tanH, wNear = hNear * this.aspect;
    const lN: Vec3 = [ right[0]*near+forward[0]*wNear,  right[1]*near+forward[1]*wNear,  right[2]*near+forward[2]*wNear];
    const rN: Vec3 = [-right[0]*near+forward[0]*wNear, -right[1]*near+forward[1]*wNear, -right[2]*near+forward[2]*wNear];
    const bN: Vec3 = [   up[0]*near+forward[0]*hNear,     up[1]*near+forward[1]*hNear,     up[2]*near+forward[2]*hNear];
    const tN: Vec3 = [  -up[0]*near+forward[0]*hNear,    -up[1]*near+forward[1]*hNear,    -up[2]*near+forward[2]*hNear];
    return [
      planeFromPointNormal(ne[0],ne[1],ne[2],   forward[0],  forward[1],  forward[2]),
      planeFromPointNormal(fe[0],fe[1],fe[2],  -forward[0], -forward[1], -forward[2]),
      planeFromPointNormal(eye[0],eye[1],eye[2], lN[0],lN[1],lN[2]),
      planeFromPointNormal(eye[0],eye[1],eye[2], rN[0],rN[1],rN[2]),
      planeFromPointNormal(eye[0],eye[1],eye[2], bN[0],bN[1],bN[2]),
      planeFromPointNormal(eye[0],eye[1],eye[2], tN[0],tN[1],tN[2]),
    ];
  }
}

export class OrthographicProjection extends Projection {
  readonly halfWidth: number;
  readonly aspect: number;

  constructor(halfWidth: number, width: number, height: number) {
    super();
    this.halfWidth = halfWidth;
    this.aspect = width / height;
  }

  project(c: Camera, px: number, py: number, pz: number): [number, number, number] | null {
    const rx = px - c.eye[0], ry = py - c.eye[1], rz = pz - c.eye[2];
    const depth = dot([rx, ry, rz], c.forward);
    if (depth <= 0) return null;
    const h = dot([rx, ry, rz], c.right);
    const v = dot([rx, ry, rz], c.up);
    const halfH = this.halfWidth / this.aspect;
    const sx = (h / this.halfWidth * 0.5 + 0.5) * c.width;
    const sy = (1 - (v / halfH * 0.5 + 0.5)) * c.height;
    return [sx, sy, depth];
  }

  refDist(_depth: number): number { return this.halfWidth; }

  footprintPx(half: number, _dist: number, camera: Camera): number {
    return half * (camera.width / this.halfWidth);
  }

  buildCullingPlanes(camera: Camera, near: number, far: number): Plane[] {
    const { eye, forward, right, up } = camera;
    const ne: Vec3 = [eye[0]+forward[0]*near, eye[1]+forward[1]*near, eye[2]+forward[2]*near];
    const fe: Vec3 = [eye[0]+forward[0]*far,  eye[1]+forward[1]*far,  eye[2]+forward[2]*far];
    const hw = this.halfWidth, hh = hw / this.aspect;
    const lp: Vec3 = [eye[0]-right[0]*hw, eye[1]-right[1]*hw, eye[2]-right[2]*hw];
    const rp: Vec3 = [eye[0]+right[0]*hw, eye[1]+right[1]*hw, eye[2]+right[2]*hw];
    const bp: Vec3 = [eye[0]-up[0]*hh,    eye[1]-up[1]*hh,    eye[2]-up[2]*hh];
    const tp: Vec3 = [eye[0]+up[0]*hh,    eye[1]+up[1]*hh,    eye[2]+up[2]*hh];
    return [
      planeFromPointNormal(ne[0],ne[1],ne[2],  forward[0], forward[1], forward[2]),
      planeFromPointNormal(fe[0],fe[1],fe[2], -forward[0],-forward[1],-forward[2]),
      planeFromPointNormal(lp[0],lp[1],lp[2],  right[0],   right[1],   right[2]),
      planeFromPointNormal(rp[0],rp[1],rp[2], -right[0],  -right[1],  -right[2]),
      planeFromPointNormal(bp[0],bp[1],bp[2],  up[0],      up[1],      up[2]),
      planeFromPointNormal(tp[0],tp[1],tp[2], -up[0],     -up[1],     -up[2]),
    ];
  }
}

function bprpToColor(bprp: number): [number, number, number] {
  if (!isFinite(bprp)) return [1, 1, 1];
  const t = Math.max(0, Math.min(1, (bprp + 0.5) / 3.5));
  const lerp = (a: Vec3, b: Vec3, t: number): Vec3 => [
    a[0] + (b[0] - a[0]) * t,
    a[1] + (b[1] - a[1]) * t,
    a[2] + (b[2] - a[2]) * t,
  ];
  if (t < 0.33) return lerp([0.6, 0.7, 1.0], [1.0, 0.95, 0.9], t / 0.33);
  if (t < 0.66) return lerp([1.0, 0.95, 0.9], [1.0, 0.85, 0.4], (t - 0.33) / 0.33);
  return lerp([1.0, 0.85, 0.4], [1.0, 0.3, 0.1], (t - 0.66) / 0.34);
}

export function rasterize(stars: Iterable<Star>, hdr: Float32Array, cfg: RasterConfig): void {
  const { camera, projection, exposure } = cfg;
  const { width, height } = camera;
  for (const s of stars) {
    if (!(s.lum > 0)) continue;
    const screenPos = projection.project(camera, s.x, s.y, s.z);
    if (!screenPos) continue;
    const [sx, sy, depth] = screenPos;
    const flux = s.lum / Math.max(projection.refDist(depth) ** 2, 0.01);
    const brightness = flux * exposure;
    const [cr, cg, cb] = bprpToColor(s.bprp);

    const rPx = Math.min(Math.max(brightness, 0.8), cfg.maxRadius ?? 1.0);
    const ir = Math.ceil(rPx);
    for (let dy = -ir; dy <= ir; dy++) {
      for (let dx = -ir; dx <= ir; dx++) {
        const xi = Math.round(sx) + dx, yi = Math.round(sy) + dy;
        if (xi < 0 || xi >= width || yi < 0 || yi >= height) continue;
        const nr = Math.sqrt(dx * dx + dy * dy) / rPx;
        const val = brightness * Math.exp(-nr * nr * 4);
        const idx = (yi * width + xi) * 3;
        hdr[idx]     += cr * val;
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
    pixels[i * 3]     = tm(hdr[i * 3]);
    pixels[i * 3 + 1] = tm(hdr[i * 3 + 1]);
    pixels[i * 3 + 2] = tm(hdr[i * 3 + 2]);
  }
  return pixels;
}

export function writePng(path: string, width: number, height: number, pixels: Buffer): void {
  const data = encodePng({ width, height, data: new Uint8Array(pixels), depth: 8, channels: 3 });
  fs.writeFileSync(path, Buffer.from(data));
}

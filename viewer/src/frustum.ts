// View frustum as six inward-facing planes, extracted from a view-projection
// matrix by the standard Gribb-Hartmann row combinations.

import { Mat4 } from "./mat4";

/** Six planes of (nx, ny, nz, d); a point is inside where every n·p + d >= 0. */
export type Frustum = Float32Array;

function setPlane(planes: Frustum, slot: number, m: Mat4, axis: number, sign: number): void {
  const a = m[3]  + sign * m[axis];
  const b = m[7]  + sign * m[4 + axis];
  const c = m[11] + sign * m[8 + axis];
  const d = m[15] + sign * m[12 + axis];
  const length = Math.hypot(a, b, c) || 1;
  planes.set([a / length, b / length, c / length, d / length], slot * 4);
}

export function fromViewProjection(m: Mat4): Frustum {
  const planes = new Float32Array(24);
  for (let axis = 0; axis < 3; axis++) {
    setPlane(planes, axis * 2,     m, axis,  1);
    setPlane(planes, axis * 2 + 1, m, axis, -1);
  }
  return planes;
}

export function sphereVisible(f: Frustum, x: number, y: number, z: number, radius: number): boolean {
  for (let i = 0; i < f.length; i += 4) {
    if (f[i] * x + f[i + 1] * y + f[i + 2] * z + f[i + 3] < -radius) return false;
  }
  return true;
}

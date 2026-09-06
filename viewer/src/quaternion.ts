import { Vec3, add, cross, scale } from "./vec3";

/** Unit quaternion as [x, y, z, w]. */
export type Quaternion = [number, number, number, number];

export function multiply(a: Quaternion, b: Quaternion): Quaternion {
  return [
    a[3]*b[0] + a[0]*b[3] + a[1]*b[2] - a[2]*b[1],
    a[3]*b[1] - a[0]*b[2] + a[1]*b[3] + a[2]*b[0],
    a[3]*b[2] + a[0]*b[1] - a[1]*b[0] + a[2]*b[3],
    a[3]*b[3] - a[0]*b[0] - a[1]*b[1] - a[2]*b[2],
  ];
}

export function normalize(q: Quaternion): Quaternion {
  const n = Math.hypot(q[0], q[1], q[2], q[3]) || 1;
  return [q[0] / n, q[1] / n, q[2] / n, q[3] / n];
}

export function fromAxisAngle(axis: Vec3, angle: number): Quaternion {
  const s = Math.sin(angle / 2);
  return [axis[0] * s, axis[1] * s, axis[2] * s, Math.cos(angle / 2)];
}

export function rotate(q: Quaternion, v: Vec3): Vec3 {
  const axis: Vec3 = [q[0], q[1], q[2]];
  const uv = cross(axis, v);
  return add(v, add(scale(uv, 2 * q[3]), scale(cross(axis, uv), 2)));
}

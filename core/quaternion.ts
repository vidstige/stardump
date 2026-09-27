import { Vec3, add, cross, normalize as normalizeVec3, scale } from "./vec3";

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

/**
 * The orientation of a camera looking along `forward` with `up` overhead,
 * inverting `basis`. Built from the rotation matrix whose columns are the
 * camera axes, with -forward in the third: cameras look down their own -z.
 */
export function lookRotation(forward: Vec3, up: Vec3): Quaternion {
  const f = normalizeVec3(forward);
  const r = normalizeVec3(cross(f, up));
  const u = cross(r, f);
  const [m00, m10, m20] = r;
  const [m01, m11, m21] = u;
  const [m02, m12, m22] = [-f[0], -f[1], -f[2]];
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

// Smooth rotation between camera orientations.
//
// A camera pinning a star while it orbits is turning at the orbit rate; one
// swinging onto the next star is turning at whatever the swing asks for.
// Making those meet without a visible change of pace means matching angular
// velocity at the join and not merely orientation, which slerp cannot do — it
// leaves at whatever rate the endpoints imply.
//
// A cubic on the quaternion sphere can. Its two inner control points sit one
// third of the span along each end tangent, exactly as they would on a cubic
// Bezier in the plane, and de Casteljau with slerp in place of lerp evaluates
// it.

import { Quaternion, multiply, normalize } from "../core/quaternion";
import { Vec3, scale } from "../core/vec3";

/** Half a frame at 60 fps: small enough to differentiate, large enough to be exact. */
const STEP = 1 / 120;

export function conjugate(q: Quaternion): Quaternion {
  return [-q[0], -q[1], -q[2], q[3]];
}

/** Rotation vector — axis times angle in radians — to unit quaternion. */
export function expMap(v: Vec3): Quaternion {
  const angle = Math.hypot(...v);
  if (angle < 1e-9) return [0, 0, 0, 1];
  const s = Math.sin(angle / 2) / angle;
  return [v[0] * s, v[1] * s, v[2] * s, Math.cos(angle / 2)];
}

/** Unit quaternion to rotation vector, taking the shorter of the two turns. */
export function logMap(q: Quaternion): Vec3 {
  const [x, y, z, w] = q[3] < 0 ? [-q[0], -q[1], -q[2], -q[3]] : q;
  const sine = Math.hypot(x, y, z);
  if (sine < 1e-9) return [0, 0, 0];
  return scale([x, y, z], (2 * Math.atan2(sine, w)) / sine);
}

function between(a: Quaternion, b: Quaternion): Vec3 {
  return logMap(multiply(conjugate(a), b));
}

export function slerp(a: Quaternion, b: Quaternion, t: number): Quaternion {
  return normalize(multiply(a, expMap(scale(between(a, b), t))));
}

/** How far it is from one orientation to another, in radians. */
export function turnAngle(a: Quaternion, b: Quaternion): number {
  return Math.hypot(...between(a, b));
}

/** Angular velocity of an orientation track, in radians per second. */
export function angularVelocity(track: (t: number) => Quaternion, t: number): Vec3 {
  return scale(between(track(t - STEP), track(t + STEP)), 1 / (2 * STEP));
}

/**
 * Turns from `from` to `to` over `span` seconds, leaving at angular velocity
 * `out` and arriving at `into`, so neither join changes pace.
 */
export function sweep(
  from: Quaternion, out: Vec3, to: Quaternion, into: Vec3, span: number, s: number,
): Quaternion {
  const a = multiply(from, expMap(scale(out, span / 3)));
  const b = multiply(to, expMap(scale(into, -span / 3)));
  const p = slerp(from, a, s);
  const q = slerp(a, b, s);
  const r = slerp(b, to, s);
  return slerp(slerp(p, q, s), slerp(q, r, s), s);
}

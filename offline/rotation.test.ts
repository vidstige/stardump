import * as assert from "node:assert";
import { test } from "node:test";

import { Quaternion, fromAxisAngle } from "../core/quaternion";
import { Vec3, normalize } from "../core/vec3";
import { angularVelocity, expMap, logMap, slerp, sweep, turnAngle } from "./rotation";

const SPAN = 8;

function near(a: number, b: number, tolerance: number, what: string): void {
  assert.ok(Math.abs(a - b) < tolerance, `${what}: ${a.toFixed(4)} against ${b.toFixed(4)}`);
}

function nearVec(a: Vec3, b: Vec3, tolerance: number, what: string): void {
  for (let i = 0; i < 3; i++) near(a[i], b[i], tolerance, `${what}[${i}]`);
}

const from: Quaternion = fromAxisAngle(normalize([0.2, 1, -0.3]), 0.7);
const to: Quaternion = fromAxisAngle(normalize([-0.5, 0.4, 1]), 2.1);
const out: Vec3 = [0.03, -0.11, 0.05];
const into: Vec3 = [-0.07, 0.02, 0.09];

/** The spline in seconds rather than in its own parameter. */
const track = (t: number) => sweep(from, out, to, into, SPAN, t / SPAN);

test("a rotation vector survives a round trip through the quaternion", () => {
  const v: Vec3 = [0.3, -1.2, 0.45];
  nearVec(logMap(expMap(v)), v, 1e-9, "round trip");
});

test("sweeping arrives at the orientation it was given", () => {
  near(turnAngle(track(0), from), 0, 1e-9, "start");
  near(turnAngle(track(SPAN), to), 0, 1e-9, "end");
});

test("sweeping leaves and arrives at the angular velocity it was given", () => {
  // The whole point of a cubic over a slerp: a slerp would leave at whatever
  // rate its endpoints imply, and the join would jolt.
  nearVec(angularVelocity(track, 0), out, 1e-3, "leaving");
  nearVec(angularVelocity(track, SPAN), into, 1e-3, "arriving");
});

test("sweeping never doubles back on itself", () => {
  let travelled = 0;
  const steps = 400;
  for (let i = 0; i < steps; i++) {
    travelled += turnAngle(track((i * SPAN) / steps), track(((i + 1) * SPAN) / steps));
  }
  // Some overshoot is the price of matching both end velocities, but a path
  // that wanders takes far longer than the turn it is making.
  assert.ok(travelled < 1.6 * turnAngle(from, to),
    `travels ${travelled.toFixed(3)} rad to turn ${turnAngle(from, to).toFixed(3)}`);
});

test("slerping turns at a constant rate", () => {
  const rate = (t: number) => Math.hypot(...angularVelocity((u) => slerp(from, to, u), t));
  near(rate(0.25), rate(0.75), 1e-6, "rate");
});

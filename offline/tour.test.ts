import * as assert from "node:assert";
import { test } from "node:test";

import { projectionMatrix, toScreen, viewMatrix } from "../core/camera";
import { Vec3, subtract } from "../core/vec3";
import { Labels } from "./dataset";
import { angularVelocity } from "./rotation";
import { buildTour } from "./tour";

const FPS = 30;
const WIDTH = 1920;
const HEIGHT = 1080;

function neighbourhood(): Labels {
  return {
    "Proxima Centauri": [-0.4743, -0.363, -1.1561],
    "Barnard's Star": [-0.0174, -1.8223, 0.1496],
    "Epsilon Eridani": [1.9003, 2.5432, -0.5289],
    "Tau Ceti": [3.1549, 1.5399, -1.0026],
    "61 Cygni A": [1.9861, -1.8699, 2.1893],
    "51 Pegasi": [14.0538, -3.9327, 5.5346],
    Sun: [0, 0, 0],
  };
}

function frames(): number[] {
  const tour = buildTour(neighbourhood());
  const out: number[] = [];
  for (let f = 0; f / FPS <= tour.duration; f++) out.push(f);
  return out;
}

const tour = buildTour(neighbourhood());

/** Degrees a second the camera is turning, whichever way. */
function turnRate(seconds: number): number {
  const spin = angularVelocity((t) => tour.shotAt(t).camera.orientation, seconds);
  return (Math.hypot(...spin) * 180) / Math.PI;
}

test("the camera never jumps between one frame and the next", () => {
  const steps = frames().slice(0, -1).map((f) =>
    Math.hypot(...subtract(
      tour.shotAt((f + 1) / FPS).camera.position,
      tour.shotAt(f / FPS).camera.position,
    )));
  const longest = Math.max(...steps);
  const changes = steps.slice(1).map((step, i) => Math.abs(step - steps[i]));
  // A continuous speed sampled 30 times a second changes only slightly per
  // frame; anything abrupt shows as a large fraction of the longest step.
  assert.ok(Math.max(...changes) < 0.05 * longest,
    `speed jumps by ${(100 * Math.max(...changes) / longest).toFixed(1)}% of the longest step`);
});

/** The interior, since turn rate is a derivative and the tour has two ends. */
function interior(): number[] {
  return frames().slice(1, -1);
}

// Ceilings on a whip pan and on a jolt, not a record of the current numbers:
// transitions are meant to swing harder than showcases, so what is being
// asserted is that neither ever gets out of hand.
test("the camera never turns faster than a slow pan", () => {
  const fastest = Math.max(...interior().map((f) => turnRate(f / FPS)));
  assert.ok(fastest < 30, `pans at up to ${fastest.toFixed(1)} degrees per second`);
});

test("how fast the camera turns never changes abruptly", () => {
  const rates = interior().map((f) => turnRate(f / FPS));
  const changes = rates.slice(1).map((rate, i) => Math.abs(rate - rates[i]) * FPS);
  const worst = Math.max(...changes);
  assert.ok(worst < 25, `turn rate jumps by ${worst.toFixed(1)} degrees per second squared`);
});

/** Captions name a star the way a viewer would; the labels key it plainly. */
function held(name: string): Vec3 {
  const labels = neighbourhood();
  return labels[name] ?? labels[name.replace(/^The /, "")];
}

test("every star in the labels is held at some point", () => {
  const names = new Set<string>();
  for (const f of frames()) {
    const focus = tour.shotAt(f / FPS).focus;
    if (focus && focus.emphasis > 0.99) names.add(focus.name);
  }
  assert.strictEqual(names.size, Object.keys(neighbourhood()).length);
});

test("a star sits in the middle of the frame while it is held", () => {
  const projection = projectionMatrix(Math.PI / 4, WIDTH / HEIGHT, 8000);
  let checked = 0;
  for (const f of frames()) {
    const shot = tour.shotAt(f / FPS);
    if (!shot.focus || shot.focus.emphasis < 0.99) continue;
    const screen = toScreen(
      held(shot.focus.name), viewMatrix(shot.camera), projection, WIDTH, HEIGHT,
    );
    assert.ok(screen, "the held star is behind the camera");
    assert.ok(Math.hypot(screen[0] - WIDTH / 2, screen[1] - HEIGHT / 2) < 1,
      `${shot.focus.name} sits ${screen[0]},${screen[1]} at ${(f / FPS).toFixed(1)}s`);
    checked++;
  }
  assert.ok(checked > 0);
});

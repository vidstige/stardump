import * as assert from "node:assert";
import { test } from "node:test";

import { projectionMatrix, toScreen, viewMatrix } from "../core/camera";
import { rotate } from "../core/quaternion";
import { add, normalize, scale, subtract } from "../core/vec3";
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

test("the framing never changes abruptly", () => {
  // Widening by 15 degrees in one frame nearly doubles the sky on screen and a
  // rim of stars appears at once, which is what the outro used to do: it is the
  // one join with no transition in front of it to ease anything across.
  const steps = frames().slice(0, -1).map((f) =>
    Math.abs(tour.shotAt((f + 1) / FPS).fovDeg - tour.shotAt(f / FPS).fovDeg));
  const worst = Math.max(...steps);
  assert.ok(worst < 1, `field of view jumps ${worst.toFixed(1)} degrees in a frame`);
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

test("every star in the labels but the Sun is held at some point", () => {
  // The Sun is where the intro stands, not something it can point at.
  const expected = Object.keys(neighbourhood()).filter((name) => name !== "Sun");
  assert.deepStrictEqual([...named()].sort(), expected.sort());
});

/** Where a star lands on screen, in pixels from the centre. */
function offCentre(name: string, seconds: number): number {
  const shot = tour.shotAt(seconds);
  const projection = projectionMatrix(Math.PI / 4, WIDTH / HEIGHT, 8000);
  const screen = toScreen(
    neighbourhood()[name], viewMatrix(shot.camera), projection, WIDTH, HEIGHT,
  );
  return screen ? Math.hypot(screen[0] - WIDTH / 2, screen[1] - HEIGHT / 2) : Infinity;
}

/** Names in the order the film first puts them up. */
function named(): string[] {
  const order: string[] = [];
  for (const f of frames()) {
    const focus = tour.shotAt(f / FPS).focus;
    if (focus && focus.emphasis > 0.99 && !order.includes(focus.name)) order.push(focus.name);
  }
  return order;
}

/** Seconds before the camera first moves at all, which is the intro. */
function still(): number {
  const start = tour.shotAt(0).camera.position;
  for (const f of frames()) {
    if (Math.hypot(...subtract(tour.shotAt(f / FPS).camera.position, start)) > 1e-9) {
      return (f - 1) / FPS;
    }
  }
  return tour.duration;
}

test("a star sits in the middle of the frame while it is held", () => {
  const projection = projectionMatrix(Math.PI / 4, WIDTH / HEIGHT, 8000);
  let checked = 0;
  for (const f of frames()) {
    const shot = tour.shotAt(f / FPS);
    if (!shot.focus || shot.focus.emphasis < 0.99) continue;
    const screen = toScreen(
      neighbourhood()[shot.focus.name], viewMatrix(shot.camera), projection, WIDTH, HEIGHT,
    );
    assert.ok(screen, "the held star is behind the camera");
    assert.ok(Math.hypot(screen[0] - WIDTH / 2, screen[1] - HEIGHT / 2) < 1,
      `${shot.focus.name} sits ${screen[0]},${screen[1]} at ${(f / FPS).toFixed(1)}s`);
    checked++;
  }
  assert.ok(checked > 0);
});

test("the intro turns without moving at all", () => {
  assert.ok(still() > 10, `the camera starts moving after ${still().toFixed(1)}s`);
});

test("the intro pins one point of sky just below the middle of the frame", () => {
  // The axis a turn is about is the one direction it leaves alone, so the point
  // of sky it picks out is the only fixed thing in the picture. It has to be on
  // screen, or there is nothing to see the turn against and the whole shot
  // reads as a drift — which is what the first two attempts at this did.
  // angularVelocity reports in the body frame, which is what sweep matches on;
  // the point of sky it picks out needs it back in world coordinates.
  const midway = tour.shotAt(still() / 2);
  const axis = rotate(
    midway.camera.orientation,
    normalize(angularVelocity((t) => tour.shotAt(t).camera.orientation, still() / 2)),
  );
  const seen = frames()
    .filter((f) => f / FPS < still())
    .map((f) => {
      const shot = tour.shotAt(f / FPS);
      const projection = projectionMatrix(
        (shot.fovDeg * Math.PI) / 180, WIDTH / HEIGHT, 8000,
      );
      return toScreen(
        add(shot.camera.position, scale(axis, 1000)),
        viewMatrix(shot.camera), projection, WIDTH, HEIGHT,
      );
    });

  const onScreen = seen.filter((p): p is [number, number] => p !== null);
  assert.strictEqual(onScreen.length, seen.length, "the pinned point is behind the camera");

  const [x, y] = onScreen[0];
  const drift = Math.max(...onScreen.map((p) => Math.hypot(p[0] - x, p[1] - y)));
  assert.ok(drift < 1, `the pinned point wanders ${drift.toFixed(1)} px`);
  assert.ok(Math.abs(x - WIDTH / 2) < 1, `the pinned point sits ${x.toFixed(0)} px across`);
  const below = (y - HEIGHT / 2) / HEIGHT;
  assert.ok(below > 0.05 && below < 0.25,
    `the pinned point sits ${(below * 100).toFixed(1)}% of the frame below the middle`);
});

test("the intro ends pointing at the star it sets off for", () => {
  const first = named()[0];
  const off = offCentre(first, still());
  assert.ok(off < 1, `${first} is ${off.toFixed(0)} px off centre when the intro ends`);
});

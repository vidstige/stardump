// The tour: which stars, in what order, and how the camera moves between them.
//
// Two phases, and one flow through both. In a showcase the camera orbits a
// star with the star pinned dead centre, so the star is the one thing in the
// picture that does not move while everything within a few parsecs slides
// behind it. In a transition it flies to the next star and swings onto it.
//
// What holds the two together is the join, not a single pace. A showcase pans
// at ORBIT and a transition is free to swing harder, but a transition leaves
// and arrives at exactly the rate whatever is either side of it is turning, so
// nothing jolts on the way in or out. Matching that takes more than matching
// orientation, since slerp leaves at whatever rate its endpoints imply; that is
// what rotation.ts is for. It is also what lets the two ends of the film be
// something other than an orbit and still belong to the same flow.
//
// Both phases move and turn at once. Turning on the spot and then flying in a
// straight stare is what an earlier cut of this did, and it looked anxious: no
// parallax while turning, no turning while moving, and the rate swinging
// twentyfold between the two.
//
// Transitions are paced by speed rather than by duration: a transition lasts as
// long as its distance needs, so short hops are over quickly. Where the turn is
// too large to make in that time the turn wins instead and the leg runs slower,
// which is what the route is chosen to avoid — see ROUTE.
//
// Turning is what the film costs. The Sun's neighbours lie in every direction,
// so a route between them doubles back constantly: some 490 degrees of turning
// whichever order they are visited in, against 27 pc of travel.

import { Camera } from "../core/camera";
import { Quaternion, fromAxisAngle, lookRotation, multiply, rotate } from "../core/quaternion";
import { Vec3, add, cross, normalize, scale, subtract } from "../core/vec3";
import { Labels } from "./dataset";
import { angularVelocity, sweep, turnAngle } from "./rotation";

/** Degrees a second the camera pans while a star is held. */
const ORBIT = 3;
/** Parsecs a second a transition flies: what paces the distance it covers. */
const CRUISE_SPEED = 1.0;
/** Ceiling on how fast it may swing: what paces the turn it has to make. */
const TURN_RATE = 14;
const MIN_TRANSIT = 3.5;

const SHOWCASE = 9;

/** The opening: seconds at the Sun, and degrees the sky turns in them. */
const OPENING = 15;
const SPIN_DEG = 40;

/** The closing rush: seconds, and the parsecs it covers in them. */
const RUSH = 13;
const RUSH_PC = 200;
/** Where the rush starts, out beyond the last star along the way in. */
const RUN_UP_PC = 4;

const STANDOFF_PC = 0.3;

const FOV_DEG = 50;
/** Wider at the two ends, where the subject is the sky rather than a star. */
const OPEN_FOV_DEG = 65;

/** Seconds a name takes to come up, and to go again. */
const FADE = 1.4;

/** Seconds the picture takes to come up from black, and to go back down. */
const FADE_IN = 3;
const FADE_OUT = 5;

// Galactic north pole and galactic centre, in the equatorial cartesian frame
// the catalogue is stored in. North keeps the galactic plane level; the centre
// is what the opening and closing shots look at. The frame is equatorial, so
// the celestial pole the sky turns about is simply z.
const NGP = normalize([-0.86703, -0.20006, 0.45673]);
const GALACTIC_CENTRE = normalize([-0.05487, -0.87344, -0.48384]);
const POLE: Vec3 = [0, 0, 1];

// Ordered so that the turn each leg asks for is in proportion to the distance
// it has to cover, since at a fixed speed the turn per parsec is exactly the
// turn rate the leg demands. Visiting the neighbours in order of distance, the
// obvious route, is the worst on that measure: it doubles back hardest where it
// has least room, asking 79 degrees per parsec against the 28 of this one.
const ROUTE = [
  "Barnard's Star",
  "Tau Ceti",
  "61 Cygni A",
  "Proxima Centauri",
  "Epsilon Eridani",
  "51 Pegasi",
];

/** The star being held, and how far its name has come up, 0 to 1. */
export type Focus = { name: string; emphasis: number };

export type Shot = { camera: Camera; fovDeg: number; focus: Focus | null };

export type Tour = {
  duration: number;
  shotAt(seconds: number): Shot;
  /** How much of the picture is up, 0 at the two ends. */
  fadeAt(seconds: number): number;
};

type Motion = { position: Vec3; velocity: Vec3 };

/** A stretch of camera motion, and the flight from it to the next. */
type Hold = {
  name: string | null;
  fovDeg: number;
  dwell: number;
  /** Seconds spent flying to the next hold. */
  transit: number;
  at(seconds: number): Motion;
  facing(seconds: number): Quaternion;
};

function smootherstep(t: number): number {
  const s = Math.min(Math.max(t, 0), 1);
  return s * s * s * (s * (s * 6 - 15) + 10);
}

function hermite(p0: Vec3, v0: Vec3, p1: Vec3, v1: Vec3, s: number, span: number): Vec3 {
  const s2 = s * s;
  const s3 = s2 * s;
  return add(
    add(scale(p0, 2 * s3 - 3 * s2 + 1), scale(p1, 3 * s2 - 2 * s3)),
    add(scale(v0, (s3 - 2 * s2 + s) * span), scale(v1, (s3 - s2) * span)),
  );
}

/** Arcing around `pivot` from `from`, `way` deciding which way round. */
function orbit(pivot: Vec3, from: Vec3, way: number): (seconds: number) => Motion {
  const away = subtract(from, pivot);
  const radius = Math.hypot(...away);
  const offset = scale(away, 1 / radius);
  // North made perpendicular to the view, so the drift is sideways not a roll.
  const axis = normalize(cross(offset, cross(NGP, offset)));
  const rate = ((ORBIT * Math.PI) / 180) * way;
  return (seconds) => {
    const turned = rotate(fromAxisAngle(axis, rate * seconds), offset);
    return {
      position: add(pivot, scale(turned, radius)),
      velocity: scale(cross(axis, turned), radius * rate),
    };
  };
}

function showcase(name: string, star: Vec3, from: Vec3, way: number): Hold {
  const at = orbit(star, from, way);
  return {
    name,
    fovDeg: FOV_DEG,
    dwell: SHOWCASE,
    transit: 0,
    at,
    facing: (seconds) => lookRotation(subtract(star, at(seconds).position), NGP),
  };
}

/**
 * The opening: the Sun held while the sky turns about the celestial pole, the
 * turn unwinding to nothing by the end. The film therefore leaves from a camera
 * that has just come to rest, and the transition out of it can start straight
 * away towards the first star rather than having to stop the spin first.
 */
function opening(sun: Vec3, from: Vec3, way: number): Hold {
  const held = showcase("The Sun", sun, from, way);
  const spin = (SPIN_DEG * Math.PI) / 180;
  return {
    ...held,
    fovDeg: OPEN_FOV_DEG,
    dwell: OPENING,
    facing(seconds) {
      const left = 1 - smootherstep(seconds / OPENING);
      return multiply(fromAxisAngle(POLE, -spin * left), held.facing(seconds));
    },
  };
}

/**
 * The closing rush: straight out along `heading` and accelerating, cubic in
 * time so it leaves at rest and is moving fastest as the picture goes. It runs
 * towards the galactic centre rather than away from anything, so the field
 * ahead only ever thickens and no edge of the catalogue comes into view.
 */
function rush(from: Vec3, heading: Vec3): Hold {
  const orientation = lookRotation(heading, NGP);
  return {
    name: null,
    fovDeg: OPEN_FOV_DEG,
    dwell: RUSH,
    transit: 0,
    at(seconds) {
      const u = seconds / RUSH;
      return {
        position: add(from, scale(heading, RUSH_PC * u * u * u)),
        velocity: scale(heading, (3 * RUSH_PC * u * u) / RUSH),
      };
    },
    facing: () => orientation,
  };
}

/**
 * Where the camera stands to hold `target`: back along the leg it arrives on,
 * so the approach is head on and the star grows in the middle of the frame
 * before the orbit starts sliding it sideways.
 */
function standoff(target: Vec3, from: Vec3, distance: number): Vec3 {
  return subtract(target, scale(normalize(subtract(target, from)), distance));
}

function holds(labels: Labels): Hold[] {
  const sun = labels["Sun"];
  const stars = ROUTE.map((name) => ({ name, position: labels[name] }));
  const targets = [sun, ...stars.map((s) => s.position)];

  // The opening stands off against the galactic centre, which puts the Sun in
  // the middle of the frame with the whole Milky Way behind it.
  const build = (i: number, way: number): Hold =>
    i === 0
      ? opening(sun, subtract(sun, scale(GALACTIC_CENTRE, STANDOFF_PC)), way)
      : showcase(
          stars[i - 1].name, targets[i],
          standoff(targets[i], targets[i - 1], STANDOFF_PC), way,
        );

  const all = targets.map((_, i) => build(i, 1));
  const last = targets[targets.length - 1];
  const outward = normalize(subtract(last, targets[targets.length - 2]));
  all.push(rush(add(last, scale(outward, RUN_UP_PC)), GALACTIC_CENTRE));

  // An arc swings the camera as far as it drifts, so which way it goes decides
  // how much of the next turn is already done. Going the wrong way around adds
  // the whole arc back onto the transition.
  for (let i = 0; i < all.length - 1; i++) {
    const arrival = all[i + 1].facing(0);
    const turn = (way: number) =>
      turnAngle(build(i, way).facing(all[i].dwell), arrival);
    all[i] = build(i, turn(-1) < turn(1) ? -1 : 1);

    const travel = Math.hypot(
      ...subtract(all[i + 1].at(0).position, all[i].at(all[i].dwell).position),
    );
    all[i].transit = Math.max(
      MIN_TRANSIT,
      (turnAngle(all[i].facing(all[i].dwell), arrival) * 180) / Math.PI / TURN_RATE,
      travel / CRUISE_SPEED,
    );
  }
  return all;
}

export function buildTour(labels: Labels): Tour {
  const all = holds(labels);
  // Where each hold starts; the transition to the next follows it.
  const starts: number[] = [];
  let at = 0;
  for (const hold of all) {
    starts.push(at);
    at += hold.dwell + hold.transit;
  }
  const duration = at;

  // The ends of every hold, which is all a transition needs in order to match.
  const leaves = all.map((hold) => ({
    orientation: hold.facing(hold.dwell),
    spin: angularVelocity(hold.facing, hold.dwell),
  }));
  const arrives = all.map((hold) => ({
    orientation: hold.facing(0),
    spin: angularVelocity(hold.facing, 0),
  }));

  function index(seconds: number): number {
    let i = all.length - 1;
    while (i > 0 && seconds < starts[i]) i--;
    return i;
  }

  function shot(seconds: number): { position: Vec3; orientation: Quaternion; fovDeg: number } {
    const i = index(seconds);
    const hold = all[i];
    const elapsed = seconds - starts[i];
    if (elapsed <= hold.dwell) {
      return {
        position: hold.at(elapsed).position,
        orientation: hold.facing(elapsed),
        fovDeg: hold.fovDeg,
      };
    }

    const next = all[i + 1];
    const s = (elapsed - hold.dwell) / hold.transit;
    const leave = hold.at(hold.dwell);
    const arrive = next.at(0);
    return {
      position: hermite(
        leave.position, leave.velocity, arrive.position, arrive.velocity, s, hold.transit,
      ),
      orientation: sweep(
        leaves[i].orientation, leaves[i].spin,
        arrives[i + 1].orientation, arrives[i + 1].spin,
        hold.transit, s,
      ),
      fovDeg: hold.fovDeg + (next.fovDeg - hold.fovDeg) * smootherstep(s),
    };
  }

  function focus(seconds: number): Focus | null {
    for (let i = 0; i < all.length; i++) {
      const { name, dwell } = all[i];
      if (!name) continue;
      const emphasis = smootherstep((seconds - starts[i]) / FADE) *
        (1 - smootherstep((seconds - (starts[i] + dwell - FADE)) / FADE));
      if (emphasis > 0.001) return { name, emphasis };
    }
    return null;
  }

  return {
    duration,
    fadeAt: (seconds) =>
      smootherstep(seconds / FADE_IN) * smootherstep((duration - seconds) / FADE_OUT),
    shotAt(seconds) {
      const { position, orientation, fovDeg } = shot(
        Math.min(Math.max(seconds, 0), duration),
      );
      return { camera: { position, orientation }, fovDeg, focus: focus(seconds) };
    },
  };
}

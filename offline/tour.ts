// The tour: which stars, in what order, and how the camera moves between them.
//
// Two phases, and one flow through both. In a showcase the camera orbits a
// star with the star pinned dead centre, so the star is the one thing in the
// picture that does not move while everything within a few parsecs slides
// behind it. In a transition it flies to the next star and swings onto it.
//
// What holds the two together is the join, not a single pace. A showcase pans
// at ORBIT and a transition is free to swing harder, but a transition leaves
// and arrives at exactly the rate the orbits either side of it are turning, so
// nothing jolts on the way in or out. Matching that takes more than matching
// orientation, since slerp leaves at whatever rate its endpoints imply; that is
// what rotation.ts is for.
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
import { Quaternion, fromAxisAngle, lookRotation, rotate } from "../core/quaternion";
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
const OPENING = 13;
/** Seconds of sky after the last star. */
const CODA = 14;

const STANDOFF_PC = 0.3;
/** The closing shot drifts on a wider arc, so it glides rather than circles. */
const CODA_STANDOFF_PC = 4;
/** How far out that shot looks, well beyond anything in the index. */
const CODA_DISTANCE_PC = 4000;

const FOV_DEG = 50;
/** Wider at the two ends, where the subject is the sky rather than a star. */
const OPEN_FOV_DEG = 65;

/** Seconds a name takes to come up, and to go again. */
const FADE = 1.4;

/** Seconds the picture takes to come up from black, and to go back down. */
const FADE_IN = 2.5;
const FADE_OUT = 4;

// Galactic north pole and galactic centre, in the equatorial cartesian frame
// the catalogue is stored in. North keeps the galactic plane level; the centre
// is what the opening and closing shots look at.
const NGP = normalize([-0.86703, -0.20006, 0.45673]);
const GALACTIC_CENTRE = normalize([-0.05487, -0.87344, -0.48384]);

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

/**
 * A stretch of orbit. The camera arcs around `pivot` while looking at
 * `target`; for a showcase the two are the same star, which is what pins it,
 * and the closing shot sets them apart so the camera drifts past 51 Pegasi
 * with the galaxy held still in frame.
 */
type Hold = {
  name: string | null;
  pivot: Vec3;
  target: Vec3;
  /** Camera position where the arc starts. */
  from: Vec3;
  /** Which way around the arc goes; whichever leans toward the next star. */
  way: number;
  fovDeg: number;
  dwell: number;
  /** Seconds spent flying to the next hold. */
  transit: number;
};

function smootherstep(t: number): number {
  const s = Math.min(Math.max(t, 0), 1);
  return s * s * s * (s * (s * 6 - 15) + 10);
}

/** Camera position and velocity `seconds` into a hold's arc. */
function onArc(hold: Hold, seconds: number): { position: Vec3; velocity: Vec3 } {
  const away = subtract(hold.from, hold.pivot);
  const radius = Math.hypot(...away);
  const offset = scale(away, 1 / radius);
  // North made perpendicular to the view, so the drift is sideways not a roll.
  const axis = normalize(cross(offset, cross(NGP, offset)));
  const rate = (ORBIT * Math.PI) / 180;
  const turned = rotate(fromAxisAngle(axis, rate * seconds * hold.way), offset);
  return {
    position: add(hold.pivot, scale(turned, radius)),
    velocity: scale(cross(axis, turned), radius * rate * hold.way),
  };
}

function aim(hold: Hold, position: Vec3): Quaternion {
  return lookRotation(subtract(hold.target, position), NGP);
}

/** Where the camera points `seconds` into a hold's arc. */
function facing(hold: Hold): (seconds: number) => Quaternion {
  return (seconds) => aim(hold, onArc(hold, seconds).position);
}

function hermite(p0: Vec3, v0: Vec3, p1: Vec3, v1: Vec3, s: number, span: number): Vec3 {
  const s2 = s * s;
  const s3 = s2 * s;
  return add(
    add(scale(p0, 2 * s3 - 3 * s2 + 1), scale(p1, 3 * s2 - 2 * s3)),
    add(scale(v0, (s3 - 2 * s2 + s) * span), scale(v1, (s3 - s2) * span)),
  );
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

  const all: Hold[] = targets.map((target, i) => ({
    name: i === 0 ? "The Sun" : stars[i - 1].name,
    pivot: target,
    target,
    // The opening stands off against the galactic centre, which puts the Sun
    // in the middle of the frame with the whole Milky Way behind it.
    from: i === 0 ? subtract(sun, scale(GALACTIC_CENTRE, STANDOFF_PC))
                  : standoff(target, targets[i - 1], STANDOFF_PC),
    way: 1,
    fovDeg: i === 0 ? OPEN_FOV_DEG : FOV_DEG,
    dwell: i === 0 ? OPENING : SHOWCASE,
    transit: 0,
  }));

  // The closing shot drifts on past the last star while looking away at the
  // galaxy, so the film settles instead of stopping.
  const last = all[all.length - 1];
  const outward = normalize(subtract(last.target, targets[targets.length - 2]));
  all.push({
    name: null,
    pivot: last.target,
    target: add(last.target, scale(GALACTIC_CENTRE, CODA_DISTANCE_PC)),
    from: add(last.target, scale(outward, CODA_STANDOFF_PC)),
    way: 1,
    fovDeg: OPEN_FOV_DEG,
    dwell: CODA,
    transit: 0,
  });

  // An arc swings the camera as far as it drifts, so which way it goes decides
  // how much of the next turn is already done. Going the wrong way around adds
  // the whole arc back onto the transition.
  for (let i = 0; i < all.length - 1; i++) {
    const arrival = facing(all[i + 1])(0);
    const turn = (way: number) =>
      turnAngle(facing({ ...all[i], way })(all[i].dwell), arrival);
    all[i].way = turn(-1) < turn(1) ? -1 : 1;

    const travel = Math.hypot(
      ...subtract(all[i + 1].from, onArc(all[i], all[i].dwell).position),
    );
    all[i].transit = Math.max(
      MIN_TRANSIT,
      (turn(all[i].way) * 180) / Math.PI / TURN_RATE,
      travel / CRUISE_SPEED,
    );
  }
  return all;
}

export function buildTour(labels: Labels): Tour {
  const all = holds(labels);
  // Where each arc starts; the transition to the next follows it.
  const starts: number[] = [];
  let at = 0;
  for (const hold of all) {
    starts.push(at);
    at += hold.dwell + hold.transit;
  }
  const duration = at;

  // The ends of every arc, which is all a transition needs in order to match.
  const leaves = all.map((hold) => ({
    orientation: facing(hold)(hold.dwell),
    spin: angularVelocity(facing(hold), hold.dwell),
  }));
  const arrives = all.map((hold) => ({
    orientation: facing(hold)(0),
    spin: angularVelocity(facing(hold), 0),
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
      const { position } = onArc(hold, elapsed);
      return { position, orientation: aim(hold, position), fovDeg: hold.fovDeg };
    }

    const next = all[i + 1];
    const s = (elapsed - hold.dwell) / hold.transit;
    const leave = onArc(hold, hold.dwell);
    const arrive = onArc(next, 0);
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

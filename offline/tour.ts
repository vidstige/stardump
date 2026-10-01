// The tour: which stars, in what order, and how the camera moves between them.
//
// Three parts under three rules.
//
// The **intro** only turns. The camera stands still at the Sun, no translation
// at all, and spins about an axis picked so two things come out right at once:
// the point of sky the turn pins lands just below the middle of the frame, and
// the turn finishes aimed at the first star.
//
// The **tour** alternates showcase and transition. In a showcase the camera
// orbits a star with the star pinned dead centre, so the star is the one thing
// in the picture that does not move while everything within a few parsecs
// slides behind it; in a transition it flies to the next star and swings onto
// it. Both move and turn at once. Turning on the spot and then flying in a
// straight stare is what an earlier cut of this did, and it looked anxious: no
// parallax while turning, no turning while moving, and the rate swinging
// twentyfold between the two.
//
// The **outro** only accelerates. It picks the last showcase up exactly where
// that leaves off and opens the throttle, with no transition in between.
//
// What lets three such different rules read as one flow is that every join
// matches angular velocity, not just orientation. Slerp leaves at whatever
// rate its endpoints imply, so it cannot do that; rotation.ts can.
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
import { Vec3, add, cross, dot, normalize, scale, subtract } from "../core/vec3";
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

/** The opening: seconds spent turning on the spot, and degrees a second. */
const OPENING = 15;
const SPIN_RATE = 2;
/**
 * Degrees the camera aims above the axis it turns about. The axis is the one
 * direction a turn leaves alone, so the point of sky it picks out is the only
 * fixed thing in the frame; this is how far below the middle it sits.
 */
const TILT = 9;

/** The closing rush: seconds, and the parsecs it covers in them. */
const RUSH = 13;
const RUSH_PC = 800;

/** Standoff for a star of solar luminosity. The rest scale from it. */
const STANDOFF_PC = 0.36;

/**
 * How much of the luminosity difference the standoff takes out. Screen
 * brightness goes as luminosity over distance squared, so holding a star at a
 * distance proportional to the fourth root of its luminosity halves that
 * exponent: the 4300-fold spread across these six comes down to 66-fold.
 *
 * Neither end is wanted. Held all at one distance a red dwarf falls below the
 * smallest splat the shader will draw and simply is not there — Proxima at
 * 0.3 pc lands under the floor. Compensated in full they would all look alike,
 * and that they do not is half of what the picture is saying.
 */
const COMPENSATION = 0.25;

/**
 * Luminosity in solar units, read out of the index. The catalogue has it per
 * star but labels.json carries only positions, and find-star already keeps
 * these same six by hand, so they are kept here too rather than threading the
 * point table through a camera path that otherwise needs nothing from it.
 */
const LUMINOSITY: Record<string, number> = {
  "Proxima Centauri": 3.19e-4,
  "Barnard's Star": 1.3e-3,
  "61 Cygni A": 1.12e-1,
  "Epsilon Eridani": 3.14e-1,
  "Tau Ceti": 4.71e-1,
  "51 Pegasi": 1.371,
};

const FOV_DEG = 50;
/** Wider at the two ends, where the subject is the sky rather than a star. */
const OPEN_FOV_DEG = 65;

/** Seconds a name takes to come up, and to go again. */
const FADE = 1.4;

/** Seconds the picture takes to come up from black, and to go back down. The
 *  way up runs most of the opening, so the stars arrive while the sky turns. */
const FADE_IN = 8;
const FADE_OUT = 5;

// The galactic north pole, in the equatorial cartesian frame the catalogue is
// stored in, which keeps the galactic plane level.
const NGP = normalize([-0.86703, -0.20006, 0.45673]);

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
  dwell: number;
  /** Seconds spent flying to the next hold. */
  transit: number;
  at(seconds: number): Motion;
  facing(seconds: number): Quaternion;
  /**
   * A function of time and not a constant, because the outro has no transition
   * in front of it to ease anything across. A hold that changes its framing has
   * to do it itself.
   */
  fov(seconds: number): number;
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
    dwell: SHOWCASE,
    transit: 0,
    at,
    facing: (seconds) => lookRotation(subtract(star, at(seconds).position), NGP),
    fov: () => FOV_DEG,
  };
}

/**
 * The opening: the camera stands still at the Sun and turns, and does nothing
 * else. No path, no target, no orbit — position is hoisted out, so it cannot
 * move even by accident.
 *
 * The axis is chosen rather than given, because two things have to come out
 * right at once. It sits TILT degrees below the direction the camera ends up
 * aimed, which puts the fixed point that far below the middle of the frame —
 * without it on screen there is nothing to see the turn against, and the whole
 * thing reads as a drift. And the turn is wound backwards from its end rather
 * than forwards from its start, so however long it runs and however fast, it
 * finishes pointing exactly where the tour sets off.
 */
function spinning(at: Vec3, towards: Vec3): Hold {
  const rest: Motion = { position: at, velocity: [0, 0, 0] };
  const forward = normalize(subtract(towards, at));
  const overhead = normalize(subtract(NGP, scale(forward, dot(NGP, forward))));
  const tilt = (TILT * Math.PI) / 180;
  const axis = subtract(scale(forward, Math.cos(tilt)), scale(overhead, Math.sin(tilt)));
  const arrived = lookRotation(forward, NGP);
  const rate = (SPIN_RATE * Math.PI) / 180;
  return {
    name: null,
    dwell: OPENING,
    transit: 0,
    at: () => rest,
    facing: (seconds) =>
      multiply(fromAxisAngle(axis, rate * (seconds - OPENING)), arrived),
    fov: () => OPEN_FOV_DEG,
  };
}

/**
 * The closing rush: no transition into it, just the throttle. It picks up the
 * last showcase exactly where that leaves off — same place, same velocity, same
 * orientation turning at the same rate — and accelerates, cubic in time, so
 * there is nothing to see happen at the join. The orbit it was on is still
 * turning it, so the swing settles into a straight stare rather than stopping
 * dead.
 *
 * Which way it runs is free, since the swing onto the heading costs nothing,
 * and it matters: the line the camera happens to be looking down leaves the
 * disc, and 800 pc along it the field has visibly thinned. Flattening that line
 * into the galactic plane costs 22 degrees of swing and buys a field that never
 * thins, so no edge of the catalogue is ever in frame.
 */
function rush(after: Hold, dwell: number): Hold {
  const leaving = after.at(dwell);
  const orientation = after.facing(dwell);
  const spin = angularVelocity(after.facing, dwell);
  const ahead = rotate(orientation, [0, 0, -1]);
  const heading = normalize(subtract(ahead, scale(NGP, dot(ahead, NGP))));
  const settled = lookRotation(heading, NGP);
  const framing = after.fov(dwell);
  return {
    name: null,
    dwell: RUSH,
    transit: 0,
    at(seconds) {
      const u = seconds / RUSH;
      return {
        position: add(
          add(leaving.position, scale(leaving.velocity, seconds)),
          scale(heading, RUSH_PC * u * u * u),
        ),
        velocity: add(leaving.velocity, scale(heading, (3 * RUSH_PC * u * u) / RUSH)),
      };
    },
    facing: (seconds) =>
      sweep(orientation, spin, settled, [0, 0, 0], RUSH, seconds / RUSH),
    // Opened out over the whole rush rather than at the start of it. Widening
    // by 15 degrees in one frame nearly doubles the sky on screen, and a rim of
    // stars appears all at once.
    fov: (seconds) =>
      framing + (OPEN_FOV_DEG - framing) * smootherstep(seconds / RUSH),
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

  const build = (i: number, way: number): Hold =>
    showcase(
      stars[i].name, stars[i].position,
      standoff(
        stars[i].position, i === 0 ? sun : stars[i - 1].position,
        STANDOFF_PC * Math.pow(LUMINOSITY[stars[i].name], COMPENSATION),
      ),
      way,
    );

  // Three parts under three rules: a spin that only turns, a tour that orbits
  // and flies between stars, and a rush that only accelerates. Transitions
  // join the first two and the stars to each other; the rush needs none,
  // because it starts from exactly where the last showcase leaves off.
  const all = [spinning(sun, stars[0].position), ...stars.map((_, i) => build(i, 1))];

  // An arc swings the camera as far as it drifts, so which way it goes decides
  // how much of the next turn is already done. Going the wrong way around adds
  // the whole arc back onto the transition.
  for (let i = 0; i < all.length - 1; i++) {
    const arrival = all[i + 1].facing(0);
    if (i > 0) {
      const turn = (way: number) => turnAngle(build(i - 1, way).facing(SHOWCASE), arrival);
      all[i] = build(i - 1, turn(-1) < turn(1) ? -1 : 1);
    }

    const travel = Math.hypot(
      ...subtract(all[i + 1].at(0).position, all[i].at(all[i].dwell).position),
    );
    all[i].transit = Math.max(
      MIN_TRANSIT,
      (turnAngle(all[i].facing(all[i].dwell), arrival) * 180) / Math.PI / TURN_RATE,
      travel / CRUISE_SPEED,
    );
  }

  all.push(rush(all[all.length - 1], SHOWCASE));
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
        fovDeg: hold.fov(elapsed),
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
      fovDeg: hold.fov(hold.dwell) +
        (next.fov(0) - hold.fov(hold.dwell)) * smootherstep(s),
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

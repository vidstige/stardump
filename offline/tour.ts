// A placeholder tour: the camera drifts a parsec toward the galactic centre,
// looking that way, for one second.
//
// Which stars, in what order, and how the camera moves between them is the
// film rather than the machinery, so the real one does not live in the repo.
// Replace this module with your own buildTour; everything around it — the
// frame renderer, the captions, the encoder and the rotation spline — is
// indifferent to what the camera does.

import { Camera } from "../core/camera";
import { lookRotation } from "../core/quaternion";
import { add, normalize, scale } from "../core/vec3";
import { Labels } from "./dataset";

const SECONDS = 1;
const SPEED_PC = 1;
const FOV_DEG = 50;

// Galactic north keeps the plane level; the centre is the richest thing to
// point at from anywhere near the Sun.
const NGP = normalize([-0.86703, -0.20006, 0.45673]);
const GALACTIC_CENTRE = normalize([-0.05487, -0.87344, -0.48384]);

/** The star being held, and how far its name has come up, 0 to 1. */
export type Focus = { name: string; emphasis: number };

export type Shot = { camera: Camera; fovDeg: number; focus: Focus | null };

export type Tour = {
  duration: number;
  shotAt(seconds: number): Shot;
  /** How much of the picture is up, 0 at the two ends. */
  fadeAt(seconds: number): number;
};

export function buildTour(labels: Labels): Tour {
  const orientation = lookRotation(GALACTIC_CENTRE, NGP);
  return {
    duration: SECONDS,
    fadeAt: () => 1,
    shotAt(seconds) {
      const position = add(labels["Sun"], scale(GALACTIC_CENTRE, SPEED_PC * seconds));
      return { camera: { position, orientation }, fovDeg: FOV_DEG, focus: null };
    },
  };
}

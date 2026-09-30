// Just the spin: the camera stands at the Sun and turns about one axis. No
// path, no target, no fade, nothing to hand over to. Its own entry point while
// the move is being got right.
//
// The axis itself is the one direction the turn leaves alone, so the point of
// sky it picks out is the only fixed thing in the picture and everything else
// wheels around it. That point has to be on screen or there is nothing to see
// the turn against: the camera aims along the axis, tilted up by TILT, which
// puts it that far below the middle of the frame.

import { Quaternion, fromAxisAngle, lookRotation, multiply } from "../core/quaternion";
import { DEFAULT_SETTINGS } from "../core/settings";
import { Vec3, add, normalize, scale, subtract } from "../core/vec3";
import * as args from "./args";
import { labels } from "./dataset";
import { openEncoder } from "./encode";
import { openFrames } from "./frame";
import { dataset, source } from "./source";

/** The celestial pole, which in this equatorial frame is simply z. */
const AXIS: Vec3 = [0, 0, 1];
/** Galactic north, which decides only which way up the first frame is. */
const UP: Vec3 = [-0.86703, -0.20006, 0.45673];
/** Degrees the camera aims above the axis, so the fixed point sits below middle. */
const TILT = 9;

const width = args.number("width", 1920);
const height = args.number("height", 1080);
const fps = args.number("fps", 30);
const seconds = args.number("seconds", 12);
/** Degrees a second. */
const rate = args.number("rate", 4);
const output = args.text("output", "renders/spin.mp4");

/**
 * Aiming along `axis`, tilted towards `up` by `tilt` degrees. The axis then
 * lands `tilt` degrees below the middle of the frame, which at a 65 degree
 * field of view is about a seventh of the frame height down.
 */
function aim(axis: Vec3, up: Vec3, tilt: number): Quaternion {
  const along = up[0] * axis[0] + up[1] * axis[1] + up[2] * axis[2];
  const side = normalize(subtract(up, scale(axis, along)));
  const angle = (tilt * Math.PI) / 180;
  return lookRotation(
    add(scale(axis, Math.cos(angle)), scale(side, Math.sin(angle))), side,
  );
}

const settings = {
  ...DEFAULT_SETTINGS,
  exposure: args.number("exposure", DEFAULT_SETTINGS.exposure),
  pixelThreshold: args.number("detail", DEFAULT_SETTINGS.pixelThreshold),
  pointBudget: args.number("budget", DEFAULT_SETTINGS.pointBudget),
  fovDeg: args.number("fov", 65),
};

async function main(): Promise<void> {
  const started = Date.now();
  const position = labels(dataset)["Sun"];
  const axis = normalize(args.vec3("axis", AXIS));
  const start = aim(axis, args.vec3("up", UP), args.number("tilt", TILT));
  const turn = (rate * Math.PI) / 180;

  const frames = await openFrames(await source(), width, height);
  const encoder = openEncoder(output, width, height, fps, {
    crf: args.number("crf", 12),
    preset: args.text("preset", "slow"),
    pixelFormat: args.text("pix-fmt", "yuv420p"),
  });

  const total = Math.round(seconds * fps);
  for (let f = 0; f < total; f++) {
    // One rigid turn of the whole camera about the axis. Aiming afresh each
    // frame would re-derive the roll from the up vector, which is a pan, not a
    // spin.
    const orientation: Quaternion = multiply(
      fromAxisAngle(axis, turn * (f / fps)), start,
    );
    const frame = await frames.render({ position, orientation }, settings);
    await encoder.write(frame.rgb);
  }
  await encoder.close();
  console.log(`${output}: ${total} frames in ${((Date.now() - started) / 1000).toFixed(1)}s`);
}

void main();

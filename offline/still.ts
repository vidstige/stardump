// One offline frame from a free camera, written to an image. Same octree,
// same level-of-detail cut, same shaders and same renderer as the viewer and
// as the video; all that differs is that it draws once and stops.

import { Camera } from "../core/camera";
import { lookRotation } from "../core/quaternion";
import { DEFAULT_SETTINGS } from "../core/settings";
import { Vec3, subtract } from "../core/vec3";
import * as args from "./args";
import { labels } from "./dataset";
import { openFrames } from "./frame";
import { writeImage } from "./image";
import { dataset, source } from "./source";

const width = args.number("width", 1920);
const height = args.number("height", 1080);
const output = args.text("output", "renders/still.png");

const settings = {
  ...DEFAULT_SETTINGS,
  exposure: args.number("exposure", DEFAULT_SETTINGS.exposure),
  sizeScale: args.number("size", DEFAULT_SETTINGS.sizeScale),
  maxRadius: args.number("size-cap", DEFAULT_SETTINGS.maxRadius),
  pixelThreshold: args.number("detail", DEFAULT_SETTINGS.pixelThreshold),
  pointBudget: args.number("budget", DEFAULT_SETTINGS.pointBudget),
  fovDeg: args.number("fov", DEFAULT_SETTINGS.fovDeg),
  far: args.number("far", DEFAULT_SETTINGS.far),
};

/** `--at <name>` aims the camera at a labelled star instead of `--dir`. */
function forward(eye: Vec3): Vec3 {
  const name = args.text("at", "");
  if (!name) return args.vec3("dir", [0, 0, -1]);
  return subtract(labels(dataset)[name], eye);
}

async function main(): Promise<void> {
  const started = Date.now();
  const eye = args.vec3("eye", [0, 0, 0]);
  const camera: Camera = {
    position: eye,
    orientation: lookRotation(forward(eye), args.vec3("up", [0, 0, 1])),
  };

  const frames = await openFrames(await source(), width, height);
  const frame = await frames.render(camera, settings);
  writeImage(output, width, height, frame.rgb);
  const elapsed = ((Date.now() - started) / 1000).toFixed(2);
  console.log(`${output}: ${frame.stars} stars, ${elapsed}s`);
}

void main();

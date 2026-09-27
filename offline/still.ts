// One offline frame from a free camera. The tour has its own entry points;
// this is for looking at a single view — trying a field of view, or putting
// the offline picture next to the viewer at the same camera.

import { Camera } from "../core/camera";
import { lookRotation } from "../core/quaternion";
import { DEFAULT_SETTINGS } from "../core/settings";
import { Vec3, subtract } from "../core/vec3";
import * as args from "./args";
import { firstDataset, labels, starcloudPath } from "./dataset";
import { openFrames } from "./frame";
import { writeImage } from "./image";

const dataset = args.text("dataset", firstDataset());
const width = args.number("width", 960);
const height = args.number("height", 540);
const output = args.text("output", "renders/still.png");

const settings = {
  ...DEFAULT_SETTINGS,
  exposure: args.number("exposure", DEFAULT_SETTINGS.exposure),
  sizeScale: args.number("size", DEFAULT_SETTINGS.sizeScale),
  maxRadius: args.number("radius", DEFAULT_SETTINGS.maxRadius),
  pixelThreshold: args.number("detail", DEFAULT_SETTINGS.pixelThreshold),
  pointBudget: args.number("budget", DEFAULT_SETTINGS.pointBudget),
  fovDeg: args.number("fov", DEFAULT_SETTINGS.fovDeg),
  far: args.number("far", DEFAULT_SETTINGS.far),
};

/** `--at <name>` aims the camera at a labelled star from where `--eye` is. */
function forward(eye: Vec3): Vec3 {
  const name = args.text("at", "");
  if (!name) return args.vec3("dir", [0, 0, -1]);
  const target = labels(dataset)[name];
  if (!target) throw new Error(`no label named ${name}`);
  return subtract(target, eye);
}

async function main(): Promise<void> {
  const started = Date.now();
  const eye = args.vec3("eye", [0, 0, 0]);
  const camera: Camera = {
    position: eye,
    orientation: lookRotation(forward(eye), args.vec3("up", [0, 0, 1])),
  };

  const frames = await openFrames(starcloudPath(dataset), width, height);
  const frame = await frames.render(camera, settings);
  writeImage(output, width, height, frame.rgb);
  const seconds = ((Date.now() - started) / 1000).toFixed(2);
  console.log(`${output}: ${frame.stars} stars, ${seconds}s`);
}

void main();

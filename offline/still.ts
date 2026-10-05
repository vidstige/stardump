// One offline frame from a free camera, written to an image. Same octree,
// same level-of-detail cut, same shaders and same renderer as the viewer and
// as the video; all that differs is that it draws once and stops.

import { Camera } from "../core/camera";
import { lookRotation } from "../core/quaternion";
import { Vec3, subtract } from "../core/vec3";
import * as args from "./args";
import { openFrames } from "./frame";
import { writeImage } from "./image";
import { Labels, openSource } from "./source";

const width = args.number("width", 1920);
const height = args.number("height", 1080);
const output = args.text("output", "renders/still.png");
const settings = args.settings();

/** `--at <name>` aims the camera at a labelled star instead of `--dir`. */
function forward(labels: Labels, eye: Vec3): Vec3 {
  const name = args.text("at", "");
  if (!name) return args.vec3("dir", [0, 0, -1]);
  return subtract(labels[name], eye);
}

async function main(): Promise<void> {
  const started = Date.now();
  const { read, labels } = await openSource();
  const eye = args.vec3("eye", [0, 0, 0]);
  const camera: Camera = {
    position: eye,
    orientation: lookRotation(forward(labels, eye), args.vec3("up", [0, 0, 1])),
  };

  const frames = await openFrames(read, width, height);
  const frame = await frames.render(camera, settings);
  writeImage(output, width, height, frame.rgb);
  const elapsed = ((Date.now() - started) / 1000).toFixed(2);
  console.log(`${output}: ${frame.stars} stars, ${elapsed}s`);
}

void main();

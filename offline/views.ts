// A fixed set of views, rendered into a directory. Render them before a change
// to anything that makes the picture and again after, and compare.ts says
// what the change did to it. The renderer is deterministic, so an unchanged
// picture compares to zero.
//
// Each view is there to exercise one thing: the densest sky and the sparsest,
// a star up close with its saturated disc, and a wide field of faint ones.

import { Camera } from "../core/camera";
import { lookRotation } from "../core/quaternion";
import { Vec3, add, scale, subtract } from "../core/vec3";
import * as args from "./args";
import { openFrames } from "./frame";
import { writeImage } from "./image";
import { Labels, openSource } from "./source";

const WIDTH = 960;
const HEIGHT = 540;

/** Galactic north pole and galactic centre, equatorial J2000 cartesian. */
const NGP: Vec3    = [-0.86703, -0.20006, 0.45673];
const CENTRE: Vec3 = [-0.05487, -0.87344, -0.48384];

/** How far the close-up stands off its star, as the tour's showcases do. */
const STANDOFF_PC = 0.3;

type View = { name: string; camera: Camera; fovDeg: number };

function views(labels: Labels): View[] {
  const sun: Vec3 = [0, 0, 0];
  const tauCeti = labels["Tau Ceti"];
  const overhead = add(tauCeti, scale(NGP, STANDOFF_PC));
  return [
    { name: "centre", fovDeg: 60,
      camera: { position: sun, orientation: lookRotation(CENTRE, NGP) } },
    { name: "pole", fovDeg: 60,
      camera: { position: sun, orientation: lookRotation(NGP, CENTRE) } },
    { name: "tau-ceti", fovDeg: 50,
      camera: { position: overhead, orientation: lookRotation(subtract(tauCeti, overhead), CENTRE) } },
    { name: "barnard", fovDeg: 35,
      camera: { position: [0, -1.1, 0.1],
                orientation: lookRotation(subtract(labels["Barnard's Star"], [0, -1.1, 0.1]), NGP) } },
  ];
}

async function main(): Promise<void> {
  const started = Date.now();
  const output = args.text("output", "renders/views");
  const settings = args.settings();
  const { read, labels } = await openSource();
  const frames = await openFrames(read, WIDTH, HEIGHT);
  for (const view of views(labels)) {
    settings.fovDeg = view.fovDeg;
    const frame = await frames.render(view.camera, settings);
    writeImage(`${output}/${view.name}.ppm`, WIDTH, HEIGHT, frame.rgb);
    console.log(`${view.name}: ${frame.stars} stars`);
  }
  console.log(`${output} in ${((Date.now() - started) / 1000).toFixed(1)}s`);
}

void main();

// Renders a contiguous run of tour frames into one H.264 segment.
//
// A run rather than a frame: starting Node, opening the index and compiling
// the shaders costs about a second, and consecutive frames want almost the
// same nodes, so the streaming cache pays for itself many times over within a
// single process. video.ts starts several of these side by side.

import { DEFAULT_SETTINGS } from "../core/settings";
import * as args from "./args";
import { composite, renderCaption } from "./caption";
import { labels } from "./dataset";
import { openEncoder } from "./encode";
import { openFrames } from "./frame";
import { dataset, source } from "./source";
import { buildTour } from "./tour";

const width = args.number("width", 1920);
const height = args.number("height", 1080);
const fps = args.number("fps", 30);
const output = args.text("output", "renders/tour.mp4");
const quality = {
  crf: args.number("crf", 12),
  preset: args.text("preset", "slow"),
  pixelFormat: args.text("pix-fmt", "yuv420p"),
};

const settings = {
  ...DEFAULT_SETTINGS,
  exposure: args.number("exposure", DEFAULT_SETTINGS.exposure),
  pixelThreshold: args.number("detail", DEFAULT_SETTINGS.pixelThreshold),
  pointBudget: args.number("budget", DEFAULT_SETTINGS.pointBudget),
};

async function main(): Promise<void> {
  const started = Date.now();
  const tour = buildTour(labels(dataset));
  const total = Math.round(tour.duration * fps);
  const first = args.number("from", 0);
  const last = Math.min(args.number("to", total), total);

  const frames = await openFrames(await source(), width, height);
  const encoder = openEncoder(output, width, height, fps, quality);
  const captions = new Map<string, Uint8Array>();
  const captionFor = (name: string) => {
    if (!captions.has(name)) captions.set(name, renderCaption(name, width, height));
    return captions.get(name)!;
  };

  const exposure = settings.exposure;
  for (let f = first; f < last; f++) {
    const shot = tour.shotAt(f / fps);
    // Fading on the exposure rather than on the finished frame: stars come out
    // of the black brightest first, the way they do at dusk, instead of the
    // whole picture being turned down at once.
    const up = tour.fadeAt(f / fps);
    settings.fovDeg = shot.fovDeg;
    settings.exposure = exposure * up;
    const frame = await frames.render(shot.camera, settings);
    if (shot.focus) {
      composite(frame.rgb, captionFor(shot.focus.name), shot.focus.emphasis * up);
    }
    await encoder.write(frame.rgb);
    if ((f - first) % 60 === 0) {
      const rate = (f - first + 1) / ((Date.now() - started) / 1000);
      process.stderr.write(
        `${output} ${f - first + 1}/${last - first} ${rate.toFixed(2)} fps ` +
        `${(frame.stars / 1e6).toFixed(1)}M stars\n`,
      );
    }
  }
  await encoder.close();
  const seconds = (Date.now() - started) / 1000;
  console.log(`${output}: ${last - first} frames in ${seconds.toFixed(1)}s`);
}

void main();

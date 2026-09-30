// Renders the whole tour by splitting its frames across several processes,
// then joins the segments without re-encoding.

import { spawn, spawnSync } from "child_process";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

import * as args from "./args";
import { labels } from "./dataset";
import { dataset } from "./source";
import { buildTour } from "./tour";

// A sketch is for judging the moves, so it goes small and coarse. That costs
// it two separate stops of light and it carries its own exposure to put them
// back: a ninth of the pixels means nine times as many stars land in each, and
// drawing a third of the stars at a coarser threshold takes some of that away
// again.
const SKETCH = {
  width: 640, height: 360, budget: 5_000_000, detail: 22, crf: 25, exposure: 330,
  preset: "medium",
};

// Above the largest cut a 1080p frame asks for, measured at 53.9M looking into
// the galactic centre, so the budget never truncates one: every node the level
// of detail selects is waited for and drawn.
const FULL_BUDGET = 64_000_000;

const sketch = args.flag("sketch");
const width = args.number("width", sketch ? SKETCH.width : 1920);
const height = args.number("height", sketch ? SKETCH.height : 1080);
const budget = args.number("budget", sketch ? SKETCH.budget : FULL_BUDGET);
const detail = args.number("detail", sketch ? SKETCH.detail : 16);
const crf = args.number("crf", sketch ? SKETCH.crf : 12);
const preset = args.text("preset", sketch ? SKETCH.preset : "slow");
const pixelFormat = args.text("pix-fmt", "yuv420p");
const exposure = args.number("exposure", sketch ? SKETCH.exposure : 0);
const url = args.text("url", "");
const fps = args.number("fps", 30);
// One at full size. A 1080p cut runs to 50M points, a gigabyte of vertex
// buffers, and a second job alongside it put this 8 GB machine into swap hard
// enough to stall both. Sketch frames are a ninth the size and parallelise.
const jobs = args.number("jobs", sketch ? 3 : 1);

/**
 * Frames per process, at full size. A render slows to a crawl on a machine this
 * size: a frame streams most of a gigabyte out of a 4 GiB index, and however
 * little the process itself holds, something accumulates until everything
 * crawls. The same frames that run at 1.1 a second from a fresh process were
 * taking 14 s each by frame 121 of a long one. Ending the process often hands
 * it all back, and the measured safe life is under a hundred frames. A sketch
 * touches a fraction of the index and just splits the film one way per job.
 */
const SEGMENT_FRAMES = 60;
const output = args.text("output", sketch ? "renders/tour-sketch.mp4" : "renders/tour.mp4");

/** Contiguous frame ranges of at most `size` frames each. */
function ranges(total: number, size: number): [number, number][] {
  const out: [number, number][] = [];
  for (let at = 0; at < total; at += size) out.push([at, Math.min(at + size, total)]);
  return out;
}

/** Runs `work` with at most `width` of them going at once, in order. */
async function pool<T>(work: (() => Promise<T>)[], width: number): Promise<void> {
  const queue = [...work];
  const run = async (): Promise<void> => {
    for (let next = queue.shift(); next; next = queue.shift()) await next();
  };
  await Promise.all(Array.from({ length: Math.min(width, work.length) }, run));
}

function renderSegment(segment: string, [first, last]: [number, number]): Promise<void> {
  const child = spawn("npx", [
    "tsx", path.join(__dirname, "render.ts"),
    "--dataset", dataset,
    "--width", String(width), "--height", String(height),
    "--fps", String(fps), "--budget", String(budget), "--detail", String(detail),
    "--crf", String(crf), "--preset", preset, "--pix-fmt", pixelFormat,
    ...(exposure > 0 ? ["--exposure", String(exposure)] : []),
    ...(url ? ["--url", url] : []),
    "--from", String(first), "--to", String(last),
    "--output", segment,
  ], { stdio: ["ignore", "inherit", "inherit"] });
  return new Promise((resolve, reject) => {
    child.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`frames ${first}-${last} failed with ${code}`));
    });
  });
}

function join(segments: string[]): void {
  const list = path.join(os.tmpdir(), `stardump-segments-${process.pid}.txt`);
  // The concat demuxer resolves relative paths against the list file.
  fs.writeFileSync(list, segments.map((s) => `file '${path.resolve(s)}'\n`).join(""));
  const ffmpeg = spawnSync("ffmpeg", [
    "-y", "-loglevel", "error", "-f", "concat", "-safe", "0",
    "-i", list, "-c", "copy", output,
  ], { stdio: "inherit" });
  fs.unlinkSync(list);
  if (ffmpeg.status !== 0) throw new Error("joining the segments failed");
}

async function main(): Promise<void> {
  const started = Date.now();
  const tour = buildTour(labels(dataset));
  const total = Math.round(tour.duration * fps);
  const size = args.number(
    "segment", sketch ? Math.ceil(total / jobs) : SEGMENT_FRAMES,
  );
  const work = ranges(total, size);
  console.log(
    `${output}: ${tour.duration.toFixed(1)}s, ${total} frames at ${width}x${height}, ` +
    `${work.length} segments, ${jobs} at a time`,
  );

  fs.mkdirSync(path.dirname(output), { recursive: true });
  const segments = work.map((_, i) => `${output.replace(/\.mp4$/, "")}.part${i}.mp4`);
  await pool(work.map((range, i) => () => renderSegment(segments[i], range)), jobs);
  join(segments);
  for (const segment of segments) fs.unlinkSync(segment);
  console.log(`${output} in ${((Date.now() - started) / 1000 / 60).toFixed(1)} min`);
}

void main();

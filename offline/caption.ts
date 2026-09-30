// Captions, drawn by ffmpeg. It has to be here for the video anyway and it
// brings a real typeface with it, which beats carrying a bitmap font.
//
// A caption comes back as a frame sized white-on-black image and is added to
// the picture rather than blended over it, so no alpha channel has to survive
// the round trip. The star field is black where the text is not.

import { spawnSync } from "child_process";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

const FONT = "Helvetica";
/** Type size and baseline as fractions of the frame height. */
const SIZE = 1 / 22;
const BASELINE = 0.62;
const COLOR = "0xEBF3FF";

/** Filter arguments are colon separated, so a path has to be escaped. */
function escape(text: string): string {
  return text.replace(/\\/g, "\\\\").replace(/:/g, "\\:").replace(/'/g, "\\'");
}

/** Text goes through a file, which spares us escaping the text itself. */
function textFile(text: string): string {
  const file = path.join(os.tmpdir(), `stardump-caption-${process.pid}.txt`);
  fs.writeFileSync(file, text);
  return file;
}

export function renderCaption(name: string, width: number, height: number): Uint8Array {
  const filter = [
    `drawtext=font=${FONT}`,
    `textfile=${escape(textFile(name))}`,
    `fontsize=${Math.round(height * SIZE)}`,
    `fontcolor=${COLOR}`,
    "x=(w-text_w)/2",
    `y=${Math.round(height * BASELINE)}`,
  ].join(":");
  const ffmpeg = spawnSync("ffmpeg", [
    "-loglevel", "error",
    "-f", "lavfi", "-i", `color=c=black:s=${width}x${height}`,
    "-vf", filter, "-frames:v", "1",
    "-f", "rawvideo", "-pix_fmt", "rgb24", "-",
  ], { maxBuffer: width * height * 4 });
  if (ffmpeg.status !== 0) throw new Error(`caption: ${ffmpeg.stderr}`);
  return new Uint8Array(ffmpeg.stdout);
}

/** Added, not blended, so the name reads as light rather than as paint. */
export function composite(rgb: Uint8Array, caption: Uint8Array, strength: number): void {
  for (let i = 0; i < rgb.length; i++) {
    rgb[i] = Math.min(255, rgb[i] + caption[i] * strength);
  }
}

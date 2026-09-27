// Writing a finished frame out as a file. P6 is written directly because
// render-check/compare.ts reads it; anything else goes through ffmpeg, which
// the video encoding needs anyway.

import { spawnSync } from "child_process";
import * as fs from "fs";

function bytes(rgb: Uint8Array): Buffer {
  return Buffer.from(rgb.buffer, rgb.byteOffset, rgb.length);
}

export function writeImage(path: string, width: number, height: number, rgb: Uint8Array): void {
  if (path.endsWith(".ppm")) {
    const header = Buffer.from(`P6\n${width} ${height}\n255\n`, "ascii");
    fs.writeFileSync(path, Buffer.concat([header, bytes(rgb)]));
    return;
  }
  const ffmpeg = spawnSync("ffmpeg", [
    "-y", "-loglevel", "error",
    "-f", "rawvideo", "-pixel_format", "rgb24", "-video_size", `${width}x${height}`,
    "-i", "-", path,
  ], { input: bytes(rgb) });
  if (ffmpeg.status !== 0) throw new Error(`ffmpeg: ${ffmpeg.stderr}`);
}

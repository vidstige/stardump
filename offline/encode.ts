// Raw frames into an H.264 file. Segments are encoded identically so that the
// whole film can be joined from them without re-encoding.
//
// A star field is nearly all fine detail, so it is expensive to encode and it
// compresses badly. A sketch is worth a coarse quantiser and a fast preset,
// since there the encoder would otherwise hold up the renderer; a final render
// goes at 2 frames a second and can afford whatever the encoder wants.
//
// Chroma subsampling is the setting that matters here and it is not the
// obvious one. A star is one pixel and its colour is the whole of what it
// carries, so 4:2:0 — which averages colour over 2x2 — costs real fidelity,
// while the chroma planes of a mostly black frame are almost free to code at
// full resolution. Measured over 60 frames of the rush against the raw
// renderer output, 4:4:4 came out both smaller and better than 4:2:0 at the
// same quantiser, twice: 27.2 MB at 45.09 dB against 27.4 MB at 44.24 dB. It
// does not play everywhere, though, which is why the default stays 4:2:0.

import { spawn } from "child_process";

export type Encoder = {
  write(rgb: Uint8Array): Promise<void>;
  close(): Promise<void>;
};

export type Quality = { crf: number; preset: string; pixelFormat: string };

export function openEncoder(
  output: string, width: number, height: number, fps: number, quality: Quality,
): Encoder {
  const ffmpeg = spawn("ffmpeg", [
    "-y", "-loglevel", "error",
    "-f", "rawvideo", "-pix_fmt", "rgb24",
    "-video_size", `${width}x${height}`, "-framerate", String(fps),
    "-i", "-",
    "-c:v", "libx264",
    "-preset", quality.preset, "-crf", String(quality.crf),
    "-pix_fmt", quality.pixelFormat,
    "-movflags", "+faststart",
    output,
  ], { stdio: ["pipe", "inherit", "inherit"] });

  return {
    write(rgb) {
      const buffer = Buffer.from(rgb.buffer, rgb.byteOffset, rgb.length);
      return new Promise((resolve) => {
        if (ffmpeg.stdin.write(buffer)) resolve();
        else ffmpeg.stdin.once("drain", () => resolve());
      });
    },
    close() {
      return new Promise((resolve, reject) => {
        ffmpeg.on("close", (code) => {
          if (code === 0) resolve();
          else reject(new Error(`ffmpeg exited ${code}`));
        });
        ffmpeg.stdin.end();
      });
    },
  };
}

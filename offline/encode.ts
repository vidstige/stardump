// Raw frames into an H.264 file. Segments are encoded identically so that the
// whole film can be joined from them without re-encoding.
//
// A star field is nearly all fine detail, so it is expensive to encode and it
// compresses badly: at preset slow the encoder, not the renderer, was what
// held up every job, and a three minute sketch came to 127 MB. Medium keeps
// the renderer in front, and a sketch is worth a coarser quantiser.

import { spawn } from "child_process";

export type Encoder = {
  write(rgb: Uint8Array): Promise<void>;
  close(): Promise<void>;
};

export function openEncoder(
  output: string, width: number, height: number, fps: number, crf: number,
): Encoder {
  const ffmpeg = spawn("ffmpeg", [
    "-y", "-loglevel", "error",
    "-f", "rawvideo", "-pix_fmt", "rgb24",
    "-video_size", `${width}x${height}`, "-framerate", String(fps),
    "-i", "-",
    "-c:v", "libx264", "-preset", "medium", "-crf", String(crf), "-pix_fmt", "yuv420p",
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

// Compares two directories of P6 renders of the same views, before and after
// a change, and says what the change did to the picture. Next to each after
// image it writes the difference, amplified, so the eye can find where.
//
// Nothing here passes or fails. Whether a difference is wanted is for whoever
// made the change to say; this gives them the numbers to say it with.

import * as fs from "fs";
import * as path from "path";

const TILE = 64;
/** Tiles whose RMSE is above this are counted as changed. */
const CHANGED = 0.05;
/** A pixel counts as bright when any channel is above this. */
const BRIGHT = 127;
/** The difference image is multiplied by this, so small changes show. */
const GAIN = 4;

type Image = { width: number; height: number; data: Uint8Array };

type Tile = { x: number; y: number; rmse: number };

function readPpm(file: string): Image {
  const buf = fs.readFileSync(file);
  const header = /^P6\s+(\d+)\s+(\d+)\s+255\s/.exec(buf.subarray(0, 32).toString("ascii"));
  if (!header) throw new Error(`not an 8-bit P6: ${file}`);
  return { width: Number(header[1]), height: Number(header[2]), data: buf.subarray(header[0].length) };
}

function writePpm(file: string, image: Image): void {
  const header = Buffer.from(`P6\n${image.width} ${image.height}\n255\n`, "ascii");
  fs.writeFileSync(file, Buffer.concat([header, image.data]));
}

function flux(image: Image): number {
  let sum = 0;
  for (const value of image.data) sum += value;
  return sum;
}

function brightPixels(image: Image): number {
  const { data } = image;
  let n = 0;
  for (let i = 0; i < data.length; i += 3) {
    if (data[i] > BRIGHT || data[i + 1] > BRIGHT || data[i + 2] > BRIGHT) n++;
  }
  return n;
}

function tiles(a: Image, b: Image): Tile[] {
  const out: Tile[] = [];
  for (let ty = 0; ty < a.height; ty += TILE) {
    for (let tx = 0; tx < a.width; tx += TILE) {
      let sum = 0;
      let count = 0;
      for (let y = ty; y < Math.min(ty + TILE, a.height); y++) {
        for (let x = tx; x < Math.min(tx + TILE, a.width); x++) {
          for (let i = (y * a.width + x) * 3; i < (y * a.width + x) * 3 + 3; i++) {
            const d = (a.data[i] - b.data[i]) / 255;
            sum += d * d;
            count++;
          }
        }
      }
      out.push({ x: tx, y: ty, rmse: Math.sqrt(sum / count) });
    }
  }
  return out.sort((p, q) => q.rmse - p.rmse);
}

function difference(a: Image, b: Image): Image {
  const data = new Uint8Array(a.data.length);
  for (let i = 0; i < data.length; i++) {
    data[i] = Math.min(255, Math.abs(a.data[i] - b.data[i]) * GAIN);
  }
  return { width: a.width, height: a.height, data };
}

function compare(name: string, before: Image, after: Image, diffFile: string): void {
  if (before.width !== after.width || before.height !== after.height) {
    throw new Error(`${name}: ${before.width}x${before.height} before, ${after.width}x${after.height} after`);
  }
  const ranked = tiles(before, after);
  const changed = ranked.filter((tile) => tile.rmse > CHANGED).length;
  const worst = ranked[0];
  writePpm(diffFile, difference(before, after));
  console.log(
    `${name}: flux x${(flux(after) / flux(before)).toFixed(4)}, ` +
    `bright pixels x${(brightPixels(after) / brightPixels(before)).toFixed(4)}, ` +
    `${changed}/${ranked.length} tiles changed, ` +
    `worst rmse ${worst.rmse.toFixed(4)} at (${worst.x},${worst.y})`,
  );
}

function main(): void {
  const [beforeDir, afterDir] = process.argv.slice(2);
  if (!afterDir) {
    console.error("usage: compare.ts <before dir> <after dir>");
    process.exit(1);
  }
  const names = fs.readdirSync(beforeDir).filter((name) => name.endsWith(".ppm")).sort();
  for (const name of names) {
    const before = readPpm(path.join(beforeDir, name));
    const after = readPpm(path.join(afterDir, name));
    compare(name, before, after, path.join(afterDir, name.replace(/\.ppm$/, ".diff.ppm")));
  }
}

main();

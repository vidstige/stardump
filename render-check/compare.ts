import * as fs from "fs";

// Compare two PPM P6 images produced by render-exact.ts and render-fast.ts.
// Metrics:
//   - flux ratio: sum(fast) / sum(exact) over all channels (target 0.9–1.1)
//   - bright-pixel count: pixels where any channel > threshold (target within 5%)
//   - per-tile RMSE over 64×64 tiles (target ≥80% of tiles under 0.05; report worst 5)
//   - difference PPM written alongside (abs diff per channel, clamped)

type Image = { width: number; height: number; data: Uint8Array };

function readPpm(path: string): Image {
  const buf = fs.readFileSync(path);
  let offset = 0;
  function readToken(): string {
    while (offset < buf.length && /\s/.test(String.fromCharCode(buf[offset]))) offset++;
    const start = offset;
    while (offset < buf.length && !/\s/.test(String.fromCharCode(buf[offset]))) offset++;
    return buf.subarray(start, offset).toString("ascii");
  }
  const magic = readToken();
  if (magic !== "P6") throw new Error(`not a P6 PPM: ${path}`);
  const width = parseInt(readToken(), 10);
  const height = parseInt(readToken(), 10);
  const maxval = parseInt(readToken(), 10);
  if (maxval !== 255) throw new Error(`unsupported maxval ${maxval} in ${path}`);
  offset++; // single whitespace byte after maxval
  const expected = width * height * 3;
  if (buf.length - offset !== expected) {
    throw new Error(`truncated PPM ${path}: ${buf.length - offset} != ${expected}`);
  }
  return { width, height, data: buf.subarray(offset, offset + expected) };
}

function writePpm(path: string, img: Image): void {
  const header = Buffer.from(`P6\n${img.width} ${img.height}\n255\n`, "ascii");
  fs.writeFileSync(path, Buffer.concat([header, img.data]));
}

function fluxSum(img: Image): number {
  let s = 0;
  for (let i = 0; i < img.data.length; i++) s += img.data[i];
  return s;
}

function brightPixelCount(img: Image, threshold: number): number {
  let n = 0;
  for (let i = 0; i < img.data.length; i += 3) {
    if (img.data[i] > threshold || img.data[i + 1] > threshold || img.data[i + 2] > threshold) n++;
  }
  return n;
}

function tileRmse(a: Image, b: Image, tile: number): { x: number; y: number; rmse: number }[] {
  const results: { x: number; y: number; rmse: number }[] = [];
  for (let ty = 0; ty < a.height; ty += tile) {
    for (let tx = 0; tx < a.width; tx += tile) {
      let sum = 0;
      let count = 0;
      const xEnd = Math.min(tx + tile, a.width);
      const yEnd = Math.min(ty + tile, a.height);
      for (let y = ty; y < yEnd; y++) {
        for (let x = tx; x < xEnd; x++) {
          const i = (y * a.width + x) * 3;
          for (let c = 0; c < 3; c++) {
            const d = (a.data[i + c] - b.data[i + c]) / 255;
            sum += d * d;
            count++;
          }
        }
      }
      results.push({ x: tx, y: ty, rmse: Math.sqrt(sum / count) });
    }
  }
  return results;
}

function diffImage(a: Image, b: Image): Image {
  const data = new Uint8Array(a.data.length);
  for (let i = 0; i < a.data.length; i++) {
    const d = Math.abs(a.data[i] - b.data[i]);
    data[i] = Math.min(255, d * 4);
  }
  return { width: a.width, height: a.height, data };
}

function comparePair(exactPath: string, fastPath: string, diffPath: string): void {
  const exact = readPpm(exactPath);
  const fast = readPpm(fastPath);
  if (exact.width !== fast.width || exact.height !== fast.height) {
    throw new Error(`size mismatch: ${exactPath} ${exact.width}×${exact.height} vs ${fastPath} ${fast.width}×${fast.height}`);
  }

  const exactFlux = fluxSum(exact);
  const fastFlux = fluxSum(fast);
  const fluxRatio = fastFlux / Math.max(exactFlux, 1);
  const exactBright = brightPixelCount(exact, 127);
  const fastBright = brightPixelCount(fast, 127);
  const brightRatio = fastBright / Math.max(exactBright, 1);

  const tiles = tileRmse(exact, fast, 64);
  tiles.sort((a, b) => b.rmse - a.rmse);
  const passing = tiles.filter(t => t.rmse < 0.05).length;
  const passingFrac = passing / tiles.length;

  writePpm(diffPath, diffImage(exact, fast));

  console.log(`\n=== ${exactPath}  vs  ${fastPath} ===`);
  console.log(`  flux:    exact=${exactFlux.toExponential(3)}  fast=${fastFlux.toExponential(3)}  ratio=${fluxRatio.toFixed(3)}  ${fluxRatio >= 0.9 && fluxRatio <= 1.1 ? "PASS" : "FAIL"}`);
  console.log(`  bright:  exact=${exactBright}  fast=${fastBright}  ratio=${brightRatio.toFixed(3)}  ${Math.abs(brightRatio - 1) <= 0.05 ? "PASS" : "FAIL"}`);
  console.log(`  tiles:   ${passing}/${tiles.length} under RMSE 0.05  (${(passingFrac * 100).toFixed(1)}%)  ${passingFrac >= 0.8 ? "PASS" : "FAIL"}`);
  console.log(`  worst 5 tiles:`);
  for (const t of tiles.slice(0, 5)) {
    console.log(`    (${t.x},${t.y})  rmse=${t.rmse.toFixed(4)}`);
  }
  console.log(`  diff:    ${diffPath}`);
}

function main(): void {
  const argv = process.argv.slice(2);
  if (argv.length < 2 || argv.length % 2 !== 0) {
    console.error("usage: compare.ts <exact-a.ppm> <fast-a.ppm> [<exact-b.ppm> <fast-b.ppm> ...]");
    process.exit(1);
  }
  for (let i = 0; i < argv.length; i += 2) {
    const exactPath = argv[i];
    const fastPath = argv[i + 1];
    const diffPath = fastPath.replace(/\.ppm$/, "") + ".diff.ppm";
    comparePair(exactPath, fastPath, diffPath);
  }
}

main();

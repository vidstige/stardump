"use strict";
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));

// compare.ts
var fs = __toESM(require("fs"));
function readPpm(path) {
  const buf = fs.readFileSync(path);
  let offset = 0;
  function readToken() {
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
  offset++;
  const expected = width * height * 3;
  if (buf.length - offset !== expected) {
    throw new Error(`truncated PPM ${path}: ${buf.length - offset} != ${expected}`);
  }
  return { width, height, data: buf.subarray(offset, offset + expected) };
}
function writePpm(path, img) {
  const header = Buffer.from(`P6
${img.width} ${img.height}
255
`, "ascii");
  fs.writeFileSync(path, Buffer.concat([header, img.data]));
}
function fluxSum(img) {
  let s = 0;
  for (let i = 0; i < img.data.length; i++) s += img.data[i];
  return s;
}
function brightPixelCount(img, threshold) {
  let n = 0;
  for (let i = 0; i < img.data.length; i += 3) {
    if (img.data[i] > threshold || img.data[i + 1] > threshold || img.data[i + 2] > threshold) n++;
  }
  return n;
}
function tileRmse(a, b, tile) {
  const results = [];
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
function diffImage(a, b) {
  const data = new Uint8Array(a.data.length);
  for (let i = 0; i < a.data.length; i++) {
    const d = Math.abs(a.data[i] - b.data[i]);
    data[i] = Math.min(255, d * 4);
  }
  return { width: a.width, height: a.height, data };
}
function comparePair(exactPath, fastPath, diffPath) {
  const exact = readPpm(exactPath);
  const fast = readPpm(fastPath);
  if (exact.width !== fast.width || exact.height !== fast.height) {
    throw new Error(`size mismatch: ${exactPath} ${exact.width}\xD7${exact.height} vs ${fastPath} ${fast.width}\xD7${fast.height}`);
  }
  const exactFlux = fluxSum(exact);
  const fastFlux = fluxSum(fast);
  const fluxRatio = fastFlux / Math.max(exactFlux, 1);
  const exactBright = brightPixelCount(exact, 127);
  const fastBright = brightPixelCount(fast, 127);
  const brightRatio = fastBright / Math.max(exactBright, 1);
  const tiles = tileRmse(exact, fast, 64);
  tiles.sort((a, b) => b.rmse - a.rmse);
  const passing = tiles.filter((t) => t.rmse < 0.05).length;
  const passingFrac = passing / tiles.length;
  writePpm(diffPath, diffImage(exact, fast));
  console.log(`
=== ${exactPath}  vs  ${fastPath} ===`);
  console.log(`  flux:    exact=${exactFlux.toExponential(3)}  fast=${fastFlux.toExponential(3)}  ratio=${fluxRatio.toFixed(3)}  ${fluxRatio >= 0.9 && fluxRatio <= 1.1 ? "PASS" : "FAIL"}`);
  console.log(`  bright:  exact=${exactBright}  fast=${fastBright}  ratio=${brightRatio.toFixed(3)}  ${Math.abs(brightRatio - 1) <= 0.05 ? "PASS" : "FAIL"}`);
  console.log(`  tiles:   ${passing}/${tiles.length} under RMSE 0.05  (${(passingFrac * 100).toFixed(1)}%)  ${passingFrac >= 0.8 ? "PASS" : "FAIL"}`);
  console.log(`  worst 5 tiles:`);
  for (const t of tiles.slice(0, 5)) {
    console.log(`    (${t.x},${t.y})  rmse=${t.rmse.toFixed(4)}`);
  }
  console.log(`  diff:    ${diffPath}`);
}
function main() {
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

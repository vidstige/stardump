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

// render-exact.ts
var https = __toESM(require("https"));
var http = __toESM(require("http"));
var fs = __toESM(require("fs"));
var args = process.argv.slice(2);
function getArg(name, def) {
  const i = args.indexOf("--" + name);
  if (i !== -1) return args[i + 1];
  if (def !== void 0) return def;
  throw new Error(`missing --${name}`);
}
function getArgNum(name, def) {
  const i = args.indexOf("--" + name);
  if (i !== -1) return parseFloat(args[i + 1]);
  if (def !== void 0) return def;
  throw new Error(`missing --${name}`);
}
var API_ROOT = getArg("url");
var DATASET = getArg("dataset");
var eyeStr = getArg("eye", "0,0,0").split(",").map(Number);
var dirStr = getArg("dir", "0,0,-1").split(",").map(Number);
var upStr = getArg("up", "0,1,0").split(",").map(Number);
var FOV_DEG = getArgNum("fov", 60);
var DEPTH = getArgNum("depth", 5e3);
var NEAR = getArgNum("near", 0.1);
var WIDTH = getArgNum("width", 1920);
var HEIGHT = getArgNum("height", 1080);
var EXPOSURE = getArgNum("exposure", 1);
var LIMIT_MAG = getArgNum("limit-mag", 20);
var SAT_MAG = getArgNum("sat-mag", 4);
var OUT = getArg("output", "stars.ppm");
function normalize(v) {
  const l = Math.hypot(...v) || 1;
  return [v[0] / l, v[1] / l, v[2] / l];
}
function cross(a, b) {
  return [a[1] * b[2] - a[2] * b[1], a[2] * b[0] - a[0] * b[2], a[0] * b[1] - a[1] * b[0]];
}
function dot(a, b) {
  return a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
}
var forward = normalize(dirStr);
var right = normalize(cross(forward, upStr));
var up = cross(right, forward);
var fovy = FOV_DEG * Math.PI / 180;
var aspect = WIDTH / HEIGHT;
var tanH = Math.tan(fovy * 0.5);
function matToQuat(r, u, f) {
  const m00 = r[0], m10 = r[1], m20 = r[2];
  const m01 = u[0], m11 = u[1], m21 = u[2];
  const m02 = -f[0], m12 = -f[1], m22 = -f[2];
  const trace = m00 + m11 + m22;
  if (trace > 0) {
    const s2 = 0.5 / Math.sqrt(trace + 1);
    return [(m21 - m12) * s2, (m02 - m20) * s2, (m10 - m01) * s2, 0.25 / s2];
  }
  if (m00 > m11 && m00 > m22) {
    const s2 = 2 * Math.sqrt(1 + m00 - m11 - m22);
    return [0.25 * s2, (m01 + m10) / s2, (m02 + m20) / s2, (m21 - m12) / s2];
  }
  if (m11 > m22) {
    const s2 = 2 * Math.sqrt(1 + m11 - m00 - m22);
    return [(m01 + m10) / s2, 0.25 * s2, (m12 + m21) / s2, (m02 - m20) / s2];
  }
  const s = 2 * Math.sqrt(1 + m22 - m00 - m11);
  return [(m02 + m20) / s, (m12 + m21) / s, 0.25 * s, (m10 - m01) / s];
}
var [qx, qy, qz, qw] = matToQuat(right, up, forward);
function fetchStars() {
  const params = new URLSearchParams({
    x: String(eyeStr[0]),
    y: String(eyeStr[1]),
    z: String(eyeStr[2]),
    qx: String(qx),
    qy: String(qy),
    qz: String(qz),
    qw: String(qw),
    near: String(NEAR),
    far: String(DEPTH),
    fovy: String(fovy),
    aspect: String(aspect),
    // The /frustum endpoint defaults to DEFAULT_LIMIT=1000; pass a huge limit
    // to effectively disable truncation without changing the server.
    limit: "10000000"
  });
  const url = `${API_ROOT}/query/${DATASET}/frustum?${params}`;
  console.log("Querying:", url);
  return new Promise((resolve, reject) => {
    const lib = url.startsWith("https") ? https : http;
    lib.get(url, (res) => {
      const stars = [];
      let tail = "";
      let firstLine = true;
      res.on("data", (chunk) => {
        const text = tail + chunk.toString("utf8");
        const lines = text.split("\n");
        tail = lines.pop();
        for (const line of lines) {
          if (!line) continue;
          if (firstLine) {
            firstLine = false;
            continue;
          }
          const c = line.split(",");
          stars.push({
            x: parseFloat(c[0]),
            y: parseFloat(c[1]),
            z: parseFloat(c[2]),
            lum: parseFloat(c[4]),
            bprp: parseFloat(c[5])
          });
        }
      });
      res.on("end", () => {
        if (tail && !firstLine) {
          const c = tail.split(",");
          if (c.length >= 6) stars.push({ x: parseFloat(c[0]), y: parseFloat(c[1]), z: parseFloat(c[2]), lum: parseFloat(c[4]), bprp: parseFloat(c[5]) });
        }
        resolve(stars);
      });
      res.on("error", reject);
    }).on("error", reject);
  });
}
function project(px, py, pz) {
  const rx = px - eyeStr[0], ry = py - eyeStr[1], rz = pz - eyeStr[2];
  const depth = dot([rx, ry, rz], forward);
  if (depth <= 0) return null;
  const h = dot([rx, ry, rz], right);
  const v = dot([rx, ry, rz], up);
  const sx = (h / (depth * tanH * aspect) * 0.5 + 0.5) * WIDTH;
  const sy = (1 - (v / (depth * tanH) * 0.5 + 0.5)) * HEIGHT;
  return [sx, sy, depth];
}
function bprpToColor(bprp) {
  if (!isFinite(bprp)) return [1, 1, 1];
  const t = Math.max(0, Math.min(1, (bprp + 0.5) / 3.5));
  const lerp = (a, b, t2) => [a[0] + (b[0] - a[0]) * t2, a[1] + (b[1] - a[1]) * t2, a[2] + (b[2] - a[2]) * t2];
  if (t < 0.33) return lerp([0.6, 0.7, 1], [1, 0.95, 0.9], t / 0.33);
  if (t < 0.66) return lerp([1, 0.95, 0.9], [1, 0.85, 0.4], (t - 0.33) / 0.33);
  return lerp([1, 0.85, 0.4], [1, 0.3, 0.1], (t - 0.66) / 0.34);
}
function rasterize(stars, hdr) {
  const magSpan = LIMIT_MAG - SAT_MAG;
  for (const s of stars) {
    if (!(s.lum > 0)) continue;
    const proj = project(s.x, s.y, s.z);
    if (!proj) continue;
    const [sx, sy, dist] = proj;
    const flux = s.lum / Math.max(dist * dist, 0.01);
    const mag = -2.5 * Math.log10(flux);
    const t = (LIMIT_MAG - mag) / magSpan;
    if (t <= 0) continue;
    const brightness = t * EXPOSURE;
    const [cr, cg, cb] = bprpToColor(s.bprp);
    const rPx = Math.min(Math.max(brightness * 2, 0.8), 8);
    const ir = Math.ceil(rPx);
    for (let dy = -ir; dy <= ir; dy++) {
      for (let dx = -ir; dx <= ir; dx++) {
        const xi = Math.round(sx) + dx, yi = Math.round(sy) + dy;
        if (xi < 0 || xi >= WIDTH || yi < 0 || yi >= HEIGHT) continue;
        const nr = Math.sqrt(dx * dx + dy * dy) / rPx;
        const val = brightness * Math.exp(-nr * nr * 4);
        const idx = (yi * WIDTH + xi) * 3;
        hdr[idx] += cr * val;
        hdr[idx + 1] += cg * val;
        hdr[idx + 2] += cb * val;
      }
    }
  }
}
async function main() {
  const stars = await fetchStars();
  console.log(`Got ${stars.length} exact stars (depth=${DEPTH} pc)`);
  if (stars.length > 0) {
    let minLum = Infinity, maxLum = -Infinity;
    for (const s of stars) {
      if (s.lum < minLum) minLum = s.lum;
      if (s.lum > maxLum) maxLum = s.lum;
    }
    console.log(`Lum range: ${minLum.toExponential(2)} \u2013 ${maxLum.toExponential(2)}`);
  }
  const hdr = new Float32Array(WIDTH * HEIGHT * 3);
  rasterize(stars, hdr);
  const header = `P6
${WIDTH} ${HEIGHT}
255
`;
  const pixels = Buffer.allocUnsafe(WIDTH * HEIGHT * 3);
  const tm = (v) => Math.min(255, Math.round(255 * Math.pow(v / (1 + v), 1 / 2.2)));
  for (let i = 0; i < WIDTH * HEIGHT; i++) {
    pixels[i * 3] = tm(hdr[i * 3]);
    pixels[i * 3 + 1] = tm(hdr[i * 3 + 1]);
    pixels[i * 3 + 2] = tm(hdr[i * 3 + 2]);
  }
  fs.writeFileSync(OUT, Buffer.concat([Buffer.from(header), pixels]));
  console.log(`Saved ${OUT}  (open with: open ${OUT})`);
}
main().catch((e) => {
  console.error(e);
  process.exit(1);
});

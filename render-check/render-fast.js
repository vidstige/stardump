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

// render-fast.ts
var https = __toESM(require("https"));
var http = __toESM(require("http"));
var fs = __toESM(require("fs"));
var path = __toESM(require("path"));

// brightness.ts
function dot(a, b) {
  return a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
}
function normalize(v) {
  const l = Math.hypot(...v) || 1;
  return [v[0] / l, v[1] / l, v[2] / l];
}
function cross(a, b) {
  return [
    a[1] * b[2] - a[2] * b[1],
    a[2] * b[0] - a[0] * b[2],
    a[0] * b[1] - a[1] * b[0]
  ];
}
function makeCamera(eye, dir, up, fovDeg, width, height) {
  const forward = normalize(dir);
  const right = normalize(cross(forward, up));
  const upOrth = cross(right, forward);
  const fovy = fovDeg * Math.PI / 180;
  return {
    eye,
    forward,
    right,
    up: upOrth,
    width,
    height,
    tanH: Math.tan(fovy * 0.5),
    aspect: width / height
  };
}
function project(c, px, py, pz) {
  const rx = px - c.eye[0], ry = py - c.eye[1], rz = pz - c.eye[2];
  const depth = dot([rx, ry, rz], c.forward);
  if (depth <= 0) return null;
  const h = dot([rx, ry, rz], c.right);
  const v = dot([rx, ry, rz], c.up);
  const sx = (h / (depth * c.tanH * c.aspect) * 0.5 + 0.5) * c.width;
  const sy = (1 - (v / (depth * c.tanH) * 0.5 + 0.5)) * c.height;
  return [sx, sy, depth];
}
function bprpToColor(bprp) {
  if (!isFinite(bprp)) return [1, 1, 1];
  const t = Math.max(0, Math.min(1, (bprp + 0.5) / 3.5));
  const lerp = (a, b, t2) => [
    a[0] + (b[0] - a[0]) * t2,
    a[1] + (b[1] - a[1]) * t2,
    a[2] + (b[2] - a[2]) * t2
  ];
  if (t < 0.33) return lerp([0.6, 0.7, 1], [1, 0.95, 0.9], t / 0.33);
  if (t < 0.66) return lerp([1, 0.95, 0.9], [1, 0.85, 0.4], (t - 0.33) / 0.33);
  return lerp([1, 0.85, 0.4], [1, 0.3, 0.1], (t - 0.66) / 0.34);
}
function rasterize(stars, hdr, cfg) {
  const { camera, exposure, limitMag, satMag } = cfg;
  const magSpan = limitMag - satMag;
  const { width, height } = camera;
  for (const s of stars) {
    if (!(s.lum > 0)) continue;
    const proj = project(camera, s.x, s.y, s.z);
    if (!proj) continue;
    const [sx, sy, dist] = proj;
    const flux = s.lum / Math.max(dist * dist, 0.01);
    const mag = -2.5 * Math.log10(flux);
    const t = (limitMag - mag) / magSpan;
    if (t <= 0) continue;
    const brightness = t * exposure;
    const [cr, cg, cb] = bprpToColor(s.bprp);
    const rPx = Math.min(Math.max(brightness * 2, 0.8), 8);
    const ir = Math.ceil(rPx);
    for (let dy = -ir; dy <= ir; dy++) {
      for (let dx = -ir; dx <= ir; dx++) {
        const xi = Math.round(sx) + dx, yi = Math.round(sy) + dy;
        if (xi < 0 || xi >= width || yi < 0 || yi >= height) continue;
        const nr = Math.sqrt(dx * dx + dy * dy) / rPx;
        const val = brightness * Math.exp(-nr * nr * 4);
        const idx = (yi * width + xi) * 3;
        hdr[idx] += cr * val;
        hdr[idx + 1] += cg * val;
        hdr[idx + 2] += cb * val;
      }
    }
  }
}
function tonemapToBytes(hdr, width, height) {
  const pixels = Buffer.allocUnsafe(width * height * 3);
  const tm = (v) => Math.min(255, Math.round(255 * Math.pow(v / (1 + v), 1 / 2.2)));
  for (let i = 0; i < width * height; i++) {
    pixels[i * 3] = tm(hdr[i * 3]);
    pixels[i * 3 + 1] = tm(hdr[i * 3 + 1]);
    pixels[i * 3 + 2] = tm(hdr[i * 3 + 2]);
  }
  return pixels;
}
function writePpm(path2, width, height, pixels) {
  const fs2 = require("fs");
  const header = `P6
${width} ${height}
255
`;
  fs2.writeFileSync(path2, Buffer.concat([Buffer.from(header), pixels]));
}

// render-fast.ts
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
var PIXEL_THRESHOLD = getArgNum("pixel-threshold", 4);
var OUT = getArg("output", "stars.ppm");
var CACHE_DIR = getArg("cache-dir", "/tmp");
var HEADER_SIZE = 32;
var NODE_SIZE = 20;
var POINT_SIZE = 20;
var MAGIC = "STRCLD\0\0";
function cacheKey(apiRoot, dataset) {
  const safe = (apiRoot + "|" + dataset).replace(/[^a-zA-Z0-9._-]/g, "_");
  return path.join(CACHE_DIR, `starcloud-${safe}.bin`);
}
function fetchStarcloud(apiRoot, dataset) {
  const cache = cacheKey(apiRoot, dataset);
  if (fs.existsSync(cache)) {
    return Promise.resolve(fs.readFileSync(cache));
  }
  const url = `${apiRoot}/datasets/${dataset}/starcloud.bin`;
  console.log("Fetching:", url);
  return new Promise((resolve, reject) => {
    const lib = url.startsWith("https") ? https : http;
    lib.get(url, (res) => {
      if (res.statusCode !== 200) {
        reject(new Error(`starcloud fetch failed: HTTP ${res.statusCode}`));
        return;
      }
      const chunks = [];
      res.on("data", (chunk) => chunks.push(chunk));
      res.on("end", () => {
        const buf = Buffer.concat(chunks);
        try {
          fs.writeFileSync(cache, buf);
        } catch (e) {
          console.warn("cache write failed:", e);
        }
        resolve(buf);
      });
      res.on("error", reject);
    }).on("error", reject);
  });
}
function parseStarcloud(buf) {
  if (buf.length < HEADER_SIZE) throw new Error("starcloud truncated");
  const magic = buf.slice(0, 8).toString("binary");
  if (magic !== MAGIC) throw new Error(`bad magic: ${JSON.stringify(magic)}`);
  const version = buf.readUInt16LE(8);
  if (version !== 1) throw new Error(`unsupported version ${version}`);
  const depth = buf.readUInt8(10);
  const halfExtentPc = buf.readFloatLE(12);
  const nodeCount = buf.readUInt32LE(16);
  const pointCount = Number(buf.readBigUInt64LE(20));
  const nodesStart = HEADER_SIZE;
  const nodesEnd = nodesStart + nodeCount * NODE_SIZE;
  const pointsEnd = nodesEnd + pointCount * POINT_SIZE;
  if (buf.length !== pointsEnd) {
    throw new Error(`starcloud size ${buf.length} != expected ${pointsEnd}`);
  }
  const nodeView = new DataView(buf.buffer, buf.byteOffset + nodesStart, nodeCount * NODE_SIZE);
  const childMask = new Uint8Array(nodeCount);
  const firstChild = new Uint32Array(nodeCount);
  const pointFirst = new Uint32Array(nodeCount);
  const pointCountArr = new Uint32Array(nodeCount);
  for (let i = 0; i < nodeCount; i++) {
    const off = i * NODE_SIZE;
    childMask[i] = nodeView.getUint8(off);
    firstChild[i] = nodeView.getUint32(off + 4, true);
    pointFirst[i] = nodeView.getUint32(off + 8, true);
    pointCountArr[i] = nodeView.getUint32(off + 12, true);
  }
  const pointBase = buf.byteOffset + nodesEnd;
  const pointFloats = new Float32Array(buf.buffer, pointBase, pointCount * 5);
  return {
    depth,
    halfExtentPc,
    nodeCount,
    pointCount,
    nodes: { childMask, firstChild, pointFirst, pointCount: pointCountArr },
    pointFloats
  };
}
function childBounds(parent, child) {
  const mx = (parent.min[0] + parent.max[0]) * 0.5;
  const my = (parent.min[1] + parent.max[1]) * 0.5;
  const mz = (parent.min[2] + parent.max[2]) * 0.5;
  return {
    min: [
      (child & 1) === 0 ? parent.min[0] : mx,
      (child & 2) === 0 ? parent.min[1] : my,
      (child & 4) === 0 ? parent.min[2] : mz
    ],
    max: [
      (child & 1) === 0 ? mx : parent.max[0],
      (child & 2) === 0 ? my : parent.max[1],
      (child & 4) === 0 ? mz : parent.max[2]
    ]
  };
}
function planeFromPointNormal(px, py, pz, nx, ny, nz) {
  const len = Math.hypot(nx, ny, nz) || 1;
  const ux = nx / len, uy = ny / len, uz = nz / len;
  return { nx: ux, ny: uy, nz: uz, d: -(ux * px + uy * py + uz * pz) };
}
function boundsMaxCorner(b, n) {
  return [
    n[0] >= 0 ? b.max[0] : b.min[0],
    n[1] >= 0 ? b.max[1] : b.min[1],
    n[2] >= 0 ? b.max[2] : b.min[2]
  ];
}
function frustumIntersectsBounds(planes, b) {
  for (const p of planes) {
    const c = boundsMaxCorner(b, [p.nx, p.ny, p.nz]);
    const dist = p.nx * c[0] + p.ny * c[1] + p.nz * c[2] + p.d;
    if (dist < 0) return false;
  }
  return true;
}
function buildFrustumPlanes(eye, forward, right, up, near, far, fovy, aspect) {
  const nearCenter = [
    eye[0] + forward[0] * near,
    eye[1] + forward[1] * near,
    eye[2] + forward[2] * near
  ];
  const farCenter = [
    eye[0] + forward[0] * far,
    eye[1] + forward[1] * far,
    eye[2] + forward[2] * far
  ];
  const tanHalf = Math.tan(fovy * 0.5);
  const hNear = near * tanHalf;
  const wNear = hNear * aspect;
  const leftNormal = [
    right[0] * near + forward[0] * wNear,
    right[1] * near + forward[1] * wNear,
    right[2] * near + forward[2] * wNear
  ];
  const rightNormal = [
    -right[0] * near + forward[0] * wNear,
    -right[1] * near + forward[1] * wNear,
    -right[2] * near + forward[2] * wNear
  ];
  const bottomNormal = [
    up[0] * near + forward[0] * hNear,
    up[1] * near + forward[1] * hNear,
    up[2] * near + forward[2] * hNear
  ];
  const topNormal = [
    -up[0] * near + forward[0] * hNear,
    -up[1] * near + forward[1] * hNear,
    -up[2] * near + forward[2] * hNear
  ];
  return [
    planeFromPointNormal(nearCenter[0], nearCenter[1], nearCenter[2], forward[0], forward[1], forward[2]),
    planeFromPointNormal(farCenter[0], farCenter[1], farCenter[2], -forward[0], -forward[1], -forward[2]),
    planeFromPointNormal(eye[0], eye[1], eye[2], leftNormal[0], leftNormal[1], leftNormal[2]),
    planeFromPointNormal(eye[0], eye[1], eye[2], rightNormal[0], rightNormal[1], rightNormal[2]),
    planeFromPointNormal(eye[0], eye[1], eye[2], bottomNormal[0], bottomNormal[1], bottomNormal[2]),
    planeFromPointNormal(eye[0], eye[1], eye[2], topNormal[0], topNormal[1], topNormal[2])
  ];
}
function boundsCenterAndHalf(b) {
  const cx = (b.min[0] + b.max[0]) * 0.5;
  const cy = (b.min[1] + b.max[1]) * 0.5;
  const cz = (b.min[2] + b.max[2]) * 0.5;
  const half = (b.max[0] - b.min[0]) * 0.5;
  return { cx, cy, cz, half };
}
function collectCut(sc, rootBounds, eye, planes, pixelsPerRadian, pixelThreshold) {
  const out = [];
  function walk(nodeIdx, bounds) {
    if (!frustumIntersectsBounds(planes, bounds)) return;
    const cm = sc.nodes.childMask[nodeIdx];
    const pCount = sc.nodes.pointCount[nodeIdx];
    const pFirst = sc.nodes.pointFirst[nodeIdx];
    if (cm === 0) {
      if (pCount > 0) out.push({ firstPoint: pFirst, count: pCount });
      return;
    }
    const { cx, cy, cz, half } = boundsCenterAndHalf(bounds);
    const dx = cx - eye[0], dy = cy - eye[1], dz = cz - eye[2];
    const dist = Math.max(Math.hypot(dx, dy, dz), half);
    const footprintPx = half / dist * pixelsPerRadian;
    if (footprintPx < pixelThreshold && pCount > 0) {
      out.push({ firstPoint: pFirst, count: pCount });
      return;
    }
    let childIdx = sc.nodes.firstChild[nodeIdx];
    for (let c = 0; c < 8; c++) {
      if ((cm & 1 << c) === 0) continue;
      walk(childIdx, childBounds(bounds, c));
      childIdx++;
    }
  }
  if (sc.nodeCount > 0) walk(0, rootBounds);
  return out;
}
function pointInFrustum(planes, px, py, pz) {
  for (const p of planes) {
    if (p.nx * px + p.ny * py + p.nz * pz + p.d < 0) return false;
  }
  return true;
}
function* iterateStars(sc, ranges, planes) {
  const pf = sc.pointFloats;
  for (const r of ranges) {
    const end = r.firstPoint + r.count;
    for (let i = r.firstPoint; i < end; i++) {
      const base = i * 5;
      const px = pf[base], py = pf[base + 1], pz = pf[base + 2];
      if (!pointInFrustum(planes, px, py, pz)) continue;
      yield { x: px, y: py, z: pz, lum: pf[base + 3], bprp: pf[base + 4] };
    }
  }
}
async function main() {
  const started = Date.now();
  const buf = await fetchStarcloud(API_ROOT, DATASET);
  const sc = parseStarcloud(buf);
  console.log(
    `starcloud: depth=${sc.depth} half_extent_pc=${sc.halfExtentPc} nodes=${sc.nodeCount} points=${sc.pointCount}`
  );
  const camera = makeCamera(eyeStr, dirStr, upStr, FOV_DEG, WIDTH, HEIGHT);
  const fovy = FOV_DEG * Math.PI / 180;
  const planes = buildFrustumPlanes(
    camera.eye,
    camera.forward,
    camera.right,
    camera.up,
    NEAR,
    DEPTH,
    fovy,
    camera.aspect
  );
  const pixelsPerRadian = HEIGHT / fovy;
  const rootBounds = {
    min: [-sc.halfExtentPc, -sc.halfExtentPc, -sc.halfExtentPc],
    max: [sc.halfExtentPc, sc.halfExtentPc, sc.halfExtentPc]
  };
  const ranges = collectCut(sc, rootBounds, camera.eye, planes, pixelsPerRadian, PIXEL_THRESHOLD);
  const starCount = ranges.reduce((a, r) => a + r.count, 0);
  console.log(`cut: ${ranges.length} node-ranges covering ${starCount} stars (M=${PIXEL_THRESHOLD}px)`);
  const hdr = new Float32Array(WIDTH * HEIGHT * 3);
  rasterize(iterateStars(sc, ranges, planes), hdr, {
    camera,
    exposure: EXPOSURE,
    limitMag: LIMIT_MAG,
    satMag: SAT_MAG
  });
  const pixels = tonemapToBytes(hdr, WIDTH, HEIGHT);
  writePpm(OUT, WIDTH, HEIGHT, pixels);
  console.log(`Saved ${OUT} in ${((Date.now() - started) / 1e3).toFixed(2)}s`);
}
main().catch((e) => {
  console.error(e);
  process.exit(1);
});

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
var fs = __toESM(require("fs"));

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
function makeCamera(eye, dir, up, width, height) {
  const forward = normalize(dir);
  const right = normalize(cross(forward, up));
  const upOrth = cross(right, forward);
  return { eye, forward, right, up: upOrth, width, height };
}
function perspectiveProjection(fovDeg, width, height) {
  const tanH = Math.tan(fovDeg * Math.PI / 360);
  return { kind: "perspective", tanH, aspect: width / height };
}
function orthographicProjection(halfWidth, width, height) {
  return { kind: "orthographic", halfWidth, aspect: width / height };
}
function project(c, proj, px, py, pz) {
  const rx = px - c.eye[0], ry = py - c.eye[1], rz = pz - c.eye[2];
  const depth = dot([rx, ry, rz], c.forward);
  if (depth <= 0) return null;
  const h = dot([rx, ry, rz], c.right);
  const v = dot([rx, ry, rz], c.up);
  let sx, sy;
  if (proj.kind === "orthographic") {
    const halfH = proj.halfWidth / proj.aspect;
    sx = (h / proj.halfWidth * 0.5 + 0.5) * c.width;
    sy = (1 - (v / halfH * 0.5 + 0.5)) * c.height;
  } else {
    sx = (h / (depth * proj.tanH * proj.aspect) * 0.5 + 0.5) * c.width;
    sy = (1 - (v / (depth * proj.tanH) * 0.5 + 0.5)) * c.height;
  }
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
  const { camera, projection, exposure } = cfg;
  const { width, height } = camera;
  const orthoRef = projection.kind === "orthographic" ? projection.halfWidth : 0;
  for (const s of stars) {
    if (!(s.lum > 0)) continue;
    const screenPos = project(camera, projection, s.x, s.y, s.z);
    if (!screenPos) continue;
    const [sx, sy, depth] = screenPos;
    const refDist = orthoRef || depth;
    const flux = s.lum / Math.max(refDist * refDist, 0.01);
    const brightness = flux * exposure;
    const [cr, cg, cb] = bprpToColor(s.bprp);
    const rPx = Math.min(Math.max(brightness, 0.8), 1);
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
function writePpm(path, width, height, pixels) {
  const fs2 = require("fs");
  const header = `P6
${width} ${height}
255
`;
  fs2.writeFileSync(path, Buffer.concat([Buffer.from(header), pixels]));
}

// render-exact.ts
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
function hasArg(name) {
  return args.includes("--" + name);
}
var STARCLOUD = getArg("starcloud");
var FOV_DEG = getArgNum("fov", 60);
var DEPTH = getArgNum("depth", 5e3);
var NEAR = getArgNum("near", 0.1);
var WIDTH = getArgNum("width", 1920);
var HEIGHT = getArgNum("height", 1080);
var OUT = getArg("output", "stars.ppm");
var ORTHO = hasArg("orthographic");
var NGP = normalize([-0.86703, -0.20006, 0.45673]);
var GC = normalize([-0.05487, -0.87344, -0.48384]);
var HEADER_SIZE = 32;
var NODE_SIZE = 20;
var POINT_SIZE = 20;
var MAGIC = "STRCLD\0\0";
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
  if (buf.length !== pointsEnd) throw new Error(`starcloud size ${buf.length} != expected ${pointsEnd}`);
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
    min: [(child & 1) === 0 ? parent.min[0] : mx, (child & 2) === 0 ? parent.min[1] : my, (child & 4) === 0 ? parent.min[2] : mz],
    max: [(child & 1) === 0 ? mx : parent.max[0], (child & 2) === 0 ? my : parent.max[1], (child & 4) === 0 ? mz : parent.max[2]]
  };
}
function planeFromPointNormal(px, py, pz, nx, ny, nz) {
  const len = Math.hypot(nx, ny, nz) || 1;
  const ux = nx / len, uy = ny / len, uz = nz / len;
  return { nx: ux, ny: uy, nz: uz, d: -(ux * px + uy * py + uz * pz) };
}
function viewIntersectsBounds(planes, b) {
  for (const p of planes) {
    const cx = p.nx >= 0 ? b.max[0] : b.min[0];
    const cy = p.ny >= 0 ? b.max[1] : b.min[1];
    const cz = p.nz >= 0 ? b.max[2] : b.min[2];
    if (p.nx * cx + p.ny * cy + p.nz * cz + p.d < 0) return false;
  }
  return true;
}
function buildCullingPlanes(camera, proj, near, far) {
  const { eye, forward, right, up } = camera;
  const ne = [eye[0] + forward[0] * near, eye[1] + forward[1] * near, eye[2] + forward[2] * near];
  const fe = [eye[0] + forward[0] * far, eye[1] + forward[1] * far, eye[2] + forward[2] * far];
  if (proj.kind === "orthographic") {
    const hw = proj.halfWidth, hh = hw / proj.aspect;
    const lp = [eye[0] - right[0] * hw, eye[1] - right[1] * hw, eye[2] - right[2] * hw];
    const rp = [eye[0] + right[0] * hw, eye[1] + right[1] * hw, eye[2] + right[2] * hw];
    const bp = [eye[0] - up[0] * hh, eye[1] - up[1] * hh, eye[2] - up[2] * hh];
    const tp = [eye[0] + up[0] * hh, eye[1] + up[1] * hh, eye[2] + up[2] * hh];
    return [
      planeFromPointNormal(ne[0], ne[1], ne[2], forward[0], forward[1], forward[2]),
      planeFromPointNormal(fe[0], fe[1], fe[2], -forward[0], -forward[1], -forward[2]),
      planeFromPointNormal(lp[0], lp[1], lp[2], right[0], right[1], right[2]),
      planeFromPointNormal(rp[0], rp[1], rp[2], -right[0], -right[1], -right[2]),
      planeFromPointNormal(bp[0], bp[1], bp[2], up[0], up[1], up[2]),
      planeFromPointNormal(tp[0], tp[1], tp[2], -up[0], -up[1], -up[2])
    ];
  }
  const hNear = near * proj.tanH, wNear = hNear * proj.aspect;
  const lN = [right[0] * near + forward[0] * wNear, right[1] * near + forward[1] * wNear, right[2] * near + forward[2] * wNear];
  const rN = [-right[0] * near + forward[0] * wNear, -right[1] * near + forward[1] * wNear, -right[2] * near + forward[2] * wNear];
  const bN = [up[0] * near + forward[0] * hNear, up[1] * near + forward[1] * hNear, up[2] * near + forward[2] * hNear];
  const tN = [-up[0] * near + forward[0] * hNear, -up[1] * near + forward[1] * hNear, -up[2] * near + forward[2] * hNear];
  return [
    planeFromPointNormal(ne[0], ne[1], ne[2], forward[0], forward[1], forward[2]),
    planeFromPointNormal(fe[0], fe[1], fe[2], -forward[0], -forward[1], -forward[2]),
    planeFromPointNormal(eye[0], eye[1], eye[2], lN[0], lN[1], lN[2]),
    planeFromPointNormal(eye[0], eye[1], eye[2], rN[0], rN[1], rN[2]),
    planeFromPointNormal(eye[0], eye[1], eye[2], bN[0], bN[1], bN[2]),
    planeFromPointNormal(eye[0], eye[1], eye[2], tN[0], tN[1], tN[2])
  ];
}
function collectLeaves(sc, rootBounds, planes) {
  const out = [];
  function walk(nodeIdx, bounds) {
    if (!viewIntersectsBounds(planes, bounds)) return;
    const cm = sc.nodes.childMask[nodeIdx];
    const pCount = sc.nodes.pointCount[nodeIdx];
    const pFirst = sc.nodes.pointFirst[nodeIdx];
    if (cm === 0) {
      if (pCount > 0) out.push({ firstPoint: pFirst, count: pCount });
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
function pointInView(planes, px, py, pz) {
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
      if (!pointInView(planes, px, py, pz)) continue;
      yield { x: px, y: py, z: pz, lum: pf[base + 3], bprp: pf[base + 4] };
    }
  }
}
async function main() {
  const started = Date.now();
  console.log("Reading:", STARCLOUD);
  const buf = fs.readFileSync(STARCLOUD);
  const sc = parseStarcloud(buf);
  console.log(`starcloud: depth=${sc.depth} half_extent_pc=${sc.halfExtentPc} nodes=${sc.nodeCount} points=${sc.pointCount}`);
  const halfWidth = ORTHO ? getArgNum("half-width", sc.halfExtentPc) : 0;
  const far = hasArg("depth") ? DEPTH : ORTHO ? sc.halfExtentPc * 4 : DEPTH;
  const exposure = hasArg("exposure") ? getArgNum("exposure") : ORTHO ? halfWidth * halfWidth / 5e3 : 500;
  let eye, dir, up;
  if (ORTHO) {
    eye = hasArg("eye") ? getArg("eye").split(",").map(Number) : [NGP[0] * sc.halfExtentPc * 2, NGP[1] * sc.halfExtentPc * 2, NGP[2] * sc.halfExtentPc * 2];
    dir = hasArg("dir") ? getArg("dir").split(",").map(Number) : [-NGP[0], -NGP[1], -NGP[2]];
    up = hasArg("up") ? getArg("up").split(",").map(Number) : [...GC];
  } else {
    eye = getArg("eye", "0,0,0").split(",").map(Number);
    dir = getArg("dir", "0,0,-1").split(",").map(Number);
    up = getArg("up", "0,1,0").split(",").map(Number);
  }
  const camera = makeCamera(eye, dir, up, WIDTH, HEIGHT);
  const projection = ORTHO ? orthographicProjection(halfWidth, WIDTH, HEIGHT) : perspectiveProjection(FOV_DEG, WIDTH, HEIGHT);
  const planes = buildCullingPlanes(camera, projection, NEAR, far);
  const rootBounds = {
    min: [-sc.halfExtentPc, -sc.halfExtentPc, -sc.halfExtentPc],
    max: [sc.halfExtentPc, sc.halfExtentPc, sc.halfExtentPc]
  };
  const ranges = collectLeaves(sc, rootBounds, planes);
  const starCount = ranges.reduce((a, r) => a + r.count, 0);
  console.log(`leaves: ${ranges.length} node-ranges covering ${starCount} stars`);
  const hdr = new Float32Array(WIDTH * HEIGHT * 3);
  rasterize(iterateStars(sc, ranges, planes), hdr, { camera, projection, exposure });
  const pixels = tonemapToBytes(hdr, WIDTH, HEIGHT);
  writePpm(OUT, WIDTH, HEIGHT, pixels);
  console.log(`Saved ${OUT} in ${((Date.now() - started) / 1e3).toFixed(2)}s`);
}
main().catch((e) => {
  console.error(e);
  process.exit(1);
});

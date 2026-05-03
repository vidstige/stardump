// Fast renderer consuming starcloud.bin. Walks the LOD octree against the
// camera, emits a disjoint cut of real stars (leaves directly; internal-node
// subsamples with descendants-over-K boost), and rasterizes through the
// shared brightness helper that backs render-exact.ts.

import * as https from "https";
import * as http from "http";
import * as fs from "fs";
import * as path from "path";

import {
  makeCamera,
  perspectiveProjection,
  orthographicProjection,
  normalize,
  rasterize,
  tonemapToBytes,
  writePpm,
  type Camera,
  type Projection,
  type Star,
} from "./brightness";

const args = process.argv.slice(2);
function getArg(name: string, def?: string): string {
  const i = args.indexOf("--" + name);
  if (i !== -1) return args[i + 1];
  if (def !== undefined) return def;
  throw new Error(`missing --${name}`);
}
function getArgNum(name: string, def?: number): number {
  const i = args.indexOf("--" + name);
  if (i !== -1) return parseFloat(args[i + 1]);
  if (def !== undefined) return def;
  throw new Error(`missing --${name}`);
}
function hasArg(name: string): boolean { return args.includes("--" + name); }

const API_ROOT = getArg("url");
const DATASET = getArg("dataset");
const FOV_DEG = getArgNum("fov", 60);
const DEPTH = getArgNum("depth", 5000);
const NEAR = getArgNum("near", 0.1);
const WIDTH = getArgNum("width", 1920);
const HEIGHT = getArgNum("height", 1080);
const PIXEL_THRESHOLD = getArgNum("pixel-threshold", 4);
const OUT = getArg("output", "stars.ppm");
const CACHE_DIR = getArg("cache-dir", "/tmp");
const ORTHO = hasArg("orthographic");

// Galactic north pole and galactic center direction in equatorial J2000 cartesian.
// Used as default camera orientation for --orthographic.
const NGP = normalize([-0.86703, -0.20006, 0.45673] as [number,number,number]);
const GC  = normalize([-0.05487, -0.87344, -0.48384] as [number,number,number]);

type Bounds = { min: [number, number, number]; max: [number, number, number] };

type ParsedStarcloud = {
  depth: number;
  halfExtentPc: number;
  nodeCount: number;
  pointCount: number;
  nodes: {
    childMask: Uint8Array;
    firstChild: Uint32Array;
    pointFirst: Uint32Array;
    pointCount: Uint32Array;
  };
  // Interleaved Float32Array: [x, y, z, lum, bprp, x, y, z, lum, bprp, ...]
  // Access: pointFloats[i*5+0]=x, +1=y, +2=z, +3=lum, +4=bprp
  pointFloats: Float32Array;
};

const HEADER_SIZE = 32;
const NODE_SIZE = 20;
const POINT_SIZE = 20;
const MAGIC = "STRCLD\0\0";

function cacheKey(apiRoot: string, dataset: string): string {
  const safe = (apiRoot + "|" + dataset).replace(/[^a-zA-Z0-9._-]/g, "_");
  return path.join(CACHE_DIR, `starcloud-${safe}.bin`);
}

function fetchStarcloud(apiRoot: string, dataset: string): Promise<Buffer> {
  const cache = cacheKey(apiRoot, dataset);
  if (fs.existsSync(cache)) {
    return Promise.resolve(fs.readFileSync(cache));
  }
  const url = `${apiRoot}/datasets/${dataset}/starcloud.bin`;
  console.log("Fetching:", url);
  return new Promise((resolve, reject) => {
    const lib = url.startsWith("https") ? https : http;
    lib
      .get(url, (res) => {
        if (res.statusCode !== 200) {
          reject(new Error(`starcloud fetch failed: HTTP ${res.statusCode}`));
          return;
        }
        const chunks: Buffer[] = [];
        res.on("data", (chunk: Buffer) => chunks.push(chunk));
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
      })
      .on("error", reject);
  });
}

function parseStarcloud(buf: Buffer): ParsedStarcloud {
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

  // Zero-copy view of the node table via DataView (nodes have mixed field sizes).
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

  // Zero-copy Float32Array view over the interleaved point table.
  // Layout: [x, y, z, lum, bprp] per point — access as pointFloats[i*5 + field].
  const pointBase = buf.byteOffset + nodesEnd;
  const pointFloats = new Float32Array(buf.buffer, pointBase, pointCount * 5);

  return {
    depth,
    halfExtentPc,
    nodeCount,
    pointCount,
    nodes: { childMask, firstChild, pointFirst, pointCount: pointCountArr },
    pointFloats,
  };
}

// Bit ordering matches octree.rs: child & 1 → x, child & 2 → y, child & 4 → z.
function childBounds(parent: Bounds, child: number): Bounds {
  const mx = (parent.min[0] + parent.max[0]) * 0.5;
  const my = (parent.min[1] + parent.max[1]) * 0.5;
  const mz = (parent.min[2] + parent.max[2]) * 0.5;
  return {
    min: [
      (child & 1) === 0 ? parent.min[0] : mx,
      (child & 2) === 0 ? parent.min[1] : my,
      (child & 4) === 0 ? parent.min[2] : mz,
    ],
    max: [
      (child & 1) === 0 ? mx : parent.max[0],
      (child & 2) === 0 ? my : parent.max[1],
      (child & 4) === 0 ? mz : parent.max[2],
    ],
  };
}

type Plane = { nx: number; ny: number; nz: number; d: number };

function planeFromPointNormal(px: number, py: number, pz: number, nx: number, ny: number, nz: number): Plane {
  const len = Math.hypot(nx, ny, nz) || 1;
  const ux = nx / len, uy = ny / len, uz = nz / len;
  return { nx: ux, ny: uy, nz: uz, d: -(ux * px + uy * py + uz * pz) };
}

function viewIntersectsBounds(planes: Plane[], b: Bounds): boolean {
  for (const p of planes) {
    const cx = p.nx >= 0 ? b.max[0] : b.min[0];
    const cy = p.ny >= 0 ? b.max[1] : b.min[1];
    const cz = p.nz >= 0 ? b.max[2] : b.min[2];
    if (p.nx * cx + p.ny * cy + p.nz * cz + p.d < 0) return false;
  }
  return true;
}

function buildCullingPlanes(camera: Camera, proj: Projection, near: number, far: number): Plane[] {
  const { eye, forward, right, up } = camera;
  const ne: [number,number,number] = [eye[0]+forward[0]*near, eye[1]+forward[1]*near, eye[2]+forward[2]*near];
  const fe: [number,number,number] = [eye[0]+forward[0]*far,  eye[1]+forward[1]*far,  eye[2]+forward[2]*far];
  if (proj.kind === "orthographic") {
    const hw = proj.halfWidth, hh = hw / proj.aspect;
    const lp: [number,number,number] = [eye[0]-right[0]*hw, eye[1]-right[1]*hw, eye[2]-right[2]*hw];
    const rp: [number,number,number] = [eye[0]+right[0]*hw, eye[1]+right[1]*hw, eye[2]+right[2]*hw];
    const bp: [number,number,number] = [eye[0]-up[0]*hh,    eye[1]-up[1]*hh,    eye[2]-up[2]*hh];
    const tp: [number,number,number] = [eye[0]+up[0]*hh,    eye[1]+up[1]*hh,    eye[2]+up[2]*hh];
    return [
      planeFromPointNormal(ne[0],ne[1],ne[2],  forward[0], forward[1], forward[2]),
      planeFromPointNormal(fe[0],fe[1],fe[2], -forward[0],-forward[1],-forward[2]),
      planeFromPointNormal(lp[0],lp[1],lp[2],  right[0],   right[1],   right[2]),
      planeFromPointNormal(rp[0],rp[1],rp[2], -right[0],  -right[1],  -right[2]),
      planeFromPointNormal(bp[0],bp[1],bp[2],  up[0],      up[1],      up[2]),
      planeFromPointNormal(tp[0],tp[1],tp[2], -up[0],     -up[1],     -up[2]),
    ];
  }
  const hNear = near * proj.tanH, wNear = hNear * proj.aspect;
  const lN: [number,number,number] = [ right[0]*near+forward[0]*wNear,  right[1]*near+forward[1]*wNear,  right[2]*near+forward[2]*wNear];
  const rN: [number,number,number] = [-right[0]*near+forward[0]*wNear, -right[1]*near+forward[1]*wNear, -right[2]*near+forward[2]*wNear];
  const bN: [number,number,number] = [   up[0]*near+forward[0]*hNear,     up[1]*near+forward[1]*hNear,     up[2]*near+forward[2]*hNear];
  const tN: [number,number,number] = [  -up[0]*near+forward[0]*hNear,    -up[1]*near+forward[1]*hNear,    -up[2]*near+forward[2]*hNear];
  return [
    planeFromPointNormal(ne[0],ne[1],ne[2],   forward[0],  forward[1],  forward[2]),
    planeFromPointNormal(fe[0],fe[1],fe[2],  -forward[0], -forward[1], -forward[2]),
    planeFromPointNormal(eye[0],eye[1],eye[2], lN[0],lN[1],lN[2]),
    planeFromPointNormal(eye[0],eye[1],eye[2], rN[0],rN[1],rN[2]),
    planeFromPointNormal(eye[0],eye[1],eye[2], bN[0],bN[1],bN[2]),
    planeFromPointNormal(eye[0],eye[1],eye[2], tN[0],tN[1],tN[2]),
  ];
}

function boundsCenterAndHalf(b: Bounds): { cx: number; cy: number; cz: number; half: number } {
  const cx = (b.min[0] + b.max[0]) * 0.5;
  const cy = (b.min[1] + b.max[1]) * 0.5;
  const cz = (b.min[2] + b.max[2]) * 0.5;
  const half = (b.max[0] - b.min[0]) * 0.5;
  return { cx, cy, cz, half };
}

function collectCut(
  sc: ParsedStarcloud,
  rootBounds: Bounds,
  eye: [number, number, number],
  planes: Plane[],
  proj: Projection,
  pixelThreshold: number,
): { firstPoint: number; count: number }[] {
  const out: { firstPoint: number; count: number }[] = [];
  // Perspective: footprint = (half / dist) * pixelsPerRadian
  // Orthographic: footprint = half * (WIDTH / halfWidth) — depth-independent
  const pixelsPerRadian = proj.kind === "perspective" ? HEIGHT / (2 * Math.atan(proj.tanH)) : 0;
  const pixelsPerPc     = proj.kind === "orthographic" ? WIDTH / proj.halfWidth : 0;

  function walk(nodeIdx: number, bounds: Bounds): void {
    if (!viewIntersectsBounds(planes, bounds)) return;
    const cm = sc.nodes.childMask[nodeIdx];
    const pCount = sc.nodes.pointCount[nodeIdx];
    const pFirst = sc.nodes.pointFirst[nodeIdx];

    if (cm === 0) {
      if (pCount > 0) out.push({ firstPoint: pFirst, count: pCount });
      return;
    }

    // Internal node: decide whether to descend.
    const { cx, cy, cz, half } = boundsCenterAndHalf(bounds);
    let footprintPx: number;
    if (proj.kind === "orthographic") {
      footprintPx = half * pixelsPerPc;
    } else {
      const dx = cx - eye[0], dy = cy - eye[1], dz = cz - eye[2];
      const dist = Math.max(Math.hypot(dx, dy, dz), half);
      footprintPx = (half / dist) * pixelsPerRadian;
    }

    if (footprintPx < pixelThreshold && pCount > 0) {
      out.push({ firstPoint: pFirst, count: pCount });
      return;
    }

    let childIdx = sc.nodes.firstChild[nodeIdx];
    for (let c = 0; c < 8; c++) {
      if ((cm & (1 << c)) === 0) continue;
      walk(childIdx, childBounds(bounds, c));
      childIdx++;
    }
  }

  if (sc.nodeCount > 0) walk(0, rootBounds);
  return out;
}

function pointInView(planes: Plane[], px: number, py: number, pz: number): boolean {
  for (const p of planes) {
    if (p.nx * px + p.ny * py + p.nz * pz + p.d < 0) return false;
  }
  return true;
}

function* iterateStars(
  sc: ParsedStarcloud,
  ranges: { firstPoint: number; count: number }[],
  planes: Plane[],
): IterableIterator<Star> {
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

async function main(): Promise<void> {
  const started = Date.now();
  const buf = await fetchStarcloud(API_ROOT, DATASET);
  const sc = parseStarcloud(buf);
  console.log(
    `starcloud: depth=${sc.depth} half_extent_pc=${sc.halfExtentPc} nodes=${sc.nodeCount} points=${sc.pointCount}`,
  );

  const halfWidth = ORTHO ? getArgNum("half-width", sc.halfExtentPc) : 0;
  const far       = hasArg("depth") ? DEPTH : ORTHO ? sc.halfExtentPc * 4 : DEPTH;
  const exposure  = hasArg("exposure") ? getArgNum("exposure") : ORTHO ? halfWidth * halfWidth / 5000 : 500;

  let eye: [number,number,number], dir: [number,number,number], up: [number,number,number];
  if (ORTHO) {
    eye = hasArg("eye") ? getArg("eye").split(",").map(Number) as [number,number,number] : [NGP[0]*sc.halfExtentPc*2, NGP[1]*sc.halfExtentPc*2, NGP[2]*sc.halfExtentPc*2];
    dir = hasArg("dir") ? getArg("dir").split(",").map(Number) as [number,number,number] : [-NGP[0], -NGP[1], -NGP[2]];
    up  = hasArg("up")  ? getArg("up").split(",").map(Number)  as [number,number,number] : [...GC];
  } else {
    eye = getArg("eye", "0,0,0").split(",").map(Number) as [number,number,number];
    dir = getArg("dir", "0,0,-1").split(",").map(Number) as [number,number,number];
    up  = getArg("up",  "0,1,0").split(",").map(Number) as [number,number,number];
  }

  const camera     = makeCamera(eye, dir, up, WIDTH, HEIGHT);
  const projection = ORTHO ? orthographicProjection(halfWidth, WIDTH, HEIGHT)
                           : perspectiveProjection(FOV_DEG, WIDTH, HEIGHT);
  const planes     = buildCullingPlanes(camera, projection, NEAR, far);
  const rootBounds: Bounds = {
    min: [-sc.halfExtentPc, -sc.halfExtentPc, -sc.halfExtentPc],
    max: [sc.halfExtentPc, sc.halfExtentPc, sc.halfExtentPc],
  };

  const ranges = collectCut(sc, rootBounds, camera.eye, planes, projection, PIXEL_THRESHOLD);
  const starCount = ranges.reduce((a, r) => a + r.count, 0);
  console.log(`cut: ${ranges.length} node-ranges covering ${starCount} stars (M=${PIXEL_THRESHOLD}px)`);

  const hdr = new Float32Array(WIDTH * HEIGHT * 3);
  rasterize(iterateStars(sc, ranges, planes), hdr, { camera, projection, exposure });

  const pixels = tonemapToBytes(hdr, WIDTH, HEIGHT);
  writePpm(OUT, WIDTH, HEIGHT, pixels);
  console.log(`Saved ${OUT} in ${((Date.now() - started) / 1000).toFixed(2)}s`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

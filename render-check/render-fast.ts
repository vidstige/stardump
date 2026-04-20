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
  rasterize,
  tonemapToBytes,
  writePpm,
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

const API_ROOT = getArg("url");
const DATASET = getArg("dataset");
const eyeStr = getArg("eye", "0,0,0").split(",").map(Number) as [number, number, number];
const dirStr = getArg("dir", "0,0,-1").split(",").map(Number) as [number, number, number];
const upStr = getArg("up", "0,1,0").split(",").map(Number) as [number, number, number];
const FOV_DEG = getArgNum("fov", 60);
const DEPTH = getArgNum("depth", 5000);
const NEAR = getArgNum("near", 0.1);
const WIDTH = getArgNum("width", 1920);
const HEIGHT = getArgNum("height", 1080);
const EXPOSURE = getArgNum("exposure", 5000.0);
const PIXEL_THRESHOLD = getArgNum("pixel-threshold", 4);
const OUT = getArg("output", "stars.ppm");
const CACHE_DIR = getArg("cache-dir", "/tmp");

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

function boundsMaxCorner(b: Bounds, n: [number, number, number]): [number, number, number] {
  return [
    n[0] >= 0 ? b.max[0] : b.min[0],
    n[1] >= 0 ? b.max[1] : b.min[1],
    n[2] >= 0 ? b.max[2] : b.min[2],
  ];
}

function frustumIntersectsBounds(planes: Plane[], b: Bounds): boolean {
  for (const p of planes) {
    const c = boundsMaxCorner(b, [p.nx, p.ny, p.nz]);
    const dist = p.nx * c[0] + p.ny * c[1] + p.nz * c[2] + p.d;
    if (dist < 0) return false;
  }
  return true;
}

function buildFrustumPlanes(
  eye: [number, number, number],
  forward: [number, number, number],
  right: [number, number, number],
  up: [number, number, number],
  near: number,
  far: number,
  fovy: number,
  aspect: number,
): Plane[] {
  const nearCenter: [number, number, number] = [
    eye[0] + forward[0] * near,
    eye[1] + forward[1] * near,
    eye[2] + forward[2] * near,
  ];
  const farCenter: [number, number, number] = [
    eye[0] + forward[0] * far,
    eye[1] + forward[1] * far,
    eye[2] + forward[2] * far,
  ];
  const tanHalf = Math.tan(fovy * 0.5);
  const hNear = near * tanHalf;
  const wNear = hNear * aspect;

  // Side planes: pass through eye; normals point inward.
  // Left: normal has positive dot with `right`
  const leftNormal: [number, number, number] = [
    right[0] * near + forward[0] * wNear,
    right[1] * near + forward[1] * wNear,
    right[2] * near + forward[2] * wNear,
  ];
  const rightNormal: [number, number, number] = [
    -right[0] * near + forward[0] * wNear,
    -right[1] * near + forward[1] * wNear,
    -right[2] * near + forward[2] * wNear,
  ];
  const bottomNormal: [number, number, number] = [
    up[0] * near + forward[0] * hNear,
    up[1] * near + forward[1] * hNear,
    up[2] * near + forward[2] * hNear,
  ];
  const topNormal: [number, number, number] = [
    -up[0] * near + forward[0] * hNear,
    -up[1] * near + forward[1] * hNear,
    -up[2] * near + forward[2] * hNear,
  ];

  return [
    planeFromPointNormal(nearCenter[0], nearCenter[1], nearCenter[2], forward[0], forward[1], forward[2]),
    planeFromPointNormal(farCenter[0], farCenter[1], farCenter[2], -forward[0], -forward[1], -forward[2]),
    planeFromPointNormal(eye[0], eye[1], eye[2], leftNormal[0], leftNormal[1], leftNormal[2]),
    planeFromPointNormal(eye[0], eye[1], eye[2], rightNormal[0], rightNormal[1], rightNormal[2]),
    planeFromPointNormal(eye[0], eye[1], eye[2], bottomNormal[0], bottomNormal[1], bottomNormal[2]),
    planeFromPointNormal(eye[0], eye[1], eye[2], topNormal[0], topNormal[1], topNormal[2]),
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
  pixelsPerRadian: number,
  pixelThreshold: number,
): { firstPoint: number; count: number }[] {
  const out: { firstPoint: number; count: number }[] = [];

  function walk(nodeIdx: number, bounds: Bounds): void {
    if (!frustumIntersectsBounds(planes, bounds)) return;
    const cm = sc.nodes.childMask[nodeIdx];
    const pCount = sc.nodes.pointCount[nodeIdx];
    const pFirst = sc.nodes.pointFirst[nodeIdx];

    if (cm === 0) {
      if (pCount > 0) out.push({ firstPoint: pFirst, count: pCount });
      return;
    }

    // Internal node: decide whether to descend.
    const { cx, cy, cz, half } = boundsCenterAndHalf(bounds);
    const dx = cx - eye[0], dy = cy - eye[1], dz = cz - eye[2];
    const dist = Math.max(Math.hypot(dx, dy, dz), half);
    const footprintPx = (half / dist) * pixelsPerRadian;

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

function pointInFrustum(planes: Plane[], px: number, py: number, pz: number): boolean {
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
      if (!pointInFrustum(planes, px, py, pz)) continue;
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

  const camera = makeCamera(eyeStr, dirStr, upStr, FOV_DEG, WIDTH, HEIGHT);
  const fovy = (FOV_DEG * Math.PI) / 180;
  const planes = buildFrustumPlanes(
    camera.eye,
    camera.forward,
    camera.right,
    camera.up,
    NEAR,
    DEPTH,
    fovy,
    camera.aspect,
  );
  const pixelsPerRadian = HEIGHT / fovy;
  const rootBounds: Bounds = {
    min: [-sc.halfExtentPc, -sc.halfExtentPc, -sc.halfExtentPc],
    max: [sc.halfExtentPc, sc.halfExtentPc, sc.halfExtentPc],
  };

  const ranges = collectCut(sc, rootBounds, camera.eye, planes, pixelsPerRadian, PIXEL_THRESHOLD);
  const starCount = ranges.reduce((a, r) => a + r.count, 0);
  console.log(`cut: ${ranges.length} node-ranges covering ${starCount} stars (M=${PIXEL_THRESHOLD}px)`);

  const hdr = new Float32Array(WIDTH * HEIGHT * 3);
  rasterize(iterateStars(sc, ranges, planes), hdr, { camera, exposure: EXPOSURE });

  const pixels = tonemapToBytes(hdr, WIDTH, HEIGHT);
  writePpm(OUT, WIDTH, HEIGHT, pixels);
  console.log(`Saved ${OUT} in ${((Date.now() - started) / 1000).toFixed(2)}s`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

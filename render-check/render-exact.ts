import * as fs from "fs";

import {
  makeCamera,
  PerspectiveProjection,
  OrthographicProjection,
  normalize,
  rasterize,
  tonemapToBytes,
  writePng,
  type Plane,
  type Star,
} from "./brightness";

// --- CLI args ---
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

const STARCLOUD = getArg("starcloud");
const FOV_DEG  = getArgNum("fov", 60);
const DEPTH    = getArgNum("depth", 5000);
const NEAR     = getArgNum("near", 0.1);
const WIDTH    = getArgNum("width", 1920);
const HEIGHT   = getArgNum("height", 1080);
const OUT      = getArg("output", "stars.png");
const ORTHO    = hasArg("orthographic");

// Galactic north pole and galactic center direction in equatorial J2000 cartesian.
// Used as default camera orientation for --orthographic.
const NGP = normalize([-0.86703, -0.20006, 0.45673] as [number,number,number]);
const GC  = normalize([-0.05487, -0.87344, -0.48384] as [number,number,number]);

// --- Starcloud binary format ---
const HEADER_SIZE = 32;
const NODE_SIZE   = 20;
const POINT_SIZE  = 20;
const MAGIC       = "STRCLD\0\0";

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
  pointFloats: Float32Array;
};

type Bounds = { min: [number, number, number]; max: [number, number, number] };

function parseStarcloud(buf: Buffer): ParsedStarcloud {
  if (buf.length < HEADER_SIZE) throw new Error("starcloud truncated");
  const magic = buf.slice(0, 8).toString("binary");
  if (magic !== MAGIC) throw new Error(`bad magic: ${JSON.stringify(magic)}`);
  const version = buf.readUInt16LE(8);
  if (version !== 1) throw new Error(`unsupported version ${version}`);
  const depth        = buf.readUInt8(10);
  const halfExtentPc = buf.readFloatLE(12);
  const nodeCount    = buf.readUInt32LE(16);
  const pointCount   = Number(buf.readBigUInt64LE(20));

  const nodesStart = HEADER_SIZE;
  const nodesEnd   = nodesStart + nodeCount * NODE_SIZE;
  const pointsEnd  = nodesEnd + pointCount * POINT_SIZE;
  if (buf.length !== pointsEnd) throw new Error(`starcloud size ${buf.length} != expected ${pointsEnd}`);

  const nodeView      = new DataView(buf.buffer, buf.byteOffset + nodesStart, nodeCount * NODE_SIZE);
  const childMask     = new Uint8Array(nodeCount);
  const firstChild    = new Uint32Array(nodeCount);
  const pointFirst    = new Uint32Array(nodeCount);
  const pointCountArr = new Uint32Array(nodeCount);
  for (let i = 0; i < nodeCount; i++) {
    const off = i * NODE_SIZE;
    childMask[i]     = nodeView.getUint8(off);
    firstChild[i]    = nodeView.getUint32(off + 4, true);
    pointFirst[i]    = nodeView.getUint32(off + 8, true);
    pointCountArr[i] = nodeView.getUint32(off + 12, true);
  }

  const pointBase   = buf.byteOffset + nodesEnd;
  const pointFloats = new Float32Array(buf.buffer, pointBase, pointCount * 5);

  return { depth, halfExtentPc, nodeCount, pointCount,
           nodes: { childMask, firstChild, pointFirst, pointCount: pointCountArr },
           pointFloats };
}

function childBounds(parent: Bounds, child: number): Bounds {
  const mx = (parent.min[0] + parent.max[0]) * 0.5;
  const my = (parent.min[1] + parent.max[1]) * 0.5;
  const mz = (parent.min[2] + parent.max[2]) * 0.5;
  return {
    min: [(child & 1) === 0 ? parent.min[0] : mx, (child & 2) === 0 ? parent.min[1] : my, (child & 4) === 0 ? parent.min[2] : mz],
    max: [(child & 1) === 0 ? mx : parent.max[0], (child & 2) === 0 ? my : parent.max[1], (child & 4) === 0 ? mz : parent.max[2]],
  };
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

// Traverse to leaf nodes only — skips internal-node subsamples for exact rendering.
function collectLeaves(sc: ParsedStarcloud, rootBounds: Bounds, planes: Plane[]): { firstPoint: number; count: number }[] {
  const out: { firstPoint: number; count: number }[] = [];

  function walk(nodeIdx: number, bounds: Bounds): void {
    if (!viewIntersectsBounds(planes, bounds)) return;
    const cm     = sc.nodes.childMask[nodeIdx];
    const pCount = sc.nodes.pointCount[nodeIdx];
    const pFirst = sc.nodes.pointFirst[nodeIdx];

    if (cm === 0) {
      if (pCount > 0) out.push({ firstPoint: pFirst, count: pCount });
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

function* iterateStars(sc: ParsedStarcloud, ranges: { firstPoint: number; count: number }[], planes: Plane[]): IterableIterator<Star> {
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
  console.log("Reading:", STARCLOUD);
  const buf = fs.readFileSync(STARCLOUD);
  const sc  = parseStarcloud(buf);
  console.log(`starcloud: depth=${sc.depth} half_extent_pc=${sc.halfExtentPc} nodes=${sc.nodeCount} points=${sc.pointCount}`);

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
  const projection = ORTHO ? new OrthographicProjection(halfWidth, WIDTH, HEIGHT)
                           : new PerspectiveProjection(FOV_DEG, WIDTH, HEIGHT);
  const planes     = projection.buildCullingPlanes(camera, NEAR, far);
  const rootBounds: Bounds = {
    min: [-sc.halfExtentPc, -sc.halfExtentPc, -sc.halfExtentPc],
    max: [ sc.halfExtentPc,  sc.halfExtentPc,  sc.halfExtentPc],
  };

  const ranges    = collectLeaves(sc, rootBounds, planes);
  const starCount = ranges.reduce((a, r) => a + r.count, 0);
  console.log(`leaves: ${ranges.length} node-ranges covering ${starCount} stars`);

  const hdr = new Float32Array(WIDTH * HEIGHT * 3);
  rasterize(iterateStars(sc, ranges, planes), hdr, { camera, projection, exposure });

  const pixels = tonemapToBytes(hdr, WIDTH, HEIGHT);
  writePng(OUT, WIDTH, HEIGHT, pixels);
  console.log(`Saved ${OUT} in ${((Date.now() - started) / 1000).toFixed(2)}s`);
}

main().catch((e) => { console.error(e); process.exit(1); });

import * as https from "https";
import * as http from "http";
import * as fs from "fs";

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

const API_ROOT = getArg("url");
const DATASET  = getArg("dataset");
const eyeStr   = getArg("eye", "0,0,0").split(",").map(Number) as [number, number, number];
const dirStr   = getArg("dir", "0,0,-1").split(",").map(Number) as [number, number, number];
const upStr    = getArg("up",  "0,1,0").split(",").map(Number) as [number, number, number];
const FOV_DEG  = getArgNum("fov", 60);
const FAR      = getArgNum("far", 5000);
const NEAR     = getArgNum("near", 0.1);
const WIDTH    = getArgNum("width", 1920);
const HEIGHT   = getArgNum("height", 1080);
const EXPOSURE = getArgNum("exposure", 0.001);
const OUT      = getArg("output", "stars.ppm");

// --- Math ---
type Vec3 = [number, number, number];
function normalize(v: Vec3): Vec3 { const l = Math.hypot(...v)||1; return [v[0]/l,v[1]/l,v[2]/l]; }
function cross(a: Vec3, b: Vec3): Vec3 { return [a[1]*b[2]-a[2]*b[1], a[2]*b[0]-a[0]*b[2], a[0]*b[1]-a[1]*b[0]]; }
function dot(a: Vec3, b: Vec3): number { return a[0]*b[0]+a[1]*b[1]+a[2]*b[2]; }

const forward = normalize(dirStr);
const right   = normalize(cross(forward, upStr));
const up      = cross(right, forward);
const fovy    = (FOV_DEG * Math.PI) / 180;
const aspect  = WIDTH / HEIGHT;
const tanH    = Math.tan(fovy * 0.5);

// Build quaternion from rotation matrix (Shepperd's method, all branches).
function matToQuat(r: Vec3, u: Vec3, f: Vec3): [number,number,number,number] {
  // Column-major basis: col0=right, col1=up, col2=-forward (OpenGL convention).
  const m00 = r[0], m10 = r[1], m20 = r[2];
  const m01 = u[0], m11 = u[1], m21 = u[2];
  const m02 = -f[0], m12 = -f[1], m22 = -f[2];
  const trace = m00 + m11 + m22;
  if (trace > 0) {
    const s = 0.5 / Math.sqrt(trace + 1);
    return [(m21 - m12) * s, (m02 - m20) * s, (m10 - m01) * s, 0.25 / s];
  }
  if (m00 > m11 && m00 > m22) {
    const s = 2 * Math.sqrt(1 + m00 - m11 - m22);
    return [0.25 * s, (m01 + m10) / s, (m02 + m20) / s, (m21 - m12) / s];
  }
  if (m11 > m22) {
    const s = 2 * Math.sqrt(1 + m11 - m00 - m22);
    return [(m01 + m10) / s, 0.25 * s, (m12 + m21) / s, (m02 - m20) / s];
  }
  const s = 2 * Math.sqrt(1 + m22 - m00 - m11);
  return [(m02 + m20) / s, (m12 + m21) / s, 0.25 * s, (m10 - m01) / s];
}
const [qx, qy, qz, qw] = matToQuat(right, up, forward);

// --- HTTP fetch ---
function fetchBuffer(url: string): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith("https") ? https : http;
    lib.get(url, (res) => {
      const chunks: Buffer[] = [];
      res.on("data", (c: Buffer) => chunks.push(c));
      res.on("end", () => resolve(Buffer.concat(chunks)));
      res.on("error", reject);
    }).on("error", reject);
  });
}

type LodUnit = { x: number; y: number; z: number; lum: number; bprp: number; radius: number };

async function fetchLodUnits(): Promise<LodUnit[]> {
  const params = new URLSearchParams({
    x: String(eyeStr[0]), y: String(eyeStr[1]), z: String(eyeStr[2]),
    qx: String(qx), qy: String(qy), qz: String(qz), qw: String(qw),
    near: String(NEAR), far: String(FAR),
    fovy: String(fovy), aspect: String(aspect),
    width: String(WIDTH), height: String(HEIGHT),
    limit: "20000",
  });
  const url = `${API_ROOT}/query/${DATASET}/lod-frustum?${params}`;
  console.log("Querying:", url);
  const buf = await fetchBuffer(url);
  const count = buf.readUInt32LE(0);
  const units: LodUnit[] = [];
  for (let i = 0; i < count; i++) {
    const base = 4 + i * 24;
    units.push({
      x: buf.readFloatLE(base),
      y: buf.readFloatLE(base+4),
      z: buf.readFloatLE(base+8),
      lum: buf.readFloatLE(base+12),
      bprp: buf.readFloatLE(base+16),
      radius: buf.readFloatLE(base+20),
    });
  }
  return units;
}

function project(px: number, py: number, pz: number): [number,number,number]|null {
  const rx=px-eyeStr[0], ry=py-eyeStr[1], rz=pz-eyeStr[2];
  const depth = dot([rx,ry,rz], forward);
  if (depth <= 0) return null;
  const h = dot([rx,ry,rz], right);
  const v = dot([rx,ry,rz], up);
  const sx = (h/(depth*tanH*aspect)*0.5+0.5)*WIDTH;
  const sy = (1-(v/(depth*tanH)*0.5+0.5))*HEIGHT;
  return [sx, sy, depth];
}

function bprpToColor(bprp: number): [number,number,number] {
  if (!isFinite(bprp)) return [1,1,1];
  const t = Math.max(0, Math.min(1, (bprp+0.5)/3.5));
  const lerp = (a:[number,number,number], b:[number,number,number], t:number): [number,number,number] =>
    [a[0]+(b[0]-a[0])*t, a[1]+(b[1]-a[1])*t, a[2]+(b[2]-a[2])*t];
  if (t < 0.33) return lerp([0.6,0.7,1.0],[1.0,0.95,0.9], t/0.33);
  if (t < 0.66) return lerp([1.0,0.95,0.9],[1.0,0.85,0.4], (t-0.33)/0.33);
  return lerp([1.0,0.85,0.4],[1.0,0.3,0.1], (t-0.66)/0.34);
}

async function main() {
  const units = await fetchLodUnits();
  console.log(`Got ${units.length} LOD units`);
  if (units.length > 0) {
    const lums = units.map(u => u.lum);
    console.log(`Lum range: ${Math.min(...lums).toExponential(2)} – ${Math.max(...lums).toExponential(2)}`);
  }

  const hdr = new Float32Array(WIDTH * HEIGHT * 3);
  const pixelsPerRadian = HEIGHT / fovy;

  for (const u of units) {
    const proj = project(u.x, u.y, u.z);
    if (!proj) continue;
    const [sx, sy, dist] = proj;
    const flux = u.lum / Math.max(dist*dist, 0.01);
    const brightness = flux * EXPOSURE;
    if (brightness < 1e-7) continue;
    const [cr,cg,cb] = bprpToColor(u.bprp);

    if (u.radius > 0) {
      // Aggregate: spread total flux over the angular footprint (flux-conserving).
      // Per-pixel brightness is small, producing a faint diffuse glow instead of a point.
      const footprintPx = Math.max((u.radius / dist) * pixelsPerRadian, 1);
      const sigma = footprintPx * 0.5;
      const twoSigmaSq = 2 * sigma * sigma;
      const norm = brightness / (Math.PI * twoSigmaSq);
      const ir = Math.ceil(footprintPx * 1.5);
      for (let dy=-ir; dy<=ir; dy++) {
        for (let dx=-ir; dx<=ir; dx++) {
          const xi=Math.round(sx)+dx, yi=Math.round(sy)+dy;
          if (xi<0||xi>=WIDTH||yi<0||yi>=HEIGHT) continue;
          const val = norm * Math.exp(-(dx*dx+dy*dy)/twoSigmaSq);
          const idx = (yi*WIDTH+xi)*3;
          hdr[idx]+=cr*val; hdr[idx+1]+=cg*val; hdr[idx+2]+=cb*val;
        }
      }
    } else {
      // Individual star: small Gaussian whose peak scales with brightness.
      const rPx = Math.min(Math.max(Math.sqrt(brightness)*4, 1), 8);
      const ir = Math.ceil(rPx);
      for (let dy=-ir; dy<=ir; dy++) {
        for (let dx=-ir; dx<=ir; dx++) {
          const xi=Math.round(sx)+dx, yi=Math.round(sy)+dy;
          if (xi<0||xi>=WIDTH||yi<0||yi>=HEIGHT) continue;
          const nr = Math.sqrt(dx*dx+dy*dy)/rPx;
          const val = brightness * Math.exp(-nr*nr*4);
          const idx = (yi*WIDTH+xi)*3;
          hdr[idx]+=cr*val; hdr[idx+1]+=cg*val; hdr[idx+2]+=cb*val;
        }
      }
    }
  }

  // Write PPM P6 (binary)
  const header = `P6\n${WIDTH} ${HEIGHT}\n255\n`;
  const pixels = Buffer.allocUnsafe(WIDTH * HEIGHT * 3);
  const tm = (v: number) => Math.min(255, Math.round(255 * Math.pow(v/(1+v), 1/2.2)));
  for (let i = 0; i < WIDTH*HEIGHT; i++) {
    pixels[i*3]   = tm(hdr[i*3]);
    pixels[i*3+1] = tm(hdr[i*3+1]);
    pixels[i*3+2] = tm(hdr[i*3+2]);
  }
  fs.writeFileSync(OUT, Buffer.concat([Buffer.from(header), pixels]));
  console.log(`Saved ${OUT}  (open with: open ${OUT})`);
}

main().catch(e => { console.error(e); process.exit(1); });

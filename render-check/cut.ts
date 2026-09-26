// The level-of-detail cut of a starcloud octree against a camera: the
// disjoint set of point ranges that stand in for the whole tree at the
// current view. Shared by the fast renderer and the video renderer, which
// reach the point table in different ways but walk the node table alike.

import type { Camera, Plane, Projection, Vec3 } from "./brightness";

export type Bounds = { min: Vec3; max: Vec3 };

/** Node table as parallel arrays, one entry per octree node. */
export type Nodes = {
  childMask: Uint8Array;
  firstChild: Uint32Array;
  pointFirst: Uint32Array;
  pointCount: Uint32Array;
};

/** A contiguous run of the point table, in points. */
export type Range = { firstPoint: number; count: number };

export function cubeBounds(halfExtent: number): Bounds {
  return {
    min: [-halfExtent, -halfExtent, -halfExtent],
    max: [halfExtent, halfExtent, halfExtent],
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

function viewIntersectsBounds(planes: Plane[], b: Bounds): boolean {
  for (const p of planes) {
    const cx = p.nx >= 0 ? b.max[0] : b.min[0];
    const cy = p.ny >= 0 ? b.max[1] : b.min[1];
    const cz = p.nz >= 0 ? b.max[2] : b.min[2];
    if (p.nx * cx + p.ny * cy + p.nz * cz + p.d < 0) return false;
  }
  return true;
}

export function pointInView(planes: Plane[], px: number, py: number, pz: number): boolean {
  for (const p of planes) {
    if (p.nx * px + p.ny * py + p.nz * pz + p.d < 0) return false;
  }
  return true;
}

/**
 * Walk the tree greedily and stop wherever a node's screen footprint has
 * shrunk below `pixelThreshold`, standing in for its subtree with the
 * flux-boosted subsample it carries. Leaves have no subsample and are
 * therefore always taken whole.
 */
export function collectCut(
  nodes: Nodes,
  rootBounds: Bounds,
  camera: Camera,
  planes: Plane[],
  projection: Projection,
  pixelThreshold: number,
): Range[] {
  const out: Range[] = [];

  function walk(nodeIdx: number, bounds: Bounds): void {
    if (!viewIntersectsBounds(planes, bounds)) return;
    const mask = nodes.childMask[nodeIdx];
    const count = nodes.pointCount[nodeIdx];
    const first = nodes.pointFirst[nodeIdx];

    if (mask === 0) {
      if (count > 0) out.push({ firstPoint: first, count });
      return;
    }

    const half = (bounds.max[0] - bounds.min[0]) * 0.5;
    const dx = (bounds.min[0] + bounds.max[0]) * 0.5 - camera.eye[0];
    const dy = (bounds.min[1] + bounds.max[1]) * 0.5 - camera.eye[1];
    const dz = (bounds.min[2] + bounds.max[2]) * 0.5 - camera.eye[2];
    const dist = Math.max(Math.hypot(dx, dy, dz), half);

    if (projection.footprintPx(half, dist, camera) < pixelThreshold && count > 0) {
      out.push({ firstPoint: first, count });
      return;
    }

    let childIdx = nodes.firstChild[nodeIdx];
    for (let c = 0; c < 8; c++) {
      if ((mask & (1 << c)) === 0) continue;
      walk(childIdx, childBounds(bounds, c));
      childIdx++;
    }
  }

  if (nodes.childMask.length > 0) walk(0, rootBounds);
  return out;
}

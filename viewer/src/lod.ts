// Level-of-detail selection: which nodes to draw, and which to stream.
//
// The tree is refined greedily, largest screen footprint first, until either
// every remaining node is smaller than the pixel threshold or the point budget
// is spent. Everything left unrefined is a cut node drawn from its own
// subsample, so the budget caps what reaches the GPU wherever the camera sits.

import { Frustum, sphereVisible } from "./frustum";
import { MaxHeap } from "./heap";
import { Starcloud, childCount } from "./starcloud";
import { Vec3 } from "./vec3";

/** Where a node's points currently live on the GPU. */
export type Location = { batch: number; first: number };

export type Locate = (node: number) => Location | undefined;

/** A node whose points the viewer wants, and its screen footprint in pixels. */
export type Wanted = { node: number; footprint: number };

export type View = { eye: Vec3; frustum: Frustum; pixelsPerRadian: number };

/** Per-node outcome of a selection pass. */
const CUT      = 0;
const EXPANDED = 1;
const CULLED   = 2;

export type Cut = {
  wanted: Wanted[];
  /** One CUT / EXPANDED / CULLED entry per node. */
  state: Uint8Array;
};

const SQRT3 = Math.sqrt(3);

function footprint(sc: Starcloud, node: number, view: View): number {
  const half = sc.halfSize[node];
  const dx = sc.center[node * 3]     - view.eye[0];
  const dy = sc.center[node * 3 + 1] - view.eye[1];
  const dz = sc.center[node * 3 + 2] - view.eye[2];
  const distance = Math.max(Math.hypot(dx, dy, dz), half);
  return (half / distance) * view.pixelsPerRadian;
}

function visible(sc: Starcloud, node: number, view: View): boolean {
  return sphereVisible(
    view.frustum,
    sc.center[node * 3], sc.center[node * 3 + 1], sc.center[node * 3 + 2],
    sc.halfSize[node] * SQRT3,
  );
}

export function selectCut(
  sc: Starcloud,
  view: View,
  pixelThreshold: number,
  pointBudget: number,
  state: Uint8Array,
): Cut {
  state.fill(CUT);
  const wanted: Wanted[] = [];
  const heap = new MaxHeap();

  const want = (node: number) => {
    const size = footprint(sc, node, view);
    wanted.push({ node, footprint: size });
    heap.push(node, size);
  };

  want(0);
  let points = sc.pointCount[0];
  while (heap.size > 0) {
    const node = heap.pop();
    const mask = sc.childMask[node];
    const own = sc.pointCount[node];
    if (mask === 0) continue;
    // A node with no subsample of its own carries no image; always refine it.
    if (own > 0 && footprint(sc, node, view) < pixelThreshold) continue;

    const first = sc.firstChild[node];
    const children = childCount(mask);
    let childPoints = 0;
    for (let i = 0; i < children; i++) {
      if (!visible(sc, first + i, view)) {
        state[first + i] = CULLED;
        continue;
      }
      childPoints += sc.pointCount[first + i];
    }
    if (points + childPoints - own > pointBudget) break;

    points += childPoints - own;
    state[node] = EXPANDED;
    for (let i = 0; i < children; i++) {
      if (state[first + i] !== CULLED) want(first + i);
    }
  }
  return { wanted, state };
}

/** Joins ranges that are adjacent within the same batch into single draws. */
function merge(ranges: number[]): Int32Array {
  const out: number[] = [];
  for (let i = 0; i < ranges.length; i += 3) {
    const n = out.length;
    if (n > 0 && out[n - 3] === ranges[i] && out[n - 2] + out[n - 1] === ranges[i + 1]) {
      out[n - 1] += ranges[i + 2];
    } else {
      out.push(ranges[i], ranges[i + 1], ranges[i + 2]);
    }
  }
  return Int32Array.from(out);
}

/**
 * Draw ranges as `[batch, firstPoint, pointCount]` triples. Where a refined
 * subtree is not fully resident yet the ancestor's own subsample is drawn
 * instead, so streaming fills detail in rather than punching holes.
 */
export function collectDraws(sc: Starcloud, cut: Cut, locate: Locate): Int32Array {
  const ranges: number[] = [];

  function drawSelf(node: number): boolean {
    const count = sc.pointCount[node];
    const at = count > 0 ? locate(node) : undefined;
    if (!at) return false;
    ranges.push(at.batch, at.first, count);
    return true;
  }

  function emit(node: number): boolean {
    if (cut.state[node] === CULLED) return true;
    if (cut.state[node] === CUT) return sc.pointCount[node] === 0 || drawSelf(node);
    const mark = ranges.length;
    const first = sc.firstChild[node];
    const children = childCount(sc.childMask[node]);
    for (let i = 0; i < children; i++) {
      if (emit(first + i)) continue;
      ranges.length = mark;
      return drawSelf(node);
    }
    return true;
  }

  emit(0);
  return merge(ranges);
}

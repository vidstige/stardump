// Level-of-detail selection: which nodes to draw, and which to stream.
//
// The tree is refined greedily, largest screen footprint first, until either
// every remaining node is smaller than the pixel threshold or the point budget
// is spent. Everything left unrefined is a cut node drawn from its own
// subsample, so the budget caps what reaches the GPU wherever the camera sits.
//
// Nodes outside the frustum are refined too, but only down to a coarser
// threshold. That base layer is never drawn; it exists so that turning the
// camera lands on data that is already resident, coarse at first and refining
// within a pass or two, instead of on a hole.

import { Frustum, sphereVisible } from "./frustum";
import { MaxHeap } from "./heap";
import { Starcloud, childCount } from "./starcloud";
import { Vec3 } from "./vec3";

/** Where a node's points currently live on the GPU. */
export type Location = { batch: number; first: number };

export type Locate = (node: number) => Location | undefined;

/** A node whose points the viewer wants, and how badly. */
export type Wanted = { node: number; priority: number };

export type View = { eye: Vec3; frustum: Frustum; pixelsPerRadian: number };

export type Limits = {
  pixelThreshold: number;
  /** Points the visible cut may use. */
  pointBudget: number;
  /** Points the off-screen base layer may use. */
  baseBudget: number;
};

/** Per-node outcome of a selection pass. */
const CUT      = 0;
const EXPANDED = 1;

/** How much coarser than the visible cut the off-screen base layer is kept. */
const OFFSCREEN_FACTOR = 4;

/** Base layer nodes queue behind everything on screen, however large. */
const OFFSCREEN_PRIORITY = 0.01;

export type Cut = {
  wanted: Wanted[];
  /** CUT or EXPANDED, per node. */
  state: Uint8Array;
  /** 1 for nodes inside the frustum, the only ones that get drawn. */
  visible: Uint8Array;
};

export function emptyCut(sc: Starcloud): Cut {
  const nodes = sc.childMask.length;
  return { wanted: [], state: new Uint8Array(nodes), visible: new Uint8Array(nodes) };
}

const SQRT3 = Math.sqrt(3);

function footprint(sc: Starcloud, node: number, view: View): number {
  const half = sc.halfSize[node];
  const dx = sc.center[node * 3]     - view.eye[0];
  const dy = sc.center[node * 3 + 1] - view.eye[1];
  const dz = sc.center[node * 3 + 2] - view.eye[2];
  const distance = Math.max(Math.hypot(dx, dy, dz), half);
  return (half / distance) * view.pixelsPerRadian;
}

function inFrustum(sc: Starcloud, node: number, view: View): boolean {
  return sphereVisible(
    view.frustum,
    sc.center[node * 3], sc.center[node * 3 + 1], sc.center[node * 3 + 2],
    sc.halfSize[node] * SQRT3,
  );
}

export function selectCut(sc: Starcloud, view: View, limits: Limits, cut: Cut): Cut {
  const { wanted, state, visible } = cut;
  wanted.length = 0;
  state.fill(CUT);
  visible.fill(0);
  visible[0] = 1;
  const heap = new MaxHeap();

  const want = (node: number, seen: boolean) => {
    const size = footprint(sc, node, view);
    wanted.push({ node, priority: seen ? size : size * OFFSCREEN_PRIORITY });
    heap.push(node, size);
  };

  want(0, true);
  const spent = [sc.pointCount[0], 0];
  const budget = [limits.pointBudget, limits.baseBudget];
  while (heap.size > 0) {
    const node = heap.pop();
    const mask = sc.childMask[node];
    const own = sc.pointCount[node];
    if (mask === 0) continue;
    // A node with no subsample of its own carries no image; always refine it.
    if (own > 0 && footprint(sc, node, view) < limits.pixelThreshold) continue;

    const first = sc.firstChild[node];
    const children = childCount(mask);
    let keep = 0;
    let childPoints = 0;
    for (let i = 0; i < children; i++) {
      const child = first + i;
      const seen = inFrustum(sc, child, view);
      if (!seen && footprint(sc, child, view) < limits.pixelThreshold * OFFSCREEN_FACTOR) {
        continue;
      }
      visible[child] = seen ? 1 : 0;
      keep |= 1 << i;
      childPoints += sc.pointCount[child];
    }
    // On screen and off screen draw on separate budgets, so a busy view behind
    // the camera can never crowd out detail in front of it.
    const purse = visible[node] === 1 ? 0 : 1;
    if (spent[purse] + childPoints - own > budget[purse]) continue;

    spent[purse] += childPoints - own;
    state[node] = EXPANDED;
    for (let i = 0; i < children; i++) {
      if (keep & (1 << i)) want(first + i, visible[first + i] === 1);
    }
  }
  return cut;
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
    if (cut.visible[node] === 0) return true;
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

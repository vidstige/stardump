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

/** The nodes a cut actually draws: the ones it did not refine past. */
export function drawn(sc: Starcloud, cut: Cut): Wanted[] {
  return cut.wanted.filter(
    (w) => cut.state[w.node] === CUT && sc.pointCount[w.node] > 0,
  );
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

/** Floats per span in Draws.stands. */
export const STAND_FLOATS = 5;

/**
 * Set in the mask of every stand-in, above the eight octant bits, so that one
 * with no octant covered yet is still told apart from an ordinary span.
 */
const STAND = 256;

/** What a frame draws: spans of resident buffers, some standing in for others. */
export type Draws = {
  /** [batch, firstPoint, pointCount] per span. */
  ranges: Int32Array;
  /**
   * [cx, cy, cz, boost, mask] per span. A mask of zero is an ordinary span.
   * Otherwise the span is a node standing in for its subtree: the shader
   * divides the boost back out and draws the subsample as the real stars it
   * is, and the low eight bits name the octants where something finer is
   * already drawn, which it leaves to that.
   */
  stands: Float32Array;
};

/**
 * Joins ordinary spans that are adjacent within the same batch into single
 * draws. A stand-in keeps its own span, since its mask applies to it alone.
 */
function merge(ranges: number[], stands: number[]): Draws {
  const outRanges: number[] = [];
  const outStands: number[] = [];
  for (let i = 0, j = 0; i < ranges.length; i += 3, j += STAND_FLOATS) {
    const n = outRanges.length;
    const plain = stands[j + 4] === 0 && n > 0 && outStands[outStands.length - 1] === 0;
    if (plain && outRanges[n - 3] === ranges[i] &&
        outRanges[n - 2] + outRanges[n - 1] === ranges[i + 1]) {
      outRanges[n - 1] += ranges[i + 2];
    } else {
      outRanges.push(ranges[i], ranges[i + 1], ranges[i + 2]);
      for (let k = 0; k < STAND_FLOATS; k++) outStands.push(stands[j + k]);
    }
  }
  return { ranges: Int32Array.from(outRanges), stands: Float32Array.from(outStands) };
}

/**
 * Collects the spans a frame draws.
 *
 * Everything visible is represented at every moment. A node whose subtree
 * has fully arrived is drawn through its children; one whose subtree is still
 * coming is drawn from its own subsample, but only in the octants where
 * nothing finer is there yet. So detail lands star by star where it lands,
 * the coarse picture recedes octant by octant beneath it, and no star is
 * drawn twice. Nothing waits for a sibling, and nothing is left black while
 * it waits.
 *
 * The subsample is shown at true brightness, not with the boost the index
 * gave it. The boost conserves flux only from far away, and its variance is
 * ruinous as an image: one boosted giant is a thousand giants of light in a
 * single disc. Unboosted, the coarse view is a sparse field of real stars,
 * exactly the ones the leaf will bring back among the rest.
 */
export function collectDraws(sc: Starcloud, cut: Cut, locate: Locate): Draws {
  const ranges: number[] = [];
  const stands: number[] = [];

  const resident = (node: number) => sc.pointCount[node] > 0 && locate(node) !== undefined;

  function span(node: number, mask: number): void {
    const at = locate(node)!;
    ranges.push(at.batch, at.first, sc.pointCount[node]);
    stands.push(
      sc.center[node * 3], sc.center[node * 3 + 1], sc.center[node * 3 + 2],
      sc.boost[node], mask,
    );
  }

  /** Draws what there is of the node; true if its box is represented at all. */
  function emit(node: number): boolean {
    if (cut.state[node] === CULLED) return true;
    if (cut.state[node] !== EXPANDED) {
      const here = resident(node);
      if (here) span(node, 0);
      return here || sc.pointCount[node] === 0;
    }
    // Octant bits, so the mask lines up with the shader and not with the
    // packed child slots.
    const mask = sc.childMask[node];
    let covered = 0;
    let child = sc.firstChild[node];
    for (let bit = 1; bit < 256; bit <<= 1) {
      if ((mask & bit) === 0) continue;
      if (emit(child++)) covered |= bit;
    }
    if (covered === mask) return true;
    if (resident(node)) {
      span(node, STAND | covered);
      return true;
    }
    return covered !== 0;
  }

  emit(0);
  return merge(ranges, stands);
}

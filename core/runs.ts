// Grouping wanted nodes into runs that can be fetched as one request.
//
// Points are written depth first, a node's own subsample ahead of its subtree,
// so nodes that are wanted together tend to be adjacent in the point table. A
// run is a maximal set of them that already is, which means one range request
// yields one buffer that is uploaded verbatim and drawn as a single span, with
// no repacking anywhere.

import { Wanted } from "./lod";
import { POINT_BYTES, Starcloud } from "./starcloud";

export type Run = { nodes: number[]; footprint: number; bytes: number };

/** `wanted` must already be sorted by `pointFirst`. */
export function groupRuns(sc: Starcloud, wanted: Wanted[], maxBytes: number): Run[] {
  const adjacent = (run: Run, node: number) => {
    const tail = run.nodes[run.nodes.length - 1];
    return sc.pointFirst[node] === sc.pointFirst[tail] + sc.pointCount[tail];
  };

  const grouped: Run[] = [];
  for (const { node, footprint } of wanted) {
    const bytes = sc.pointCount[node] * POINT_BYTES;
    const run = grouped[grouped.length - 1];
    if (run && adjacent(run, node) && run.bytes + bytes <= maxBytes) {
      run.nodes.push(node);
      run.bytes += bytes;
      run.footprint = Math.max(run.footprint, footprint);
    } else {
      grouped.push({ nodes: [node], footprint, bytes });
    }
  }
  return grouped;
}

/** Byte range of a whole run within starcloud.bin, both ends inclusive. */
export function runBytes(sc: Starcloud, run: Run): [number, number] {
  const first = sc.pointFirst[run.nodes[0]];
  const last = run.nodes[run.nodes.length - 1];
  return [
    sc.pointsOffset + first * POINT_BYTES,
    sc.pointsOffset + (sc.pointFirst[last] + sc.pointCount[last]) * POINT_BYTES - 1,
  ];
}

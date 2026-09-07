// Streaming cache of point data living on the GPU.
//
// Wanted nodes are grouped into batches: maximal runs that are already adjacent
// in the point table, so one range request yields one vertex buffer that is
// uploaded verbatim, with no repacking. Batches nobody wants any more are freed
// least-recently-wanted first once the memory budget is exceeded.

import { Locate, Location, Wanted } from "./lod";
import { POINT_BYTES, Starcloud } from "./starcloud";
import { fetchRange } from "./starcloud_io";

const MAX_BATCH_BYTES = 1 << 20;
const MAX_REQUESTS    = 12;

type Batch = { id: number; nodes: number[]; bytes: number; lastWanted: number };

type Run = { nodes: number[]; footprint: number; bytes: number };

export type Cache = {
  locate: Locate;
  /** `memoryBudget` caps what is kept beyond the nodes currently wanted. */
  update(wanted: Wanted[], memoryBudget: number): void;
};

export function createCache(
  sc: Starcloud,
  url: string,
  onUpload: (batch: number, data: ArrayBuffer) => void,
  onFree: (batch: number) => void,
): Cache {
  const batches   = new Map<number, Batch>();
  const locations = new Map<number, Location>();
  const inFlight  = new Set<number>();
  let requests = 0;
  let nextBatch = 0;
  let resident = 0;
  let tick = 0;

  function drop(batch: Batch): void {
    for (const node of batch.nodes) locations.delete(node);
    batches.delete(batch.id);
    resident -= batch.bytes;
    onFree(batch.id);
  }

  function evict(wantedNodes: Set<number>, memoryBudget: number): void {
    const idle = [...batches.values()]
      .filter((batch) => !batch.nodes.some((node) => wantedNodes.has(node)))
      .sort((a, b) => a.lastWanted - b.lastWanted);
    for (const batch of idle) {
      if (resident <= memoryBudget) return;
      drop(batch);
    }
  }

  async function load(nodes: number[]): Promise<void> {
    for (const node of nodes) inFlight.add(node);
    requests++;
    try {
      const first = sc.pointFirst[nodes[0]];
      const last = nodes[nodes.length - 1];
      const start = sc.pointsOffset + first * POINT_BYTES;
      const end = sc.pointsOffset + (sc.pointFirst[last] + sc.pointCount[last]) * POINT_BYTES - 1;
      const data = await fetchRange(url, start, end);
      const id = nextBatch++;
      batches.set(id, { id, nodes, bytes: data.byteLength, lastWanted: tick });
      for (const node of nodes) locations.set(node, { batch: id, first: sc.pointFirst[node] - first });
      resident += data.byteLength;
      onUpload(id, data);
    } finally {
      requests--;
      for (const node of nodes) inFlight.delete(node);
    }
  }

  function adjacent(run: Run, node: number): boolean {
    const tail = run.nodes[run.nodes.length - 1];
    return sc.pointFirst[node] === sc.pointFirst[tail] + sc.pointCount[tail];
  }

  /** Groups the missing nodes into runs that are contiguous on the server. */
  function runs(wanted: Wanted[]): Run[] {
    const missing = wanted
      .filter((w) => sc.pointCount[w.node] > 0 && !locations.has(w.node) && !inFlight.has(w.node))
      .sort((a, b) => sc.pointFirst[a.node] - sc.pointFirst[b.node]);

    const grouped: Run[] = [];
    for (const { node, footprint } of missing) {
      const bytes = sc.pointCount[node] * POINT_BYTES;
      const run = grouped[grouped.length - 1];
      if (run && adjacent(run, node) && run.bytes + bytes <= MAX_BATCH_BYTES) {
        run.nodes.push(node);
        run.bytes += bytes;
        run.footprint = Math.max(run.footprint, footprint);
      } else {
        grouped.push({ nodes: [node], footprint, bytes });
      }
    }
    return grouped;
  }

  function schedule(wanted: Wanted[]): void {
    const slots = MAX_REQUESTS - requests;
    if (slots <= 0) return;
    // Biggest on screen first, which also means shallow nodes before deep ones.
    const queue = runs(wanted).sort((a, b) => b.footprint - a.footprint);
    for (let i = 0; i < slots && i < queue.length; i++) void load(queue[i].nodes);
  }

  return {
    locate: (node) => locations.get(node),
    update(wanted, memoryBudget) {
      tick++;
      const wantedNodes = new Set<number>();
      for (const { node } of wanted) {
        wantedNodes.add(node);
        const at = locations.get(node);
        if (at) batches.get(at.batch)!.lastWanted = tick;
      }
      evict(wantedNodes, memoryBudget);
      schedule(wanted);
    },
  };
}

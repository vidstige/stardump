// Streaming cache of point data living on the GPU.
//
// Wanted nodes are grouped into batches: maximal runs that are already adjacent
// in the point table, so one range request yields one vertex buffer that is
// uploaded verbatim, with no repacking. Batches nobody wants any more are freed
// least-recently-wanted first once the memory budget is exceeded.
//
// Fetch order is screen area per byte. An interior node is a few kilobytes
// standing in for a whole region of sky; a leaf near the camera is a megabyte
// that sharpens one small box. Taking the cheap wide ones first is what makes a
// view fill in coarse everywhere and then sharpen, instead of arriving as a
// handful of sharp patches in the dark.

import { Locate, Location, Wanted } from "./lod";
import { Run, groupRuns, runBytes } from "./runs";
import { Starcloud } from "./starcloud";
import { ReadRange } from "./starcloud_io";

const MAX_BATCH_BYTES = 1 << 20;
// Two caps: one on bytes, which is what the pipe fills on, and one on count,
// so that a thousand five-kilobyte subsamples still go many at a time rather
// than queueing behind each other on latency.
const MAX_IN_FLIGHT_BYTES = 12 << 20;
const MAX_REQUESTS        = 48;

type Batch = { id: number; nodes: number[]; bytes: number; lastWanted: number };

export type Cache = {
  locate: Locate;
  /** Whether anything is still queued or in flight from the last update. */
  busy(): boolean;
  /** `memoryBudget` caps what is kept beyond the nodes currently wanted. */
  update(wanted: Wanted[], memoryBudget: number): void;
};

export function createCache(
  sc: Starcloud,
  read: ReadRange,
  onUpload: (batch: number, data: ArrayBuffer) => void,
  onFree: (batch: number) => void,
): Cache {
  const batches   = new Map<number, Batch>();
  const locations = new Map<number, Location>();
  const inFlight  = new Set<number>();
  let requests = 0;
  let inFlightBytes = 0;
  // Least valuable first, so pump takes the most valuable off the end.
  let queue: Run[] = [];
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

  /** Screen area a run improves per byte it costs to fetch. */
  const value = (run: Run) => (run.footprint * run.footprint) / run.bytes;

  async function load(run: Run): Promise<void> {
    const { nodes } = run;
    for (const node of nodes) inFlight.add(node);
    requests++;
    inFlightBytes += run.bytes;
    try {
      const first = sc.pointFirst[nodes[0]];
      const [start, end] = runBytes(sc, run);
      const data = await read(start, end);
      const id = nextBatch++;
      batches.set(id, { id, nodes, bytes: data.byteLength, lastWanted: tick });
      for (const node of nodes) locations.set(node, { batch: id, first: sc.pointFirst[node] - first });
      resident += data.byteLength;
      onUpload(id, data);
    } finally {
      requests--;
      inFlightBytes -= run.bytes;
      for (const node of nodes) inFlight.delete(node);
    }
    // Keep the pipe full rather than waiting for the next selection pass.
    pump();
  }

  /** Groups the missing nodes into runs that are contiguous on the server. */
  function runs(wanted: Wanted[]): Run[] {
    const missing = wanted
      .filter((w) => sc.pointCount[w.node] > 0 && !locations.has(w.node) && !inFlight.has(w.node))
      .sort((a, b) => sc.pointFirst[a.node] - sc.pointFirst[b.node]);
    return groupRuns(sc, missing, MAX_BATCH_BYTES);
  }

  /**
   * The queue is built once per selection pass and drained as requests
   * complete. Rebuilding it per completed batch means sorting every wanted
   * node again, which at a cut of tens of thousands of nodes costs far more
   * than the reads it schedules.
   */
  function pump(): void {
    while (requests < MAX_REQUESTS && queue.length > 0) {
      const next = queue[queue.length - 1];
      if (requests > 0 && inFlightBytes + next.bytes > MAX_IN_FLIGHT_BYTES) return;
      queue.pop();
      void load(next);
    }
  }

  return {
    locate: (node) => locations.get(node),
    busy: () => requests > 0 || queue.length > 0,
    update(wanted, memoryBudget) {
      tick++;
      const wantedNodes = new Set<number>();
      for (const { node } of wanted) {
        wantedNodes.add(node);
        const at = locations.get(node);
        if (at) batches.get(at.batch)!.lastWanted = tick;
      }
      evict(wantedNodes, memoryBudget);
      queue = runs(wanted).sort((a, b) => value(a) - value(b));
      pump();
    },
  };
}

// Owns the octree, the level-of-detail cut and the streaming cache, so the
// page thread only ever binds buffers and draws.

import { Frustum } from "../../core/frustum";
import { View, collectDraws, selectCut } from "../../core/lod";
import { FromWorker, ToWorker } from "./protocol";
import { Cache, createCache } from "../../core/residency";
import { POINT_BYTES, Starcloud } from "../../core/starcloud";
import { httpRange, loadStarcloud } from "../../core/starcloud_io";

const SELECT_INTERVAL_MS = 100;
/** Room kept for nodes that have dropped out of view, over the cut itself. */
const CACHE_FACTOR = 1.5;

let starcloud: Starcloud | null = null;
let cache: Cache;
let state: Uint8Array;

let stale = true;
// Buffers the page must drop, held back until the draw list stops naming them.
const freed: number[] = [];
let selectedFrustum: Frustum = new Float32Array(24);
let selectedThreshold = 0;
let selectedBudget = 0;
let selectedAt = 0;

function post(message: FromWorker, transfer: Transferable[] = []): void {
  self.postMessage(message, transfer);
}

async function init(url: string): Promise<void> {
  const read = httpRange(url);
  const sc = await loadStarcloud(read);
  state = new Uint8Array(sc.childMask.length);
  cache = createCache(
    sc, read,
    (batch, data) => { post({ type: "upload", batch, data }, [data]); stale = true; },
    (batch) => freed.push(batch),
  );
  starcloud = sc;
  stale = true;
  post({ type: "ready", halfExtentPc: sc.halfExtentPc });
}

function refresh(sc: Starcloud, view: View, pixelThreshold: number, pointBudget: number): void {
  const cut = selectCut(sc, view, pixelThreshold, pointBudget, state);
  cache.update(cut.wanted, pointBudget * POINT_BYTES * CACHE_FACTOR);
  const ranges = collectDraws(sc, cut, cache.locate);
  let stars = 0;
  for (let i = 2; i < ranges.length; i += 3) stars += ranges[i];
  post({ type: "draws", ranges, stars }, [ranges.buffer]);
  for (const batch of freed) post({ type: "free", batch });
  freed.length = 0;
}

function changed(view: View, pixelThreshold: number, pointBudget: number): boolean {
  return pixelThreshold !== selectedThreshold || pointBudget !== selectedBudget ||
    view.frustum.some((value, i) => value !== selectedFrustum[i]);
}

self.addEventListener("message", (event: MessageEvent<ToWorker>) => {
  const message = event.data;
  if (message.type === "init") {
    void init(message.url);
    return;
  }
  if (!starcloud) return;

  const now = performance.now();
  if (!(stale || changed(message, message.pixelThreshold, message.pointBudget))) return;
  if (now - selectedAt < SELECT_INTERVAL_MS) return;
  stale = false;
  selectedAt = now;
  selectedFrustum = message.frustum;
  selectedThreshold = message.pixelThreshold;
  selectedBudget = message.pointBudget;
  refresh(starcloud, message, message.pixelThreshold, message.pointBudget);
});

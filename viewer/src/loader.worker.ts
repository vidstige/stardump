// Owns the octree, the level-of-detail cut and the streaming cache, so the
// page thread only ever binds buffers and draws.

import { Frustum } from "./frustum";
import { View, collectDraws, selectCut } from "./lod";
import { FromWorker, ToWorker } from "./protocol";
import { Cache, createCache } from "./residency";
import { Starcloud } from "./starcloud";
import { fetchStarcloud } from "./starcloud_io";

const POINT_BUDGET = 4_000_000;
const SELECT_INTERVAL_MS = 100;

let starcloud: Starcloud | null = null;
let cache: Cache;
let state: Uint8Array;

let stale = true;
let selectedFrustum: Frustum = new Float32Array(24);
let selectedThreshold = 0;
let selectedAt = 0;

function post(message: FromWorker, transfer: Transferable[] = []): void {
  self.postMessage(message, transfer);
}

async function init(url: string): Promise<void> {
  const sc = await fetchStarcloud(url);
  state = new Uint8Array(sc.childMask.length);
  cache = createCache(
    sc, url,
    (batch, data) => { post({ type: "upload", batch, data }, [data]); stale = true; },
    (batch) => post({ type: "free", batch }),
  );
  starcloud = sc;
  stale = true;
  post({ type: "ready", halfExtentPc: sc.halfExtentPc });
}

function refresh(sc: Starcloud, view: View, pixelThreshold: number): void {
  const cut = selectCut(sc, view, pixelThreshold, POINT_BUDGET, state);
  cache.update(cut.wanted);
  const ranges = collectDraws(sc, cut, cache.locate);
  let stars = 0;
  for (let i = 2; i < ranges.length; i += 3) stars += ranges[i];
  post({ type: "draws", ranges, stars }, [ranges.buffer]);
}

function changed(view: View, pixelThreshold: number): boolean {
  return pixelThreshold !== selectedThreshold ||
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
  if (!(stale || changed(message, message.pixelThreshold))) return;
  if (now - selectedAt < SELECT_INTERVAL_MS) return;
  stale = false;
  selectedAt = now;
  selectedFrustum = message.frustum;
  selectedThreshold = message.pixelThreshold;
  refresh(starcloud, message, message.pixelThreshold);
});

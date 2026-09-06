// Owns the octree, the level-of-detail cut and the streaming cache, so the
// page thread only ever binds buffers and draws.

import { View, collectDraws, selectCut } from "./lod";
import { Frustum } from "./frustum";
import { FromWorker, ToWorker } from "./protocol";
import { Cache, createCache } from "./residency";
import { Starcloud } from "./starcloud";
import { fetchStarcloud } from "./starcloud_io";

const PIXEL_THRESHOLD = 16;
const POINT_BUDGET    = 4_000_000;
const SELECT_INTERVAL_MS = 100;

let starcloud: Starcloud | null = null;
let cache: Cache;
let state: Uint8Array;

let view: View | null = null;
let stale = true;
let selectedFrustum: Frustum = new Float32Array(24);
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
}

function refresh(sc: Starcloud, at: View): void {
  const cut = selectCut(sc, at, PIXEL_THRESHOLD, POINT_BUDGET, state);
  cache.update(cut.wanted);
  const ranges = collectDraws(sc, cut, cache.locate);
  let stars = 0;
  for (let i = 2; i < ranges.length; i += 3) stars += ranges[i];
  post({ type: "draws", ranges, stars }, [ranges.buffer]);
}

function moved(frustum: Frustum): boolean {
  return frustum.some((value, i) => value !== selectedFrustum[i]);
}

self.addEventListener("message", (event: MessageEvent<ToWorker>) => {
  const message = event.data;
  if (message.type === "init") {
    void init(message.url);
    return;
  }
  view = message;
  if (!starcloud) return;

  const now = performance.now();
  if (!(stale || moved(view.frustum)) || now - selectedAt < SELECT_INTERVAL_MS) return;
  stale = false;
  selectedFrustum = view.frustum;
  selectedAt = now;
  refresh(starcloud, view);
});

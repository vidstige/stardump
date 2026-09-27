// Offline frame rendering: the viewer core, fulfilled the other way round.
//
// The viewer asks for a cut, draws whatever has arrived and refines over
// later frames. Here we block until every wanted node is resident and then
// draw once, so a frame is deterministic and never half loaded. Everything
// else — the octree, the cut, the streaming cache, the shaders — is the
// viewer's, unchanged.

import createContext from "gl";

import { Camera, pixelsPerRadian, projectionMatrix, viewMatrix } from "../core/camera";
import { fromViewProjection } from "../core/frustum";
import { Wanted, collectDraws, selectCut } from "../core/lod";
import { multiply } from "../core/mat4";
import { createRenderer } from "../core/renderer";
import { createCache } from "../core/residency";
import { Settings, fovY } from "../core/settings";
import { POINT_BYTES } from "../core/starcloud";
import { ReadRange, loadStarcloud } from "../core/starcloud_io";
import { SOURCES } from "./shaders";

/** Room kept for nodes the previous frame wanted, over the current cut. */
const CACHE_FACTOR = 2;
const POLL_MS = 1;

export type Frame = { rgb: Uint8Array; stars: number };

export type Frames = {
  halfExtentPc: number;
  render(camera: Camera, settings: Settings): Promise<Frame>;
};

/** Bottom-up RGBA as readPixels gives it, to top-down RGB as everyone wants it. */
function flipToRgb(rgba: Uint8Array, width: number, height: number): Uint8Array {
  const rgb = new Uint8Array(width * height * 3);
  for (let y = 0; y < height; y++) {
    let from = (height - 1 - y) * width * 4;
    let to = y * width * 3;
    for (let x = 0; x < width; x++) {
      rgb[to] = rgba[from];
      rgb[to + 1] = rgba[from + 1];
      rgb[to + 2] = rgba[from + 2];
      from += 4;
      to += 3;
    }
  }
  return rgb;
}

export async function openFrames(
  read: ReadRange, width: number, height: number,
): Promise<Frames> {
  const sc = await loadStarcloud(read);
  const gl = createContext(width, height, { preserveDrawingBuffer: true });
  const renderer = createRenderer(gl, SOURCES);
  renderer.resize(width, height);
  const cache = createCache(sc, read, renderer.upload, renderer.free);
  const state = new Uint8Array(sc.childMask.length);
  const rgba = new Uint8Array(width * height * 4);

  const pending = (wanted: Wanted[]) =>
    wanted.some((w) => sc.pointCount[w.node] > 0 && !cache.locate(w.node));

  /**
   * The budget follows the cut rather than capping it, so the cache can never
   * be asked to hold less than the frame needs and evict what it just read.
   */
  async function fill(wanted: Wanted[]): Promise<void> {
    let bytes = 0;
    for (const { node } of wanted) bytes += sc.pointCount[node] * POINT_BYTES;
    for (;;) {
      cache.update(wanted, bytes * CACHE_FACTOR);
      while (cache.busy()) await new Promise((resolve) => setTimeout(resolve, POLL_MS));
      if (!pending(wanted)) return;
    }
  }

  return {
    halfExtentPc: sc.halfExtentPc,

    async render(camera, settings) {
      const fov = fovY(settings);
      const projection = projectionMatrix(fov, width / height, settings.far);
      const view = viewMatrix(camera);
      const cut = selectCut(
        sc,
        {
          eye: camera.position,
          frustum: fromViewProjection(multiply(projection, view)),
          pixelsPerRadian: pixelsPerRadian(height, fov),
        },
        settings.pixelThreshold,
        settings.pointBudget,
        state,
      );
      await fill(cut.wanted);

      const ranges = collectDraws(sc, cut, cache.locate);
      renderer.render(projection, view, camera.position, ranges, settings);
      gl.readPixels(0, 0, width, height, gl.RGBA, gl.UNSIGNED_BYTE, rgba);

      let stars = 0;
      for (let i = 2; i < ranges.length; i += 3) stars += ranges[i];
      return { rgb: flipToRgb(rgba, width, height), stars };
    },
  };
}

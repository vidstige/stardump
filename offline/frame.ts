// Offline frame rendering: the viewer core, fulfilled the other way round.
//
// The viewer asks for a cut, draws whatever has arrived and refines over later
// frames, keeping it all on the GPU because the next frame wants most of it
// again. An offline frame has the opposite problem. Nothing is interactive, but
// the cut is enormous — at 1080p it is very nearly every star in the frustum,
// since a leaf carries no subsample to stand in for it and so is always taken
// whole, and that runs to a gigabyte of vertex buffers. Holding it took this
// machine into swap and stalled it.
//
// So it is not held. Splats accumulate additively with no depth test, which
// makes the draws order independent and splittable: the cut is read, uploaded,
// drawn and freed a chunk at a time, and the peak is CHUNK_BYTES rather than
// the whole cut. The picture is identical — addition commutes.
//
// Each frame re-reads its own cut rather than caching it across frames. That
// costs a sequential pass over the wanted bytes, which the page cache largely
// absorbs since consecutive frames want nearly the same ones, and it puts the
// memory somewhere the operating system can reclaim under pressure instead of
// in GPU allocations it cannot touch.

import createContext from "gl";

import { Camera, pixelsPerRadian, projectionMatrix, viewMatrix } from "../core/camera";
import { fromViewProjection } from "../core/frustum";
import { drawn, selectCut } from "../core/lod";
import { multiply } from "../core/mat4";
import { createRenderer } from "../core/renderer";
import { Run, groupRuns, runBytes } from "../core/runs";
import { Settings, fovY } from "../core/settings";
import { POINT_BYTES } from "../core/starcloud";
import { ReadRange, loadStarcloud } from "../core/starcloud_io";
import { SOURCES } from "./shaders";

/** How much of the cut is on the GPU at once. */
const CHUNK_BYTES = 64 << 20;
/** The single buffer every chunk is uploaded over. */
const CHUNK = 0;

export type Frame = { rgb: Uint8Array; stars: number };

export type Frames = {
  render(camera: Camera, settings: Settings): Promise<Frame>;
};

/** Bottom-up RGBA as readPixels gives it, to top-down RGB as everyone wants it. */
function flipToRgb(rgba: Uint8Array, rgb: Uint8Array, width: number, height: number): void {
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
}

export async function openFrames(
  read: ReadRange, width: number, height: number,
): Promise<Frames> {
  const sc = await loadStarcloud(read);
  const gl = createContext(width, height, { preserveDrawingBuffer: true });
  const renderer = createRenderer(gl, SOURCES);
  renderer.resize(width, height);
  const state = new Uint8Array(sc.childMask.length);
  const rgba = new Uint8Array(width * height * 4);
  const rgb = new Uint8Array(width * height * 3);

  // A run is contiguous in the point table by construction, so the whole of it
  // is one span of one buffer and one draw call.
  const span = (run: Run) => {
    let points = 0;
    for (const node of run.nodes) points += sc.pointCount[node];
    return Int32Array.of(0, 0, points);
  };

  return {
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

      const wanted = drawn(sc, cut)
        .sort((a, b) => sc.pointFirst[a.node] - sc.pointFirst[b.node]);
      const runs = groupRuns(sc, wanted, CHUNK_BYTES);

      renderer.begin(projection, view, camera.position, settings);
      // One chunk ahead, so the next read overlaps the current draw.
      let pending = runs.length > 0 ? read(...runBytes(sc, runs[0])) : null;
      let stars = 0;
      for (let i = 0; i < runs.length; i++) {
        const data = await pending!;
        pending = i + 1 < runs.length ? read(...runBytes(sc, runs[i + 1])) : null;
        // One buffer, re-specified per chunk. The read for the next chunk is
        // already in flight while this one uploads and draws.
        renderer.upload(CHUNK, data);
        renderer.draw(span(runs[i]));
        stars += runs[i].bytes / POINT_BYTES;
      }
      renderer.end();

      gl.readPixels(0, 0, width, height, gl.RGBA, gl.UNSIGNED_BYTE, rgba);
      flipToRgb(rgba, rgb, width, height);
      return { rgb, stars };
    },
  };
}

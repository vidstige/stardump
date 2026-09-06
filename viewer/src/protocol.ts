// Messages between the page and the streaming worker.

import { View } from "./lod";

export type ToWorker =
  | { type: "init"; url: string }
  | ({ type: "view"; pixelThreshold: number } & View);

export type FromWorker =
  | { type: "ready";  halfExtentPc: number }
  | { type: "upload"; batch: number; data: ArrayBuffer }
  | { type: "free";   batch: number }
  | { type: "draws";  ranges: Int32Array; stars: number };

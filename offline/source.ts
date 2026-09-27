// Where an offline render gets its index: a local dataset by default, or a
// running query API with `--url`. The viewer reaches the same file the same
// way, so the only thing that differs is which ReadRange is handed over.

import { ReadRange, httpRange } from "../core/starcloud_io";
import * as args from "./args";
import { firstDataset, starcloudPath } from "./dataset";
import { fileRange } from "./file_range";

export const dataset = args.text("dataset", firstDataset());

export function source(): Promise<ReadRange> {
  const url = args.text("url", "");
  if (url) return Promise.resolve(httpRange(`${url}/datasets/${dataset}/starcloud.bin`));
  return fileRange(starcloudPath(dataset));
}

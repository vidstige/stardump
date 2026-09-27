// Where the local datasets live, and which one to use when none is named.

import * as fs from "fs";
import * as path from "path";

import { Vec3 } from "../core/vec3";

const ROOT = path.join(__dirname, "..", "data");

export type Labels = Record<string, Vec3>;

export function starcloudPath(dataset: string): string {
  return path.join(ROOT, dataset, "starcloud.bin");
}

export function labels(dataset: string): Labels {
  return JSON.parse(fs.readFileSync(path.join(ROOT, dataset, "labels.json"), "utf8"));
}

export function firstDataset(): string {
  return fs.readdirSync(ROOT).sort()[0];
}

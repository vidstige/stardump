// Where an offline render gets its data: a local dataset by default, or a
// running query API with `--url`. The viewer reaches the same files the same
// way, so all that differs is which ReadRange is handed over and where the
// labels are read from.

import * as fs from "fs";
import * as path from "path";

import { ReadRange, httpRange } from "../core/starcloud_io";
import { Vec3 } from "../core/vec3";
import * as args from "./args";
import { fileRange } from "./file_range";

const ROOT = path.join(__dirname, "..", "data");

export type Labels = Record<string, Vec3>;

export type Source = { dataset: string; read: ReadRange; labels: Labels };

export function starcloudPath(dataset: string): string {
  return path.join(ROOT, dataset, "starcloud.bin");
}

export function firstDataset(): string {
  return fs.readdirSync(ROOT).sort()[0];
}

async function local(): Promise<Source> {
  const dataset = args.text("dataset", firstDataset());
  return {
    dataset,
    read: await fileRange(starcloudPath(dataset)),
    labels: JSON.parse(fs.readFileSync(path.join(ROOT, dataset, "labels.json"), "utf8")),
  };
}

async function remote(url: string): Promise<Source> {
  const names = await (await fetch(`${url}/indices`)).text();
  const dataset = args.text("dataset", names.trim().split("\n")[0]);
  const at = `${url}/datasets/${dataset}`;
  return {
    dataset,
    read: httpRange(`${at}/starcloud.bin`),
    labels: await (await fetch(`${at}/labels.json`)).json(),
  };
}

export function openSource(): Promise<Source> {
  const url = args.text("url", "");
  return url ? remote(url) : local();
}

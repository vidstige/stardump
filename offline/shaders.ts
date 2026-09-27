// The shared shader sources, read off disk. The browser inlines the same four
// files through its bundler instead.

import * as fs from "fs";
import * as path from "path";

import { Sources } from "../core/renderer";

const read = (name: string) =>
  fs.readFileSync(path.join(__dirname, "..", "core", name), "utf8");

export const SOURCES: Sources = {
  starsVert:   read("stars.vert.glsl"),
  starsFrag:   read("stars.frag.glsl"),
  tonemapVert: read("tonemap.vert.glsl"),
  tonemapFrag: read("tonemap.frag.glsl"),
};

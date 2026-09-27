// The shared shader sources, inlined by the bundler. Node reads the same
// files off disk instead, which is the only difference between the two.

import { Sources } from "../../core/renderer";
import starsVert from "../../core/stars.vert.glsl";
import starsFrag from "../../core/stars.frag.glsl";
import tonemapVert from "../../core/tonemap.vert.glsl";
import tonemapFrag from "../../core/tonemap.frag.glsl";

export const SOURCES: Sources = { starsVert, starsFrag, tonemapVert, tonemapFrag };

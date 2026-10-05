// Long options for the offline tools: --name value, or --name alone for a flag.

import { DEFAULT_SETTINGS, Settings } from "../core/settings";
import { Vec3 } from "../core/vec3";
import { Quality } from "./encode";

const argv = process.argv.slice(2);

function value(name: string): string | undefined {
  const at = argv.indexOf(`--${name}`);
  return at === -1 ? undefined : argv[at + 1];
}

export function flag(name: string): boolean {
  return argv.includes(`--${name}`);
}

export function text(name: string, fallback: string): string {
  return value(name) ?? fallback;
}

export function number(name: string, fallback: number): number {
  const given = value(name);
  return given === undefined ? fallback : parseFloat(given);
}

export function vec3(name: string, fallback: Vec3): Vec3 {
  const given = value(name);
  return given === undefined ? fallback : (given.split(",").map(Number) as Vec3);
}

/** The viewer's own defaults, each with a flag to override it. */
export function settings(): Settings {
  return {
    ...DEFAULT_SETTINGS,
    exposure: number("exposure", DEFAULT_SETTINGS.exposure),
    sizeScale: number("size", DEFAULT_SETTINGS.sizeScale),
    minRadius: number("size-min", DEFAULT_SETTINGS.minRadius),
    maxRadius: number("size-cap", DEFAULT_SETTINGS.maxRadius),
    pixelThreshold: number("detail", DEFAULT_SETTINGS.pixelThreshold),
    pointBudget: number("budget", DEFAULT_SETTINGS.pointBudget),
    fovDeg: number("fov", DEFAULT_SETTINGS.fovDeg),
    far: number("far", DEFAULT_SETTINGS.far),
  };
}

/** Encoding for a final render, which can afford whatever the encoder wants. */
export function quality(): Quality {
  return {
    crf: number("crf", 12),
    preset: text("preset", "slow"),
    pixelFormat: text("pix-fmt", "yuv420p"),
  };
}

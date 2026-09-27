// Long options for the offline tools: --name value, or --name alone for a flag.

import { Vec3 } from "../core/vec3";

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

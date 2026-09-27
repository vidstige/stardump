// Named points of interest, drawn on a 2D overlay above the star field.

import { Mat4, transform } from "../../core/mat4";
import { Vec3, subtract } from "../../core/vec3";

export type Label = { name: string; position: Vec3 };

/** Labels fade out between these distances, in parsecs. */
const FADE_START = 12;
const FADE_END   = 16;

const LEADER = 20;

export async function fetchLabels(url: string): Promise<Label[]> {
  const response = await fetch(url);
  if (!response.ok) return [];
  const named: Record<string, Vec3> = await response.json();
  return Object.entries(named).map(([name, position]) => ({ name, position }));
}

function toScreen(
  position: Vec3, view: Mat4, projection: Mat4, width: number, height: number,
): [number, number] | null {
  const [x, y, z, w] = transform(view, [...position, 1]);
  const clip = transform(projection, [x, y, z, w]);
  if (clip[3] <= 0) return null;
  return [(clip[0] / clip[3] + 1) * 0.5 * width, (1 - clip[1] / clip[3]) * 0.5 * height];
}

export function drawLabels(
  context: CanvasRenderingContext2D,
  labels: Label[],
  eye: Vec3,
  view: Mat4,
  projection: Mat4,
): void {
  const { width, height } = context.canvas;
  context.clearRect(0, 0, width, height);
  context.font = "11px 'IBM Plex Sans', system-ui, sans-serif";
  context.fillStyle = "rgba(235, 243, 255, 0.85)";
  context.strokeStyle = "rgba(235, 243, 255, 0.5)";
  context.lineWidth = 1;

  for (const label of labels) {
    const [dx, dy, dz] = subtract(label.position, eye);
    const distance = Math.hypot(dx, dy, dz);
    const alpha = 1 - (distance - FADE_START) / (FADE_END - FADE_START);
    const screen = alpha > 0 ? toScreen(label.position, view, projection, width, height) : null;
    if (!screen) continue;

    const [x, y] = screen;
    context.globalAlpha = Math.min(alpha, 1);
    context.beginPath();
    context.arc(x, y, 2, 0, Math.PI * 2);
    context.fill();
    context.beginPath();
    context.moveTo(x, y);
    context.lineTo(x + LEADER, y - LEADER);
    context.stroke();
    context.fillText(label.name, x + LEADER + 4, y - LEADER + 4);
  }
  context.globalAlpha = 1;
}

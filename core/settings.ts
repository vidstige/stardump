// The knobs the HUD exposes, shared by the renderer, the camera and the worker.

export type Settings = {
  exposure: number;
  sizeScale: number;
  maxRadius: number;
  pixelThreshold: number;
  /** Points the visible cut may use, and so how much lands on the GPU. */
  pointBudget: number;
  /** Vertical field of view, in degrees. */
  fovDeg: number;
  far: number;
};

export const DEFAULT_SETTINGS: Settings = {
  exposure: 2000,
  sizeScale: 0.02,
  maxRadius: 16,
  pixelThreshold: 16,
  pointBudget: 16_000_000,
  fovDeg: 60,
  far: 8000,
};

export function fovY(settings: Settings): number {
  return (settings.fovDeg * Math.PI) / 180;
}

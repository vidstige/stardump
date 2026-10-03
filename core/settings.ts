// The knobs the HUD exposes, shared by the renderer, the camera and the worker.

export type Settings = {
  exposure: number;
  sizeScale: number;
  /** Smallest a star may be drawn, in pixels. Zero leaves it a single pixel. */
  minRadius: number;
  maxRadius: number;
  pixelThreshold: number;
  /** Points the visible cut may use, and so how much lands on the GPU. */
  pointBudget: number;
  /**
   * How much of the boost the index baked into a subsample a stand-in keeps,
   * as an exponent: 1 shows the subsample as stored, carrying the flux of
   * every star in its box; 0 shows its stars at their own brightness.
   */
  standBoost: number;
  /** Vertical field of view, in degrees. */
  fovDeg: number;
  far: number;
};

export const DEFAULT_SETTINGS: Settings = {
  exposure: 2000,
  sizeScale: 0.02,
  minRadius: 0.8,
  maxRadius: 16,
  pixelThreshold: 16,
  pointBudget: 16_000_000,
  standBoost: 1,
  fovDeg: 60,
  far: 8000,
};

export function fovY(settings: Settings): number {
  return (settings.fovDeg * Math.PI) / 180;
}

// The knobs the HUD exposes, shared by the renderer, the camera and the worker.

export type Settings = {
  exposure: number;
  /** Standard deviations a star grows per decade of screen brightness. */
  sizeScale: number;
  /** Largest a star may get, in standard deviations. A sprite spans ten of
   *  them and the driver caps a sprite at 64 px, so beyond 6 is clipped. */
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
  sizeScale: 0.7,
  maxRadius: 4,
  pixelThreshold: 16,
  pointBudget: 16_000_000,
  fovDeg: 60,
  far: 8000,
};

export function fovY(settings: Settings): number {
  return (settings.fovDeg * Math.PI) / 180;
}

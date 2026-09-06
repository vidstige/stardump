// The knobs the HUD exposes, shared by the renderer, the camera and the worker.

export type Settings = {
  exposure: number;
  sizeScale: number;
  maxRadius: number;
  pixelThreshold: number;
  far: number;
};

export const DEFAULT_SETTINGS: Settings = {
  exposure: 2000,
  sizeScale: 2,
  maxRadius: 1.5,
  pixelThreshold: 16,
  far: 8000,
};

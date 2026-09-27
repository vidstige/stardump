import { Mat4, lookAt, perspective, transform } from "./mat4";
import { Quaternion, rotate } from "./quaternion";
import { Vec3, add } from "./vec3";

export const NEAR_PC = 0.1;

export type Camera = {
  position: Vec3;
  orientation: Quaternion;
};

export type Basis = { forward: Vec3; right: Vec3; up: Vec3 };

export function basis(camera: Camera): Basis {
  return {
    forward: rotate(camera.orientation, [0, 0, -1]),
    right:   rotate(camera.orientation, [1, 0, 0]),
    up:      rotate(camera.orientation, [0, 1, 0]),
  };
}

export function viewMatrix(camera: Camera): Mat4 {
  const { forward, up } = basis(camera);
  return lookAt(camera.position, add(camera.position, forward), up);
}

export function projectionMatrix(fovY: number, aspect: number, far: number): Mat4 {
  return perspective(fovY, aspect, NEAR_PC, far);
}

/** Screen pixels spanned by one radian of vertical field of view. */
export function pixelsPerRadian(heightPx: number, fovY: number): number {
  return heightPx / fovY;
}

/** Pixel position of a world point, or null when it is behind the camera. */
export function toScreen(
  position: Vec3, view: Mat4, projection: Mat4, width: number, height: number,
): [number, number] | null {
  const clip = transform(projection, transform(view, [...position, 1]));
  if (clip[3] <= 0) return null;
  return [(clip[0] / clip[3] + 1) * 0.5 * width, (1 - clip[1] / clip[3]) * 0.5 * height];
}

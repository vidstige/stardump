// A top-down view of the galactic plane, cropped around the camera.
//
// The index is stored in equatorial coordinates, so the map is projected onto
// galactic axes: looking down the north pole, with the galactic centre to one
// side. The camera sits at the centre of the crop and the arrow shows heading.

import { Camera, basis } from "./camera";
import { Vec3, cross, normalize, scale } from "./vec3";

/** Galactic north pole and galactic centre, equatorial J2000 cartesian. */
const NORTH_POLE: Vec3 = [-0.86703, -0.20006, 0.45673];
const CENTRE: Vec3     = [-0.05487, -0.87344, -0.48384];

const DOWN  = normalize(scale(NORTH_POLE, -1));
const RIGHT = normalize(cross(DOWN, CENTRE));
const UP    = normalize(cross(RIGHT, DOWN));

const ZOOM = 10;
const MAX_SIZE = 130;
const ARROW = 9;
const ARROW_BASE = 4;

export type Minimap = { draw(camera: Camera): void };

function project(v: Vec3, axis: Vec3): number {
  return v[0] * axis[0] + v[1] * axis[1] + v[2] * axis[2];
}

async function loadImage(url: string): Promise<HTMLImageElement | null> {
  const response = await fetch(url);
  if (!response.ok) return null;
  const objectUrl = URL.createObjectURL(await response.blob());
  const image = new Image();
  await new Promise((resolve, reject) => {
    image.onload = resolve;
    image.onerror = reject;
    image.src = objectUrl;
  });
  URL.revokeObjectURL(objectUrl);
  return image;
}

export async function loadMinimap(
  canvas: HTMLCanvasElement,
  url: string,
  halfExtentPc: number,
): Promise<Minimap | null> {
  const image = await loadImage(url);
  if (!image) return null;

  const context = canvas.getContext("2d")!;
  const fit = Math.min(MAX_SIZE / image.naturalWidth, MAX_SIZE / image.naturalHeight);
  canvas.width  = Math.round(image.naturalWidth  * fit);
  canvas.height = Math.round(image.naturalHeight * fit);
  canvas.style.display = "block";

  return {
    draw(camera) {
      const width = canvas.width;
      const height = canvas.height;
      const cropWidth = image.naturalWidth / ZOOM;
      const cropHeight = image.naturalHeight / ZOOM;

      const x = (project(camera.position, RIGHT) / halfExtentPc + 1) * 0.5;
      const y = (project(camera.position, UP) / halfExtentPc + 1) * 0.5;
      const left = Math.max(0, Math.min(image.naturalWidth  - cropWidth,
        x * image.naturalWidth - cropWidth * 0.5));
      const top = Math.max(0, Math.min(image.naturalHeight - cropHeight,
        (1 - y) * image.naturalHeight - cropHeight * 0.5));
      context.drawImage(image, left, top, cropWidth, cropHeight, 0, 0, width, height);

      const forward = basis(camera).forward;
      const heading = normalize([project(forward, RIGHT), project(forward, UP), 0]);
      const [nx, ny] = heading;
      const cx = width * 0.5;
      const cy = height * 0.5;
      context.beginPath();
      context.moveTo(cx + nx * ARROW, cy - ny * ARROW);
      context.lineTo(cx + ny * ARROW_BASE, cy + nx * ARROW_BASE);
      context.lineTo(cx - ny * ARROW_BASE, cy - nx * ARROW_BASE);
      context.closePath();
      context.fillStyle = "rgba(255, 220, 100, 0.9)";
      context.strokeStyle = "rgba(0, 0, 0, 0.6)";
      context.lineWidth = 1.5;
      context.fill();
      context.stroke();
    },
  };
}

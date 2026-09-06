// Free-flight camera controls: WASD to move, mouse to look, Q/E to roll.

import { Camera, basis } from "./camera";
import { Quaternion, fromAxisAngle, multiply, normalize as normalizeQuaternion } from "./quaternion";
import { Vec3, add, normalize, scale, subtract } from "./vec3";

const SPEED_PC_PER_S = 2;
const BOOST          = 20;
const ROLL_PER_S     = 0.25;
const RADIANS_PER_PX = 0.0025;

const MOVE_KEYS = ["KeyW", "KeyA", "KeyS", "KeyD", "KeyQ", "KeyE"];

function turn(camera: Camera, axis: Vec3, angle: number): void {
  camera.orientation = normalizeQuaternion(
    multiply(camera.orientation, fromAxisAngle(axis, angle)),
  );
}

/** Wires input to `camera` and returns a per-frame update taking seconds. */
export function attachControls(canvas: HTMLCanvasElement, camera: Camera): (dt: number) => void {
  const pressed = new Set<string>();

  const onMouseMove = (event: MouseEvent) => {
    turn(camera, [0, 1, 0], -event.movementX * RADIANS_PER_PX);
    turn(camera, [1, 0, 0], -event.movementY * RADIANS_PER_PX);
  };

  canvas.addEventListener("click", () => {
    if (document.pointerLockElement === canvas) document.exitPointerLock();
    else void canvas.requestPointerLock();
  });

  document.addEventListener("pointerlockchange", () => {
    if (document.pointerLockElement === canvas) {
      document.addEventListener("mousemove", onMouseMove);
    } else {
      document.removeEventListener("mousemove", onMouseMove);
      pressed.clear();
    }
  });

  window.addEventListener("keydown", (event) => {
    pressed.add(event.code);
    if (MOVE_KEYS.includes(event.code)) event.preventDefault();
  });
  window.addEventListener("keyup", (event) => pressed.delete(event.code));
  window.addEventListener("blur", () => pressed.clear());

  return (dt: number) => {
    const { forward, right } = basis(camera);
    let movement: Vec3 = [0, 0, 0];
    if (pressed.has("KeyW")) movement = add(movement, forward);
    if (pressed.has("KeyS")) movement = subtract(movement, forward);
    if (pressed.has("KeyD")) movement = add(movement, right);
    if (pressed.has("KeyA")) movement = subtract(movement, right);

    const boost = pressed.has("ShiftLeft") || pressed.has("ShiftRight") ? BOOST : 1;
    if (movement[0] || movement[1] || movement[2]) {
      const step = normalize(movement);
      camera.position = add(camera.position, scale(step, SPEED_PC_PER_S * boost * dt));
    }

    if (pressed.has("KeyQ")) turn(camera, [0, 0, 1], -ROLL_PER_S * dt);
    if (pressed.has("KeyE")) turn(camera, [0, 0, 1],  ROLL_PER_S * dt);
  };
}

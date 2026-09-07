// Page entry point: canvas, camera, and the pipe between worker and renderer.

import { Camera, pixelsPerRadian, projectionMatrix, viewMatrix } from "./camera";
import { attachControls } from "./controls";
import { fromViewProjection } from "./frustum";
import { Endpoint, createHud } from "./hud";
import { Label, drawLabels, fetchLabels } from "./labels";
import { multiply } from "./mat4";
import { Minimap, loadMinimap } from "./minimap";
import { FromWorker, ToWorker } from "./protocol";
import { createRenderer } from "./renderer";
import { DEFAULT_SETTINGS } from "./settings";
import { Vec3, subtract } from "./vec3";

const ENDPOINTS: Endpoint[] = [
  { label: "Cloud Run", url: "https://star-dump-query-api-494247280614.europe-west1.run.app" },
  { label: "Local", url: "http://127.0.0.1:3000" },
];

const params = new URLSearchParams(window.location.search);
const onLoopback = ["localhost", "127.0.0.1"].includes(window.location.hostname);
const api = params.get("api") ?? ENDPOINTS[onLoopback ? 1 : 0].url;

async function fetchDatasetNames(): Promise<string[]> {
  const response = await fetch(`${api}/indices`);
  if (!response.ok) throw new Error(`cannot list datasets: ${response.status}`);
  return (await response.text()).split("\n").map((name) => name.trim()).filter(Boolean);
}

/** Restores a view shared from the HUD's copy button. */
function cameraFromParams(): Camera {
  const shared = params.get("camera")?.split(",").map(Number) ?? [];
  if (shared.length !== 7 || shared.some(isNaN)) {
    return { position: [0, 0, 0], orientation: [0, 0, 0, 1] };
  }
  const [x, y, z, qx, qy, qz, qw] = shared;
  return { position: [x, y, z], orientation: [qx, qy, qz, qw] };
}

const canvas = document.querySelector<HTMLCanvasElement>("#view")!;
const overlay = document.querySelector<HTMLCanvasElement>("#overlay")!;
const overlayContext = overlay.getContext("2d")!;
const minimapCanvas = document.querySelector<HTMLCanvasElement>("#minimap")!;

const settings = { ...DEFAULT_SETTINGS };
const renderer = createRenderer(canvas);
const camera = cameraFromParams();
const control = attachControls(canvas, camera);
const hud = createHud(settings, ENDPOINTS, api, camera);

const worker = new Worker("dist/loader.worker.js", { type: "module" });
let ranges: Int32Array = new Int32Array(0);
let stars = 0;
let dataset = "";
let labels: Label[] = [];
let minimap: Minimap | null = null;

worker.addEventListener("message", (event: MessageEvent<FromWorker>) => {
  const message = event.data;
  if (message.type === "upload") renderer.upload(message.batch, message.data);
  else if (message.type === "free") renderer.free(message.batch);
  else if (message.type === "draws") {
    ranges = message.ranges;
    stars = message.stars;
  } else {
    const url = `${api}/datasets/${dataset}/minimap.png`;
    void loadMinimap(minimapCanvas, url, message.halfExtentPc).then((it) => { minimap = it; });
  }
});

async function start(): Promise<void> {
  const names = await fetchDatasetNames();
  dataset = params.get("dataset") ?? names[0];
  hud.setDatasets(names, dataset);
  worker.postMessage({
    type: "init",
    url: `${api}/datasets/${dataset}/starcloud.bin`,
  } as ToWorker);
  labels = await fetchLabels(`${api}/datasets/${dataset}/labels.json`);
}

let previous = performance.now();
let smoothFps = 0;
let before: Vec3 = [0, 0, 0];

function frame(now: number): void {
  requestAnimationFrame(frame);
  const dt = Math.min((now - previous) / 1000, 0.1);
  previous = now;
  control(dt);

  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  if (canvas.width !== width || canvas.height !== height) {
    canvas.width = width;
    canvas.height = height;
    overlay.width = width;
    overlay.height = height;
    renderer.resize(width, height);
  }

  const projection = projectionMatrix(width / height, settings.far);
  const view = viewMatrix(camera);
  worker.postMessage({
    type: "view",
    eye: camera.position,
    frustum: fromViewProjection(multiply(projection, view)),
    pixelsPerRadian: pixelsPerRadian(height),
    pixelThreshold: settings.pixelThreshold,
    pointBudget: settings.pointBudget,
  } as ToWorker);
  renderer.render(projection, view, camera.position, ranges, settings);
  drawLabels(overlayContext, labels, camera.position, view, projection);
  minimap?.draw(camera);

  const [dx, dy, dz] = subtract(camera.position, before);
  before = camera.position;
  smoothFps += (1 / dt - smoothFps) * 0.1;
  hud.show({ fps: smoothFps, stars, speed: Math.hypot(dx, dy, dz) / dt });
}

void start();
requestAnimationFrame(frame);

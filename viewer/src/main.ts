// Page entry point: canvas, camera, and the pipe between worker and renderer.

import { Camera, pixelsPerRadian, projectionMatrix, viewMatrix } from "./camera";
import { attachControls } from "./controls";
import { fromViewProjection } from "./frustum";
import { multiply } from "./mat4";
import { FromWorker, ToWorker } from "./protocol";
import { createRenderer } from "./renderer";

const REMOTE_API = "https://star-dump-query-api-494247280614.europe-west1.run.app";
const LOCAL_API  = "http://127.0.0.1:3000";

const params = new URLSearchParams(window.location.search);
const onLoopback = ["localhost", "127.0.0.1"].includes(window.location.hostname);
const api = params.get("api") ?? (onLoopback ? LOCAL_API : REMOTE_API);

async function starcloudUrl(): Promise<string> {
  const named = params.get("dataset");
  if (named) return `${api}/datasets/${named}/starcloud.bin`;
  const response = await fetch(`${api}/indices`);
  if (!response.ok) throw new Error(`cannot list datasets: ${response.status}`);
  const [first] = (await response.text()).split("\n").map((name) => name.trim()).filter(Boolean);
  return `${api}/datasets/${first}/starcloud.bin`;
}

function formatStars(count: number): string {
  if (count >= 1e6) return `${(count / 1e6).toFixed(1)}M`;
  if (count >= 1e3) return `${(count / 1e3).toFixed(0)}k`;
  return String(count);
}

const canvas = document.querySelector("canvas")!;
const renderer = createRenderer(canvas);
const camera: Camera = { position: [0, 0, 0], orientation: [0, 0, 0, 1] };
const control = attachControls(canvas, camera);

const worker = new Worker("dist/loader.worker.js", { type: "module" });
let ranges: Int32Array = new Int32Array(0);

worker.addEventListener("message", (event: MessageEvent<FromWorker>) => {
  const message = event.data;
  if (message.type === "upload") renderer.upload(message.batch, message.data);
  else if (message.type === "free") renderer.free(message.batch);
  else {
    ranges = message.ranges;
    document.title = `star-dump — ${formatStars(message.stars)} stars`;
  }
});

void starcloudUrl().then((url) => worker.postMessage({ type: "init", url } as ToWorker));

let previous = performance.now();

function frame(now: number): void {
  requestAnimationFrame(frame);
  control(Math.min((now - previous) / 1000, 0.1));
  previous = now;

  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  if (canvas.width !== width || canvas.height !== height) {
    canvas.width = width;
    canvas.height = height;
    renderer.resize(width, height);
  }

  const projection = projectionMatrix(width / height);
  const view = viewMatrix(camera);
  worker.postMessage({
    type: "view",
    eye: camera.position,
    frustum: fromViewProjection(multiply(projection, view)),
    pixelsPerRadian: pixelsPerRadian(height),
  } as ToWorker);
  renderer.render(projection, view, camera.position, ranges);
}

requestAnimationFrame(frame);

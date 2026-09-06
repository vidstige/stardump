// The overlay panels: sliders bound straight to the settings object on the
// left, live readouts on the right. Switching endpoint or dataset reloads the
// page with new query parameters rather than tearing the worker down.

import { Settings } from "./settings";
import { Vec3 } from "./vec3";

export type Endpoint = { label: string; url: string };

export type Stats = { fps: number; stars: number; position: Vec3; speed: number };

export type Hud = {
  setDatasets(names: string[], selected: string): void;
  show(stats: Stats): void;
};

type Slider = {
  key: keyof Settings;
  label: string;
  min: number;
  max: number;
  step: number;
  /** Slide in decades instead of linearly, for ranges spanning orders. */
  log: boolean;
  format: (value: number) => string;
};

const LOG_STEPS = 1000;

const SLIDERS: Slider[] = [
  { key: "exposure", label: "Exposure", min: 1e-1, max: 1e6, step: 1, log: true,
    format: (v) => v.toExponential(1) },
  { key: "sizeScale", label: "Size", min: 0.1, max: 20, step: 0.1, log: false,
    format: (v) => v.toFixed(1) },
  { key: "maxRadius", label: "Radius", min: 0.5, max: 16, step: 0.1, log: false,
    format: (v) => `${v.toFixed(1)} px` },
  { key: "pixelThreshold", label: "Detail", min: 4, max: 64, step: 1, log: false,
    format: (v) => `${v} px` },
  { key: "far", label: "Far plane", min: 100, max: 8000, step: 100, log: false,
    format: (v) => `${v} pc` },
];

const C_PC_PER_S = 9.716e-9;

function formatSpeed(pcPerSecond: number): string {
  const c = pcPerSecond / C_PC_PER_S;
  if (c < 1e3) return `${c.toFixed(1)}c`;
  if (c < 1e6) return `${(c / 1e3).toFixed(1)}kc`;
  return `${(c / 1e6).toFixed(1)}Mc`;
}

function formatStars(count: number): string {
  if (count >= 1e6) return `${(count / 1e6).toFixed(2)}M`;
  if (count >= 1e3) return `${(count / 1e3).toFixed(0)}k`;
  return String(count);
}

function decades(spec: Slider, value: number): number {
  const low = Math.log10(spec.min);
  return ((Math.log10(value) - low) / (Math.log10(spec.max) - low)) * LOG_STEPS;
}

function fromDecades(spec: Slider, raw: number): number {
  const low = Math.log10(spec.min);
  return 10 ** (low + (raw / LOG_STEPS) * (Math.log10(spec.max) - low));
}

function element<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  className: string,
  parent: HTMLElement,
): HTMLElementTagNameMap[K] {
  const node = document.createElement(tag);
  node.className = className;
  parent.append(node);
  return node;
}

function addRow(parent: HTMLElement, label: string): HTMLDivElement {
  const row = element("div", "row", parent);
  element("span", "label", row).textContent = label;
  return row;
}

function addReadout(parent: HTMLElement, label: string): HTMLSpanElement {
  return element("span", "value", addRow(parent, label));
}

function addSlider(parent: HTMLElement, spec: Slider, settings: Settings): void {
  const row = addRow(parent, spec.label);
  const input = element("input", "slider", row);
  const readout = element("span", "value", row);
  input.type = "range";
  input.min = String(spec.log ? 0 : spec.min);
  input.max = String(spec.log ? LOG_STEPS : spec.max);
  input.step = String(spec.log ? 1 : spec.step);
  input.value = String(spec.log ? decades(spec, settings[spec.key]) : settings[spec.key]);
  readout.textContent = spec.format(settings[spec.key]);
  input.addEventListener("input", () => {
    const value = spec.log ? fromDecades(spec, Number(input.value)) : Number(input.value);
    settings[spec.key] = value;
    readout.textContent = spec.format(value);
  });
}

function addSelect(parent: HTMLElement, label: string, parameter: string): HTMLSelectElement {
  const select = element("select", "select", addRow(parent, label));
  select.addEventListener("change", () => {
    const url = new URL(window.location.href);
    url.searchParams.set(parameter, select.value);
    window.location.href = url.toString();
  });
  return select;
}

function fill(select: HTMLSelectElement, options: Endpoint[], selected: string): void {
  select.replaceChildren();
  for (const { label, url } of options) {
    const option = document.createElement("option");
    option.value = url;
    option.textContent = label;
    option.selected = url === selected;
    select.append(option);
  }
  select.disabled = options.length <= 1;
}

export function createHud(settings: Settings, endpoints: Endpoint[], api: string): Hud {
  const controls = element("div", "panel", document.body);
  controls.id = "controls";
  element("h1", "title", controls).textContent = "star-dump";
  for (const spec of SLIDERS) addSlider(controls, spec, settings);
  fill(addSelect(controls, "Endpoint", "api"), endpoints, api);
  const datasets = addSelect(controls, "Dataset", "dataset");

  const panel = element("div", "panel", document.body);
  panel.id = "stats";
  const fps = addReadout(panel, "FPS");
  const stars = addReadout(panel, "Stars");
  const position = addReadout(panel, "Position");
  const speed = addReadout(panel, "Speed");

  return {
    setDatasets(names, selected) {
      fill(datasets, names.map((name) => ({ label: name, url: name })), selected);
    },

    show(status) {
      fps.textContent = status.fps.toFixed(0);
      stars.textContent = formatStars(status.stars);
      position.textContent = status.position.map((v) => v.toFixed(1)).join(", ");
      speed.textContent = formatSpeed(status.speed);
    },
  };
}

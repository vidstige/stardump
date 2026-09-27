import { ReadRange } from "../../core/starcloud_io";

export function httpRange(url: string): ReadRange {
  return async (start, end) => {
    const response = await fetch(url, { headers: { Range: `bytes=${start}-${end}` } });
    if (!response.ok) throw new Error(`range ${start}-${end} failed: ${response.status}`);
    return response.arrayBuffer();
  };
}

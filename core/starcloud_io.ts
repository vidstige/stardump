import { HEADER_BYTES, NODE_BYTES, Starcloud, decodeHeader, decodeNodes } from "./starcloud";

/**
 * How bytes of starcloud.bin are reached: an HTTP Range request, or a
 * positioned read of a local file. Both ends inclusive, as a Range header is.
 * This is the whole of the transport seam.
 */
export type ReadRange = (start: number, end: number) => Promise<ArrayBuffer>;

/** Serves the browser and Node alike; fetch is global in both. */
export function httpRange(url: string): ReadRange {
  return async (start, end) => {
    const response = await fetch(url, { headers: { Range: `bytes=${start}-${end}` } });
    if (!response.ok) throw new Error(`range ${start}-${end} failed: ${response.status}`);
    return response.arrayBuffer();
  };
}

/** Reads header and node table; the point table stays where it is. */
export async function loadStarcloud(read: ReadRange): Promise<Starcloud> {
  const header = decodeHeader(await read(0, HEADER_BYTES - 1));
  const end = HEADER_BYTES + header.nodeCount * NODE_BYTES - 1;
  return decodeNodes(header, await read(HEADER_BYTES, end));
}

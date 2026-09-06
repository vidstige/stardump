import { HEADER_BYTES, NODE_BYTES, Starcloud, decodeHeader, decodeNodes } from "./starcloud";

export async function fetchRange(url: string, start: number, end: number): Promise<ArrayBuffer> {
  const response = await fetch(url, { headers: { Range: `bytes=${start}-${end}` } });
  if (!response.ok) throw new Error(`range ${start}-${end} failed: ${response.status}`);
  return response.arrayBuffer();
}

/** Reads header and node table; the point table stays on the server. */
export async function fetchStarcloud(url: string): Promise<Starcloud> {
  const header = decodeHeader(await fetchRange(url, 0, HEADER_BYTES - 1));
  const end = HEADER_BYTES + header.nodeCount * NODE_BYTES - 1;
  return decodeNodes(header, await fetchRange(url, HEADER_BYTES, end));
}

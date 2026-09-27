import { HEADER_BYTES, NODE_BYTES, Starcloud, decodeHeader, decodeNodes } from "./starcloud";

/**
 * How bytes of starcloud.bin are reached: an HTTP Range request in the
 * browser, a positioned read in Node. Both ends inclusive, as a Range header
 * is. This is the whole of the transport seam.
 */
export type ReadRange = (start: number, end: number) => Promise<ArrayBuffer>;

/** Reads header and node table; the point table stays where it is. */
export async function loadStarcloud(read: ReadRange): Promise<Starcloud> {
  const header = decodeHeader(await read(0, HEADER_BYTES - 1));
  const end = HEADER_BYTES + header.nodeCount * NODE_BYTES - 1;
  return decodeNodes(header, await read(HEADER_BYTES, end));
}

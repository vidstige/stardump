import * as fs from "fs/promises";

import { ReadRange } from "../core/starcloud_io";

export async function fileRange(path: string): Promise<ReadRange> {
  const file = await fs.open(path, "r");
  return async (start, end) => {
    // allocUnsafeSlow rather than allocUnsafe: a pooled buffer would hand
    // back the whole pool as its ArrayBuffer.
    const buffer = Buffer.allocUnsafeSlow(end - start + 1);
    await file.read(buffer, 0, buffer.length, start);
    return buffer.buffer;
  };
}

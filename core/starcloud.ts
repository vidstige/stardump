// The starcloud.bin octree index: a header, a node table, and a point table.
// Every node owns a contiguous run of the point table. Leaves hold all their
// stars; interior nodes hold a flux-conserving subsample of their descendants,
// so any cut through the tree is a complete, correctly lit view of the sky.
//
// Points are written depth first, a node's own subsample ahead of its subtree,
// which is why wanted nodes tend to form long contiguous byte ranges.

export const HEADER_BYTES = 32;
export const NODE_BYTES   = 20;
export const POINT_BYTES  = 20;

const MAGIC = "STRCLD\0\0";

export type Starcloud = {
  halfExtentPc: number;
  /** Byte offset of the point table within starcloud.bin. */
  pointsOffset: number;
  childMask:  Uint8Array;
  firstChild: Uint32Array;
  pointFirst: Uint32Array;
  pointCount: Uint32Array;
  /** Node centres, three floats per node. */
  center: Float32Array;
  /** Half the side length of each node's cube. */
  halfSize: Float32Array;
};

export type Header = { halfExtentPc: number; nodeCount: number };

export function decodeHeader(bytes: ArrayBuffer): Header {
  const view = new DataView(bytes);
  const magic = String.fromCharCode(...new Uint8Array(bytes, 0, 8));
  if (magic !== MAGIC) throw new Error("not a starcloud file");
  const version = view.getUint16(8, true);
  if (version !== 1) throw new Error(`unsupported starcloud version ${version}`);
  return {
    halfExtentPc: view.getFloat32(12, true),
    nodeCount:    view.getUint32(16, true),
  };
}

/** Walks the tree once to give every node its cube, so later passes are flat. */
function computeGeometry(sc: Starcloud): void {
  const stack = [0, 0, 0, 0, sc.halfExtentPc];
  while (stack.length > 0) {
    const half = stack.pop()!;
    const cz = stack.pop()!, cy = stack.pop()!, cx = stack.pop()!;
    const node = stack.pop()!;
    sc.center[node * 3]     = cx;
    sc.center[node * 3 + 1] = cy;
    sc.center[node * 3 + 2] = cz;
    sc.halfSize[node] = half;

    const mask = sc.childMask[node];
    const quarter = half * 0.5;
    let child = sc.firstChild[node];
    for (let octant = 0; octant < 8; octant++) {
      if ((mask & (1 << octant)) === 0) continue;
      stack.push(
        child,
        cx + (octant & 1 ? quarter : -quarter),
        cy + (octant & 2 ? quarter : -quarter),
        cz + (octant & 4 ? quarter : -quarter),
        quarter,
      );
      child++;
    }
  }
}

export function decodeNodes(header: Header, bytes: ArrayBuffer): Starcloud {
  const { nodeCount } = header;
  const view = new DataView(bytes);
  const sc: Starcloud = {
    halfExtentPc: header.halfExtentPc,
    pointsOffset: HEADER_BYTES + nodeCount * NODE_BYTES,
    childMask:  new Uint8Array(nodeCount),
    firstChild: new Uint32Array(nodeCount),
    pointFirst: new Uint32Array(nodeCount),
    pointCount: new Uint32Array(nodeCount),
    center:     new Float32Array(nodeCount * 3),
    halfSize:   new Float32Array(nodeCount),
  };
  for (let i = 0; i < nodeCount; i++) {
    const at = i * NODE_BYTES;
    sc.childMask[i]  = view.getUint8(at);
    sc.firstChild[i] = view.getUint32(at + 4,  true);
    sc.pointFirst[i] = view.getUint32(at + 8,  true);
    sc.pointCount[i] = view.getUint32(at + 12, true);
  }
  computeGeometry(sc);
  return sc;
}

/** Children of a node occupy `childCount(mask)` slots from `firstChild`. */
export function childCount(mask: number): number {
  let n = 0;
  for (let octant = 0; octant < 8; octant++) n += (mask >> octant) & 1;
  return n;
}

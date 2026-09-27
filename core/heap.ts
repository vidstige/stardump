/** Binary max-heap over integer values keyed by a floating point priority. */
export class MaxHeap {
  private values: number[] = [];
  private keys: number[] = [];

  get size(): number {
    return this.values.length;
  }

  clear(): void {
    this.values.length = 0;
    this.keys.length = 0;
  }

  private swap(a: number, b: number): void {
    [this.values[a], this.values[b]] = [this.values[b], this.values[a]];
    [this.keys[a], this.keys[b]] = [this.keys[b], this.keys[a]];
  }

  push(value: number, key: number): void {
    this.values.push(value);
    this.keys.push(key);
    for (let i = this.size - 1; i > 0; ) {
      const parent = (i - 1) >> 1;
      if (this.keys[parent] >= this.keys[i]) break;
      this.swap(parent, i);
      i = parent;
    }
  }

  pop(): number {
    const top = this.values[0];
    const value = this.values.pop()!;
    const key = this.keys.pop()!;
    if (this.size > 0) {
      this.values[0] = value;
      this.keys[0] = key;
      for (let i = 0; ; ) {
        const left = i * 2 + 1;
        if (left >= this.size) break;
        const right = left + 1;
        const child = right < this.size && this.keys[right] > this.keys[left] ? right : left;
        if (this.keys[i] >= this.keys[child]) break;
        this.swap(i, child);
        i = child;
      }
    }
    return top;
  }
}

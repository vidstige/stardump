# StarDump

A system for ingesting, indexing, serving, and visualizing the [Gaia DR3](https://www.cosmos.esa.int/web/gaia/dr3) star catalog in 3D space. Gaia DR3 is the third data release from the ESA Gaia mission and contains astrometry, photometry, and spectra for roughly 1.8 billion sources.

The pipeline consists of:

1. **Ingestion** — stream Gaia bulk CSV.GZ files into a compact canonical binary format
2. **Index build** — pack canonical data into a spatially indexed `starcloud.bin` octree with precomputed LOD subsamples
3. **Query API** — serve the index over HTTP with radius queries and byte-range streaming
4. **Viewer** — WebGL interactive star viewer that streams LOD nodes on demand
5. **Offline renderer** — produce high-resolution PNG renders from a local or remote dataset

Data references:
- [Gaia DR3 overview](https://www.cosmos.esa.int/web/gaia/dr3)
- [Gaia source table schema](https://gea.esac.esa.int/archive/documentation/GDR3/Gaia_archive/chap_datamodel/sec_dm_main_source_catalogue/ssec_dm_gaia_source.html)
- Architecture details: [docs/gaia-3d-storage.md](docs/gaia-3d-storage.md)

## Technical notes

**Coordinate system** — Stars are stored in Sun-centered ICRS Cartesian coordinates in parsecs. RA and declination from Gaia are converted using the parallax distance `d = 1000 / parallax_mas`:

```
x = d · cos(dec) · cos(ra)
y = d · cos(dec) · sin(ra)
z = d · sin(dec)
```

The index covers a cube of ±4000 pc centered on the Sun, corresponding to a sphere of radius ~2000 pc for well-measured stars.

**Quality filter** — Only stars with a reliable parallax are included. The quality metric is `parallax / σ_eff`, where `σ_eff` is the larger of the reported `parallax_error` and a brightness-dependent reference floor derived from Gaia DR3 median uncertainties (0.025 mas at G ≤ 15, interpolated to 1.3 mas at G = 21). The default threshold is **10**, meaning the parallax must be at least 10× its effective uncertainty. This leaves ~1.47 billion stars.

**LOD subsampling** — The octree index precomputes a subsample at each interior node using flux-conserving selection: K=256 points per node are chosen, and their luminosity is boosted proportionally to the number of descendants they represent. This allows the viewer to render approximate images at any zoom level without loading all leaves.

**Rendering** — Each star is projected onto the image plane and splatted as a Gaussian with radius proportional to its screen-space brightness. Flux falls off with distance squared. Colors are derived from the Gaia BP−RP color index. The HDR accumulation buffer is tone-mapped with a Reinhard curve and gamma-corrected (γ = 2.2) before writing to PNG.

## Prerequisites

- [Rust](https://rustup.rs/) (Cargo) for the backend binaries
- [Node.js](https://nodejs.org/) and npm for the viewer and offline renderers
- `sips` (macOS built-in) or `convert` (ImageMagick) for PPM → PNG conversion

## Generating the data

### Step 1 — Ingest Gaia CSV files into canonical format

The `ingest` binary reads Gaia bulk CSV.GZ files and writes a compact 32-byte-per-row canonical binary format (`source_id`, RA/Dec, parallax, G magnitude, BP−RP color).

```bash
cargo build --release --bin ingest

cargo run --release --bin ingest -- \
  --input /path/to/GaiaSource_000000.csv.gz \
  --input /path/to/GaiaSource_000001.csv.gz \
  --output-root ./data
```

Repeat `--input` for each bulk file. Output lands in `./data/canonical/<range>/`. Each source file is identified by its MD5 checksum so reruns skip already-ingested files automatically.

For the full Gaia DR3 dataset on Google Cloud Run, use the Python orchestration CLI:

```bash
python3 -m stardump ingest start    # fetch manifest, upload inputs.txt, launch Cloud Run job
python3 -m stardump ingest status   # check progress
```

### Step 2 — Build the starcloud.bin index

Once canonical files are ingested, build the packed octree index:

```bash
cargo build --release --bin build-starcloud

cargo run --release --bin build-starcloud -- \
  --data-root ./data/<dataset-name>
```

Replace `<dataset-name>` with the directory name created under `./data/` during ingestion (it is the MD5 of the sorted input URL list). The output is `./data/<dataset-name>/starcloud.bin`, a single binary file containing a fixed header, octree node table, and point table with precomputed LOD subsamples.

On Cloud Run:

```bash
python3 -m stardump ingest build-index
```

## Downloading starcloud.bin from a remote API instance

The query API supports HTTP Range requests on the `/starcloud/<dataset>` endpoint, so you can stream the full index file from a running instance:

```bash
DATASET=8fbfbc19d3f4d71f76b76fef607d4dfb
API=https://star-dump-query-api-494247280614.europe-west1.run.app

mkdir -p ./data/$DATASET
curl "$API/starcloud/$DATASET" -o ./data/$DATASET/starcloud.bin
```

To list available datasets on the remote instance first:

```bash
curl "$API/indices"
```

## Running the query API

```bash
cargo run --release --bin query-api -- \
  --data-root ./data \
  --bind 127.0.0.1:3000
```

The API exposes:

| Endpoint | Description |
|---|---|
| `GET /health` | Liveness check |
| `GET /indices` | List available dataset names |
| `GET /starcloud/<name>` | Stream the full binary index (supports Range) |
| `GET /query/<name>/radius?x=&y=&z=&r=` | Radius query, returns CSV (`x,y,z,source_id`) |

Example query — all stars within 25 pc of the Sun:

```bash
curl 'http://127.0.0.1:3000/query/8fbfbc19d3f4d71f76b76fef607d4dfb/radius?x=0&y=0&z=0&r=25'
```

## Running the viewer locally

```bash
npm -C viewer/ install
npm -C viewer/ run dev
```

Open [http://localhost:8000](http://localhost:8000) in a browser. The viewer streams octree nodes from the query API on demand, selecting LOD levels based on camera distance and screen-space coverage. By default it connects to `http://127.0.0.1:3000`; a running query API is required.

## Offline renderer

The offline renderers produce a PNG image of the sky without a running browser. Two modes are available:

- **exact** — reads a local `starcloud.bin` and raycasts exact star positions
- **fast** — queries a live HTTP API, caches downloaded nodes, and renders LOD subsamples

```bash
# Exact mode (requires local starcloud.bin under ./data/<dataset>/)
sh sh/render.sh --mode exact --output /tmp/render.png

# Fast mode (requires a running query API)
sh sh/render.sh --mode fast --url http://127.0.0.1:3000 --output /tmp/render.png

# Override dataset and output resolution
sh sh/render.sh --mode exact \
  --dataset 8fbfbc19d3f4d71f76b76fef607d4dfb \
  --output /tmp/render.png \
  --width 3840 --height 2160
```

The script auto-detects the first dataset in `./data/` if `--dataset` is omitted. Additional flags are forwarded to the renderer.

## Author
Samuel Carlsson & Claude

#!/usr/bin/env python3
import concurrent.futures
import os
import subprocess
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


PREFIX = "Gaia/gdr3/gaia_source/"
CDN_BASE = "https://cdn.gea.esac.esa.int/"
LISTING_BASE = "https://gaia.eu-1.cdn77-storage.com/"
S3_NAMESPACE = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}


def run(args: list[str], *, capture_output: bool = False) -> str:
    result = subprocess.run(
        args,
        check=True,
        text=True,
        capture_output=capture_output,
    )
    return result.stdout.strip() if capture_output else ""


def env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    return int(value) if value else default


def current_project_id() -> str:
    project_id = run(["gcloud", "config", "get-value", "project"], capture_output=True)
    if not project_id or project_id == "(unset)":
        raise SystemExit("gcloud project is not set; run 'gcloud config set project <project-id>' first")
    return project_id


def current_project_number(project_id: str) -> str:
    return run(
        ["gcloud", "projects", "describe", project_id, "--format=value(projectNumber)"],
        capture_output=True,
    )


def current_image_tag() -> str:
    return os.environ.get("IMAGE_TAG") or run(["git", "rev-parse", "--short", "HEAD"], capture_output=True)


def join_with_commas(args: list[str]) -> str:
    return ",".join(args)


def job_exists(name: str) -> bool:
    result = subprocess.run(
        ["gcloud", "beta", "run", "jobs", "describe", name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    return result.returncode == 0


def create_or_update_job(name: str, args: list[str]) -> None:
    command = "update" if job_exists(name) else "create"
    run(["gcloud", "beta", "run", "jobs", command, name, *args])


def execute_job(name: str) -> None:
    run(["gcloud", "beta", "run", "jobs", "execute", name, "--wait"])


def fetch_gaia_source_urls() -> list[str]:
    urls: list[str] = []
    marker = None

    while True:
        params = {"prefix": PREFIX, "delimiter": "/"}
        if marker:
            params["marker"] = marker

        listing_url = f"{LISTING_BASE}?{urllib.parse.urlencode(params)}"
        with urllib.request.urlopen(listing_url) as response:
            root = ET.fromstring(response.read())

        for key in root.findall("s3:Contents/s3:Key", S3_NAMESPACE):
            value = key.text or ""
            if value.endswith(".csv.gz"):
                urls.append(f"{CDN_BASE}{value}")

        is_truncated = (root.findtext("s3:IsTruncated", default="false", namespaces=S3_NAMESPACE) or "").lower()
        if is_truncated != "true":
            return urls

        marker = root.findtext("s3:NextMarker", namespaces=S3_NAMESPACE)
        if not marker:
            raise SystemExit("listing is truncated but NextMarker is missing")


def ingest_job_args(image_uri: str, service_account_email: str, data_root: str, parallax_filter_mas: int, urls: list[str]) -> list[str]:
    ingest_args = []
    for url in urls:
        ingest_args.extend(["--input", url])
    ingest_args.extend(["--output-root", data_root, "--parallax-filter-mas", str(parallax_filter_mas)])

    return [
        "--image",
        image_uri,
        "--service-account",
        service_account_email,
        "--memory",
        "1Gi",
        "--task-timeout=3600",
        "--max-retries=0",
        "--command",
        "/usr/local/bin/ingest",
        "--args=" + join_with_commas(ingest_args),
    ]


def build_index_job_args(image_uri: str, service_account_email: str, data_root: str, octree_depth: int) -> list[str]:
    return [
        "--image",
        image_uri,
        "--service-account",
        service_account_email,
        "--memory",
        "1Gi",
        "--task-timeout=3600",
        "--max-retries=0",
        "--command",
        "/usr/local/bin/build-index",
        "--args=" + join_with_commas(["--data-root", data_root, "--octree-depth", str(octree_depth)]),
    ]


def run_ingest_batch(job_name: str, job_args: list[str]) -> None:
    print(f"starting {job_name}", flush=True)
    create_or_update_job(job_name, job_args)
    execute_job(job_name)
    print(f"finished {job_name}", flush=True)


def batched(values: list[str], size: int) -> list[list[str]]:
    return [values[index:index + size] for index in range(0, len(values), size)]


def main() -> None:
    project_id = current_project_id()
    project_number = current_project_number(project_id)
    image_tag = current_image_tag()
    image_uri = os.environ.get("IMAGE_URI", f"gcr.io/{project_id}/star-dump:{image_tag}")
    bucket_uri = os.environ.get("BUCKET_URI", f"gs://star-dump-data-{project_number}")
    data_root = os.environ.get("DATA_ROOT", f"{bucket_uri}/runs/full-gaia-dr3")
    job_prefix = os.environ.get("JOB_PREFIX", "star-dump-ingest-full")
    build_index_job_name = os.environ.get("BUILD_INDEX_JOB_NAME", "star-dump-build-index")
    service_account_name = os.environ.get("SERVICE_ACCOUNT_NAME", "star-dump-run")
    service_account_email = os.environ.get(
        "SERVICE_ACCOUNT_EMAIL",
        f"{service_account_name}@{project_id}.iam.gserviceaccount.com",
    )
    parallax_filter_mas = env_int("PARALLAX_FILTER_MAS", 10)
    octree_depth = env_int("OCTREE_DEPTH", 6)
    batch_size = env_int("BATCH_SIZE", 32)
    max_parallel = env_int("MAX_PARALLEL", 8)

    urls = fetch_gaia_source_urls()
    if not urls:
        raise SystemExit("failed to fetch Gaia source URL list")

    batches = batched(urls, batch_size)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = []
        for batch_index, batch_urls in enumerate(batches):
            job_name = f"{job_prefix}-{batch_index:03d}"
            job_args = ingest_job_args(
                image_uri,
                service_account_email,
                data_root,
                parallax_filter_mas,
                batch_urls,
            )
            futures.append(executor.submit(run_ingest_batch, job_name, job_args))

        for future in concurrent.futures.as_completed(futures):
            future.result()

    print(f"starting {build_index_job_name}", flush=True)
    create_or_update_job(
        build_index_job_name,
        build_index_job_args(image_uri, service_account_email, data_root, octree_depth),
    )
    execute_job(build_index_job_name)
    print(f"finished {build_index_job_name}", flush=True)

    print(f"gaia_source_files: {len(urls)}")
    print(f"ingest_batches: {len(batches)}")
    print(f"data_root: {data_root}")
    print(f"build_index_job: {build_index_job_name}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


PREFIX = "Gaia/gdr3/gaia_source/"
CDN_BASE = "https://cdn.gea.esac.esa.int/"
LISTING_BASE = "https://gaia.eu-1.cdn77-storage.com/"
S3_NAMESPACE = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
STATE_PATH = ".stardump/ingest-state.json"


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


def current_settings() -> dict[str, str | int]:
    project_id = current_project_id()
    project_number = current_project_number(project_id)
    image_tag = current_image_tag()
    bucket_name = os.environ.get("BUCKET_NAME", f"star-dump-data-{project_number}")
    mount_root = os.environ.get("MOUNT_ROOT", "/mnt/gcs")

    return {
        "image_uri": os.environ.get("IMAGE_URI", f"gcr.io/{project_id}/star-dump:{image_tag}"),
        "bucket_name": bucket_name,
        "mount_root": mount_root,
        "data_root": os.environ.get("DATA_ROOT", f"{mount_root}/runs/full-gaia-dr3"),
        "job_prefix": os.environ.get("JOB_PREFIX", "star-dump-ingest-full"),
        "build_index_job_name": os.environ.get("BUILD_INDEX_JOB_NAME", "star-dump-build-index"),
        "service_account_email": os.environ.get(
            "SERVICE_ACCOUNT_EMAIL",
            f"{os.environ.get('SERVICE_ACCOUNT_NAME', 'star-dump-run')}@{project_id}.iam.gserviceaccount.com",
        ),
        "parallax_filter_mas": env_int("PARALLAX_FILTER_MAS", 10),
        "octree_depth": env_int("OCTREE_DEPTH", 6),
        "batch_size": env_int("BATCH_SIZE", 32),
    }


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


def execution_names_for_job(job_name: str) -> set[str]:
    output = run(
        [
            "gcloud",
            "beta",
            "run",
            "jobs",
            "executions",
            "list",
            f"--job={job_name}",
            "--format=value(metadata.name)",
        ],
        capture_output=True,
    )
    return {line for line in output.splitlines() if line}


def execute_job_async(job_name: str) -> str:
    before = execution_names_for_job(job_name)
    run(["gcloud", "beta", "run", "jobs", "execute", job_name, "--async"])

    for _ in range(30):
        new_names = execution_names_for_job(job_name) - before
        if len(new_names) == 1:
            return next(iter(new_names))
        if len(new_names) > 1:
            return sorted(new_names)[-1]
        time.sleep(1)

    raise SystemExit(f"failed to resolve execution name for {job_name}")


def execute_job_wait(job_name: str) -> None:
    run(["gcloud", "beta", "run", "jobs", "execute", job_name, "--wait"])


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


def ingest_job_args(
    image_uri: str,
    service_account_email: str,
    bucket_name: str,
    mount_root: str,
    data_root: str,
    parallax_filter_mas: int,
    urls: list[str],
) -> list[str]:
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
        "--add-volume",
        f"name=gcs,type=cloud-storage,bucket={bucket_name},readonly=false",
        "--add-volume-mount",
        f"volume=gcs,mount-path={mount_root}",
        "--command",
        "/usr/local/bin/ingest",
        "--args=" + join_with_commas(ingest_args),
    ]


def build_index_job_args(
    image_uri: str,
    service_account_email: str,
    bucket_name: str,
    mount_root: str,
    data_root: str,
    octree_depth: int,
) -> list[str]:
    return [
        "--image",
        image_uri,
        "--service-account",
        service_account_email,
        "--memory",
        "1Gi",
        "--task-timeout=3600",
        "--max-retries=0",
        "--add-volume",
        f"name=gcs,type=cloud-storage,bucket={bucket_name},readonly=false",
        "--add-volume-mount",
        f"volume=gcs,mount-path={mount_root}",
        "--command",
        "/usr/local/bin/build-index",
        "--args=" + join_with_commas(["--data-root", data_root, "--octree-depth", str(octree_depth)]),
    ]


def batched(values: list[str], size: int) -> list[list[str]]:
    return [values[index:index + size] for index in range(0, len(values), size)]


def write_state(state: dict) -> None:
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as file:
        json.dump(state, file, indent=2, sort_keys=True)
        file.write("\n")


def read_state() -> dict:
    with open(STATE_PATH, "r", encoding="utf-8") as file:
        return json.load(file)


def execution_condition(execution_name: str) -> dict:
    execution = json.loads(
        run(
            [
                "gcloud",
                "beta",
                "run",
                "jobs",
                "executions",
                "describe",
                execution_name,
                "--format=json",
            ],
            capture_output=True,
        )
    )

    completed = None
    for condition in execution.get("status", {}).get("conditions", []):
        if condition.get("type") == "Completed":
            completed = condition
            break

    if completed is None or completed.get("status") == "Unknown":
        state = "running"
    elif completed.get("status") == "True":
        state = "succeeded"
    else:
        state = "failed"

    return {
        "state": state,
        "message": (completed or {}).get("message", ""),
        "log_uri": execution.get("status", {}).get("logUri", ""),
    }


def status_rows(state: dict) -> list[dict]:
    rows = []
    for batch in state["batches"]:
        rows.append(
            {
                "job_name": batch["job_name"],
                "execution_name": batch["execution_name"],
                "input_count": batch["input_count"],
                **execution_condition(batch["execution_name"]),
            }
        )
    return rows


def print_status(rows: list[dict]) -> None:
    succeeded = sum(1 for row in rows if row["state"] == "succeeded")
    running = sum(1 for row in rows if row["state"] == "running")
    failed = sum(1 for row in rows if row["state"] == "failed")
    total = len(rows)

    print(f"succeeded: {succeeded}/{total}  running: {running}  failed: {failed}")

    for row in rows:
        if row["state"] != "failed":
            continue
        print(f"failed execution: {row['execution_name']}")
        if row["message"]:
            print(row["message"])
        if row["log_uri"]:
            print(row["log_uri"])


def start_ingest() -> None:
    settings = current_settings()
    urls = fetch_gaia_source_urls()
    if not urls:
        raise SystemExit("failed to fetch Gaia source URL list")

    batches = batched(urls, settings["batch_size"])
    started_batches = []

    for batch_index, batch_urls in enumerate(batches):
        job_name = f"{settings['job_prefix']}-{batch_index:03d}"
        job_args = ingest_job_args(
            settings["image_uri"],
            settings["service_account_email"],
            settings["bucket_name"],
            settings["mount_root"],
            settings["data_root"],
            settings["parallax_filter_mas"],
            batch_urls,
        )
        create_or_update_job(job_name, job_args)
        execution_name = execute_job_async(job_name)
        started_batches.append(
            {
                "job_name": job_name,
                "execution_name": execution_name,
                "input_count": len(batch_urls),
            }
        )
        print(f"started {job_name} as {execution_name}")

    write_state(
        {
            "batches": started_batches,
            "build_index_job_name": settings["build_index_job_name"],
            "data_root": settings["data_root"],
            "octree_depth": settings["octree_depth"],
        }
    )

    print(f"gaia_source_files: {len(urls)}")
    print(f"ingest_batches: {len(batches)}")
    print(f"data_root: {settings['data_root']}")
    print(f"state_file: {STATE_PATH}")
    print("next: python3 -m stardump ingest status")
    print("then: python3 -m stardump ingest status")
    print("then: python3 -m stardump ingest build-index")


def status_ingest() -> None:
    try:
        state = read_state()
    except FileNotFoundError:
        raise SystemExit(f"missing state file: {STATE_PATH}")

    rows = status_rows(state)
    print_status(rows)

    if any(row["state"] == "failed" for row in rows):
        raise SystemExit(1)


def start_build_index() -> None:
    settings = current_settings()

    try:
        state = read_state()
        data_root = state["data_root"]
        octree_depth = state["octree_depth"]
        rows = status_rows(state)
        if any(row["state"] != "succeeded" for row in rows):
            print_status(rows)
            raise SystemExit("cannot start build-index before all ingest batches succeeded")
    except FileNotFoundError:
        data_root = settings["data_root"]
        octree_depth = settings["octree_depth"]

    job_name = settings["build_index_job_name"]
    create_or_update_job(
        job_name,
        build_index_job_args(
            settings["image_uri"],
            settings["service_account_email"],
            settings["bucket_name"],
            settings["mount_root"],
            data_root,
            octree_depth,
        ),
    )

    print(f"starting {job_name}")
    execute_job_wait(job_name)
    print(f"finished {job_name}")

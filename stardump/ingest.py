from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


PREFIX = "Gaia/gdr3/gaia_source/"
CDN_BASE = "https://cdn.gea.esac.esa.int/"
LISTING_BASE = "https://gaia.eu-1.cdn77-storage.com/"
MD5SUM_URL = f"{CDN_BASE}{PREFIX}_MD5SUM.txt"
S3_NAMESPACE = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
STATE_PATH = ".stardump/ingest-state.json"
MOUNT_ROOT = "/mnt/gcs"
CONFIG_ROOT = ".stardump"
INGEST_CONFIG_OBJECT = f"{CONFIG_ROOT}/ingest-config.txt"
BUILD_INDEX_CONFIG_OBJECT = f"{CONFIG_ROOT}/build-index-config.txt"


def run(args: list[str], *, capture_output: bool = False) -> str:
    result = subprocess.run(
        args,
        check=True,
        text=True,
        capture_output=capture_output,
    )
    return result.stdout.strip() if capture_output else ""


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


def default_bucket_name(project_number: str) -> str:
    return f"star-dump-data-{project_number}"


def run_name() -> str:
    digest = hashlib.md5()
    digest.update(str(int(time.time())).encode("utf-8"))
    return digest.hexdigest()[:7]


def default_data_root(mount_root: str) -> str:
    return f"{mount_root}/{run_name()}"


def fetch_gaia_source_md5s() -> dict[str, str]:
    checksums = {}
    with urllib.request.urlopen(MD5SUM_URL) as response:
        for line in response.read().decode("utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            checksum, filename = line.split(None, 1)
            checksums[filename] = checksum
    return checksums


def upload_manifest(bucket_name: str, run_name: str, urls: list[str], checksums: dict[str, str]) -> str:
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as file:
        for url in urls:
            filename = url.rsplit("/", 1)[-1]
            checksum = checksums.get(filename)
            if not checksum:
                raise SystemExit(f"missing checksum for {filename} in {MD5SUM_URL}")
            file.write(checksum)
            file.write("\t")
            file.write(url)
            file.write("\n")
        manifest_path = file.name

    object_uri = f"gs://{bucket_name}/{run_name}/inputs.txt"
    try:
        run(["gcloud", "storage", "cp", manifest_path, object_uri])
    finally:
        os.unlink(manifest_path)

    return object_uri


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


def upload_text(bucket_name: str, object_name: str, text: str) -> str:
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as file:
        file.write(text)
        local_path = file.name

    object_uri = f"gs://{bucket_name}/{object_name}"
    try:
        run(["gcloud", "storage", "cp", local_path, object_uri])
    finally:
        os.unlink(local_path)

    return object_uri


def write_state(state: dict) -> None:
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as file:
        json.dump(state, file, indent=2, sort_keys=True)
        file.write("\n")


def read_state() -> dict:
    with open(STATE_PATH, "r", encoding="utf-8") as file:
        return json.load(file)


def bucket_name_from_state_or_project(bucket_name: str | None) -> str:
    if bucket_name is not None:
        return bucket_name
    return default_bucket_name(current_project_number(current_project_id()))


def data_root_from_state(data_root: str | None) -> str:
    if data_root is not None:
        return data_root

    try:
        state = read_state()
    except FileNotFoundError:
        raise SystemExit(f"missing state file: {STATE_PATH}; pass --data-root")

    data_root = state.get("data_root")
    if not data_root:
        raise SystemExit(f"missing data_root in state file: {STATE_PATH}; pass --data-root")
    return data_root


def object_prefix(data_root: str) -> str:
    prefix = f"{MOUNT_ROOT}/"
    if data_root.startswith(prefix):
        return data_root[len(prefix) :]
    raise SystemExit(f"unsupported data root: {data_root}; expected to start with {prefix}")


def storage_ls(url: str) -> list[str]:
    result = subprocess.run(
        ["gcloud", "storage", "ls", "--recursive", url],
        text=True,
        capture_output=True,
    )
    if result.returncode == 0:
        return [line for line in result.stdout.splitlines() if line]

    output = result.stderr + result.stdout
    if "matched no objects" in output:
        return []

    raise subprocess.CalledProcessError(result.returncode, result.args, result.stdout, result.stderr)


def index_status(
    *,
    bucket_name: str | None,
    data_root: str | None,
) -> None:
    bucket_name = bucket_name_from_state_or_project(bucket_name)
    data_root = data_root_from_state(data_root)
    prefix = object_prefix(data_root)
    index_urls = storage_ls(f"gs://{bucket_name}/{prefix}/index.octree")
    index_size = storage_size(index_urls[0]) if index_urls else None

    print(f"data_root: {data_root}")
    print(f"index_octree: {'present' if index_urls else 'absent'}")
    if index_size is not None:
        print(f"index_octree_bytes: {index_size}")


def storage_size(url: str) -> int:
    output = run(["gcloud", "storage", "du", url], capture_output=True)
    size, _ = output.split(maxsplit=1)
    return int(size)


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
    spec = execution.get("spec", {})
    status = execution.get("status", {})

    completed = None
    for condition in status.get("conditions", []):
        if condition.get("type") == "Completed":
            completed = condition
            break

    if completed is None or completed.get("status") == "Unknown":
        state = "running"
    elif completed.get("status") == "True":
        state = "succeeded"
    else:
        state = "failed"

    task_count = int(spec.get("taskCount", 0) or 0)
    succeeded_count = int(status.get("succeededCount", 0) or 0)
    running_count = int(status.get("runningCount", 0) or 0)
    failed_count = int(status.get("failedCount", 0) or 0)
    pending_count = max(task_count - succeeded_count - running_count - failed_count, 0)

    return {
        "state": state,
        "message": (completed or {}).get("message", ""),
        "log_uri": status.get("logUri", ""),
        "task_count": task_count,
        "succeeded_count": succeeded_count,
        "running_count": running_count,
        "failed_count": failed_count,
        "pending_count": pending_count,
    }


def state_executions(state: dict) -> list[dict]:
    executions = state.get("executions")
    if executions is not None:
        return executions
    return state.get("batches", [])


def status_rows(state: dict) -> list[dict]:
    rows = []
    for execution in state_executions(state):
        rows.append(
            {
                "job_name": execution["job_name"],
                "execution_name": execution["execution_name"],
                "input_count": execution["input_count"],
                **execution_condition(execution["execution_name"]),
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
        task_count = row["task_count"]
        if task_count:
            print(
                f"{row['execution_name']}: "
                f"tasks succeeded {row['succeeded_count']}/{task_count}  "
                f"running {row['running_count']}  "
                f"pending {row['pending_count']}  "
                f"failed {row['failed_count']}"
            )

    for row in rows:
        if row["state"] != "failed":
            continue
        print(f"failed execution: {row['execution_name']}")
        if row["message"]:
            print(row["message"])
        if row["log_uri"]:
            print(row["log_uri"])


def start_ingest(
    *,
    bucket_name: str | None,
    ingest_job_name: str,
    build_index_job_name: str,
    data_root: str | None,
) -> None:
    project_id = current_project_id()
    project_number = current_project_number(project_id)
    bucket_name = bucket_name or default_bucket_name(project_number)
    urls = fetch_gaia_source_urls()
    if not urls:
        raise SystemExit("failed to fetch Gaia source URL list")

    checksums = fetch_gaia_source_md5s()
    run_id = run_name()
    data_root = data_root or default_data_root(MOUNT_ROOT)
    input_manifest = upload_manifest(bucket_name, run_id, urls, checksums)
    task_count = len(urls)
    job_name = ingest_job_name
    upload_text(
        bucket_name,
        INGEST_CONFIG_OBJECT,
        (
            f"input_manifest={MOUNT_ROOT}/{run_id}/inputs.txt\n"
            f"output_root={data_root}\n"
        ),
    )
    execution_name = execute_job_async(job_name)
    executions = [
        {
            "job_name": job_name,
            "execution_name": execution_name,
            "input_count": len(urls),
        }
    ]
    print(f"started {job_name} as {execution_name}")

    write_state(
        {
            "executions": executions,
            "build_index_job_name": build_index_job_name,
            "data_root": data_root,
            "input_manifest": input_manifest,
            "run_name": run_id,
        }
    )

    print(f"gaia_source_files: {len(urls)}")
    print(f"ingest_tasks: {task_count}")
    print("parallelism: deployed job setting")
    print(f"run_name: {run_id}")
    print(f"data_root: {data_root}")
    print(f"state_file: {STATE_PATH}")
    print("next: python3 -m stardump ingest status")
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


def start_build_index(
    *,
    bucket_name: str | None,
    build_index_job_name: str,
    octree_depth: int,
    data_root: str | None,
) -> None:
    project_number = current_project_number(current_project_id())

    try:
        state = read_state()
    except FileNotFoundError:
        state = None

    if state is not None:
        rows = status_rows(state)
        if any(row["state"] != "succeeded" for row in rows):
            print_status(rows)
            raise SystemExit("cannot start build-index before all ingest batches succeeded")

    bucket_name = bucket_name or default_bucket_name(project_number)
    data_root = data_root or (state or {}).get("data_root")
    if not data_root:
        raise SystemExit(f"missing state file: {STATE_PATH}; pass --data-root to run build-index")

    job_name = (state or {}).get("build_index_job_name", build_index_job_name)
    upload_text(
        bucket_name,
        BUILD_INDEX_CONFIG_OBJECT,
        (
            f"data_root={data_root}\n"
            f"octree_depth={octree_depth}\n"
        ),
    )

    print(f"starting {job_name}")
    execute_job_wait(job_name)
    print(f"finished {job_name}")

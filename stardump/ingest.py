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


def default_image_uri(project_id: str) -> str:
    return f"gcr.io/{project_id}/star-dump:latest"


def default_bucket_name(project_number: str) -> str:
    return f"star-dump-data-{project_number}"


def default_service_account_email(project_id: str) -> str:
    return f"star-dump-run@{project_id}.iam.gserviceaccount.com"


def run_name(urls: list[str]) -> str:
    digest = hashlib.md5()
    for url in sorted(urls):
        digest.update(url.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def default_data_root(mount_root: str, urls: list[str]) -> str:
    return f"{mount_root}/{run_name(urls)}"


def join_with_commas(args: list[str]) -> str:
    return ",".join(args)


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
    input_manifest: str,
    task_count: int,
    parallelism: int,
) -> list[str]:
    ingest_args = [
        f"--input-manifest={input_manifest}",
        f"--output-root={data_root}",
    ]
    return [
        "--image",
        image_uri,
        "--service-account",
        service_account_email,
        "--memory",
        "1Gi",
        "--task-timeout=3600",
        "--max-retries=0",
        "--tasks",
        str(task_count),
        "--parallelism",
        str(parallelism),
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
        "--cpu",
        "4",
        "--memory",
        "8Gi",
        "--task-timeout=14400",
        "--max-retries=0",
        "--add-volume",
        f"name=gcs,type=cloud-storage,bucket={bucket_name},readonly=false",
        "--add-volume-mount",
        f"volume=gcs,mount-path={mount_root}",
        "--command",
        "/usr/local/bin/build-index",
        "--args=" + join_with_commas(["--data-root", data_root, "--octree-depth", str(octree_depth)]),
    ]


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
    image_uri: str | None,
    bucket_name: str | None,
    mount_root: str,
    ingest_job_name: str,
    build_index_job_name: str,
    service_account_email: str | None,
    parallelism: int,
    data_root: str | None,
) -> None:
    project_id = current_project_id()
    project_number = current_project_number(project_id)
    image_uri = image_uri or default_image_uri(project_id)
    bucket_name = bucket_name or default_bucket_name(project_number)
    service_account_email = service_account_email or default_service_account_email(project_id)
    urls = fetch_gaia_source_urls()
    if not urls:
        raise SystemExit("failed to fetch Gaia source URL list")

    checksums = fetch_gaia_source_md5s()
    run_id = run_name(urls)
    data_root = data_root or default_data_root(mount_root, urls)
    input_manifest = upload_manifest(bucket_name, run_id, urls, checksums)
    task_count = len(urls)
    job_name = ingest_job_name
    job_args = ingest_job_args(
        image_uri,
        service_account_email,
        bucket_name,
        mount_root,
        data_root,
        f"{mount_root}/{run_id}/inputs.txt",
        task_count,
        parallelism,
    )
    create_or_update_job(job_name, job_args)
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
            "image_uri": image_uri,
            "input_manifest": input_manifest,
            "run_name": run_id,
        }
    )

    print(f"gaia_source_files: {len(urls)}")
    print(f"ingest_tasks: {task_count}")
    print(f"parallelism: {parallelism}")
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
    image_uri: str | None,
    bucket_name: str | None,
    mount_root: str,
    build_index_job_name: str,
    service_account_email: str | None,
    octree_depth: int,
    data_root: str | None,
) -> None:
    project_id = current_project_id()
    project_number = current_project_number(project_id)

    try:
        state = read_state()
    except FileNotFoundError:
        state = None

    if state is not None:
        rows = status_rows(state)
        if any(row["state"] != "succeeded" for row in rows):
            print_status(rows)
            raise SystemExit("cannot start build-index before all ingest batches succeeded")

    image_uri = image_uri or (state or {}).get("image_uri") or default_image_uri(project_id)
    bucket_name = bucket_name or default_bucket_name(project_number)
    service_account_email = service_account_email or default_service_account_email(project_id)
    data_root = data_root or (state or {}).get("data_root")
    if not data_root:
        raise SystemExit(f"missing state file: {STATE_PATH}; pass --data-root to run build-index")

    job_name = build_index_job_name
    create_or_update_job(
        job_name,
        build_index_job_args(
            image_uri,
            service_account_email,
            bucket_name,
            mount_root,
            data_root,
            octree_depth,
        ),
    )

    print(f"starting {job_name}")
    execute_job_wait(job_name)
    print(f"finished {job_name}")

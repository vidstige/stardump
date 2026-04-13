import argparse

from .ingest import start_build_index, start_ingest, status_ingest


def add_runtime_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--image-uri")
    parser.add_argument("--bucket-name")
    parser.add_argument("--mount-root", default="/mnt/gcs")
    parser.add_argument("--service-account-email")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="stardump")
    commands = parser.add_subparsers(dest="command", required=True)

    ingest = commands.add_parser("ingest")
    ingest_commands = ingest.add_subparsers(dest="action", required=True)

    start = ingest_commands.add_parser("start")
    add_runtime_flags(start)
    start.add_argument("--ingest-job-name", default="star-dump-ingest")
    start.add_argument("--build-index-job-name", default="star-dump-build-index")
    start.add_argument("--parallelism", type=int, default=64)
    start.add_argument("--data-root")

    ingest_commands.add_parser("status")

    build_index = ingest_commands.add_parser("build-index")
    add_runtime_flags(build_index)
    build_index.add_argument("--build-index-job-name", default="star-dump-build-index")
    build_index.add_argument("--octree-depth", type=int, default=6)
    build_index.add_argument("--data-root")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.command != "ingest":
        raise SystemExit(f"unsupported command: {args.command}")

    if args.action == "start":
        start_ingest(
            image_uri=args.image_uri,
            bucket_name=args.bucket_name,
            mount_root=args.mount_root,
            ingest_job_name=args.ingest_job_name,
            build_index_job_name=args.build_index_job_name,
            service_account_email=args.service_account_email,
            parallelism=args.parallelism,
            data_root=args.data_root,
        )
        return
    if args.action == "status":
        status_ingest()
        return
    if args.action == "build-index":
        start_build_index(
            image_uri=args.image_uri,
            bucket_name=args.bucket_name,
            mount_root=args.mount_root,
            build_index_job_name=args.build_index_job_name,
            service_account_email=args.service_account_email,
            octree_depth=args.octree_depth,
            data_root=args.data_root,
        )
        return

    raise SystemExit(f"unsupported action: {args.action}")


if __name__ == "__main__":
    main()

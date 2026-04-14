import argparse

from .ingest import index_status, start_build_index, start_ingest, status_ingest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="stardump")
    commands = parser.add_subparsers(dest="command", required=True)

    ingest = commands.add_parser("ingest")
    ingest_commands = ingest.add_subparsers(dest="action", required=True)

    start = ingest_commands.add_parser("start")
    start.add_argument("--bucket-name")
    start.add_argument("--ingest-job-name", default="star-dump-ingest")
    start.add_argument("--build-index-job-name", default="star-dump-build-index")
    start.add_argument("--data-root")

    ingest_commands.add_parser("status")

    index_status_parser = ingest_commands.add_parser("index-status")
    index_status_parser.add_argument("--bucket-name")
    index_status_parser.add_argument("--data-root")
    index_status_parser.add_argument("--octree-depth", type=int, default=6)

    build_index = ingest_commands.add_parser("build-index")
    build_index.add_argument("--bucket-name")
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
            bucket_name=args.bucket_name,
            ingest_job_name=args.ingest_job_name,
            build_index_job_name=args.build_index_job_name,
            data_root=args.data_root,
        )
        return
    if args.action == "status":
        status_ingest()
        return
    if args.action == "index-status":
        index_status(
            bucket_name=args.bucket_name,
            data_root=args.data_root,
            octree_depth=args.octree_depth,
        )
        return
    if args.action == "build-index":
        start_build_index(
            bucket_name=args.bucket_name,
            build_index_job_name=args.build_index_job_name,
            octree_depth=args.octree_depth,
            data_root=args.data_root,
        )
        return

    raise SystemExit(f"unsupported action: {args.action}")


if __name__ == "__main__":
    main()

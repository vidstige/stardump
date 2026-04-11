import argparse

from .full_ingest import start_build_index, start_full_ingest, status_full_ingest, wait_full_ingest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="stardump")
    commands = parser.add_subparsers(dest="command", required=True)

    full_ingest = commands.add_parser("full-ingest")
    full_ingest_commands = full_ingest.add_subparsers(dest="action", required=True)
    full_ingest_commands.add_parser("start")
    full_ingest_commands.add_parser("status")
    full_ingest_commands.add_parser("wait")
    full_ingest_commands.add_parser("build-index")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.command != "full-ingest":
        raise SystemExit(f"unsupported command: {args.command}")

    if args.action == "start":
        start_full_ingest()
        return
    if args.action == "status":
        status_full_ingest()
        return
    if args.action == "wait":
        wait_full_ingest()
        return
    if args.action == "build-index":
        start_build_index()
        return

    raise SystemExit(f"unsupported action: {args.action}")

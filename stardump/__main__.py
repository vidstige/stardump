import argparse

from .ingest import start_build_index, start_ingest, status_ingest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="stardump")
    commands = parser.add_subparsers(dest="command", required=True)

    ingest = commands.add_parser("ingest")
    ingest_commands = ingest.add_subparsers(dest="action", required=True)
    ingest_commands.add_parser("start")
    ingest_commands.add_parser("status")
    ingest_commands.add_parser("build-index")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.command != "ingest":
        raise SystemExit(f"unsupported command: {args.command}")

    if args.action == "start":
        start_ingest()
        return
    if args.action == "status":
        status_ingest()
        return
    if args.action == "build-index":
        start_build_index()
        return

    raise SystemExit(f"unsupported action: {args.action}")


if __name__ == "__main__":
    main()

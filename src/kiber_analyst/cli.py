from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from kiber_analyst.config_paths import config_path, data_path
from kiber_analyst.embed import embed_job
from kiber_analyst.errors import IngestError
from kiber_analyst.fetch import fetch_archive
from kiber_analyst.load import load_job
from kiber_analyst.parse import parse_job
from kiber_analyst.report import build_report, print_report
from kiber_analyst.search import print_search_result
from kiber_analyst.search import search as search_corpus


def _parser(prog: str = "kiber-analyst") -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=prog)
    from kiber_analyst import __version__

    parser.add_argument(
        "--version",
        action="version",
        version=f"{prog} {__version__} build={os.environ.get('KIBER_ANALYST_BUILD_ID', __version__)}",
    )
    top = parser.add_subparsers(dest="source", required=True)
    telegram = top.add_parser("telegram", help="ingest Telegram Desktop exports")
    commands = telegram.add_subparsers(dest="command", required=True)

    fetch = commands.add_parser("fetch", help="fetch and safely unpack one archive")
    fetch.add_argument("--archive", required=True)
    fetch.add_argument("--work-root", type=Path, default=data_path("jobs"))
    fetch.add_argument("--scope", choices=("topic", "chat"), default="chat")
    fetch.add_argument("--topic-id")
    fetch.add_argument("--topic-title")
    fetch.add_argument("--chat-id", required=True)
    fetch.add_argument("--md5")
    fetch.add_argument("--size", type=int)

    parse = commands.add_parser("parse", help="normalize result.json into JSONL")
    parse.add_argument("--job", type=Path, required=True)

    load = commands.add_parser("load", help="load normalized messages into knowledge")
    load.add_argument("--job", type=Path, required=True)
    load.add_argument(
        "--dry-run",
        action="store_true",
        help="execute the full database transaction and roll it back",
    )

    telegram_ingest = commands.add_parser(
        "ingest", help="queue a complete restricted Telegram topic export for import"
    )
    telegram_ingest.add_argument("--job", type=Path, required=True)
    telegram_ingest.add_argument("--project", default="default")
    telegram_ingest.add_argument("--dry-run", action="store_true")

    report = commands.add_parser("report", help="write report.json and print a summary")
    report.add_argument("--job", type=Path, required=True)

    asr = commands.add_parser(
        "asr", help="transcribe job audio with sequential Groq profile rotation"
    )
    asr.add_argument("--job", type=Path, required=True)
    asr.add_argument(
        "--rotation-config",
        type=Path,
        default=config_path("asr-rotation.yaml"),
    )
    asr.add_argument(
        "--profile",
        action="append",
        dest="profiles",
        help="override profile order; may be supplied more than once",
    )
    asr.add_argument(
        "--max-cycles",
        type=int,
        help=(
            "maximum full profile passes per audio file "
            "(default from config, normally 5)"
        ),
    )

    run = commands.add_parser("run", help="run fetch, parse, load, and report")
    run.add_argument("--archive", required=True)
    run.add_argument("--work-root", type=Path, default=data_path("jobs"))
    run.add_argument("--scope", choices=("topic", "chat"), default="chat")
    run.add_argument("--topic-id")
    run.add_argument("--topic-title")
    run.add_argument("--chat-id", required=True)
    run.add_argument("--md5")
    run.add_argument("--size", type=int)

    provider = top.add_parser(
        "provider-check", help="check one provider profile through its proxy"
    )
    provider.add_argument("--profile", required=True)
    provider.add_argument("--audio", type=Path, help="send one OGG to Groq ASR")
    provider.add_argument(
        "--egress", action="store_true", help="report only egress country and ASN"
    )

    asr_commands = top.add_parser("asr", help="store, embed, and search ASR results")
    asr_subcommands = asr_commands.add_subparsers(dest="command", required=True)
    asr_load = asr_subcommands.add_parser(
        "load", help="load validated ASR JSON into knowledge"
    )
    asr_load.add_argument("--manifest", type=Path, required=True)
    asr_load.add_argument("--job", type=Path, required=True)
    asr_load.add_argument("--project", required=True)
    asr_load.add_argument("--dry-run", action="store_true")
    asr_load.add_argument("--report", type=Path)

    asr_embed = asr_subcommands.add_parser("embed", help="embed stored ASR segments")
    asr_scope = asr_embed.add_mutually_exclusive_group(required=True)
    asr_scope.add_argument("--run", dest="run_key")
    asr_scope.add_argument("--all-runs", action="store_true")
    asr_embed.add_argument("--project", required=True)
    asr_embed.add_argument("--limit", type=int)
    asr_embed.add_argument("--report", type=Path)
    asr_embed.add_argument("--source-id")

    embed = top.add_parser("embed", help="create Gemini embeddings for a loaded job")
    embed.add_argument("--job", type=Path, required=True)
    embed.add_argument("--project", help="override project slug from the job manifest")

    search = top.add_parser("search", help="search the knowledge corpus")
    search.add_argument("query")
    search.add_argument("--project", required=True)
    search.add_argument("--limit", type=int, default=10)
    search.add_argument(
        "--source-kind", choices=("telegram", "document", "web", "video")
    )
    search.add_argument("--source", choices=("segment", "asr"), dest="source_filter")
    search.add_argument("--date-from", type=date.fromisoformat)
    search.add_argument("--date-to", type=date.fromisoformat)
    search.add_argument("--author")
    search.add_argument("--topic")
    search.add_argument("--source-id")
    search.add_argument("--json", action="store_true", dest="as_json")

    jobs = top.add_parser("jobs", help="submit and manage durable worker jobs")
    job_actions = jobs.add_subparsers(dest="action", required=True)
    submit = job_actions.add_parser("submit", help="submit one supported operation")
    submit.add_argument("--json", action="store_true", dest="as_json")
    submit.add_argument("--pool")
    submit.add_argument("--profile")
    submit.add_argument(
        "--strategy", choices=("round_robin", "failover", "weighted_round_robin")
    )
    submit.add_argument("--idempotency-key")
    submit.add_argument("command_tokens", nargs=argparse.REMAINDER)
    for action in ("status", "result", "ack", "cancel"):
        cmd = job_actions.add_parser(action)
        cmd.add_argument("job_id")
        cmd.add_argument("--json", action="store_true", dest="as_json")
    wait = job_actions.add_parser("wait")
    wait.add_argument("job_id")
    wait.add_argument("--timeout", type=int, default=30)
    wait.add_argument("--json", action="store_true", dest="as_json")
    backup = job_actions.add_parser("backup")
    backup.add_argument("--pre-upgrade", action="store_true")
    backup.add_argument("--json", action="store_true", dest="as_json")
    extend = job_actions.add_parser("extend")
    extend.add_argument("job_id")
    extend.add_argument("--hours", required=True, type=float)
    extend.add_argument("--json", action="store_true", dest="as_json")
    reset_profile = job_actions.add_parser("reset-profile")
    reset_profile.add_argument("profile")
    reset_profile.add_argument("--json", action="store_true", dest="as_json")

    doctor = top.add_parser("doctor", help="check worker build and queue storage")
    doctor.add_argument("--json", action="store_true", dest="as_json")
    return parser


def _print_json(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2))


def _run(args: argparse.Namespace) -> int:
    if args.source == "jobs":
        from kiber_analyst.jobapi import api_call, submit_command, wait_job

        if args.action == "submit":
            if args.profile and (args.pool or args.strategy):
                raise ValueError("--profile cannot be combined with --pool or --strategy")
            command_tokens = list(args.command_tokens)
            if command_tokens[:1] == ["--"]:
                command_tokens = command_tokens[1:]
            if not command_tokens:
                raise ValueError("Provide a supported command after --")
            result = submit_command(
                command_tokens,
                pool=args.pool,
                strategy=args.strategy,
                profile=args.profile,
                idempotency_key=args.idempotency_key,
            )
        elif args.action == "wait":
            result = wait_job(args.job_id, args.timeout)
        elif args.action in {"status", "result", "ack", "cancel"}:
            result = api_call(args.action, job_id=args.job_id)
        elif args.action == "backup":
            result = api_call("backup", pre_upgrade=args.pre_upgrade)
        elif args.action == "extend":
            result = api_call("extend", job_id=args.job_id, hours=args.hours)
        elif args.action == "reset-profile":
            result = api_call("reset_profile", profile=args.profile)
        else:
            raise AssertionError(f"Unhandled jobs action: {args.action}")
        _print_json(result)
        return 0
    if args.source == "doctor":
        from kiber_analyst.jobapi import api_call

        _print_json(api_call("doctor"))
        return 0
    if args.source == "provider-check":
        from kiber_analyst.provider_preflight import preflight

        _print_json(preflight(args.profile, audio=args.audio, egress=args.egress))
        return 0
    if args.source == "embed":
        report = embed_job(args.job, project=args.project)
        _print_json(report)
        return 0
    if args.source == "asr":
        if args.command == "load":
            from kiber_analyst.asr_store import load_asr

            report = load_asr(
                args.manifest,
                args.job,
                project=args.project,
                dry_run=args.dry_run,
                report_path=args.report,
            )
        elif args.command == "embed":
            from kiber_analyst.asr_embed import embed_asr

            report = embed_asr(
                project=args.project,
                run_key=args.run_key,
                all_runs=args.all_runs,
                source_id=args.source_id,
                limit=args.limit,
                report_path=args.report,
            )
        else:
            raise AssertionError(f"Unhandled ASR command: {args.command}")
        _print_json(report)
        return 0
    if getattr(args, "command", None) == "asr":
        from kiber_analyst.asr import run_asr_batch

        report = run_asr_batch(
            args.job,
            config_path=args.rotation_config,
            max_cycles=args.max_cycles,
            profiles=tuple(args.profiles) if args.profiles else None,
        )
        _print_json(report)
        return 0 if report["status"] == "complete" else 2
    if args.source == "search":
        if not 1 <= args.limit <= 100:
            raise ValueError("--limit must be between 1 and 100")
        if args.date_from and args.date_to and args.date_from > args.date_to:
            raise ValueError("--date-from must not be later than --date-to")
        result = search_corpus(
            args.query,
            project=args.project,
            limit=args.limit,
            source_kind=args.source_kind,
            source=args.source_filter,
            date_from=args.date_from,
            date_to=args.date_to,
            author=args.author,
            topic=args.topic,
            source_id=args.source_id,
        )
        print_search_result(result, as_json=args.as_json)
        return 0
    if args.command == "fetch":
        job = fetch_archive(
            args.archive,
            args.work_root,
            scope=args.scope,
            topic_id=args.topic_id if args.scope == "topic" else None,
            topic_title=args.topic_title if args.scope == "topic" else None,
            chat_id=args.chat_id,
            expected_md5=args.md5,
            expected_size=args.size,
        )
        print(job)
        return 0
    if args.command == "parse":
        _print_json(parse_job(args.job))
        return 0
    if args.command == "load":
        _print_json(load_job(args.job, dry_run=args.dry_run))
        return 0
    if args.command == "report":
        report = build_report(args.job)
        print_report(report)
        return 0
    if args.command == "run":
        job = fetch_archive(
            args.archive,
            args.work_root,
            scope=args.scope,
            topic_id=args.topic_id if args.scope == "topic" else None,
            topic_title=args.topic_title if args.scope == "topic" else None,
            chat_id=args.chat_id,
            expected_md5=args.md5,
            expected_size=args.size,
        )
        parse_job(job)
        load_job(job)
        report = build_report(job)
        print_report(report)
        return 0
    raise AssertionError(f"Unhandled command: {args.command}")


def _main(argv: Sequence[str] | None, prog: str) -> int:
    parser = _parser(prog)
    args = parser.parse_args(argv)
    try:
        is_worker_operation = (
            args.source in {"embed", "search"}
            or (args.source == "asr" and args.command == "embed")
            or (args.source == "telegram" and args.command in {"asr", "ingest"})
        )
        if is_worker_operation:
            from kiber_analyst.jobapi import api_call, submit_command, wait_job

            command = list(argv) if argv is not None else sys.argv[1:]
            submitted = submit_command(command)
            job_id = submitted["job_id"]
            state = wait_job(job_id, 30)
            if state["state"] in {"done", "failed", "timed_out", "cancelled"}:
                result = api_call("result", job_id=job_id)
                if args.source == "search" and not args.as_json:
                    print_search_result(result, as_json=False)
                    print(f"job_id={job_id}")
                else:
                    _print_json(result)
                return 0 if state["state"] == "done" else 2
            _print_json({"job_id": job_id, **state})
            return 3
        return _run(args)
    except (IngestError, OSError, ValueError) as exc:
        print(f"{prog}: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print(f"{prog}: interrupted", file=sys.stderr)
        return 130


def main(argv: Sequence[str] | None = None) -> int:
    """Run the canonical Kiber-analyst command line interface."""
    return _main(argv, "kiber-analyst")


if __name__ == "__main__":
    raise SystemExit(main())

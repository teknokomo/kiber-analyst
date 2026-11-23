from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from kiber_analyst.cli import main


def test_load_dry_run_flag_is_forwarded(tmp_path: Path, capsys) -> None:
    result = {
        "status": "dry_run",
        "dry_run": True,
        "transaction": "rolled_back",
        "rollback_confirmed": True,
        "inserted": 2,
        "updated": 0,
        "skipped": 0,
        "absent": 0,
    }
    with patch("kiber_analyst.cli.load_job", return_value=result) as load_job:
        exit_code = main(["telegram", "load", "--job", str(tmp_path), "--dry-run"])

    assert exit_code == 0
    load_job.assert_called_once_with(tmp_path, dry_run=True)
    assert json.loads(capsys.readouterr().out) == result


def test_entry_point_reports_canonical_name(capsys) -> None:
    from kiber_analyst import __version__

    try:
        main(["--version"])
    except SystemExit as exc:
        assert exc.code == 0
    else:
        raise AssertionError("--version must exit")
    assert capsys.readouterr().out.startswith(f"kiber-analyst {__version__} ")


def test_cli_parses_asr_load_embed_and_search_source_filter(tmp_path: Path) -> None:
    from kiber_analyst.cli import _parser

    parser = _parser()
    load = parser.parse_args(
        [
            "asr",
            "load",
            "--manifest",
            str(tmp_path / "manifest.json"),
            "--job",
            str(tmp_path),
            "--project",
            "sample-project",
            "--dry-run",
        ]
    )
    embed = parser.parse_args(
        ["asr", "embed", "--all-runs", "--project", "sample-project", "--limit", "64"]
    )
    search = parser.parse_args(
        ["search", "voice phrase", "--project", "sample-project", "--source", "asr"]
    )

    assert load.source == "asr" and load.command == "load" and load.dry_run
    assert embed.source == "asr" and embed.command == "embed" and embed.all_runs
    assert embed.limit == 64
    assert search.source == "search" and search.source_filter == "asr"


def test_search_command_runs_without_telegram_subcommand_namespace(capsys) -> None:
    from unittest.mock import patch

    result = {
        "query": "voice phrase",
        "query_sha256": "a" * 64,
        "query_length": 12,
        "mode": "fts",
        "results": [],
    }
    with (
        patch("kiber_analyst.jobapi.submit_command", return_value={"job_id": "job-1"}),
        patch("kiber_analyst.jobapi.wait_job", return_value={"state": "done"}),
        patch("kiber_analyst.jobapi.api_call", return_value=result) as api_call,
    ):
        assert (
            main(["search", "voice phrase", "--project", "sample-project", "--json"]) == 0
        )
    api_call.assert_called_once_with("result", job_id="job-1")
    assert json.loads(capsys.readouterr().out) == result


def test_jobs_submit_parser_allows_pool_strategy_and_profile_mode() -> None:
    from kiber_analyst.cli import _parser

    parser = _parser()
    selected_strategy = parser.parse_args(
        ["jobs", "submit", "--pool", "groq_asr", "--strategy", "round_robin", "--", "telegram", "asr", "--job", "/tmp/disallowed-job"]
    )
    selected_profile = parser.parse_args(
        ["jobs", "submit", "--profile", "groq_asr", "--", "telegram", "asr", "--job", "/tmp/disallowed-job"]
    )

    assert selected_strategy.pool == "groq_asr"
    assert selected_strategy.strategy == "round_robin"
    assert selected_profile.profile == "groq_asr"

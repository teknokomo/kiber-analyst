from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from kiber_analyst import jobapi
from kiber_analyst.jobqueue import JobsError


def test_job_api_accepts_supported_search_and_fingerprints_filters() -> None:
    operation, params, items = jobapi.parse_job_command(
        ["search", "кольцо", "--project", "sample-project", "--author", "Владимир"]
    )

    assert operation == "search"
    assert params["author"] == "Владимир"
    assert len(items[0]["fingerprint"]) == 64


def test_worker_submit_and_schedule_controls_use_explicit_allowlisted_actions(monkeypatch) -> None:
    captured = []
    monkeypatch.setattr(jobapi, "request", lambda _socket, payload, **_kwargs: captured.append(payload) or {})
    jobapi.submit_command(
        ["search", "query", "--project", "sample-project"],
        idempotency_key="scheduled-request",
        deadline_at="2026-11-24T12:00:00Z",
        next_run_at="2026-11-24T10:00:00Z",
    )
    jobapi.api_call("set-deadline", job_id="job", deadline_at="2026-11-24T12:00:00Z")
    jobapi.api_call("reschedule", job_id="job", at="2026-11-24T10:00:00Z")
    assert captured[0]["deadline_at"] == "2026-11-24T12:00:00Z"
    assert captured[0]["next_run_at"] == "2026-11-24T10:00:00Z"
    assert [payload["action"] for payload in captured[1:]] == ["set-deadline", "reschedule"]
    with pytest.raises(JobsError, match="Unsupported worker operation"):
        jobapi.api_call("execute-shell")


def test_telegram_ingest_item_fingerprint_matches_worker_import_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import kiber_analyst.telegram_ingest as telegram_ingest

    export = SimpleNamespace(
        path=tmp_path,
        import_key="a" * 64,
        message_sha256="b" * 64,
        service_sha256="c" * 64,
    )
    monkeypatch.setattr(telegram_ingest, "inspect_export", lambda _path: export)

    operation, params, items = jobapi.parse_job_command(
        ["telegram", "ingest", "--job", str(tmp_path), "--project", "sample-project"]
    )

    assert operation == "telegram_ingest"
    assert params["import_key"] == export.import_key
    assert items == [{"key": "telegram-export", "fingerprint": export.import_key}]


@pytest.mark.parametrize(
    "command",
    [
        ["bash", "-c", "touch /tmp/not-allowed"],
        ["telegram", "load", "--job", "/tmp/disallowed-job"],
        ["asr", "load", "--manifest", "manifest.json", "--job", "job", "--project", "sample-project"],
    ],
)
def test_job_api_rejects_unsupported_or_arbitrary_commands(command: list[str]) -> None:
    with pytest.raises(JobsError, match="Supported operations"):
        jobapi.parse_job_command(command)


def test_invalid_command_does_not_echo_arguments_to_stderr(capsys) -> None:
    with pytest.raises(JobsError):
        jobapi.parse_job_command(
            ["search", "q", "--project", "p", "--unknown-secret-option", "private-value"]
        )
    assert capsys.readouterr().err == ""


def test_job_input_path_must_be_inside_data_root(tmp_path: Path, monkeypatch) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(jobapi, "DATA_PREFIX", tmp_path / "approved")
    with pytest.raises(JobsError, match="under the configured data directory"):
        jobapi._safe_input_path(str(outside))


def test_asr_job_supports_source_load_and_selected_refs(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / 'media-root'
    job = root / 'asr-input'
    audio = job / 'unpacked' / 'media' / '123' / 'voice.ogg'
    audio.parent.mkdir(mode=0o700, parents=True)
    audio.write_bytes(b'fake ogg')
    audio.chmod(0o600)
    (job / 'normalized').mkdir(mode=0o700, parents=True)
    record = {
        'chat_id': '-1009000', 'message_id': '123', 'media_type': 'voice_message',
        'media_path': 'media/123/voice.ogg',
    }
    (job / 'normalized' / 'messages.jsonl').write_text(json.dumps(record) + '\n')
    (job / 'normalized' / 'messages.jsonl').chmod(0o600)
    for directory in (root, job, job / 'unpacked', job / 'unpacked/media',
                      job / 'unpacked/media/123', job / 'normalized'):
        directory.chmod(0o700)
    monkeypatch.setattr(jobapi, 'DATA_PREFIX', root)

    operation, params, items = jobapi.parse_job_command([
        'telegram', 'asr', '--job', str(job), '--load-results',
        '--project', 'sample-project', '--run-id', 'ka-abc123',
        '--external-ref', 'tg:-1009000:123',
    ])

    assert operation == 'telegram_asr'
    assert params['load_results'] is True
    assert params['run_id'] == 'ka-abc123'
    assert params['external_refs'] == ['tg:-1009000:123']
    assert items[0]['key'] == 'tg:-1009000:123'


def test_asr_job_rejects_routing_and_unbounded_selection(tmp_path: Path) -> None:
    with pytest.raises(JobsError, match='requires --project'):
        jobapi.parse_job_command([
            'telegram', 'asr', '--job', str(tmp_path), '--load-results',
        ])


def test_asr_embed_accepts_only_valid_source_scope() -> None:
    operation, params, items = jobapi.parse_job_command([
        'asr', 'embed', '--all-runs', '--project', 'sample-project',
        '--source-id', 'telegram:-10012345:topic:1',
    ])
    assert operation == 'asr_embed'
    assert params['source_id'] == 'telegram:-10012345:topic:1'
    assert len(items[0]['fingerprint']) == 64
    with pytest.raises(JobsError, match='Invalid Telegram source ID'):
        jobapi.parse_job_command([
            'asr', 'embed', '--all-runs', '--project', 'sample-project',
            '--source-id', 'telegram:-10012345:topic:../1',
        ])

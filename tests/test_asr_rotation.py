from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from kiber_analyst.asr import ASRError, load_rotation_config, run_asr_batch


class HTTPFailure(Exception):
    def __init__(self, status_code: int, retry_after: str | None = None) -> None:
        self.status_code = status_code
        self.response = SimpleNamespace(
            headers={"retry-after": retry_after} if retry_after is not None else {}
        )


class RawResponse:
    status_code = 200

    def __init__(self, payload: dict[str, Any]) -> None:
        self._body = json.dumps(payload, ensure_ascii=False).encode()
        self.http_response = SimpleNamespace(headers={})
        self.closed = False

    def read(self) -> bytes:
        return self._body

    def close(self) -> None:
        self.closed = True


class FakeSDK:
    def __init__(self, outcome: Any, calls: list[dict[str, Any]]) -> None:
        self._outcome = outcome
        self._calls = calls
        creator = SimpleNamespace(create=self._create)
        self.audio = SimpleNamespace(
            transcriptions=SimpleNamespace(with_raw_response=creator)
        )
        self.closed = False

    def _create(self, **kwargs: Any) -> RawResponse:
        self._calls.append(
            {
                "model": kwargs["model"],
                "language": kwargs["language"],
                "response_format": kwargs["response_format"],
                "prompt": kwargs["prompt"],
                "temperature": kwargs["temperature"],
                "file_name": kwargs["file"][0],
            }
        )
        if isinstance(self._outcome, Exception):
            raise self._outcome
        return RawResponse(self._outcome)

    def close(self) -> None:
        self.closed = True


class FakeClock:
    def __init__(self) -> None:
        self.value = 100.0
        self.sleeps: list[float] = []

    def now(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value += seconds


def make_job(tmp_path: Path, *, file_size: int = 12) -> tuple[Path, Path]:
    job = tmp_path / "job"
    unpacked = (
        job
        / "unpacked"
        / "ChatExport"
        / "chats"
        / "chat_123"
        / "topic_1"
        / "voice_messages"
    )
    unpacked.mkdir(parents=True)
    audio = unpacked / "audio_1.ogg"
    audio.write_bytes(b"a" * file_size)
    normalized = job / "normalized"
    normalized.mkdir()
    (normalized / "messages.jsonl").write_text(
        json.dumps(
            {
                "chat_id": "-100123",
                "message_id": 7,
                "media_type": "voice_message",
                "media_path": "voice_messages/audio_1.ogg",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return job, audio


def write_config(path: Path, profiles: list[str], *, cycles: int = 5) -> Path:
    path.write_text(
        "profiles:\n"
        + "".join(f"  - {profile}\n" for profile in profiles)
        + f"max_cycles: {cycles}\n"
        + "request_timeout_seconds: 60\n"
        + "max_runtime_seconds: 300\n"
        + "backoff_initial_seconds: 10\n"
        + "backoff_max_seconds: 30\n",
        encoding="utf-8",
    )
    return path


def run_with_outcomes(
    tmp_path: Path,
    outcomes: list[Any],
    profiles: list[str],
    *,
    cycles: int = 5,
    run_id: str = "test-run",
) -> tuple[dict[str, Any], list[str], list[dict[str, Any]], FakeClock]:
    job, _audio = make_job(tmp_path)
    config = write_config(tmp_path / "rotation.yaml", profiles, cycles=cycles)
    called_profiles: list[str] = []
    request_calls: list[dict[str, Any]] = []
    pending = iter(outcomes)

    def factory(profile: str, **_kwargs: Any) -> FakeSDK:
        called_profiles.append(profile)
        return FakeSDK(next(pending), request_calls)

    clock = FakeClock()
    report = run_asr_batch(
        job,
        config_path=config,
        profile_loader=lambda _profile: SimpleNamespace(provider="groq"),
        client_factory=factory,
        sleep_fn=clock.sleep,
        clock=clock.now,
        run_id=run_id,
    )
    return report, called_profiles, request_calls, clock


def successful_payload() -> dict[str, Any]:
    return {
        "text": "готовая транскрипция",
        "language": "Russian",
        "segments": [
            {
                "start": 0.0,
                "end": 1.0,
                "text": "готовая транскрипция",
                "avg_logprob": -0.1,
                "no_speech_prob": 0.0,
            }
        ],
    }


def test_rotation_defaults_to_five_cycles_and_glossary(tmp_path: Path) -> None:
    config_path = write_config(tmp_path / "rotation.yaml", ["groq_asr"])
    config = load_rotation_config(config_path)
    assert config.max_cycles == 5
    assert "Example Station" in config.prompt
    assert "Советский Союз" in config.prompt


def test_transient_error_rotates_to_next_profile_and_saves_private_raw_json(
    tmp_path: Path,
) -> None:
    report, called, requests, _clock = run_with_outcomes(
        tmp_path,
        [HTTPFailure(503), successful_payload()],
        ["groq_asr", "groq_asr_2", "groq_asr_3"],
    )

    assert report["status"] == "complete"
    assert report["attempts"] == 2
    assert called == ["groq_asr", "groq_asr_2"]
    assert requests[0]["model"] == "whisper-large-v3-turbo"
    assert requests[0]["language"] == "ru"
    assert requests[0]["response_format"] == "verbose_json"
    assert requests[0]["temperature"] == 0
    assert "Example Station" in requests[0]["prompt"]

    item = report["items"][0]
    result_path = Path(report["run_dir"]) / item["result"]
    assert (
        json.loads(result_path.read_text(encoding="utf-8"))["text"]
        == "готовая транскрипция"
    )
    assert result_path.stat().st_mode & 0o777 == 0o600
    manifest = json.loads((Path(report["run_dir"]) / "manifest.json").read_text())
    assert manifest["items"][0]["profile"] == "groq_asr_2"
    assert "api_key" not in json.dumps(manifest)
    assert "proxy_url" not in json.dumps(manifest)


def test_retry_after_is_waited_before_retrying_the_same_profile(tmp_path: Path) -> None:
    report, called, _requests, clock = run_with_outcomes(
        tmp_path,
        [HTTPFailure(429, "12"), HTTPFailure(502), successful_payload()],
        ["groq_asr", "groq_asr_2"],
        cycles=2,
    )
    assert report["status"] == "complete"
    assert called == ["groq_asr", "groq_asr_2", "groq_asr"]
    assert 12.0 in clock.sleeps


def test_401_quarantines_profile_for_rest_of_job(tmp_path: Path) -> None:
    report, called, _requests, _clock = run_with_outcomes(
        tmp_path,
        [
            HTTPFailure(401),
            HTTPFailure(502),
            HTTPFailure(504),
            successful_payload(),
        ],
        ["groq_asr", "groq_asr_2", "groq_asr_3"],
        cycles=2,
    )
    assert report["status"] == "complete"
    assert called == ["groq_asr", "groq_asr_2", "groq_asr_3", "groq_asr_2"]


@pytest.mark.parametrize("status", [400, 403, 413, 422])
def test_nonretryable_request_or_policy_error_stops_without_rotation(
    tmp_path: Path, status: int
) -> None:
    report, called, _requests, _clock = run_with_outcomes(
        tmp_path,
        [HTTPFailure(status), successful_payload()],
        ["groq_asr", "groq_asr_2"],
    )
    assert report["status"] == "stopped"
    assert report["attempts"] == 1
    assert called == ["groq_asr"]


def test_same_server_error_from_every_profile_stops_after_one_cycle(
    tmp_path: Path,
) -> None:
    report, called, _requests, _clock = run_with_outcomes(
        tmp_path,
        [HTTPFailure(503), HTTPFailure(503), HTTPFailure(503)],
        ["groq_asr", "groq_asr_2", "groq_asr_3"],
        cycles=5,
    )
    assert report["status"] == "stopped"
    assert report["stop_reason"] == "same_server_error_on_all_profiles"
    assert report["attempts"] == 3
    assert called == ["groq_asr", "groq_asr_2", "groq_asr_3"]


def test_cycle_limit_bounds_attempts_per_audio(tmp_path: Path) -> None:
    report, called, _requests, _clock = run_with_outcomes(
        tmp_path,
        [HTTPFailure(503), HTTPFailure(504)] * 2,
        ["groq_asr", "groq_asr_2"],
        cycles=2,
    )
    assert report["status"] == "stopped"
    assert report["stop_reason"] == "retry_exhausted"
    assert report["attempts"] == 4
    assert called == ["groq_asr", "groq_asr_2"] * 2


def test_single_profile_retries_until_cycle_limit(tmp_path: Path) -> None:
    report, called, _requests, _clock = run_with_outcomes(
        tmp_path,
        [HTTPFailure(503), HTTPFailure(504), HTTPFailure(503)],
        ["groq_asr"],
        cycles=3,
    )
    assert report["status"] == "stopped"
    assert report["stop_reason"] == "retry_exhausted"
    assert report["attempts"] == 3
    assert called == ["groq_asr"] * 3


def test_oversize_audio_fails_before_any_provider_client_is_created(
    tmp_path: Path,
) -> None:
    job, audio = make_job(tmp_path)
    with audio.open("wb") as stream:
        stream.truncate(25_000_001)
    config = write_config(tmp_path / "rotation.yaml", ["groq_asr"])
    created: list[str] = []
    with pytest.raises(ASRError, match="supported range"):
        run_asr_batch(
            job,
            config_path=config,
            profile_loader=lambda _profile: SimpleNamespace(provider="groq"),
            client_factory=lambda profile, **_kwargs: created.append(profile),
            run_id="too-large",
        )
    assert created == []

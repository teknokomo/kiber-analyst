from __future__ import annotations

import argparse
import asyncio
import fcntl
import grp
import hashlib
import json
import os
import pwd
import signal
import socket
import socketserver
import stat
import struct
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from typing import Any

from kiber_analyst.config_paths import config_path
from kiber_analyst.embedding_contract import DIMENSIONS, MODEL, MODEL_KEY
from kiber_analyst.jobapi import (
    MAX_REQUEST_BYTES,
    MAX_RESPONSE_BYTES,
    parse_job_command,
)
from kiber_analyst.jobqueue import (
    DEFAULT_DATA_ROOT,
    DEFAULT_SOCKET,
    DEFAULT_UUID,
    JobRecord,
    JobsError,
    JobStore,
    canonical_json,
    iso_utc,
    parse_utc,
    utc_now,
)
from kiber_analyst.routing import (
    MeteredResult,
    PermanentProviderError,
    PoolConfig,
    ProviderRouter,
    RoutingError,
    WaitForQuota,
    load_pools,
)

ROUTING_PATH = Path(
    os.environ.get("KIBER_ANALYST_ROUTING_CONFIG", str(config_path("provider-routing.yaml")))
)
SOCKET_PATH = Path(os.environ.get("KIBER_ANALYST_SOCKET", str(DEFAULT_SOCKET)))
DATA_ROOT = Path(os.environ.get("KIBER_ANALYST_DATA_ROOT", str(DEFAULT_DATA_ROOT)))
DATA_UUID = os.environ.get("KIBER_ANALYST_DATA_UUID", DEFAULT_UUID)
TERMINAL_JOB_STATES = {"done", "failed", "timed_out", "cancelled"}
RERUNNABLE_OPERATIONS = {"external_ingest", "external_embed"}

DEFAULT_POOLS = {
    "telegram_asr": "groq_asr",
    "embed": "gemini_embed",
    "asr_embed": "gemini_asr_embed",
    "search": "gemini_query",
    "external_embed": "gemini_embed",
}
MAX_TELEGRAM_INGEST_ATTEMPTS = 3


class JobCancelled(Exception):
    """Internal job cancellation signal."""


class ReportedProviderError(Exception):
    def __init__(self, code: int | None, category: str, retry_after: float | None = None) -> None:
        self.status_code = code
        self.category = category
        self.retry_after_seconds = retry_after
        self.timed_out = category in {"request_timeout", "network_timeout"}
        self.response = type("Response", (), {"headers": {}})()
        super().__init__(category)


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _private_json(path: Path, value: Any) -> str:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent,0o700)
    payload=(json.dumps(value,ensure_ascii=False,separators=(",", ":"))+"\n").encode()
    temporary=path.with_name(path.name+".tmp")
    descriptor=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
    with os.fdopen(descriptor,"wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary,path)
    os.chmod(path,0o600)
    directory_fd=os.open(path.parent,os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return _digest(payload)


def _read_private_json(path_value: str | None, expected_sha256: str | None, expected_root: Path) -> Any | None:
    if not path_value or not expected_sha256:
        return None
    path=Path(path_value)
    try:
        if path.is_symlink():
            raise JobsError("Stored item artifact path is invalid")
        resolved=path.resolve(strict=True)
        root=expected_root.resolve(strict=True)
        if root not in resolved.parents:
            raise JobsError("Stored item artifact path is invalid")
        payload=resolved.read_bytes()
    except (OSError,RuntimeError):
        raise JobsError("Stored item artifact is unavailable") from None
    if _digest(payload)!=expected_sha256:
        raise JobsError("Stored item artifact checksum mismatch")
    try:
        return json.loads(payload)
    except (UnicodeDecodeError,json.JSONDecodeError):
        raise JobsError("Stored item artifact is invalid") from None


def _duration_seconds(path: Path) -> float:
    try:
        result=subprocess.run(
            ["ffprobe","-v","error","-show_entries","format=duration","-of","default=noprint_wrappers=1:nokey=1",str(path)],
            capture_output=True,text=True,check=False,timeout=20,
        )
    except (OSError,subprocess.TimeoutExpired):
        raise JobsError("ffprobe could not inspect an ASR input") from None
    if result.returncode!=0:
        raise JobsError("ffprobe could not inspect an ASR input")
    try:
        duration=float(result.stdout.strip())
    except ValueError:
        raise JobsError("ffprobe returned an invalid audio duration") from None
    if not 0 < duration < 24*60*60:
        raise JobsError("ASR input duration is outside the supported range")
    return duration


def _provider_embed(
    profile: str,
    texts: list[str],
    *,
    timeout: float = 120.0,
) -> tuple[list[list[float]], int | None]:
    from kiber_analyst.embedding_api import embed_texts
    from kiber_analyst.provider_http import ProfileError, client, load_profile

    try:
        settings=load_profile(profile)
        if settings.provider!="google":
            raise PermanentProviderError("Profile provider mismatch",category="profile_configuration")
        sdk=client(profile,timeout=timeout,retry_attempts=1)
    except PermanentProviderError:
        raise
    except Exception as exc:
        if isinstance(exc,ProfileError):
            raise
        raise PermanentProviderError(f"Google profile setup failed ({type(exc).__name__})",category="profile_configuration") from None
    try:
        return embed_texts(sdk,texts)
    finally:
        sdk.close()


class JobWorker:
    def __init__(
        self,
        *,
        store: JobStore | None = None,
        routing_path: Path = ROUTING_PATH,
        socket_path: Path = SOCKET_PATH,
        pools: dict[str,PoolConfig] | None = None,
    ) -> None:
        self.store=store or JobStore(DATA_ROOT,expected_uuid=DATA_UUID)
        self.routing_path=routing_path
        self.socket_path=socket_path
        self.pools=pools if pools is not None else load_pools(routing_path)
        self.router=ProviderRouter(self.store,self.pools)
        self.stop_event=threading.Event()
        self._server: ControlServer | None=None

    def submit(self,request:dict[str,Any], *, uid:int) -> dict[str,Any]:
        argv=request.get("argv")
        if not isinstance(argv,list) or not all(isinstance(item,str) for item in argv):
            raise JobsError("submit requires a command argument list")
        operation,params,items=parse_job_command(argv)
        profile=request.get("profile")
        strategy=request.get("strategy")
        requested_pool=request.get("pool")
        if operation in {"telegram_ingest", "external_ingest"}:
            if requested_pool is not None or profile is not None or strategy is not None:
                raise JobsError("Ingestion does not accept provider routing options")
            pool_name=None
        else:
            pool_name=requested_pool or DEFAULT_POOLS[operation]
            if not isinstance(pool_name,str) or pool_name not in self.pools:
                raise JobsError("Unknown provider pool")
            pool=self.pools[pool_name]
            if pool.operation!=operation and not (
                pool.operation=="embed" and operation=="external_embed"
            ):
                raise JobsError("Selected provider pool does not support this operation")
        if profile is not None and (request.get("pool") or strategy):
            raise JobsError("--profile cannot be combined with --pool or --strategy")
        if profile is not None and profile not in pool.profiles:
            raise JobsError("Explicit profile must belong to the selected pool")
        if profile is not None:
            self._require_root(uid)
        if strategy is not None and strategy not in {"round_robin","failover","weighted_round_robin"}:
            raise JobsError("Unsupported routing strategy")
        idempotency_key=request.get("idempotency_key")
        if idempotency_key is not None and (not isinstance(idempotency_key,str) or not 1<=len(idempotency_key)<=200):
            raise JobsError("Invalid idempotency key")
        command={
            "params":params,
            "pool":pool_name,
            "strategy":strategy,
            "profile":profile,
        }
        fingerprint=_digest(canonical_json({"operation":operation,"command":command,"items":items}).encode())
        default_key = idempotency_key or _digest(
            canonical_json({"operation": operation, "fingerprint": fingerprint}).encode()
        )
        job,created=self.store.create_job(
            operation=operation,
            command=command,
            fingerprint=fingerprint,
            priority=100 if operation=="search" else 0,
            items=items,
            idempotency_key=default_key,
        )
        if (
            not created
            and idempotency_key is None
            and operation in RERUNNABLE_OPERATIONS
            and job.state in TERMINAL_JOB_STATES
        ):
            # An explicit new run of a finished external operation must query
            # the provider again; only an active identical job is deduplicated.
            job,created=self.store.create_job(
                operation=operation,
                command=command,
                fingerprint=fingerprint,
                priority=100 if operation=="search" else 0,
                items=items,
                idempotency_key=f"{default_key}:rerun:{uuid.uuid4().hex}",
            )
        return {"job_id":job.id,"state":job.state,"created":created,"deadline_at":job.deadline_at,"operation":job.operation}

    def handle(self,uid:int,request:dict[str,Any]) -> dict[str,Any]:
        action=request.get("action")
        if action=="query-embedding":
            if uid not in {0,pwd.getpwnam("media").pw_uid}:
                raise JobsError("Interactive embeddings require the media worker identity")
            return self.query_embedding(request)
        if action=="submit":
            return self.submit(request,uid=uid)
        if action=="status":
            job_id=str(request.get("job_id",""))
            status=self.store.status(job_id)
            attempts = self.store.attempts_summary(job_id)
            status["attempts"] = attempts["count"]
            last_error = next((attempt for attempt in reversed(attempts["items"])
                               if attempt["status"] in {"failed", "uncertain"}), None)
            if last_error:
                pool = self.pools.get(self.store.get(job_id).command.get("pool"))
                status["last_provider_error"] = {
                    "http_code": last_error["http_code"],
                    "category": last_error["category"],
                    "retry_after_seconds": last_error.get("retry_info", {}).get("retry_after_seconds"),
                    "scopes": list(pool.scopes_for(last_error["profile"])) if pool else [],
                }
            status["items"]=[{key:item[key] for key in ("id","key","fingerprint","state","checkpoint","artifact_sha256")} for item in self.store.job_items(job_id)]
            return status
        if action=="wait":
            job_id=str(request.get("job_id",""))
            timeout=request.get("timeout",30)
            if type(timeout) is not int or not 0<=timeout<=60:
                raise JobsError("wait timeout must be from 0 to 60 seconds")
            stop=time.monotonic()+timeout
            terminal={"done","failed","timed_out","cancelled"}
            while True:
                result=self.store.status(job_id)
                if result["state"] in terminal or time.monotonic()>=stop:
                    result["attempts"]=self.store.attempts_summary(job_id)["count"]
                    return result
                time.sleep(min(0.5,max(0.05,stop-time.monotonic())))
        if action=="result":
            result=self.store.result(str(request.get("job_id","")))
            result.setdefault("_job",{})
            result["_job"].update(self.store.status(str(request.get("job_id",""))))
            return result
        if action=="ack":
            record=self.store.acknowledge(str(request.get("job_id","")))
            return {"job_id":record.id,"state":record.state,"delivered_at":record.delivered_at}
        if action=="cancel":
            record=self.store.cancel(str(request.get("job_id","")))
            return {"job_id":record.id,"state":record.state,"cancel_requested":record.cancel_requested}
        if action=="backup":
            pre_upgrade=bool(request.get("pre_upgrade",False))
            if pre_upgrade:
                self._require_root(uid)
            elif uid not in {0,pwd.getpwnam("media").pw_uid}:
                raise JobsError("This control operation requires media or root")
            path=self.store.backup(pre_upgrade=pre_upgrade)
            return {"backup_path":str(path)}
        if action=="extend":
            self._require_root(uid)
            hours=request.get("hours")
            if not isinstance(hours,(int,float)):
                raise JobsError("hours must be numeric")
            record=self.store.extend_deadline(str(request.get("job_id","")),float(hours))
            return {"job_id":record.id,"deadline_at":record.deadline_at,"state":record.state}
        if action=="reset_profile":
            self._require_root(uid)
            profile=request.get("profile")
            if not isinstance(profile,str):
                raise JobsError("profile is required")
            return self.reset_profile(profile)
        if action in {"queue-overview", "provider-overview", "provider-probe", "job-log"}:
            if uid not in {0, pwd.getpwnam("media").pw_uid}:
                raise JobsError("Queue overview requires media or root")
            from kiber_analyst.queue_overview import health, job_log

            if action == "provider-probe":
                from kiber_analyst.gemini_quota import probe
                return probe(self, _provider_embed)
            if action == "provider-overview":
                from kiber_analyst.gemini_quota import profile_hashes, profile_windows
                pool = self.pools.get("gemini_embed")
                profiles = {p: pool.scopes_for(p) for p in pool.profiles} if pool and pool.provider == "google" else {}
                report = health(self.store, providers_only=True)
                with self.store.connect() as db:
                    diagnostics = profile_windows(db, profiles, profile_hashes(profiles), utc_now())
                by_profile = {item["profile"]: item for item in report["profiles"]}
                for name, detail in diagnostics.items():
                    by_profile.setdefault(name, {"profile": name, "http_429": 0, "http_5xx": 0}).update(detail)
                report["profiles"] = list(by_profile.values())
                return report
            if action == "job-log":
                return job_log(self.store, request.get("job_id"), request.get("limit"))
            return health(self.store, providers_only=action == "provider-overview")
        if action in {"source-overview", "asr-source-overview"}:
            if uid not in {0, pwd.getpwnam("media").pw_uid}:
                raise JobsError("Source overview requires media or root")

            try:
                if action == "asr-source-overview":
                    from kiber_analyst.asr_source_overview import asr_source_overview

                    return asr_source_overview(
                        request.get("source_id"), request.get("project")
                    )
                from kiber_analyst.connectors.source_ids import parse_source_id
                from kiber_analyst.source_overview import source_overview

                source_value = request.get("source_id")
                if parse_source_id(source_value) is not None:
                    from kiber_analyst.external_source_overview import (
                        external_source_overview,
                    )

                    return external_source_overview(source_value, request.get("project"))
                return source_overview(source_value, request.get("project"))
            except Exception:
                raise JobsError("Source overview is unavailable") from None
        if action=="doctor":
            with self.store.connect() as db:
                schema_version=int(db.execute("PRAGMA user_version").fetchone()[0])
            from kiber_analyst import __version__
            return {"status":"ready","version":__version__,"build_id":os.environ.get("KIBER_ANALYST_BUILD_ID",__version__),"storage_uuid":DATA_UUID,"queue_schema_version":schema_version,"socket":"available"}
        raise JobsError("Unsupported worker control operation")

    @staticmethod
    def _require_root(uid:int) -> None:
        if uid!=0:
            raise JobsError("This control operation requires root")

    def reset_profile(self,profile:str) -> dict[str,Any]:
        found=False
        now=utc_now()
        with self.store.transaction() as db:
            for pool in self.pools.values():
                if profile not in pool.profiles:
                    continue
                row=db.execute("SELECT disabled_profiles_json,cooldowns_json FROM routing_state WHERE pool_name=?",(pool.name,)).fetchone()
                disabled=json.loads(row[0]) if row else []
                cooldowns=json.loads(row[1]) if row else {}
                disabled=[name for name in disabled if name!=profile]
                cooldowns.pop(profile,None)
                db.execute(
                    """INSERT INTO routing_state(pool_name,cursor_index,weighted_current_json,disabled_profiles_json,cooldowns_json,updated_at)
                       VALUES(?,0,'{}',?,?,?) ON CONFLICT(pool_name) DO UPDATE SET
                       disabled_profiles_json=excluded.disabled_profiles_json,
                       cooldowns_json=excluded.cooldowns_json,updated_at=excluded.updated_at""",
                    (pool.name,json.dumps(disabled),json.dumps(cooldowns),iso_utc(now)),
                )
                self.store.event(
                    db,
                    None,
                    "profile_reset",
                    {"profile": profile, "pool": pool.name},
                )
                found=True
            if not found:
                raise JobsError("Profile is not referenced by a routing pool")
            # A profile reset also releases jobs waiting for an operator fix.
            db.execute(
                """UPDATE jobs SET state='queued',next_run_at=NULL,updated_at=?
                   WHERE state='waiting_quota' AND diagnostic_json LIKE ?""",
                (iso_utc(now),'%"reason":"all_profiles_disabled"%'),
            )
        return {"profile":profile,"reset":True}

    def _check_cancel_deadline(self,job:JobRecord) -> None:
        if self.store.cancelled(job.id):
            raise JobCancelled
        if utc_now()>=parse_utc(job.deadline_at):
            raise PermanentProviderError("Job deadline exceeded",category="deadline")

    def _route_call(
        self,
        job:JobRecord,
        item_id:str,
        invoke:Any,
        usage:dict[str,float],
        *,
        max_wait_seconds:float=0,
    ) -> Any:
        command=job.command
        pool_name=command["pool"]
        started=time.monotonic()
        while True:
            self._check_cancel_deadline(job)
            remaining = max(0.05, (parse_utc(job.deadline_at) - utc_now()).total_seconds())
            if not self.router.api_slots.acquire(timeout=min(remaining, 5.0)):
                raise WaitForQuota(
                    next_run_at=utc_now()+timedelta(seconds=1),
                    scope=self.pools[pool_name].scope_for(
                        command.get("profile") or self.pools[pool_name].profiles[0]
                    ),
                    reason="worker_busy",
                )
            try:
                try:
                    return self.router.call(
                        pool_name=pool_name,job_id=job.id,item_id=item_id,invoke=invoke,
                        usage=usage,explicit_profile=command.get("profile"),
                        strategy_override=command.get("strategy"),deadline_at=job.deadline_at,
                    )
                except WaitForQuota as exc:
                    remaining=max_wait_seconds-(time.monotonic()-started)
                    if max_wait_seconds and exc.next_run_at and remaining>0:
                        delay=max(0.0,(exc.next_run_at-utc_now()).total_seconds())
                        if delay<=remaining:
                            time.sleep(delay)
                            continue
                    raise
            except WaitForQuota:
                raise
            finally:
                self.router.api_slots.release()

    def query_embedding(self,request:dict[str,Any])->dict[str,Any]:
        query=request.get("query")
        project=request.get("project")
        model=request.get("model")
        request_id=request.get("request_id")
        if request_id is None:
            request_id=str(uuid.uuid4())
        else:
            try:
                request_id=str(uuid.UUID(request_id)) if isinstance(request_id,str) else ""
            except (ValueError,TypeError,AttributeError):
                request_id=""
        if not isinstance(query,str) or not isinstance(project,str) or not isinstance(model,str):
            return {"status":"failed","reason":"invalid_argument","request_id":request_id}
        pool=self.pools.get("gemini_query")
        if (
            not request_id
            or not query.strip()
            or len(query)>2000
            or not 1<=len(project)<=128
            or pool is None
            or pool.operation!="search"
            or pool.provider!="google"
            or pool.model!=MODEL_KEY
            or model!=pool.model
        ):
            return {"status":"failed","reason":"invalid_argument","request_id":request_id}
        if not self.router.begin_interactive_request(request_id,project,model,query):
            return {"status":"failed","reason":"duplicate_request","request_id":request_id}
        if not self.router.api_slots.acquire(blocking=False):
            self.router._finish_unattempted_interactive(request_id,"worker_busy",None)
            return {"status":"failed","reason":"worker_busy","request_id":request_id}
        try:
            def invoke(profile:str,timeout:float)->MeteredResult:
                from google.genai import types

                from kiber_analyst.provider_http import genai_client

                sdk=genai_client(profile,timeout=timeout,retry_attempts=1)
                try:
                    async def call()->Any:
                        async with sdk.aio as client:
                            return await client.models.embed_content(
                                model=MODEL,
                                contents=[types.Content(parts=[types.Part.from_text(text=query)])],
                                config=types.EmbedContentConfig(output_dimensionality=DIMENSIONS),
                            )
                    response=asyncio.run(asyncio.wait_for(call(),timeout=timeout))
                finally:
                    sdk.close()
                values=getattr(response,"embeddings",None)
                if values is None or len(values)!=1:
                    raise PermanentProviderError("Embedding response is invalid",status_code=200,category="invalid_response")
                raw=getattr(values[0],"values",None)
                if raw is None or len(raw)!=DIMENSIONS:
                    raise PermanentProviderError("Embedding dimensions are invalid",status_code=200,category="invalid_response")
                try:
                    vector=[float(value) for value in raw]
                except (TypeError,ValueError):
                    raise PermanentProviderError("Embedding vector is invalid",status_code=200,category="invalid_response") from None
                usage=getattr(response,"usage_metadata",None)
                tokens=getattr(usage,"prompt_token_count",None)
                return MeteredResult(vector,{"requests":1,"items":1,"tokens":float(tokens) if isinstance(tokens,int) else max(1,round(len(query)/4))})
            return self.router.interactive_call(
                request_id=request_id,project=project,model=model,query=query,invoke=invoke,
                timeout_seconds=10,request_started=True,
            )
        finally:
            self.router.api_slots.release()

    def _embedding_callback(self,job:JobRecord,item_key_prefix:str="embed"):
        item_ids:list[str]=[]
        def callback(texts:list[str])->tuple[list[list[float]],int|None]:
            self._check_cancel_deadline(job)
            fp=_digest(canonical_json(texts).encode())
            item=self.store.ensure_item(job.id,f"{item_key_prefix}:{fp}",fp)
            item_id=item["id"]
            item_ids.append(item_id)
            artifact_dir=self.store.jobs_dir/job.id/"items"
            artifact=artifact_dir/f"{item_id}.embedding.json"
            if not item["artifact_path"] and artifact.is_file():
                digest=_digest(artifact.read_bytes())
                self.store.update_item(item_id,"running",artifact_path=str(artifact),artifact_sha256=digest)
                item["artifact_path"]=str(artifact)
                item["artifact_sha256"]=digest
            if item["artifact_path"] and item["artifact_sha256"]:
                stored=_read_private_json(item["artifact_path"],item["artifact_sha256"],self.store.jobs_dir/job.id)
                self.store.update_item(
                    item_id,
                    "done",
                    checkpoint={"response_saved": True, "dimensions": len(stored["vectors"][0]) if stored["vectors"] else 0},
                )
                return stored["vectors"],stored.get("tokens")
            self.store.update_item(item_id,"running",checkpoint={"request_items":len(texts),"input_sha256":fp})
            usage_estimate={"requests":1,"items":len(texts),"tokens":max(1,round(sum(map(len,texts))/4))}
            def invoke(profile:str)->MeteredResult:
                remaining = (parse_utc(job.deadline_at) - utc_now()).total_seconds()
                if remaining <= 0:
                    raise PermanentProviderError(
                        "Job deadline exceeded", category="deadline"
                    )
                vectors,tokens=_provider_embed(
                    profile,texts,timeout=min(120.0,remaining)
                )
                actual=tokens if tokens is not None else usage_estimate["tokens"]
                saved={"vectors":vectors,"tokens":tokens}
                digest=_private_json(artifact,saved)
                self.store.update_item(item_id,"running",checkpoint={"response_saved":True,"dimensions":len(vectors[0]) if vectors else 0},artifact_path=str(artifact),artifact_sha256=digest)
                return MeteredResult(saved,{"requests":1,"items":len(texts),"tokens":actual})
            try:
                output=self._route_call(job,item_id,invoke,usage_estimate)
            except WaitForQuota as exc:
                if exc.reason == "quota_budget_exhausted" and len(texts) > 1:
                    self.store.update_item(
                        item_id,
                        "skipped",
                        checkpoint={"split_for_quota": True, "original_items": len(texts)},
                    )
                    if item_id in item_ids:
                        item_ids.remove(item_id)
                    midpoint=len(texts)//2
                    left,left_tokens=callback(texts[:midpoint])
                    right,right_tokens=callback(texts[midpoint:])
                    total_tokens=(left_tokens+right_tokens) if left_tokens is not None and right_tokens is not None else None
                    return left+right,total_tokens
                self.store.update_item(item_id,"waiting")
                raise
            self.store.update_item(
                item_id,
                "done",
                checkpoint={"response_saved": True, "dimensions": len(output["vectors"][0]) if output["vectors"] else 0},
            )
            completed_batches=sum(
                1 for entry in self.store.job_items(job.id)
                if entry["key"].startswith(f"{item_key_prefix}:") and entry["state"] == "done"
            )
            self.store.update_progress(job.id,{"completed_batches":completed_batches})
            return output["vectors"],output.get("tokens")
        callback.item_ids=item_ids  # type: ignore[attr-defined]
        return callback

    def _run_asr(self,job:JobRecord)->dict[str,Any]:
        from kiber_analyst.asr import (
            ROTATION_CONFIG_PATH,
            _collect_audio,
            run_asr_batch,
        )

        params=job.command["params"]
        job_dir=Path(params["job_dir"])
        audio_items=_collect_audio(job_dir)
        requested_refs=params.get("external_refs") or [item.external_ref for item in audio_items]
        audio_by_ref={item.external_ref:item for item in audio_items}
        if len(requested_refs)!=len(set(requested_refs)) or any(ref not in audio_by_ref for ref in requested_refs):
            raise JobsError("ASR selection is not present in the prepared source")
        selected_audio=[audio_by_ref[ref] for ref in requested_refs]
        input_durations={}
        run_id=params.get("run_id") or job.id
        run_dir=job_dir/"asr"/"batches"/run_id
        manifest_path=run_dir/"manifest.json"
        completed=[]
        failed=[]

        def persist_results()->dict[str,Any]|None:
            if not params.get("load_results") or not manifest_path.is_file():
                return None
            from kiber_analyst.asr_store import load_asr

            report_path=self.store.jobs_dir/job.id/"asr-load-report.json"
            return load_asr(
                manifest_path,
                job_dir,
                project=params["project"],
                report_path=report_path,
                external_refs=requested_refs,
                input_durations=input_durations,
            )

        def saved_result(external_ref:str)->tuple[dict[str,Any],Path]|None:
            try:
                info=manifest_path.lstat()
            except FileNotFoundError:
                return None
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1
                    or stat.S_IMODE(info.st_mode)!=0o600):
                raise JobsError("Saved ASR manifest is unsafe")
            try:
                saved=json.loads(manifest_path.read_text(encoding="utf-8"))
            except (OSError,UnicodeError,json.JSONDecodeError):
                raise JobsError("Saved ASR manifest is invalid") from None
            if saved.get("run_id")!=run_id or not isinstance(saved.get("items"),list):
                raise JobsError("Saved ASR manifest does not match this source")
            state=next((entry for entry in saved["items"]
                        if isinstance(entry,dict) and entry.get("external_ref")==external_ref),None)
            if not state or state.get("status")!="complete":
                return None
            relative=Path(str(state.get("result","")))
            if relative.is_absolute() or ".." in relative.parts or not relative.parts:
                raise JobsError("Saved ASR result path is unsafe")
            result_path=run_dir/relative
            try:
                result_info=result_path.lstat()
            except OSError:
                raise JobsError("Saved ASR result is missing") from None
            if (not stat.S_ISREG(result_info.st_mode) or result_info.st_nlink!=1
                    or stat.S_IMODE(result_info.st_mode)!=0o600):
                raise JobsError("Saved ASR result is unsafe")
            payload=result_path.read_bytes()
            if hashlib.sha256(payload).hexdigest()!=state.get("result_sha256"):
                raise JobsError("Saved ASR result checksum does not match")
            try:
                document=json.loads(payload)
            except (UnicodeDecodeError,json.JSONDecodeError):
                raise JobsError("Saved ASR result is invalid") from None
            if not isinstance(document,dict) or not isinstance(document.get("text"),str) or not isinstance(document.get("segments"),list):
                raise JobsError("Saved ASR result is incomplete")
            return state,result_path

        for index,audio in enumerate(selected_audio,1):
            self._check_cancel_deadline(job)
            item=self.store.ensure_item(job.id,audio.external_ref,audio.sha256)
            if item["state"]=="done":
                completed.append(audio.external_ref)
                continue
            cached=saved_result(audio.external_ref)
            if cached is not None:
                _state,result_path=cached
                input_durations[audio.external_ref]=_duration_seconds(audio.path)
                digest=_digest(result_path.read_bytes())
                self.store.update_item(item["id"],"done",checkpoint={"response_saved":True},artifact_path=str(result_path),artifact_sha256=digest)
                completed.append(audio.external_ref)
                continue
            self.store.update_item(item["id"],"running",checkpoint={"external_ref":audio.external_ref,"audio_sha256":audio.sha256})
            duration=_duration_seconds(audio.path)
            input_durations[audio.external_ref]=duration
            def invoke(
                profile: str,
                *,
                current_audio=audio,
                current_item=item,
                current_duration=duration,
            ):
                remaining=max(1,int((parse_utc(job.deadline_at)-utc_now()).total_seconds()))
                report=run_asr_batch(
                    job_dir,config_path=ROTATION_CONFIG_PATH,max_cycles=1,profiles=(profile,),
                    run_id=run_id,external_refs={current_audio.external_ref},
                    request_timeout_seconds=min(900,remaining),
                    _audio_items=audio_items,
                )
                item_report=next((value for value in report["items"] if value["external_ref"]==current_audio.external_ref),None)
                if not item_report:
                    raise JobsError("ASR worker did not return the requested item")
                if item_report["status"]!="complete":
                    attempt=(item_report.get("attempts") or [{}])[-1]
                    category=attempt.get("outcome","provider_error")
                    if category in {"unclassified_error","invalid_response"}:
                        raise PermanentProviderError("ASR provider setup or response validation failed",category=category)
                    raise ReportedProviderError(
                        attempt.get("status_code"),category,
                        attempt.get("retry_after_seconds"),
                    )
                result_path=Path(report["run_dir"])/item_report["result"]
                digest=_digest(result_path.read_bytes())
                self.store.update_item(current_item["id"],"running",checkpoint={"response_saved":True,"duration_seconds":current_duration},artifact_path=str(result_path),artifact_sha256=digest)
                return MeteredResult(item_report,{"requests":1,"items":1,"audio_seconds":current_duration})
            try:
                item_result=self._route_call(
                    job,item["id"],invoke,{"requests":1,"items":1,"audio_seconds":duration}
                )
            except WaitForQuota:
                self.store.update_item(item["id"],"waiting")
                persist_results()
                raise
            except PermanentProviderError as exc:
                self.store.update_item(item["id"],"failed")
                failed.append(audio.external_ref)
                self.store.update_progress(job.id,{
                    "files_done":len(completed),"files_failed":len(failed),
                    "files_total":len(selected_audio),"last_error_code":f"HTTP_{exc.status_code}" if exc.status_code else exc.category,
                })
                continue
            result_path=run_dir/item_result["result"]
            result_digest=_digest(result_path.read_bytes())
            self.store.update_item(item["id"],"done",checkpoint={"response_saved":True,"duration_seconds":duration},artifact_path=str(result_path),artifact_sha256=result_digest)
            completed.append(audio.external_ref)
            self.store.update_progress(job.id,{"completed":len(completed),"total":len(selected_audio),"percent":round(100*len(completed)/len(selected_audio),1)})
        load_report=persist_results()
        manifest=json.loads(manifest_path.read_text(encoding="utf-8"))
        outputs=[]
        selected_set=set(requested_refs)
        segments_total=0
        empty_total=0
        for state in manifest.get("items",[]):
            if state.get("external_ref") not in selected_set or state.get("status")!="complete":
                continue
            result_file=run_dir/state["result"]
            if result_file.is_file():
                try:
                    payload=json.loads(result_file.read_text(encoding="utf-8"))
                except (OSError,UnicodeError,json.JSONDecodeError):
                    continue
                if isinstance(payload,dict) and isinstance(payload.get("segments"),list):
                    segments_total+=len(payload["segments"])
                    empty_total+=sum(1 for segment in payload["segments"] if isinstance(segment,dict) and not str(segment.get("text","")).strip())
        for state in manifest.get("items",[]):
            if state.get("external_ref") not in selected_set:
                continue
            if state.get("status") != "complete" or not isinstance(state.get("result"),str):
                continue
            result_file=manifest_path.parent/state["result"]
            if state.get("status")=="complete" and result_file.is_file():
                outputs.append({"external_ref":state["external_ref"],"profile":state.get("profile"),"result":json.loads(result_file.read_text(encoding="utf-8"))})
        failed_total=sum(1 for state in manifest.get("items",[]) if state.get("external_ref") in selected_set and state.get("status")=="failed")
        last_error_code=None
        for state in manifest.get("items",[]):
            if state.get("external_ref") not in selected_set or state.get("status")!="failed":
                continue
            failure=state.get("failure") or {}
            code=failure.get("status_code")
            category=failure.get("category")
            last_error_code=(f"HTTP_{code}" if type(code) is int and 400<=code<=599
                             else str(category).upper() if isinstance(category,str) else "ASR_FAILED")
        self.store.update_progress(job.id,{
            "files_done":len(outputs),"files_failed":failed_total,"files_total":len(selected_audio),
            "segments":segments_total,"empty":empty_total,
            "last_error_code":last_error_code,
        })
        return {
            "status":"partial" if failed_total else "complete",
            "run_id":run_id,"run_dir":str(manifest_path.parent),"items":outputs,
            "files_done":len(outputs),"files_failed":failed_total,"files_total":len(selected_audio),
            "segments":segments_total,"empty":empty_total,
            "last_error_code":last_error_code,"load_report":load_report,
            "attempts":self.store.attempts_summary(job.id),
        }

    @staticmethod
    def _retryable_ingest_error(error: BaseException) -> bool:
        current: BaseException | None = error
        for _ in range(5):
            if current is None:
                return False
            state = getattr(current, "sqlstate", None)
            if isinstance(state, str) and (
                state.startswith(("08", "40"))
                or state in {"53300", "55P03", "57P01", "57P02", "57P03"}
            ):
                return True
            current = current.__cause__
        return False

    def _run_telegram_ingest(self,job:JobRecord)->dict[str,Any]:
        from kiber_analyst.load import ENV_PATH
        from kiber_analyst.telegram_ingest import load_telegram_export

        params=job.command["params"]
        item=self.store.ensure_item(job.id,"telegram-export",params["import_key"])
        self.store.update_item(item["id"],"running",checkpoint={
            "import_key":params["import_key"],
            "messages_sha256":params["messages_sha256"],
            "services_sha256":params["services_sha256"],
        })
        artifact_dir=self.store.jobs_dir/job.id/"telegram-ingest"
        for attempt in range(1,MAX_TELEGRAM_INGEST_ATTEMPTS+1):
            self._check_cancel_deadline(job)
            try:
                result=load_telegram_export(
                    Path(params["job_dir"]),artifact_dir,env_path=ENV_PATH,
                    project_slug=params["project"],dry_run=params["dry_run"],
                )
            except Exception as exc:
                if attempt < MAX_TELEGRAM_INGEST_ATTEMPTS and self._retryable_ingest_error(exc):
                    self.store.update_item(item["id"],"waiting",checkpoint={
                        "attempt":attempt,"retry":"transient_database_error",
                    })
                    time.sleep(min(2 ** (attempt-1),4))
                    self.store.update_item(item["id"],"running",checkpoint={"attempt":attempt+1})
                    continue
                self.store.update_item(item["id"],"failed",checkpoint={
                    "attempt":attempt,"error_type":type(exc).__name__,
                })
                raise JobsError(f"Telegram ingestion failed ({type(exc).__name__})") from exc
            self.store.update_item(item["id"],"done",checkpoint={
                "attempts":attempt,
                "inserted":result.get("inserted",0),
                "updated":result.get("updated",0),
                "skipped":result.get("skipped",0),
                "media_missing":result.get("media_missing",0),
                "service_events":result.get("service_events",0),
                "errors":result.get("errors",0),
            })
            result["retry_attempts"]=attempt
            self.store.update_progress(job.id,{
                "completed":result.get("inserted",0)+result.get("updated",0)+result.get("skipped",0),
                "total":result.get("inserted",0)+result.get("updated",0)+result.get("skipped",0),
                "inserted":result.get("inserted",0),
                "updated":result.get("updated",0),
                "skipped":result.get("skipped",0),
                "media_missing":result.get("media_missing",0),
                "service_events":result.get("service_events",0),
                "errors":result.get("errors",0),
            })
            return result
        raise JobsError("Telegram ingestion retry limit exceeded")

    def _run_external_ingest(self,job:JobRecord)->dict[str,Any]:
        from kiber_analyst.errors import IngestError
        from kiber_analyst.external_ingest import run_external_ingest
        from kiber_analyst.load import ENV_PATH

        params=job.command["params"]
        # The job API registered this item with the parameter digest; the worker
        # must use the same fingerprint or ensure_item rejects the job.
        item_fingerprint=_digest(canonical_json(params).encode())
        item=self.store.ensure_item(job.id,"external-source",item_fingerprint)
        self.store.update_item(item["id"],"running",checkpoint={
            "provider":params["provider"],
            "since":params.get("since"),
            "limit":params.get("limit"),
        })
        artifact_dir=self.store.jobs_dir/job.id/"external-ingest"
        last_code="UNKNOWN"
        for attempt in range(1,MAX_TELEGRAM_INGEST_ATTEMPTS+1):
            self._check_cancel_deadline(job)
            try:
                result=run_external_ingest(
                    provider=params["provider"],source_id=params["source_id"],
                    project_slug=params["project"],since=params.get("since"),
                    limit=params.get("limit"),dry_run=params.get("dry_run",False),
                    artifact_dir=artifact_dir,env_path=ENV_PATH,
                    check_stop=lambda: self._check_cancel_deadline(job),
                )
            except IngestError as exc:
                code=getattr(exc,"code","")
                retryable_connector=getattr(exc,"retryable",False)
                last_code=code or type(exc).__name__
                if attempt < MAX_TELEGRAM_INGEST_ATTEMPTS and (
                    retryable_connector or self._retryable_ingest_error(exc)
                ):
                    self.store.update_item(item["id"],"waiting",checkpoint={
                        "attempt":attempt,"retry":code or "transient_error",
                    })
                    time.sleep(min(2 ** (attempt-1),4))
                    self.store.update_item(item["id"],"running",checkpoint={"attempt":attempt+1})
                    continue
                self.store.update_item(item["id"],"failed",checkpoint={
                    "attempt":attempt,"error_code":last_code,
                })
                if retryable_connector:
                    # Preserve the retryable connector code so process() can
                    # report a retryable failure to the broker.
                    raise
                raise JobsError(f"External ingestion failed ({last_code})") from exc
            self.store.update_item(item["id"],"done",checkpoint={
                "attempts":attempt,
                "threads":result.get("threads",0),
                "items":result.get("items",0),
                "inserted":result.get("load",{}).get("inserted",0),
                "updated":result.get("load",{}).get("updated",0),
                "skipped":result.get("load",{}).get("skipped",0),
                "deleted":result.get("load",{}).get("deleted",0),
                "status":result.get("status"),
            })
            result["retry_attempts"]=attempt
            self.store.update_progress(job.id,{
                "threads":result.get("threads",0),
                "items":result.get("items",0),
                "nonempty":result.get("nonempty",0),
                "skipped_empty":result.get("skipped_empty",0),
                "status":result.get("status"),
                "inserted":result.get("load",{}).get("inserted",0),
                "updated":result.get("load",{}).get("updated",0),
                "skipped":result.get("load",{}).get("skipped",0),
                "deleted":result.get("load",{}).get("deleted",0),
            })
            return result
        raise JobsError("External ingestion retry limit exceeded")

    def _run_external_embedding(self,job:JobRecord)->dict[str,Any]:
        from kiber_analyst.external_embed import embed_external

        params=job.command["params"]
        item_fingerprint=_digest(canonical_json(params).encode())
        item=self.store.ensure_item(job.id,"external-embed",item_fingerprint)
        self.store.update_item(item["id"],"running",checkpoint={"source_id":params["source_id"]})
        callback=self._embedding_callback(job,"external-embed")
        report_path=self.store.jobs_dir/job.id/"external-embed-report.json"
        try:
            result=embed_external(
                params["source_id"],project=params["project"],limit=params.get("limit"),
                embedding_call=callback,report_path=report_path,
            )
        except Exception:
            self.store.update_item(item["id"],"failed",checkpoint={"source_id":params["source_id"]})
            raise
        items_by_id={entry["id"]:entry for entry in self.store.job_items(job.id)}
        for item_id in callback.item_ids:
            if item_id in items_by_id:
                self.store.update_item(item_id,"done",checkpoint={"response_saved":True,"embedding_status":result.get("status")})
        self.store.update_item(item["id"],"done",checkpoint={
            "source_id":params["source_id"],
            "inserted":result.get("inserted",0),
            "skipped_existing":result.get("skipped_existing",0),
        })
        progress={
            "completed_batches":sum(
                1 for entry in self.store.job_items(job.id)
                if entry["key"].startswith("external-embed:") and entry["state"] == "done"
            ),
        }
        for key in ("inserted", "updated_vectors", "skipped_existing", "skipped_empty"):
            if isinstance(result.get(key), int):
                progress[key]=result[key]
        self.store.update_progress(job.id,progress)
        result["provider_attempts"]=self.store.attempts_summary(job.id)
        return result

    def _run_embedding(self,job:JobRecord,*,asr:bool)->dict[str,Any]:
        callback=self._embedding_callback(job,"asr-embed" if asr else "telegram-embed")
        params=job.command["params"]
        if asr:
            from kiber_analyst.asr_embed import embed_asr
            report_path=self.store.jobs_dir/job.id/"asr-embed-report.json"
            result=embed_asr(
                project=params["project"],run_key=params["run_key"],all_runs=params["all_runs"],
                source_id=params.get("source_id"),limit=params["limit"],embedding_call=callback,report_path=report_path,
            )
        else:
            from kiber_analyst.embed import embed_job
            result=embed_job(Path(params["job_dir"]),project=params["project"],embedding_call=callback)
        items_by_id={entry["id"]:entry for entry in self.store.job_items(job.id)}
        for item_id in callback.item_ids:
            if item_id in items_by_id:
                self.store.update_item(item_id,"done",checkpoint={"response_saved":True,"embedding_status":result.get("status")})
        progress={
            "completed_batches":sum(
                1 for item in self.store.job_items(job.id)
                if item["key"].startswith(("asr-embed:", "telegram-embed:")) and item["state"] == "done"
            ),
        }
        for key in ("selected", "segments_total", "inserted", "skipped_existing", "skipped_empty"):
            if isinstance(result.get(key), int):
                progress[key]=result[key]
        self.store.update_progress(job.id,progress)
        result["provider_attempts"]=self.store.attempts_summary(job.id)
        return result

    def _search(self,job:JobRecord)->dict[str,Any]:
        from datetime import date

        from kiber_analyst.search import search

        params=job.command["params"]
        search_fingerprint=_digest(canonical_json(params).encode())
        item=self.store.ensure_item(job.id,"search-query",search_fingerprint)
        self.store.update_item(item["id"],"running",checkpoint={"query_sha256":_digest(params["query"].encode())})
        embedding_fp=_digest(params["query"].encode())
        embedding_item=self.store.ensure_item(job.id,f"query-embedding:{embedding_fp}",embedding_fp)
        def vector_provider(query:str)->list[float]:
            expected_artifact=self.store.jobs_dir/job.id/"items"/f"{embedding_item['id']}.query.json"
            if not embedding_item["artifact_path"] and expected_artifact.is_file():
                digest=_digest(expected_artifact.read_bytes())
                self.store.update_item(embedding_item["id"],"running",artifact_path=str(expected_artifact),artifact_sha256=digest)
                embedding_item["artifact_path"]=str(expected_artifact)
                embedding_item["artifact_sha256"]=digest
            if embedding_item["artifact_path"] and embedding_item["artifact_sha256"]:
                saved=_read_private_json(embedding_item["artifact_path"],embedding_item["artifact_sha256"],self.store.jobs_dir/job.id)
                return saved["vectors"][0]
            usage={"requests":1,"items":1,"tokens":max(1,round(len(query)/4))}
            artifact=self.store.jobs_dir/job.id/"items"/f"{embedding_item['id']}.query.json"
            def invoke(profile:str)->MeteredResult:
                remaining = (parse_utc(job.deadline_at) - utc_now()).total_seconds()
                if remaining <= 0:
                    raise PermanentProviderError(
                        "Job deadline exceeded", category="deadline"
                    )
                vectors,tokens=_provider_embed(
                    profile,[query],timeout=min(120.0,remaining)
                )
                actual=tokens if tokens is not None else usage["tokens"]
                saved={"vectors":vectors,"tokens":tokens}
                digest=_private_json(artifact,saved)
                self.store.update_item(embedding_item["id"],"running",checkpoint={"response_saved":True},artifact_path=str(artifact),artifact_sha256=digest)
                embedding_item["artifact_path"]=str(artifact)
                embedding_item["artifact_sha256"]=digest
                return MeteredResult(saved,{"requests":1,"items":1,"tokens":actual})
            self.store.update_item(embedding_item["id"],"running",checkpoint={"query_sha256":embedding_fp})
            saved=self._route_call(job,embedding_item["id"],invoke,usage,max_wait_seconds=30)
            return saved["vectors"][0]
        result=search(
            params["query"],project=params["project"],limit=params["limit"],
            source_kind=params["source_kind"],source=params["source"],
            date_from=date.fromisoformat(params["date_from"]) if params["date_from"] else None,
            date_to=date.fromisoformat(params["date_to"]) if params["date_to"] else None,
            author=params["author"],topic=params["topic"],embedding_provider=vector_provider,
            source_id=params.get("source_id"),
        )
        self.store.update_item(item["id"],"done",checkpoint={"degraded":result["degraded"],"result_count":len(result["results"])})
        if embedding_item["artifact_path"]:
            self.store.update_item(embedding_item["id"],"done",checkpoint={"response_saved":True})
        else:
            self.store.update_item(
                embedding_item["id"],
                "skipped",
                checkpoint={"vector_unavailable": True, "reason": result.get("degraded_reason")},
            )
        self.store.update_progress(
            job.id,
            {"completed": 1, "total": 1, "degraded": result["degraded"], "result_count": len(result["results"])},
        )
        return result

    def process(self,job:JobRecord)->None:
        try:
            self._check_cancel_deadline(job)
            if job.operation=="telegram_asr":
                result=self._run_asr(job)
            elif job.operation=="embed":
                result=self._run_embedding(job,asr=False)
            elif job.operation=="asr_embed":
                result=self._run_embedding(job,asr=True)
            elif job.operation=="search":
                result=self._search(job)
            elif job.operation=="telegram_ingest":
                result=self._run_telegram_ingest(job)
            elif job.operation=="external_ingest":
                result=self._run_external_ingest(job)
            elif job.operation=="external_embed":
                result=self._run_external_embedding(job)
            else:
                raise JobsError("Unsupported queued operation")
            self.store.complete(job.id,{**result,"job_id":job.id},state="done")
        except WaitForQuota as exc:
            next_at=exc.next_run_at or parse_utc(job.deadline_at)
            next_at=min(next_at,parse_utc(job.deadline_at))
            state="waiting_retry" if exc.reason in {
                "profile_retry_cooldown", "profile_cycles_exhausted", "server_error",
                "network_error", "request_timeout", "network_timeout",
            } else "waiting_quota"
            self.store.postpone(job.id,state,next_at,{"reason":exc.reason,"next_run_at":iso_utc(next_at)})
        except JobCancelled:
            self.store.complete(job.id,{"job_id":job.id,"status":"cancelled","progress":self.store.status(job.id)["progress"]},state="cancelled")
        except PermanentProviderError as exc:
            state="timed_out" if exc.category=="deadline" else "failed"
            self.store.complete(job.id,{"job_id":job.id,"status":state,"error":{"category":exc.category,"http_code":exc.status_code,"message":str(exc)},"attempts":self.store.attempts_summary(job.id)},state=state)
        except Exception as exc:  # noqa: BLE001 - keep private data and request payloads out of diagnostics.
            temporary_database_error = (
                job.operation == "telegram_ingest" and self._retryable_ingest_error(exc)
            )
            category = "transient_database_error" if temporary_database_error else "worker_error"
            error_detail = {"category": category, "type": type(exc).__name__}
            connector_code = getattr(exc, "code", None)
            if job.operation == "external_ingest" and isinstance(
                connector_code, str
            ) and connector_code:
                # Map stable connector codes onto broker categories: retryable
                # throttling/unavailability stays retryable for the broker.
                if connector_code == "RATE_LIMITED":
                    category = "rate_limited"
                elif getattr(exc, "retryable", False):
                    category = "server_error"
                else:
                    category = "connector_error"
                error_detail = {
                    "category": category,
                    "code": connector_code,
                    "type": type(exc).__name__,
                }
            self.store.complete(job.id,{"job_id":job.id,"status":"failed","error":error_detail,"attempts":self.store.attempts_summary(job.id)},state="failed")

    def serve(self)->None:
        self.store.verify_storage()
        self.store.data_root.mkdir(mode=0o700,parents=True,exist_ok=True)
        lock_path=self.store.data_root/"worker.lock"
        descriptor=os.open(lock_path,os.O_CREAT|os.O_RDWR,0o600)
        try:
            fcntl.flock(descriptor,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(descriptor)
            raise JobsError("Another Kiber-analyst worker is already running") from None
        try:
            self.store.initialize()
            self.store.recover_after_restart()
            self.store.backup_if_due()
            self._server=ControlServer(self,self.socket_path)
            server_thread=threading.Thread(target=self._server.serve_forever,daemon=True,name="control-socket")
            server_thread.start()
            last_backup=time.monotonic()
            self.store.cleanup_interactive()
            futures:set[Future[Any]]=set()
            with ThreadPoolExecutor(max_workers=2,thread_name_prefix="corpus-job") as executor:
                while not self.stop_event.is_set():
                    done={future for future in futures if future.done()}
                    futures-=done
                    if time.monotonic()-last_backup>=3600:
                        self.store.backup_if_due()
                        self.store.cleanup_interactive()
                        last_backup=time.monotonic()
                    while len(futures)<2:
                        job=self.store.claim()
                        if job is None:
                            break
                        futures.add(executor.submit(self.process,job))
                    if not futures:
                        self.stop_event.wait(self.store.next_wake_seconds(cap=1.0))
                    else:
                        self.stop_event.wait(0.2)
        finally:
            if self._server is not None:
                self._server.shutdown()
                self._server.server_close()
                try:
                    self.socket_path.unlink()
                except FileNotFoundError:
                    pass
            fcntl.flock(descriptor,fcntl.LOCK_UN)
            os.close(descriptor)


class ControlHandler(socketserver.StreamRequestHandler):
    def handle(self)->None:
        try:
            self.request.settimeout(10)
            credentials=self.request.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,struct.calcsize("3i"))
            _pid,uid,_gid=struct.unpack("3i",credentials)
            allowed={0}
            for name in ("media","opencode"):
                try:
                    allowed.add(pwd.getpwnam(name).pw_uid)
                except KeyError:
                    pass
            if uid not in allowed:
                self._respond({"ok":False,"error":"Peer is not authorized"})
                return
            line=self.rfile.readline(MAX_REQUEST_BYTES+1)
            if len(line)>MAX_REQUEST_BYTES or not line.endswith(b"\n"):
                self._respond({"ok":False,"error":"Invalid control request"})
                return
            try:
                request=json.loads(line)
            except (UnicodeDecodeError,json.JSONDecodeError):
                self._respond({"ok":False,"error":"Invalid JSON request"})
                return
            if not isinstance(request,dict):
                self._respond({"ok":False,"error":"Control request must be an object"})
                return
            result=self.server.worker.handle(uid,request)  # type: ignore[attr-defined]
            self._respond({"ok":True,"result":result})
        except JobsError as exc:
            self._respond({"ok":False,"error":str(exc)})
        except Exception as exc:  # noqa: BLE001
            self._respond({"ok":False,"error":f"Worker control failed ({type(exc).__name__})"})

    def _respond(self,value:dict[str,Any])->None:
        encoded=(json.dumps(value,ensure_ascii=False,separators=(",", ":"))+"\n").encode()
        if len(encoded)>MAX_RESPONSE_BYTES:
            encoded=b'{"ok":false,"error":"Worker response is too large"}\n'
        self.wfile.write(encoded)


class ControlServer(socketserver.ThreadingUnixStreamServer):
    daemon_threads=True
    allow_reuse_address=True

    def __init__(self,worker:JobWorker,path:Path)->None:
        self.worker=worker
        path.parent.mkdir(mode=0o750,parents=True,exist_ok=True)
        if path.exists() or path.is_symlink():
            if not path.is_socket():
                raise JobsError("Refusing to replace a non-socket control path")
            path.unlink()
        super().__init__(str(path),ControlHandler)
        os.chmod(path,0o660)
        try:
            os.chown(path,pwd.getpwnam("media").pw_uid,grp.getgrnam("opencode").gr_gid)
        except (KeyError,PermissionError):
            # Unit Group=opencode gives the socket the required group even if
            # the process cannot resolve the account database at startup.
            pass


def _main()->int:
    parser=argparse.ArgumentParser(prog="kiber-analyst-worker")
    parser.add_argument("--check",action="store_true")
    args=parser.parse_args()
    try:
        worker=JobWorker()
        worker.store.verify_storage()
        if args.check:
            print(json.dumps({"status":"ready","build_id":os.environ.get("KIBER_ANALYST_BUILD_ID","source")},separators=(",",":")))
            return 0
        signal.signal(signal.SIGTERM,lambda *_:worker.stop_event.set())
        signal.signal(signal.SIGINT,lambda *_:worker.stop_event.set())
        worker.serve()
        return 0
    except (JobsError,RoutingError,OSError) as exc:
        print(f"kiber-analyst-worker: {exc}",file=sys.stderr)
        return 2


if __name__=="__main__":
    raise SystemExit(_main())

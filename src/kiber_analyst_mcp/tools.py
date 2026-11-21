from __future__ import annotations

import base64
import hashlib
import json
import logging
import math
import socket
import time
import uuid
from collections.abc import Callable
from datetime import date
from typing import Any

from kiber_analyst.config_paths import runtime_path
from kiber_analyst.embedding_contract import DIMENSIONS, MODEL_KEY
from kiber_analyst.errors import SearchError
from kiber_analyst.search_core import execute_search
from kiber_analyst_mcp.config import MCPConfig
from kiber_analyst_mcp.db import readonly_connection

LOGGER = logging.getLogger("kiber_analyst_mcp.tools")
WORKER_SOCKET = str(runtime_path("control.sock"))
MAX_WORKER_RESPONSE = 2 * 1024 * 1024


class ToolFailure(Exception):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(message)


def _cursor_encode(document_id: str, segment_id: int) -> str:
    payload=json.dumps({"v":1,"d":document_id,"s":segment_id},separators=(",",":" )).encode()
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")


def _cursor_decode(value: str | None, document_id: str) -> int:
    if value is None:
        return 0
    if not isinstance(value,str) or not 1<=len(value)<=256:
        raise ToolFailure("invalid_argument","Cursor is invalid")
    try:
        raw=base64.urlsafe_b64decode(value+"="*((4-len(value)%4)%4))
        data=json.loads(raw)
    except (ValueError,UnicodeDecodeError,json.JSONDecodeError):
        raise ToolFailure("invalid_argument","Cursor is invalid") from None
    if not isinstance(data,dict) or data.get("v")!=1 or data.get("d")!=document_id or type(data.get("s")) is not int or data["s"]<0:
        raise ToolFailure("invalid_argument","Cursor is invalid")
    return data["s"]


def worker_query_embedding(
    payload: dict[str,Any], *, socket_path: str = WORKER_SOCKET, timeout: float = 10.0
) -> dict[str,Any]:
    request=json.dumps({"action":"query-embedding",**payload},ensure_ascii=False,separators=(",",":")).encode()+b"\n"
    if len(request)>1_048_576:
        raise ToolFailure("invalid_argument","Search request is too large")
    sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect(socket_path)
        sock.sendall(request)
        chunks=bytearray()
        while len(chunks)<=MAX_WORKER_RESPONSE:
            chunk=sock.recv(65536)
            if not chunk:
                break
            chunks.extend(chunk)
            if b"\n" in chunk:
                break
        if len(chunks)>MAX_WORKER_RESPONSE:
            raise ToolFailure("worker_unavailable","Embedding worker response is too large")
    except TimeoutError:
        raise ToolFailure("timeout","Embedding worker timed out") from None
    except OSError:
        raise ToolFailure("worker_unavailable","Embedding worker is unavailable") from None
    finally:
        sock.close()
    try:
        response=json.loads(bytes(chunks).split(b"\n",1)[0])
        if not isinstance(response,dict) or not response.get("ok") or not isinstance(response.get("result"),dict):
            raise ValueError
        return response["result"]
    except (UnicodeDecodeError,json.JSONDecodeError,ValueError,IndexError):
        raise ToolFailure("worker_unavailable","Embedding worker returned an invalid response") from None


class ToolService:
    def __init__(
        self,
        config: MCPConfig,
        *,
        connection_factory: Callable[[MCPConfig],Any] = readonly_connection,
        worker_call: Callable[...,dict[str,Any]] = worker_query_embedding,
    ) -> None:
        self.config=config
        self.connection_factory=connection_factory
        self.worker_call=worker_call

    def _guarded(self,name:str,operation:Callable[[],dict[str,Any]],*,query_sha256:str|None=None)->dict[str,Any]:
        started=time.monotonic()
        request_id=str(uuid.uuid4())
        try:
            result=operation()
        except ToolFailure as exc:
            LOGGER.warning("mcp tool=%s status=error code=%s request_id=%s latency_ms=%d",name,exc.code,request_id,round((time.monotonic()-started)*1000))
            raise
        except Exception as exc:  # noqa: BLE001 - never forward database or SDK exception text.
            LOGGER.error("mcp tool=%s status=error code=internal exception=%s request_id=%s latency_ms=%d",name,type(exc).__name__,request_id,round((time.monotonic()-started)*1000))
            raise ToolFailure("internal","The request could not be completed") from None
        suffix=f" query_sha256={query_sha256}" if query_sha256 else ""
        LOGGER.info("mcp tool=%s status=ok request_id=%s latency_ms=%d%s",name,request_id,round((time.monotonic()-started)*1000),suffix)
        return result

    def _project_exists(self,project:str)->bool:
        with self.connection_factory(self.config) as conn, conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM projects WHERE slug=%s",(project,))
            return cursor.fetchone() is not None

    def search(
        self,
        query:str,
        project:str,
        limit:int=10,
        source:str="all",
        source_kind:str|None=None,
        date_from:str|None=None,
        date_to:str|None=None,
        author:str|None=None,
        topic:str|None=None,
    )->dict[str,Any]:
        query_hash=hashlib.sha256(query.encode("utf-8")).hexdigest() if isinstance(query,str) else None
        def execute()->dict[str,Any]:
            if not isinstance(query,str) or not query.strip() or len(query)>2000:
                raise ToolFailure("invalid_argument","query must contain 1..2000 characters")
            if not isinstance(project,str) or not 1<=len(project)<=128:
                raise ToolFailure("invalid_argument","project is invalid")
            if type(limit) is not int or not 1<=limit<=50:
                raise ToolFailure("invalid_argument","limit must be between 1 and 50")
            if source not in {"all","segment","asr"}:
                raise ToolFailure("invalid_argument","source is invalid")
            if source_kind not in {None,"telegram","document","web","video"}:
                raise ToolFailure("invalid_argument","source_kind is invalid")
            dates=[]
            for value in (date_from,date_to):
                if value is None:
                    dates.append(None)
                elif not isinstance(value,str) or len(value)!=10 or value[4]!="-" or value[7]!="-":
                    raise ToolFailure("invalid_argument","dates must use YYYY-MM-DD")
                else:
                    try:
                        dates.append(date.fromisoformat(value))
                    except ValueError:
                        raise ToolFailure("invalid_argument","dates must use YYYY-MM-DD") from None
            if dates[0] and dates[1] and dates[0]>dates[1]:
                raise ToolFailure("invalid_argument","date_from must not be later than date_to")
            if author is not None and (not isinstance(author,str) or len(author)>256):
                raise ToolFailure("invalid_argument","author is invalid")
            if topic is not None and (not isinstance(topic,str) or len(topic)>128):
                raise ToolFailure("invalid_argument","topic is invalid")
            try:
                if not self._project_exists(project):
                    raise ToolFailure("not_found","Project was not found")
            except ToolFailure:
                raise
            except Exception:  # noqa: BLE001 - redact database driver details.
                raise ToolFailure("db_unavailable","Knowledge database is unavailable") from None
            request_id=str(uuid.uuid4())
            vector=None
            degraded_reason=None
            try:
                embedded=self.worker_call(
                    {"query":query,"project":project,"model":MODEL_KEY,"request_id":request_id},
                    timeout=10.0,
                )
                status=embedded.get("status")
                if status=="done":
                    raw=embedded.get("vector")
                    if embedded.get("dimensions")!=DIMENSIONS or embedded.get("model")!=MODEL_KEY or not isinstance(raw,list) or len(raw)!=DIMENSIONS:
                        degraded_reason="provider_unavailable"
                    else:
                        try:
                            candidate=[float(value) for value in raw]
                            if any(not math.isfinite(value) for value in candidate):
                                raise ValueError
                            vector=candidate
                        except (TypeError,ValueError):
                            degraded_reason="provider_unavailable"
                else:
                    reason=embedded.get("reason")
                    degraded_reason=reason if reason in {"quota","timeout","worker_unavailable","worker_busy","provider_unavailable"} else "provider_unavailable"
            except ToolFailure as exc:
                degraded_reason=exc.code if exc.code in {"quota","timeout","worker_unavailable","worker_busy","provider_unavailable"} else "worker_unavailable"
            with self.connection_factory(self.config) as conn:
                try:
                    result=execute_search(
                        conn,query,project=project,vector=vector,limit=limit,
                        source=None if source=="all" else source,
                        source_kind=source_kind,date_from=dates[0],date_to=dates[1],
                        author=author,topic=topic,degraded_reason=degraded_reason,
                    )
                except SearchError as exc:
                    if "does not exist" in str(exc):
                        raise ToolFailure("not_found","Project was not found") from None
                    raise ToolFailure("db_unavailable","Knowledge query failed") from None
            result["count"]=len(result["results"])
            result["degraded"]=vector is None
            result["degraded_reason"]=degraded_reason
            return result
        return self._guarded("search",execute,query_sha256=query_hash)

    def get_document(self,document_id:str,limit:int=100,cursor:str|None=None,include_asr:bool=True)->dict[str,Any]:
        def execute()->dict[str,Any]:
            if type(limit) is not int or not 1<=limit<=100:
                raise ToolFailure("invalid_argument","limit must be between 1 and 100")
            if type(include_asr) is not bool:
                raise ToolFailure("invalid_argument","include_asr must be boolean")
            try:
                parsed=str(uuid.UUID(document_id))
            except (ValueError,TypeError,AttributeError):
                raise ToolFailure("invalid_argument","document_id must be a UUID") from None
            after=_cursor_decode(cursor,parsed)
            try:
                with self.connection_factory(self.config) as conn, conn.cursor() as cur:
                    cur.execute(
                        """SELECT d.id,so.kind,d.doc_type,d.language,d.created_at,
                           (SELECT count(*) FROM segments s WHERE s.document_id=d.id),
                           (SELECT count(*) FROM asr_results r JOIN segments s ON s.external_ref=r.external_ref
                             JOIN asr_runs ar ON ar.id=r.run_id
                             WHERE s.document_id=d.id AND r.status='complete'
                               AND ar.status IN ('done','partial'))
                           FROM documents d JOIN sources so ON so.id=d.source_id WHERE d.id=%s""",
                        (parsed,),
                    )
                    row=cur.fetchone()
                    if row is None:
                        raise ToolFailure("not_found","Document was not found")
                    document={"id":str(row[0]),"source_kind":row[1],"doc_type":row[2],"language":row[3],"created_at":row[4].isoformat() if row[4] else None,"segments_count":int(row[5]),"asr_results_count":int(row[6])}
                    cur.execute(
                        """SELECT id,external_ref,posted_at,speaker,text,meta->>'url',
                                  meta->>'media_type',meta->>'topic_id'
                           FROM segments WHERE document_id=%s AND id>%s ORDER BY id LIMIT %s""",
                        (parsed,after,limit+1),
                    )
                    rows=cur.fetchall()
                    page=rows[:limit]
                    segments=[{"segment_id":int(r[0]),"external_ref":r[1],"posted_at":r[2].isoformat() if r[2] else None,"author":r[3],"text":r[4],"url":r[5],"media_type":r[6],"topic_id":r[7]} for r in page]
                    asr_results=[]
                    if include_asr and page:
                        refs=[r[1] for r in page if r[1] is not None]
                        if refs:
                            cur.execute(
                                """SELECT r.external_ref,ar.run_key,ar.model,ar.language,r.duration_seconds,r.text
                                   FROM asr_results r JOIN asr_runs ar ON ar.id=r.run_id
                                   WHERE r.status='complete' AND ar.status IN ('done','partial')
                                     AND r.external_ref=ANY(%s) AND ar.project_id=(
                                       SELECT so.project_id FROM documents d JOIN sources so ON so.id=d.source_id WHERE d.id=%s)
                                   ORDER BY r.external_ref,ar.created_at,ar.run_key""",
                                (refs,parsed),
                            )
                            asr_results=[{"external_ref":r[0],"run_key":r[1],"model":r[2],"language":r[3],"duration_seconds":float(r[4]) if r[4] is not None else None,"text":r[5]} for r in cur.fetchall()]
            except ToolFailure:
                raise
            except Exception:  # noqa: BLE001 - redact database driver details.
                raise ToolFailure("db_unavailable","Document query failed") from None
            next_cursor=_cursor_encode(parsed,int(page[-1][0])) if len(rows)>limit and page else None
            return {"document":document,"segments":segments,"asr_results":asr_results,"next_cursor":next_cursor}
        return self._guarded("get_document",execute)

    def list_projects(self)->dict[str,Any]:
        def execute()->dict[str,Any]:
            try:
                with self.connection_factory(self.config) as conn, conn.cursor() as cur:
                    cur.execute(
                        """SELECT p.slug,p.title,
                           (SELECT count(*) FROM sources so WHERE so.project_id=p.id),
                           (SELECT count(*) FROM documents d JOIN sources so ON so.id=d.source_id WHERE so.project_id=p.id),
                           (SELECT count(*) FROM segments s JOIN documents d ON d.id=s.document_id JOIN sources so ON so.id=d.source_id WHERE so.project_id=p.id),
                           (SELECT count(*) FROM asr_runs ar WHERE ar.project_id=p.id),
                           (SELECT count(*) FROM asr_results r JOIN asr_runs ar ON ar.id=r.run_id WHERE ar.project_id=p.id),
                           (SELECT count(*) FROM asr_segments a JOIN asr_runs ar ON ar.id=a.run_id WHERE ar.project_id=p.id),
                           (SELECT count(*) FROM embeddings e WHERE e.project_id=p.id)
                           FROM projects p ORDER BY p.slug"""
                    )
                    projects=[{"slug":r[0],"title":r[1],"sources":int(r[2]),"documents":int(r[3]),"segments":int(r[4]),"asr_runs":int(r[5]),"asr_results":int(r[6]),"asr_segments":int(r[7]),"embeddings":int(r[8])} for r in cur.fetchall()]
            except Exception:  # noqa: BLE001 - redact database driver details.
                raise ToolFailure("db_unavailable","Project list is unavailable") from None
            return {"projects":projects}
        return self._guarded("list_projects",execute)

    def ingest_status(self,project:str)->dict[str,Any]:
        def execute()->dict[str,Any]:
            if not isinstance(project,str) or not 1<=len(project)<=128:
                raise ToolFailure("invalid_argument","project is invalid")
            try:
                with self.connection_factory(self.config) as conn, conn.cursor() as cur:
                    cur.execute("SELECT id FROM projects WHERE slug=%s",(project,))
                    project_row=cur.fetchone()
                    if not project_row:
                        raise ToolFailure("not_found","Project was not found")
                    project_id=project_row[0]
                    cur.execute(
                        """SELECT
                          (SELECT count(*) FROM sources WHERE project_id=%s),
                          (SELECT count(*) FROM documents d JOIN sources so ON so.id=d.source_id WHERE so.project_id=%s),
                          (SELECT count(*) FROM segments s JOIN documents d ON d.id=s.document_id JOIN sources so ON so.id=d.source_id WHERE so.project_id=%s),
                          (SELECT count(*) FROM asr_runs WHERE project_id=%s),
                          (SELECT count(*) FROM asr_results r JOIN asr_runs ar ON ar.id=r.run_id WHERE ar.project_id=%s),
                          (SELECT count(*) FROM asr_segments a JOIN asr_runs ar ON ar.id=a.run_id WHERE ar.project_id=%s),
                          (SELECT count(*) FROM asr_attempts a JOIN asr_runs ar ON ar.id=a.run_id WHERE ar.project_id=%s),
                          (SELECT count(*) FROM embeddings e WHERE e.project_id=%s AND e.owner_table='asr_segments'),
                          (SELECT count(*) FROM embeddings e WHERE e.project_id=%s AND e.owner_table='segments')""",
                        (project_id,)*9,
                    )
                    counts_row=cur.fetchone()
                    counts=dict(zip(("sources","documents","segments","asr_runs","asr_results","asr_segments","asr_attempts","asr_embeddings","source_embeddings"),(int(x) for x in counts_row),strict=True))
                    cur.execute("SELECT kind,status,started_at,finished_at FROM runs WHERE project_id=%s ORDER BY started_at DESC,id DESC LIMIT 10",(project_id,))
                    latest_runs=[{"kind":r[0],"status":r[1],"started_at":r[2].isoformat() if r[2] else None,"finished_at":r[3].isoformat() if r[3] else None} for r in cur.fetchall()]
                    cur.execute("SELECT run_key,model,status,finished_at FROM asr_runs WHERE project_id=%s ORDER BY created_at DESC,id DESC LIMIT 10",(project_id,))
                    latest_asr=[{"run_key":r[0],"model":r[1],"status":r[2],"finished_at":r[3].isoformat() if r[3] else None} for r in cur.fetchall()]
                    cur.execute("SELECT s.scope,s.sha256,s.created_at FROM snapshots s JOIN sources so ON so.id=s.source_id WHERE so.project_id=%s ORDER BY s.created_at DESC,s.id DESC LIMIT 1",(project_id,))
                    row=cur.fetchone()
                    snapshot={"scope":row[0],"sha256":row[1],"created_at":row[2].isoformat() if row[2] else None} if row else None
            except ToolFailure:
                raise
            except Exception:  # noqa: BLE001 - redact database driver details.
                raise ToolFailure("db_unavailable","Ingest status is unavailable") from None
            return {"project":project,"counts":counts,"latest_runs":latest_runs,"latest_asr_runs":latest_asr,"latest_snapshot":snapshot}
        return self._guarded("ingest_status",execute)

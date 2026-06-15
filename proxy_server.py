#!/usr/bin/env python3
"""LLM 代理服务 - 按模型串行转发请求到上游 API"""

from config import *
import asyncio
import codecs
import json
import time
from typing import Any, Awaitable, Callable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from anthropic import Anthropic
from anthropic import APIError as AnthropicAPIError
import httpx
from loguru import logger
from openai import OpenAI
from openai import APIError as OpenAIAPIError

OPENAI_CHAT = "openai_chat"
ANTHROPIC = "anthropic"


def _normalize_provider(cfg: dict) -> str:
    provider = cfg.get("provider") or cfg.get("api_type") or cfg.get("api") or OPENAI_CHAT
    provider = provider.lower().replace("-", "_")
    if provider in {"openai", "openai_chat", "chat_completions"}:
        return OPENAI_CHAT
    if provider in {"anthropic", "claude"}:
        return ANTHROPIC
    raise ValueError(f"Unsupported provider: {provider}")


def _client_kwargs(cfg: dict) -> dict[str, Any]:
    kwargs: dict[str, Any] = {"api_key": cfg["api_key"]}
    base_url = cfg.get("base_url") or cfg.get("api_base")
    if base_url:
        kwargs["base_url"] = base_url
    default_headers = cfg.get("default_headers") or cfg.get("headers")
    if default_headers:
        kwargs["default_headers"] = default_headers
    kwargs["http_client"] = httpx.Client(trust_env=False)
    return kwargs


# 每个模型一个独立客户端
_clients: dict[str, Any] = {}
_providers: dict[str, str] = {}

for name, cfg in MODELS.items():
    provider = _normalize_provider(cfg)
    _providers[name] = provider
    if provider == OPENAI_CHAT:
        client = OpenAI(**_client_kwargs(cfg))
    elif provider == ANTHROPIC:
        client = Anthropic(**_client_kwargs(cfg))
    _clients[name] = client

# 每个模型一把锁，保证同一模型的请求串行处理
_model_locks: dict[str, asyncio.Lock] = {}
_lock_mutex = asyncio.Lock()


async def _get_lock(model: str) -> asyncio.Lock:
    async with _lock_mutex:
        if model not in _model_locks:
            _model_locks[model] = asyncio.Lock()
        return _model_locks[model]


def _upstream_body(local_model: str, body: dict) -> dict:
    upstream_model = MODELS[local_model].get("upstream_model")
    if not upstream_model:
        return body

    payload = dict(body)
    payload["model"] = upstream_model
    return payload


async def _run_with_queue(
    local_model: str,
    cfg: dict,
    call: Callable[[asyncio.Lock | None, float], Awaitable[Response]],
) -> Response:
    lock = await _get_lock(local_model) if cfg.get("serial", True) else None
    if lock is not None:
        await lock.acquire()

    await asyncio.sleep(cfg.get("delay", 0))
    t_start = time.monotonic()

    try:
        return await call(lock, t_start)
    except asyncio.CancelledError:
        if lock is not None and lock.locked():
            lock.release()
        raise
    except Exception:
        if lock is not None and lock.locked():
            lock.release()
        raise


def _release_lock(lock: asyncio.Lock | None):
    if lock is not None and lock.locked():
        lock.release()


app = FastAPI(title="LLM Proxy")


def _model_error(local_model: str, error_type: str = "invalid_request_error") -> JSONResponse:
    return JSONResponse(status_code=400, content={
        "error": {
            "message": f"Model '{local_model}' not found. Available: {list(MODELS.keys())}",
            "type": error_type,
        }
    })


def _provider_error(local_model: str, expected: str, error_type: str = "invalid_request_error") -> JSONResponse:
    return JSONResponse(status_code=400, content={
        "error": {
            "message": (
                f"Model '{local_model}' is configured for '{_providers[local_model]}', "
                f"but this endpoint requires '{expected}'."
            ),
            "type": error_type,
        }
    })


def _api_error_response(e: OpenAIAPIError | AnthropicAPIError, error_type: str = "upstream_error") -> Response:
    response = getattr(e, "response", None)
    if response is not None:
        return _raw_response(response)

    status_code = getattr(e, "status_code", None) or 500
    return JSONResponse(status_code=status_code, content={
        "error": {"message": str(e), "type": error_type}
    })


def _response_headers(headers) -> dict[str, str]:
    blocked = {
        "connection",
        "content-encoding",
        "content-length",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
    return {
        key: value
        for key, value in headers.items()
        if key.lower() not in blocked
    }


def _content_type(headers) -> str | None:
    return headers.get("content-type")


def _raw_response(raw) -> Response:
    return Response(
        content=raw.content,
        status_code=raw.status_code,
        headers=_response_headers(raw.headers),
        media_type=_content_type(raw.headers),
    )


class _StreamUsageTracker:
    def __init__(self, model: str, t_start: float):
        self.model = model
        self.t_start = t_start
        self.first_chunk_time: float | None = None
        self.prompt_tokens = 0
        self.completion_tokens = 0
        self._decoder = codecs.getincrementaldecoder("utf-8")()
        self._buffer = ""
        self._last_data: dict[str, Any] | None = None
        self._last_usage_data: dict[str, Any] | None = None

    def feed(self, chunk: bytes):
        if not chunk:
            return
        if self.first_chunk_time is None:
            self.first_chunk_time = time.monotonic()

        try:
            self._buffer += self._decoder.decode(chunk)
            self._buffer = self._buffer.replace("\r\n", "\n").replace("\r", "\n")
            self._consume_events()
        except Exception:
            logger.debug("[{}] failed to parse stream usage", self.model)

    def log(self):
        try:
            self._buffer += self._decoder.decode(b"", final=True)
            self._buffer = self._buffer.replace("\r\n", "\n").replace("\r", "\n")
            self._consume_events(flush=True)
            self._extract_final_usage()
        except Exception:
            logger.debug("[{}] failed to flush stream usage", self.model)

        elapsed = time.monotonic() - self.t_start
        ttft = (self.first_chunk_time - self.t_start) if self.first_chunk_time else 0
        tps = (
            self.completion_tokens / (time.monotonic() - self.first_chunk_time)
            if self.first_chunk_time and self.completion_tokens
            else 0
        )
        logger.info(
            "[{}] prompt={}  ttft={:.2f}s  completion={}  tps={:.1f}  total={:.2f}s",
            self.model,
            self.prompt_tokens,
            ttft,
            self.completion_tokens,
            tps,
            elapsed,
        )

    def _consume_events(self, flush: bool = False):
        while True:
            sep = self._buffer.find("\n\n")
            if sep < 0:
                break
            event = self._buffer[:sep]
            self._buffer = self._buffer[sep + 2:]
            self._parse_event(event)

        if flush and self._buffer.strip():
            event = self._buffer
            self._buffer = ""
            self._parse_event(event)

    def _parse_event(self, event: str):
        data_lines = []
        for line in event.splitlines():
            if line.startswith("data:"):
                data_lines.append(line[5:].lstrip(" "))
        if not data_lines:
            return

        data_text = "\n".join(data_lines)
        if data_text == "[DONE]":
            return

        try:
            data = json.loads(data_text)
        except Exception:
            return

        self._last_data = data
        if self._usage_from_data(data):
            self._last_usage_data = data

    def _extract_final_usage(self):
        data = self._last_data
        if not self._usage_from_data(data):
            data = self._last_usage_data
        if data is None:
            return

        self._update_usage(data.get("usage"))
        message = data.get("message")
        if isinstance(message, dict):
            self._update_usage(message.get("usage"))

    def _usage_from_data(self, data: Any) -> bool:
        if not isinstance(data, dict):
            return False
        if isinstance(data.get("usage"), dict):
            return True
        message = data.get("message")
        return isinstance(message, dict) and isinstance(message.get("usage"), dict)

    def _update_usage(self, usage: Any):
        if not isinstance(usage, dict):
            return

        prompt = usage.get("prompt_tokens", usage.get("input_tokens"))
        completion = usage.get("completion_tokens", usage.get("output_tokens"))
        if isinstance(prompt, int):
            self.prompt_tokens = prompt
        if isinstance(completion, int):
            self.completion_tokens = completion


# ---- 认证中间件 ----

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    if API_KEYS:
        auth = request.headers.get("Authorization", "")
        token = auth.removeprefix("Bearer ").strip()
        if token not in API_KEYS:
            return JSONResponse(status_code=401, content={
                "error": {"message": "Invalid API key", "type": "authentication_error"}
            })
    return await call_next(request)


# ---- OpenAI /v1/chat/completions ----

@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    body = await request.json()
    local_model = body.get("model", "")

    if local_model not in MODELS:
        return _model_error(local_model)
    if _providers[local_model] != OPENAI_CHAT:
        return _provider_error(local_model, OPENAI_CHAT)

    cfg = MODELS[local_model]
    upstream_body = _upstream_body(local_model, body)
    stream = body.get("stream", False)
    is_stream = stream.lower() == "true" if isinstance(stream, str) else bool(stream)

    try:
        async def call(lock: asyncio.Lock | None, t_start: float) -> Response:
            if is_stream:
                return await _stream_chat(local_model, upstream_body, lock, t_start)
            resp = await _chat(local_model, upstream_body)
            _release_lock(lock)
            usage_body = resp.body.decode() if isinstance(resp.body, bytes) else ""
            _log_tps(local_model, t_start, usage_body)
            return resp

        return await _run_with_queue(local_model, cfg, call)
    except OpenAIAPIError as e:
        return _api_error_response(e)


async def _chat(local_model: str, body: dict) -> JSONResponse:
    c = _clients[local_model]
    loop = asyncio.get_running_loop()
    raw = await loop.run_in_executor(
        None, lambda: c.chat.completions.with_raw_response.create(**body)
    )
    return _raw_response(raw)


async def _stream_chat(
    local_model: str,
    body: dict,
    lock: asyncio.Lock | None,
    t_start: float,
) -> StreamingResponse:
    c = _clients[local_model]
    loop = asyncio.get_running_loop()
    stream_cm = await loop.run_in_executor(
        None, lambda: c.chat.completions.with_streaming_response.create(**body)
    )
    stream = await loop.run_in_executor(
        None, stream_cm.__enter__
    )
    byte_iter = stream.iter_bytes()
    usage_tracker = _StreamUsageTracker(local_model, t_start)

    async def generate():
        try:
            while True:
                chunk = await _next_chunk(byte_iter, loop)
                if chunk is None:
                    break
                usage_tracker.feed(chunk)
                yield chunk
        finally:
            usage_tracker.log()
            try:
                await loop.run_in_executor(None, stream_cm.__exit__, None, None, None)
            finally:
                _release_lock(lock)

    return StreamingResponse(
        generate(),
        status_code=stream.status_code,
        headers=_response_headers(stream.headers),
        media_type=_content_type(stream.headers) or "text/event-stream",
    )


def _next_chunk_sync(stream):
    try:
        return next(stream)
    except StopIteration:
        return None


async def _next_chunk(stream, loop):
    return await loop.run_in_executor(None, _next_chunk_sync, stream)


def _log_tps(model: str, t_start: float, body: str):
    elapsed = time.monotonic() - t_start
    try:
        usage = json.loads(body).get("usage", {})
        prompt = usage.get("prompt_tokens", 0)
        prompt = usage.get("prompt_tokens", usage.get("input_tokens", prompt))
        completion = usage.get("completion_tokens", usage.get("output_tokens", 0))
    except Exception:
        prompt = 0
        completion = 0
    tps = completion / elapsed if completion and elapsed else 0
    logger.info("[{}] prompt={}  completion={}  tps={:.1f}  total={:.2f}s",
                model, prompt, completion, tps, elapsed)


@app.get("/v1/models")
async def list_models():
    data = [
        {"id": name, "object": "model", "owned_by": "proxy"}
        for name in MODELS
    ]
    return JSONResponse(content={"object": "list", "data": data})


# ---- Anthropic /v1/messages ----

@app.post("/v1/messages")
async def messages(request: Request):
    body = await request.json()
    local_model = body.get("model", "")

    if local_model not in MODELS:
        return _model_error(local_model)
    if _providers[local_model] != ANTHROPIC:
        return _provider_error(local_model, ANTHROPIC)

    cfg = MODELS[local_model]
    upstream_body = _upstream_body(local_model, body)
    stream = body.get("stream", False)
    is_stream = stream.lower() == "true" if isinstance(stream, str) else bool(stream)

    try:
        async def call(lock: asyncio.Lock | None, t_start: float) -> Response:
            if is_stream:
                return await _stream_anthropic(local_model, upstream_body, lock, t_start)
            resp = await _anthropic_message(local_model, upstream_body)
            _release_lock(lock)
            usage_body = resp.body.decode() if isinstance(resp.body, bytes) else ""
            _log_tps(local_model, t_start, usage_body)
            return resp

        return await _run_with_queue(local_model, cfg, call)
    except AnthropicAPIError as e:
        return _api_error_response(e)


async def _anthropic_message(local_model: str, body: dict) -> Response:
    c = _clients[local_model]
    loop = asyncio.get_running_loop()
    raw = await loop.run_in_executor(
        None, lambda: c.messages.with_raw_response.create(**body)
    )
    return _raw_response(raw)


async def _stream_anthropic(
    local_model: str,
    body: dict,
    lock: asyncio.Lock | None,
    t_start: float,
) -> StreamingResponse:
    """流式转发 Anthropic SDK 事件。"""
    c = _clients[local_model]
    loop = asyncio.get_running_loop()
    stream_cm = await loop.run_in_executor(
        None, lambda: c.messages.with_streaming_response.create(**body)
    )
    stream = await loop.run_in_executor(
        None, stream_cm.__enter__
    )
    byte_iter = stream.iter_bytes()
    usage_tracker = _StreamUsageTracker(local_model, t_start)

    async def generate():
        try:
            while True:
                chunk = await _next_chunk(byte_iter, loop)
                if chunk is None:
                    break
                usage_tracker.feed(chunk)
                yield chunk
        finally:
            usage_tracker.log()
            try:
                await loop.run_in_executor(None, stream_cm.__exit__, None, None, None)
            finally:
                _release_lock(lock)

    return StreamingResponse(
        generate(),
        status_code=stream.status_code,
        headers=_response_headers(stream.headers),
        media_type=_content_type(stream.headers) or "text/event-stream",
    )


if __name__ == "__main__":
    import uvicorn
    print(f"LLM Proxy: {SERVER_HOST}:{SERVER_PORT}")
    print(f"  Auth     : {'enabled' if API_KEYS else 'disabled'}")
    for name, cfg in MODELS.items():
        provider = _providers[name]
        base_url = cfg.get("base_url") or cfg.get("api_base") or "default"
        print(f"  {name} [{provider}] @ {base_url} (delay={cfg['delay']}s)")
    uvicorn.run(app, host=SERVER_HOST, port=SERVER_PORT)

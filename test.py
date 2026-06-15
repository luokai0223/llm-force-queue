#!/usr/bin/env python3
"""Concurrent proxy test for per-model queueing and delay."""

import asyncio
import json
import os
import time
from typing import Any

import httpx


PROXY_URL = os.getenv("PROXY_URL", "http://127.0.0.1:4002").rstrip("/")
API_KEY = os.getenv("API_KEY", "sk-xx")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "deepseek-v4-flash")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "deepseek-v4-flash_coding")
TIMEOUT = float(os.getenv("TIMEOUT", "120"))


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }


def _summarize_response(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {"raw": str(data)[:300]}

    return {
        "id": data.get("id"),
        "model": data.get("model"),
        "usage": data.get("usage"),
        "error": data.get("error"),
    }


async def post_json(
    client: httpx.AsyncClient,
    label: str,
    path: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        response = await client.post(path, json=payload)
        elapsed = time.perf_counter() - started
        try:
            data = response.json()
        except Exception:
            data = {"raw": response.text[:300]}

        return {
            "label": label,
            "status": response.status_code,
            "elapsed": round(elapsed, 3),
            **_summarize_response(data),
        }
    except Exception as exc:
        elapsed = time.perf_counter() - started
        return {
            "label": label,
            "elapsed": round(elapsed, 3),
            "exception": type(exc).__name__,
            "message": str(exc),
        }


async def post_stream(
    client: httpx.AsyncClient,
    label: str,
    path: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    started = time.perf_counter()
    first_chunk_at = None
    total_bytes = 0
    preview = bytearray()

    try:
        async with client.stream("POST", path, json=payload) as response:
            async for chunk in response.aiter_bytes():
                if not chunk:
                    continue
                if first_chunk_at is None:
                    first_chunk_at = time.perf_counter()
                total_bytes += len(chunk)
                if len(preview) < 300:
                    preview.extend(chunk[:300 - len(preview)])

            elapsed = time.perf_counter() - started
            return {
                "label": label,
                "status": response.status_code,
                "elapsed": round(elapsed, 3),
                "ttfb": round(first_chunk_at - started, 3) if first_chunk_at else None,
                "bytes": total_bytes,
                "preview": preview.decode(errors="replace"),
            }
    except Exception as exc:
        elapsed = time.perf_counter() - started
        return {
            "label": label,
            "elapsed": round(elapsed, 3),
            "exception": type(exc).__name__,
            "message": str(exc),
        }


async def run_case(name: str, requests: list[tuple[str, str, dict[str, Any]]]):
    print(f"\n=== {name} ===")
    started = time.perf_counter()

    async with httpx.AsyncClient(
        base_url=PROXY_URL,
        headers=_headers(),
        timeout=TIMEOUT,
        trust_env=False,
    ) as client:
        results = await asyncio.gather(
            *[
                post_json(client, label, path, payload)
                for label, path, payload in requests
            ]
        )

    total = time.perf_counter() - started
    for result in results:
        print(json.dumps(result, ensure_ascii=False))
    print(f"case_total={total:.3f}s")


async def run_stream_case(name: str, requests: list[tuple[str, str, dict[str, Any]]]):
    print(f"\n=== {name} ===")
    started = time.perf_counter()

    async with httpx.AsyncClient(
        base_url=PROXY_URL,
        headers=_headers(),
        timeout=TIMEOUT,
        trust_env=False,
    ) as client:
        results = await asyncio.gather(
            *[
                post_stream(client, label, path, payload)
                for label, path, payload in requests
            ]
        )

    total = time.perf_counter() - started
    for result in results:
        print(json.dumps(result, ensure_ascii=False))
    print(f"case_total={total:.3f}s")


def openai_payload(content: str) -> dict[str, Any]:
    return {
        "model": OPENAI_MODEL,
        "messages": [{"role": "user", "content": content}],
        "max_tokens": 8,
        "temperature": 0,
    }


def openai_stream_payload(content: str) -> dict[str, Any]:
    payload = openai_payload(content)
    payload["stream"] = True
    payload["stream_options"] = {"include_usage": True}
    return payload


def anthropic_payload(content: str) -> dict[str, Any]:
    return {
        "model": ANTHROPIC_MODEL,
        "messages": [{"role": "user", "content": content}],
        "max_tokens": 8,
        "temperature": 0,
    }


def anthropic_stream_payload(content: str) -> dict[str, Any]:
    payload = anthropic_payload(content)
    payload["stream"] = True
    return payload


async def main():
    print(f"proxy={PROXY_URL}")
    print(f"openai_model={OPENAI_MODEL}")
    print(f"anthropic_model={ANTHROPIC_MODEL}")

    await run_case(
        "same OpenAI model, 2 concurrent requests",
        [
            ("openai-1", "/v1/chat/completions", openai_payload("reply with one word: one")),
            ("openai-2", "/v1/chat/completions", openai_payload("reply with one word: two")),
        ],
    )

    await run_case(
        "same Anthropic model, 2 concurrent requests",
        [
            ("anthropic-1", "/v1/messages", anthropic_payload("reply with one word: one")),
            ("anthropic-2", "/v1/messages", anthropic_payload("reply with one word: two")),
        ],
    )

    await run_case(
        "different models, 2 concurrent requests",
        [
            ("openai", "/v1/chat/completions", openai_payload("reply with one word: openai")),
            ("anthropic", "/v1/messages", anthropic_payload("reply with one word: anthropic")),
        ],
    )

    await run_stream_case(
        "streaming different models, 2 concurrent requests",
        [
            ("openai-stream", "/v1/chat/completions", openai_stream_payload("reply with one short sentence")),
            ("anthropic-stream", "/v1/messages", anthropic_stream_payload("reply with one short sentence")),
        ],
    )

    await run_stream_case(
        "same OpenAI model, 2 concurrent streaming requests",
        [
            ("openai-stream-1", "/v1/chat/completions", openai_stream_payload("reply with one short sentence")),
            ("openai-stream-2", "/v1/chat/completions", openai_stream_payload("reply with one short sentence")),
        ],
    )


if __name__ == "__main__":
    asyncio.run(main())

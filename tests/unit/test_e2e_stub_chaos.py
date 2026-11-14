"""The stub LLM's latency and failure rate (das-scale-resilience task 12.3; Phase 7 Req 4.5): the values it reads and
refuses, and the behaviour of the real server with them, so a load test can set the provider's speed and reliability."""

from __future__ import annotations

import json
import random
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from http.server import ThreadingHTTPServer

import pytest

from tests.integration.e2e.llm_stub import FAILURE_ENV, LATENCY_ENV, MAX_LATENCY_MS, WORKER_MARK, Chaos, Handler

CHAT = {"model": "fake-llm", "messages": [
    {"role": "system", "content": WORKER_MARK}, {"role": "user", "content": "Instruction:\nsay hello"},
]}


# --------------------------------------------------------------------------- the values


def test_by_default_the_stub_is_instant_and_never_fails() -> None:
    chaos = Chaos()
    assert chaos.latency_s == 0 and chaos.failure_rate == 0 and not any(chaos.fails() for _ in range(1000))


def test_the_settings_are_read_from_the_environment() -> None:
    chaos = Chaos.from_environment({LATENCY_ENV: "250", FAILURE_ENV: "0.3"})
    assert chaos.latency_s == 0.25 and chaos.failure_rate == 0.3


@pytest.mark.parametrize("environ", [{}, {LATENCY_ENV: "", FAILURE_ENV: " "}])
def test_missing_or_blank_settings_mean_none(environ: dict[str, str]) -> None:
    chaos = Chaos.from_environment(environ)
    assert chaos.latency_s == 0 and chaos.failure_rate == 0


@pytest.mark.parametrize(("environ", "name"), [
    ({LATENCY_ENV: "fast"}, LATENCY_ENV), ({FAILURE_ENV: "often"}, FAILURE_ENV),
    ({LATENCY_ENV: "-1"}, LATENCY_ENV), ({LATENCY_ENV: str(MAX_LATENCY_MS + 1)}, LATENCY_ENV),
    ({FAILURE_ENV: "-0.1"}, FAILURE_ENV), ({FAILURE_ENV: "1.1"}, FAILURE_ENV), ({FAILURE_ENV: "nan"}, FAILURE_ENV),
])
def test_a_value_that_makes_no_sense_stops_the_stub_naming_the_variable(environ: dict[str, str], name: str) -> None:
    with pytest.raises(ValueError, match=name):
        Chaos.from_environment(environ)


@pytest.mark.parametrize(
    "edge", [{LATENCY_ENV: "0", FAILURE_ENV: "0"}, {LATENCY_ENV: str(MAX_LATENCY_MS), FAILURE_ENV: "1"}]
)
def test_values_on_the_edge_are_accepted(edge: dict[str, str]) -> None:
    Chaos.from_environment(edge)


def test_a_rate_of_one_always_fails_and_a_rate_of_zero_never_does() -> None:
    assert all(Chaos(failure_rate=1.0).fails() for _ in range(100))
    assert not any(Chaos(failure_rate=0.0).fails() for _ in range(100))


def test_the_failure_rate_is_the_fraction_of_answers_that_fail() -> None:
    chaos = Chaos(failure_rate=0.25, rng=random.Random(7))
    failures = sum(chaos.fails() for _ in range(20_000))
    assert 0.23 < failures / 20_000 < 0.27


# --------------------------------------------------------------------------- the real server


@pytest.fixture
def server() -> Iterator[tuple[str, type[Handler]]]:
    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    http.daemon_threads = True
    thread = threading.Thread(target=http.serve_forever, daemon=True)
    thread.start()
    saved = Handler.chaos
    try:
        yield f"http://127.0.0.1:{http.server_address[1]}", Handler
    finally:
        Handler.chaos = saved
        http.shutdown()
        http.server_close()


def chat(url: str) -> tuple[int, float]:
    request = urllib.request.Request(
        f"{url}/api/chat", data=json.dumps(CHAT).encode(), headers={"Content-Type": "application/json"}
    )
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()
            return response.status, time.monotonic() - started
    except urllib.error.HTTPError as exc:
        return exc.code, time.monotonic() - started


def test_every_answer_takes_the_configured_latency(server: tuple[str, type[Handler]]) -> None:
    url, handler = server
    handler.chaos = Chaos(latency_ms=300)
    status, took = chat(url)
    assert status == 200 and took >= 0.3


def test_without_latency_the_answer_is_immediate(server: tuple[str, type[Handler]]) -> None:
    url, _ = server
    status, took = chat(url)
    assert status == 200 and took < 0.25


def test_a_failure_rate_of_one_answers_every_chat_with_a_500_after_the_latency(
    server: tuple[str, type[Handler]],
) -> None:
    url, handler = server
    handler.chaos = Chaos(latency_ms=200, failure_rate=1.0)
    status, took = chat(url)
    assert status == 500 and took >= 0.2, "a failing provider takes its time first"


def test_the_health_check_is_never_affected(server: tuple[str, type[Handler]]) -> None:
    url, handler = server
    handler.chaos = Chaos(latency_ms=2000, failure_rate=1.0)
    started = time.monotonic()
    with urllib.request.urlopen(f"{url}/api/tags", timeout=5) as response:
        assert response.status == 200
    assert time.monotonic() - started < 1.0


def test_roughly_the_configured_share_of_real_answers_fail(server: tuple[str, type[Handler]]) -> None:
    url, handler = server
    handler.chaos = Chaos(failure_rate=0.5, rng=random.Random(3))
    statuses = [chat(url)[0] for _ in range(60)]
    assert set(statuses) == {200, 500} and 18 <= statuses.count(500) <= 42

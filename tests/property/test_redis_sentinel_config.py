"""Property 1: validation accepts exactly the Sentinel configurations of Req 1.2 and refuses every other with a message
naming the field (das-scale-resilience task 8.1; Phase 7 Req 6.1, 1.1 to 1.3).

The oracle is written from the requirement, not from the schema: it decides by parsing, never by calling the code under
test. The generator mixes valid and invalid values for every key, so a wrong accept and a wrong refusal both show up."""

from __future__ import annotations

from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from das.config.schema import validate_config
from das.errors import ConfigError

NAME_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"


def is_host(text: str) -> bool:
    if not text or len(text) > 253 or not text[0].isalnum() or not text[-1].isalnum():
        return False
    return all(ch in NAME_CHARS + ".-" for ch in text)


def is_address(text: object) -> bool:
    if not isinstance(text, str) or text.count(":") != 1:
        return False
    host, port = text.split(":")
    return is_host(host) and port.isascii() and port.isdigit() and 1 <= int(port) <= 65535 and not port.startswith("0")


def is_master_name(text: object) -> bool:
    return (isinstance(text, str) and 1 <= len(text) <= 64 and text[0] in NAME_CHARS
            and all(ch in NAME_CHARS + "_.-" for ch in text))


def is_redis_url(text: object) -> bool:
    return isinstance(text, str) and any(text.startswith(p) for p in ("redis://", "rediss://", "unix://")) \
        and len(text) > len("redis://")


def expected_valid(redis: dict[str, Any]) -> bool:
    mode = redis.get("mode", "standalone")
    if mode == "standalone":
        return set(redis) <= {"mode", "url"} and "url" in redis and is_redis_url(redis["url"])
    if mode != "sentinel" or "url" in redis:
        return False
    sentinels = redis.get("sentinels")
    if not isinstance(sentinels, list) or not 1 <= len(sentinels) <= 10 or not all(is_address(s) for s in sentinels):
        return False
    if not is_master_name(redis.get("master_name")):
        return False
    if "db" in redis and not (isinstance(redis["db"], int) and 0 <= redis["db"] <= 15):
        return False
    return "sentinel_tls" not in redis or isinstance(redis["sentinel_tls"], bool)


hosts = st.one_of(
    st.text(alphabet=NAME_CHARS + ".-", min_size=1, max_size=20),
    st.sampled_from(["sentinel-1", "10.0.0.5", "redis.internal", "-bad", "bad-", "", "a b", "a_b", "x" * 300]),
)
ports = st.one_of(st.integers(-5, 70_000).map(str), st.sampled_from(["", "abc", "0", "26379", "065", "6.5"]))
addresses = st.one_of(
    st.builds(lambda h, p: f"{h}:{p}", hosts, ports),
    st.sampled_from(["sentinel", "http://s:26379", "a:b:c", "s:26379", "s1:26379", ""]),
)
master_names = st.one_of(
    st.text(alphabet=NAME_CHARS + "_.-", min_size=0, max_size=70),
    st.sampled_from(["das-master", "mymaster", "a b", "-x", "x/y", "ok_name.1"]),
)
urls = st.sampled_from(
    ["redis://r:6379/0", "rediss://r:6380/1", "unix:///tmp/redis.sock", "http://r", "r:6379", "redis://"]
)


@st.composite
def redis_sections(draw: Any) -> dict[str, Any]:
    section: dict[str, Any] = {}
    mode = draw(st.sampled_from(["absent", "standalone", "sentinel", "cluster", "Cluster", "bogus"]))
    if mode != "absent":
        section["mode"] = mode
    if draw(st.booleans()):
        section["url"] = draw(urls)
    if draw(st.booleans()):
        section["sentinels"] = draw(st.lists(addresses, min_size=0, max_size=12))
    if draw(st.booleans()):
        section["master_name"] = draw(master_names)
    if draw(st.booleans()):
        section["db"] = draw(st.integers(-3, 20))
    if draw(st.booleans()):
        section["sentinel_tls"] = draw(st.sampled_from([True, False, "maybe", [1]]))
    return section


def worker_config(redis: dict[str, Any]) -> dict[str, Any]:
    return {
        "role": "worker", "host": "10.0.0.7", "transports": {"jobs": "rabbitmq", "control": "grpc"},
        "rabbitmq": {"url": "amqp://rabbit.internal:5672/"}, "redis": redis,
        "grpc": {"bind": "0.0.0.0:50051", "advertise_host": "10.0.0.7", "advertise_port": 50051},
        "llm": {"provider": "ollama", "base_url": "http://ollama:11434", "model": "m"},
        "worker": {"capabilities": ["research"]},
    }


@settings(max_examples=400, deadline=None)
@given(redis_sections())
def test_validation_accepts_exactly_the_configurations_of_requirement_1_2(redis: dict[str, Any]) -> None:
    try:
        validate_config(worker_config(redis))
        accepted, fields = True, ()
    except ConfigError as error:
        accepted, fields = False, error.fields
    assert accepted == expected_valid(redis), (redis, fields)
    if not accepted:
        assert any(f == "redis" or f.startswith("redis.") for f in fields), (redis, fields)

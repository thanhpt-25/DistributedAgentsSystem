"""Property 4: ``http_fetch`` refuses every private, loopback, link-local and reserved address however it is
written, and never sends a request to a host that is not allowed (das-smarter-agents task 5.4; Phase 6 Req 8.4, 5.9).

The oracle is independent of ``das.tools.http_fetch``: an explicit list of blocked ranges as integers, and the
embedded IPv4 address of the IPv6 forms taken by bit arithmetic, not by the ``ipaddress`` flags the implementation
uses."""

from __future__ import annotations

import asyncio
import ipaddress
from collections.abc import Sequence

import httpx
from hypothesis import given, settings
from hypothesis import strategies as st

from das.tools.http_fetch import HttpFetch, is_public
from das.tools.types import ToolError

# (first address, prefix length) of the ranges that must never be fetched.
BLOCKED_V4 = [
    ("0.0.0.0", 8), ("10.0.0.0", 8), ("100.64.0.0", 10), ("127.0.0.0", 8), ("169.254.0.0", 16), ("172.16.0.0", 12),
    ("192.168.0.0", 16), ("198.18.0.0", 15), ("192.0.2.0", 24), ("198.51.100.0", 24), ("203.0.113.0", 24),
    ("224.0.0.0", 4), ("240.0.0.0", 4),
]
# Ranges the oracle makes no claim about: some of it is global in some standards.
UNDECIDED_V4 = [("192.0.0.0", 24), ("192.88.99.0", 24)]
BLOCKED_V6 = [
    ("::", 128), ("::1", 128), ("fe80::", 10), ("fec0::", 10), ("fc00::", 7), ("ff00::", 8), ("2001:db8::", 32),
]


def span(first: str, prefix: int, bits: int) -> tuple[int, int]:
    start = int(ipaddress.ip_address(first))
    return start, start + (1 << (bits - prefix)) - 1


V4_RANGES = [span(a, p, 32) for a, p in BLOCKED_V4]
V4_UNDECIDED = [span(a, p, 32) for a, p in UNDECIDED_V4]
V6_RANGES = [span(a, p, 128) for a, p in BLOCKED_V6]


def v4_blocked(n: int) -> bool:
    return any(lo <= n <= hi for lo, hi in V4_RANGES)


def v6_blocked(n: int) -> bool:
    if any(lo <= n <= hi for lo, hi in V6_RANGES):
        return True
    if (n >> 32) == 0xFFFF:  # ::ffff:a.b.c.d
        return v4_blocked(n & 0xFFFFFFFF)
    if (n >> 32) == 0x0064FF9B << 64:  # 64:ff9b::/96
        return v4_blocked(n & 0xFFFFFFFF)
    if (n >> 112) == 0x2002:  # 6to4: the IPv4 address is the 32 bits after the prefix
        return v4_blocked((n >> 80) & 0xFFFFFFFF)
    if (n >> 96) == 0x20010000:  # Teredo: the server, and the client in inverted bits
        return v4_blocked((n >> 64) & 0xFFFFFFFF) or v4_blocked(~n & 0xFFFFFFFF)
    return False


v4_numbers = st.integers(0, 2**32 - 1)
v4_blocked_numbers = st.sampled_from(V4_RANGES).flatmap(lambda r: st.integers(r[0], r[1]))
v6_blocked_numbers = st.sampled_from(V6_RANGES).flatmap(lambda r: st.integers(r[0], min(r[1], r[0] + 2**64)))
low32 = st.integers(0, 2**32 - 1)
wrapped_v4 = st.one_of(
    low32.map(lambda v: (0xFFFF << 32) | v),  # IPv4-mapped
    low32.map(lambda v: (0x0064FF9B << 96) | v),  # NAT64
    st.tuples(low32, st.integers(0, 2**80 - 1)).map(lambda t: (0x2002 << 112) | (t[0] << 80) | t[1]),  # 6to4
    st.tuples(low32, low32, st.integers(0, 0xFFFF)).map(
        lambda t: (0x20010000 << 96) | (t[0] << 64) | (t[2] << 48) | (t[1] ^ 0xFFFFFFFF)
    ),  # Teredo
)


def text(n: int, version: int) -> str:
    return str(ipaddress.IPv4Address(n) if version == 4 else ipaddress.IPv6Address(n))


@settings(max_examples=300)
@given(st.one_of(v4_blocked_numbers.map(lambda n: text(n, 4)), v6_blocked_numbers.map(lambda n: text(n, 6))))
def test_every_address_in_a_blocked_range_is_refused(address: str) -> None:
    assert not is_public(address)


@settings(max_examples=300)
@given(wrapped_v4)
def test_an_ipv4_address_wrapped_in_ipv6_is_judged_by_the_ipv4_address(n: int) -> None:
    if v6_blocked(n):
        assert not is_public(text(n, 6))


@settings(max_examples=300)
@given(v4_numbers)
def test_an_ipv4_address_outside_every_blocked_range_is_public(n: int) -> None:
    if not v4_blocked(n) and not any(lo <= n <= hi for lo, hi in V4_UNDECIDED):
        assert is_public(text(n, 4))


@settings(max_examples=300)
@given(st.integers(0, 2**128 - 1))
def test_an_ipv6_address_the_oracle_blocks_is_never_public(n: int) -> None:
    if v6_blocked(n):
        assert not is_public(text(n, 6))


@settings(max_examples=200)
@given(st.text(max_size=40))
def test_text_that_is_not_an_address_is_never_public(candidate: str) -> None:
    try:
        ipaddress.ip_address(candidate.split("%", 1)[0])
    except ValueError:
        assert not is_public(candidate)


# --------------------------------------------------------------------------- end to end: no request is ever sent

PUBLIC = "93.184.216.34"
hosts = st.from_regex(r"[a-z]{1,10}(\.[a-z]{1,10}){0,3}", fullmatch=True)


def run_fetch(allowed: str, url: str, addresses: Sequence[str]) -> tuple[list[httpx.Request], list[str], bool]:
    requests: list[httpx.Request] = []
    looked_up: list[str] = []

    async def resolve(host: str) -> Sequence[str]:
        looked_up.append(host)
        return addresses

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, text="ok")

    fetch = HttpFetch((allowed,), resolver=resolve, transport=httpx.MockTransport(handler))
    try:
        asyncio.run(fetch.fetch(url))
        refused = False
    except ToolError:
        refused = True
    return requests, looked_up, refused


@settings(max_examples=200, deadline=None)
@given(hosts, hosts, st.sampled_from(["", "/", "/a/b", "?q=1", "/x?y=z#frag"]))
def test_a_host_that_is_not_allowed_gets_no_lookup_and_no_request(allowed: str, other: str, tail: str) -> None:
    if other == allowed:
        return
    requests, looked_up, refused = run_fetch(allowed, f"https://{other}{tail}", [PUBLIC])
    assert refused and requests == [] and looked_up == []


@settings(max_examples=200, deadline=None)
@given(
    hosts,
    st.lists(
        st.one_of(v4_numbers.map(lambda n: text(n, 4)), v6_blocked_numbers.map(lambda n: text(n, 6))),
        max_size=3,
    ),
    v4_blocked_numbers,
)
def test_a_host_with_a_blocked_address_among_its_answers_gets_no_request(
    host: str, addresses: list[str], blocked: int
) -> None:
    requests, _, refused = run_fetch(host, f"https://{host}/", [*addresses, text(blocked, 4)])
    assert refused and requests == []


@settings(max_examples=100, deadline=None)
@given(hosts)
def test_an_allowed_host_with_only_public_addresses_is_fetched_from_the_checked_address(host: str) -> None:
    requests, looked_up, refused = run_fetch(host, f"https://{host}/", [PUBLIC])
    assert not refused and looked_up == [host] and [r.url.host for r in requests] == [PUBLIC]

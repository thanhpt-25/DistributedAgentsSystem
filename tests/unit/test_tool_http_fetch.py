"""The http_fetch tool (das-smarter-agents task 5.4; Phase 6 Req 5.9): GET only, https only, the allowed hosts only, no
redirects, a timeout and a size cap, and never to a private, loopback, link-local or reserved address. No network: an
``httpx.MockTransport`` and a fake resolver stand in for the internet and for DNS."""

from __future__ import annotations

from collections.abc import Sequence

import httpx
import pytest

from das.tools.http_fetch import HttpFetch, is_public
from das.tools.types import ToolError

PUBLIC = "93.184.216.34"
PUBLIC_V6 = "2606:4700:4700::1111"


class Net:
    """A fake internet: records every request and answers with a scripted response."""

    def __init__(self, response: httpx.Response | Exception | None = None) -> None:
        self.requests: list[httpx.Request] = []
        self.response = response if response is not None else httpx.Response(200, text="hello")

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def resolver(*addresses: str):  # noqa: ANN201
    async def resolve(host: str) -> Sequence[str]:
        return addresses

    return resolve


def tool(net: Net, *addresses: str, hosts: tuple[str, ...] = ("example.com",), **kw: object) -> HttpFetch:
    return HttpFetch(
        hosts, resolver=resolver(*(addresses or (PUBLIC,))), transport=httpx.MockTransport(net), **kw  # type: ignore[arg-type]
    )


# --------------------------------------------------------------------------- a fetch that is allowed


async def test_an_allowed_https_url_is_fetched_and_the_status_and_body_come_back() -> None:
    net = Net(httpx.Response(200, text="hello world"))
    assert await tool(net).fetch("https://example.com/page?q=1") == "HTTP 200\nhello world"
    [request] = net.requests
    assert request.method == "GET" and request.url.path == "/page" and request.url.query == b"q=1"


async def test_the_request_goes_to_the_checked_address_with_the_original_host_and_tls_name() -> None:
    """Connecting to the address that was checked, not resolving again, is what stops DNS rebinding."""
    net = Net()
    await tool(net).fetch("https://example.com/")
    [request] = net.requests
    assert request.url.host == PUBLIC  # the address, not the name
    assert request.headers["Host"] == "example.com"
    assert request.extensions["sni_hostname"] == "example.com"  # the certificate is checked against the name


async def test_an_ipv6_address_is_put_in_brackets() -> None:
    net = Net()
    await tool(net, PUBLIC_V6).fetch("https://example.com/")
    assert net.requests[0].url.host == PUBLIC_V6 and "[" in str(net.requests[0].url)


async def test_the_request_carries_no_credentials_or_cookies() -> None:
    net = Net()
    await tool(net).fetch("https://example.com/")
    names = {name.lower() for name in net.requests[0].headers}
    assert names <= {"host", "user-agent", "accept", "accept-encoding", "connection"}
    assert "authorization" not in names and "cookie" not in names


async def test_the_host_is_matched_ignoring_case_and_a_trailing_dot() -> None:
    for url in ("https://EXAMPLE.com/", "https://example.com./", "https://Example.COM:443/"):
        assert (await tool(Net()).fetch(url)).startswith("HTTP 200")


async def test_a_url_with_no_path_fetches_the_root() -> None:
    net = Net()
    await tool(net).fetch("https://example.com")
    assert net.requests[0].url.path == "/"


async def test_the_fragment_is_not_sent() -> None:
    net = Net()
    await tool(net).fetch("https://example.com/a#section")
    assert b"section" not in net.requests[0].url.raw_path


# --------------------------------------------------------------------------- the scheme and the port (Req 5.9)


@pytest.mark.parametrize(
    "url",
    ["http://example.com/", "ftp://example.com/", "file:///etc/passwd", "javascript:1", "gopher://example.com/",
     "//example.com/", "example.com"],
)
async def test_only_https_is_allowed_by_default(url: str) -> None:
    net = Net()
    with pytest.raises(ToolError):
        await tool(net).fetch(url)
    assert net.requests == []


async def test_http_is_allowed_when_configured_and_uses_port_80() -> None:
    net = Net()
    assert (await tool(net, allow_http=True).fetch("http://example.com/")).startswith("HTTP 200")
    assert net.requests[0].url.scheme == "http" and net.requests[0].url.port in (None, 80)


@pytest.mark.parametrize("url", ["https://example.com:8443/", "https://example.com:80/", "https://example.com:1/"])
async def test_only_the_default_port_is_allowed(url: str) -> None:
    net = Net()
    with pytest.raises(ToolError, match="port"):
        await tool(net).fetch(url)
    assert net.requests == []


# --------------------------------------------------------------------------- the allow-list (Req 5.9)


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.com/",
        "https://example.com.evil.com/",
        "https://evilexample.com/",
        "https://sub.example.com/",  # exact hosts only: a subdomain is another host
        "https://example.com@evil.com/",
        "https://user:pass@example.com/",
        "https://evil.com#@example.com/",
        "https://evil.com\\@example.com/",
        "https://evil.com/?@example.com",
        "https://2130706433/",
        "https://0x7f000001/",
        "https://127.1/",
        "https://[::1]/",
        "https://localhost/",
    ],
)
async def test_a_url_that_is_not_for_an_allowed_host_is_refused_without_any_lookup(url: str) -> None:
    looked_up: list[str] = []

    async def resolve(host: str) -> Sequence[str]:
        looked_up.append(host)
        return [PUBLIC]

    net = Net()
    fetch = HttpFetch(("example.com",), resolver=resolve, transport=httpx.MockTransport(net))
    with pytest.raises(ToolError):
        await fetch.fetch(url)
    assert net.requests == [] and looked_up == []  # not even DNS was asked about a host that is not allowed


async def test_credentials_in_the_url_are_refused_even_for_an_allowed_host() -> None:
    with pytest.raises(ToolError, match="credentials"):
        await tool(Net()).fetch("https://user:pass@example.com/")


# --------------------------------------------------------------------------- where the host resolves (Req 5.9)


UNSAFE = [
    "127.0.0.1", "127.255.255.254", "10.0.0.1", "172.16.5.4", "172.31.255.255", "192.168.1.1", "169.254.169.254",
    "0.0.0.0", "100.64.0.1", "192.0.0.1", "198.18.0.1", "224.0.0.1", "240.0.0.1", "255.255.255.255",
    "::1", "::", "fe80::1", "fc00::1", "fd12:3456::1", "ff02::1", "2001:db8::1",
    "::ffff:127.0.0.1", "::ffff:10.0.0.1", "::ffff:169.254.169.254", "64:ff9b::7f00:1",
    "2002:7f00:1::", "2002:a00:1::", "2001:0:4136:e378:8000:63bf:80ff:fffe",
]


@pytest.mark.parametrize("address", UNSAFE)
async def test_a_host_that_resolves_to_an_unsafe_address_is_refused_before_any_request(address: str) -> None:
    net = Net()
    with pytest.raises(ToolError, match="not allowed"):
        await tool(net, address).fetch("https://example.com/")
    assert net.requests == []


async def test_one_unsafe_address_among_safe_ones_refuses_the_whole_host() -> None:
    net = Net()
    with pytest.raises(ToolError, match="not allowed"):
        await tool(net, PUBLIC, "10.0.0.5").fetch("https://example.com/")
    assert net.requests == []


async def test_a_host_that_does_not_resolve_is_refused() -> None:
    net = Net()
    fetch = HttpFetch(("example.com",), resolver=resolver(), transport=httpx.MockTransport(net))
    with pytest.raises(ToolError, match="resolve"):
        await fetch.fetch("https://example.com/")
    assert net.requests == []


async def test_a_resolver_that_fails_is_a_tool_error_not_an_exception() -> None:
    async def broken(host: str) -> Sequence[str]:
        raise OSError("no dns")

    fetch = HttpFetch(("example.com",), resolver=broken, transport=httpx.MockTransport(Net()))
    with pytest.raises(ToolError, match="resolve"):
        await fetch.fetch("https://example.com/")


@pytest.mark.parametrize(
    "address", ["93.184.216.34", "8.8.8.8", "1.1.1.1", "2606:4700:4700::1111", "2001:4860:4860::8888"]
)
def test_public_addresses_are_public(address: str) -> None:
    assert is_public(address)


@pytest.mark.parametrize("address", [*UNSAFE, "not an address", "", "1.2.3", "300.1.1.1", "fe80::1%eth0"])
def test_unsafe_or_unreadable_addresses_are_not_public(address: str) -> None:
    assert not is_public(address)


# --------------------------------------------------------------------------- the response (Req 5.9)


async def test_a_redirect_is_not_followed_and_is_reported() -> None:
    net = Net(httpx.Response(302, headers={"Location": "http://169.254.169.254/latest/meta-data/"}, text="moved"))
    result = await tool(net).fetch("https://example.com/")
    assert result.startswith("HTTP 302") and "redirect not followed" in result
    assert len(net.requests) == 1  # it did not go to the Location


async def test_an_error_status_is_a_result_not_an_error() -> None:
    result = await tool(Net(httpx.Response(404, text="nothing here"))).fetch("https://example.com/missing")
    assert result == "HTTP 404\nnothing here"


async def test_a_body_over_the_size_cap_is_cut_and_says_so() -> None:
    net = Net(httpx.Response(200, content=b"x" * 5000))
    result = await tool(net, max_bytes=1024).fetch("https://example.com/")
    assert result.startswith("HTTP 200\n") and "truncated" in result
    assert result.count("x") == 1024  # exactly the cap, no more


async def test_a_body_at_the_cap_is_not_marked_truncated() -> None:
    result = await tool(Net(httpx.Response(200, content=b"x" * 1024)), max_bytes=1024).fetch("https://example.com/")
    assert "truncated" not in result and result.count("x") == 1024


async def test_a_body_that_is_not_utf8_is_decoded_with_replacement() -> None:
    result = await tool(Net(httpx.Response(200, content=b"caf\xe9 \xff\xfe ok"))).fetch("https://example.com/")
    assert result.startswith("HTTP 200\n") and "caf" in result and "ok" in result


async def test_the_declared_charset_is_honoured() -> None:
    response = httpx.Response(
        200, content="café".encode("latin-1"), headers={"Content-Type": "text/plain; charset=latin-1"}
    )
    assert (await tool(Net(response)).fetch("https://example.com/")).endswith("café")


async def test_a_timeout_is_a_tool_error() -> None:
    net = Net(httpx.ReadTimeout("slow", request=httpx.Request("GET", "https://x")))
    with pytest.raises(ToolError, match="timed out"):
        await tool(net).fetch("https://example.com/")


async def test_a_connection_failure_is_a_tool_error_that_does_not_leak_internals() -> None:
    net = Net(httpx.ConnectError("secret internal detail 10.1.2.3", request=httpx.Request("GET", "https://x")))
    with pytest.raises(ToolError) as info:
        await tool(net).fetch("https://example.com/")
    assert "10.1.2.3" not in str(info.value) and "secret" not in str(info.value)


# --------------------------------------------------------------------------- bad arguments


@pytest.mark.parametrize("bad", [None, 5, [], {}, "", "   ", "https://", "https:///path", "http://[::1", "x" * 3000])
async def test_a_url_that_is_not_a_usable_url_is_a_tool_error(bad: object) -> None:
    with pytest.raises(ToolError):
        await tool(Net()).fetch(bad)


async def test_the_tool_with_no_hosts_refuses_everything() -> None:
    fetch = HttpFetch((), resolver=resolver(PUBLIC), transport=httpx.MockTransport(Net()))
    with pytest.raises(ToolError):
        await fetch.fetch("https://example.com/")


async def test_a_url_over_the_length_limit_is_refused_even_for_an_allowed_host() -> None:
    with pytest.raises(ToolError, match="too long"):
        await tool(Net()).fetch("https://example.com/?" + "a" * 2100)


async def test_the_allowed_hosts_are_normalised_when_the_tool_is_built() -> None:
    fetch = HttpFetch((" EXAMPLE.com. ",), resolver=resolver(PUBLIC), transport=httpx.MockTransport(Net()))
    assert (await fetch.fetch("https://example.com/")).startswith("HTTP 200")


@pytest.mark.parametrize(
    "address", ["::ffff:8.8.8.8", "64:ff9b::808:808", "2002:808:808::", "2001:0:808:808:0:0:f7f7:f7f7"]
)
def test_an_ipv4_address_inside_an_ipv6_one_is_public_when_the_ipv4_address_is(address: str) -> None:
    assert is_public(address)


async def test_the_message_of_a_refusal_is_short() -> None:
    with pytest.raises(ToolError) as info:
        await tool(Net()).fetch("https://" + "a" * 500 + ".com/")
    assert len(str(info.value)) < 200

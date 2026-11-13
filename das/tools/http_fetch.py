"""The http_fetch tool: fetch a page for the model, and nothing else (Phase 6 Req 5.9).

Every rule is a refusal the model can read as a :class:`ToolError`:

* ``GET`` only, ``https`` only (``http`` only when ``allow_http`` is set), the default port only, no credentials in the
  URL, no redirect followed, a timeout, and a cap on the bytes read.
* The URL's host must be one of ``allowed_hosts``, compared exactly (a subdomain is another host). A host that is not
  allowed is refused before DNS is asked about it.
* The host is resolved by the tool, and every address it resolves to must be public (:func:`is_public`): no private,
  loopback, link-local, reserved, multicast or unspecified address, however it is written, including an IPv4 address
  wrapped in an IPv6 one (IPv4-mapped, 6to4, Teredo, NAT64).
* The request then goes to the address that was checked, with the original ``Host`` header and the original name for TLS
  (``sni_hostname``), so DNS answering differently a second time (rebinding) changes nothing.

The request carries no credentials or cookies, and a failure is reported without the transport's own message, which can
name internal addresses.
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import Final
from urllib.parse import urlsplit

import httpx

from das.tools.types import ToolError

MAX_URL_LENGTH: Final = 2048
USER_AGENT: Final = "das-http-fetch/1"
_MAX_SHOWN: Final = 60
_MAX_LOCATION: Final = 200
_DEFAULT_PORTS: Final = {"https": 443, "http": 80}
_REDIRECTS: Final = frozenset({301, 302, 303, 307, 308})
_NAT64: Final = ipaddress.ip_network("64:ff9b::/96")

#: Resolves a host name to the addresses it points at.
Resolver = Callable[[str], Awaitable[Sequence[str]]]


async def system_resolver(host: str) -> list[str]:
    """The addresses the system resolver gives for ``host`` (an IP literal resolves to itself)."""
    infos = await asyncio.get_running_loop().getaddrinfo(host, None, type=socket.SOCK_STREAM)
    return sorted({str(info[4][0]) for info in infos})


def _public(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if isinstance(ip, ipaddress.IPv6Address):
        # An IPv4 address carried inside an IPv6 one is as public as the IPv4 address is.
        if ip.ipv4_mapped is not None:  # CPython >= 3.12.4 unwraps this itself; older 3.12 patch levels do not
            return _public(ip.ipv4_mapped)
        if ip.sixtofour is not None:
            return _public(ip.sixtofour)
        if ip.teredo is not None:
            server, client = ip.teredo
            return _public(server) and _public(client)
        if ip in _NAT64:
            return _public(ipaddress.IPv4Address(int(ip) & 0xFFFFFFFF))
        if ip.is_site_local:
            return False
    return ip.is_global and not (
        ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast or ip.is_unspecified
    )


def is_public(address: str) -> bool:
    """Whether ``address`` is a public, routable address. Anything that is not an IP address is not."""
    try:
        ip = ipaddress.ip_address(address.split("%", 1)[0])  # an IPv6 zone id ("fe80::1%eth0") is not part of it
    except ValueError:
        return False
    return _public(ip)


def _shown(text: str) -> str:
    return repr(text if len(text) <= _MAX_SHOWN else text[:_MAX_SHOWN] + "...")


class HttpFetch:
    """One configured http_fetch tool. :meth:`fetch` is the tool call."""

    def __init__(
        self,
        allowed_hosts: Iterable[str],
        *,
        allow_http: bool = False,
        timeout: float = 10.0,
        max_bytes: int = 1_048_576,
        resolver: Resolver | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._hosts = frozenset(h.strip().lower().rstrip(".") for h in allowed_hosts)
        self._allow_http = allow_http
        self._timeout = timeout
        self._max_bytes = max_bytes
        self._resolver: Resolver = resolver if resolver is not None else system_resolver
        self._transport = transport

    # ------------------------------------------------------------------ the URL

    def _parse(self, url: object) -> tuple[str, str, str]:
        """``(scheme, host, path and query)`` of an allowed URL, or a refusal."""
        if not isinstance(url, str) or not url.strip():
            raise ToolError("url must be a non-empty string")
        if len(url) > MAX_URL_LENGTH:
            raise ToolError(f"the url is too long (at most {MAX_URL_LENGTH} characters)")
        try:
            parts = urlsplit(url.strip())
            host, port = parts.hostname, parts.port
        except ValueError:
            raise ToolError("the url is not valid") from None
        scheme = parts.scheme.lower()
        if scheme != "https" and not (scheme == "http" and self._allow_http):
            allowed = "http and https" if self._allow_http else "https"
            raise ToolError(f"only {allowed} urls are allowed")
        if parts.username is not None or parts.password is not None or "@" in parts.netloc:
            raise ToolError("credentials in the url are not allowed")
        if not host:
            raise ToolError("the url has no host")
        host = host.lower().rstrip(".")
        if port not in (None, _DEFAULT_PORTS[scheme]):
            raise ToolError("only the default port is allowed")
        if host not in self._hosts:
            raise ToolError(f"host {_shown(host)} is not allowed")
        target = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
        return scheme, host, target

    # ------------------------------------------------------------------ the address

    async def _addresses(self, host: str) -> Sequence[str]:
        try:
            addresses = list(await self._resolver(host))
        except (OSError, UnicodeError):
            raise ToolError("cannot resolve the host") from None
        if not addresses:
            raise ToolError("cannot resolve the host")
        if not all(is_public(address) for address in addresses):
            raise ToolError("the host resolves to an address that is not allowed")
        return addresses

    # ------------------------------------------------------------------ the request

    async def _read(self, response: httpx.Response) -> tuple[bytes, bool]:
        chunks: list[bytes] = []
        size = 0
        async for chunk in response.aiter_bytes():
            room = self._max_bytes - size
            if len(chunk) > room:
                chunks.append(chunk[:room])
                return b"".join(chunks), True  # more was there than the cap allows
            chunks.append(chunk)
            size += len(chunk)
        return b"".join(chunks), False

    async def fetch(self, url: object) -> str:
        """GET ``url`` and return ``HTTP <status>`` and the body as text, or raise :class:`ToolError`."""
        scheme, host, target = self._parse(url)
        address = (await self._addresses(host))[0]
        ip = ipaddress.ip_address(address.split("%", 1)[0])
        literal = f"[{ip}]" if ip.version == 6 else str(ip)
        headers = {"Host": host, "User-Agent": USER_AGENT, "Accept": "*/*"}
        extensions = {"sni_hostname": host} if scheme == "https" else {}
        try:
            async with httpx.AsyncClient(
                transport=self._transport, timeout=httpx.Timeout(self._timeout), follow_redirects=False
            ) as client:
                async with client.stream(
                    "GET", f"{scheme}://{literal}{target}", headers=headers, extensions=extensions
                ) as response:
                    body, truncated = await self._read(response)
                    status, location = response.status_code, response.headers.get("location")
                    encoding = response.charset_encoding or "utf-8"
        except httpx.TimeoutException:
            raise ToolError("the request timed out") from None
        except httpx.HTTPError:
            raise ToolError("the request failed") from None
        try:
            text = body.decode(encoding, errors="replace")
        except LookupError:  # a charset Python does not know
            text = body.decode("utf-8", errors="replace")
        head = f"HTTP {status}"
        if status in _REDIRECTS:
            head += f" (redirect not followed; location: {(location or '')[:_MAX_LOCATION]})"
        note = f"\n[truncated at {self._max_bytes} bytes]" if truncated else ""
        return f"{head}\n{text}{note}"


__all__ = ["MAX_URL_LENGTH", "HttpFetch", "Resolver", "is_public", "system_resolver"]

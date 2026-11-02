"""HMAC key of the Page_Cursor (design D10, Req 19.9).

The key comes from ``gateway.cursor_secret`` when configured. Otherwise every
Gateway replica shares one random 32-byte key stored at ``das:gateway:cursor_key``:
the first replica creates it with ``SET NX`` and every replica reads the stored
value back, so a cursor issued by one replica is accepted by any other.

Used by ``GET /v1/admin/dlq`` and ``GET /v1/requests``.
"""

from __future__ import annotations

import secrets
from collections.abc import Callable
from typing import Any, Final

from pydantic import SecretStr

from das.state import keys

#: Length in bytes of the generated key.
GENERATED_KEY_BYTES: Final = 32


class CursorKeyProvider:
    """Resolve (and cache) the Page_Cursor HMAC key.

    ``client`` returns the Redis client; it is only called when no secret is
    configured and the key has not been loaded yet.
    """

    def __init__(
        self, client: Callable[[], Any], secret: SecretStr | str | None = None
    ) -> None:
        self._client = client
        self._key: bytes | None = None
        if secret is not None:
            raw = secret.get_secret_value() if isinstance(secret, SecretStr) else secret
            if raw:
                self._key = raw.encode("utf-8")

    async def get(self) -> bytes:
        """The key; reads or creates ``das:gateway:cursor_key`` on first use.

        Redis errors propagate (the caller maps them to 503); nothing is cached then.
        """
        if self._key is not None:
            return self._key
        client = self._client()
        candidate = secrets.token_hex(GENERATED_KEY_BYTES)
        await client.set(keys.GATEWAY_CURSOR_KEY, candidate, nx=True)
        stored = await client.get(keys.GATEWAY_CURSOR_KEY)
        if stored is None:
            raise RuntimeError("cursor key missing after SET NX")
        text = stored.decode("ascii") if isinstance(stored, bytes) else str(stored)
        self._key = text.encode("ascii")
        return self._key


__all__ = ["GENERATED_KEY_BYTES", "CursorKeyProvider"]

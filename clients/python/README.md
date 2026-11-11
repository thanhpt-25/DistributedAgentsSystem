# das-client

Python client for the DAS Gateway API (`POST /v1/requests`, status, cancel, events, webhooks). Python 3.10 to 3.13; one
runtime dependency (`httpx`).

The client is under construction (`.kiro/specs/das-client-api`, task group 9).

## Develop

The package is **not** a uv workspace member: the server is Python 3.12 only, and a member would pin the client to the
same range. The server repo installs it as an editable path source in its dev group, so `uv sync` in the repo root
makes `das_client` importable for the contract test.

Run the client's own tests in an isolated environment, on any supported Python:

```bash
uv run --isolated --no-project --python 3.10 \
  --with-editable clients/python \
  --with pytest==9.1.1 --with pytest-asyncio==1.4.0 --with respx==0.23.1 \
  pytest clients/python/tests -c clients/python/pyproject.toml --rootdir clients/python
```

Replace `3.10` with `3.12` or `3.13` to check the others.

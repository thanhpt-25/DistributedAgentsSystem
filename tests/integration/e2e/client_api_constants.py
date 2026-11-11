"""Names and secrets shared by the client API e2e stack's files and tests (das-client-api task 11.1).

The Compose override, the e2e Gateway config, the webhook sink and the tests all have to agree on these, so
they live in one place and a unit test (``tests/unit/test_e2e_client_api_static.py``) checks each file against
them. Everything here is test-only: the keys and the secret are placeholders for a throw-away stack.
"""

from __future__ import annotations

PROJECT = "das-e2e-client"

#: The key the tests use for everything except the rate limit test; it is not throttled and its Requests
#: deliver their webhooks to the sink.
MAIN_KEY = "e2e-api-key-0123456789"
#: A second key with a deliberately low rate (``rate 1.0``, ``burst 5``), used only by the shared-bucket test.
LIMITED_KEY = "e2e-limited-key-0123456789"
#: ``key_fingerprint(LIMITED_KEY)``: the first 8 hex characters of its sha256. Checked by a unit test.
LIMITED_FINGERPRINT = "f22c46dc"

WEBHOOK_SECRET_NAME = "e2e"
WEBHOOK_SECRET = "e2e-webhook-secret-0123456789"

#: Environment variables (in the env file the fixture writes) that set the host ports of the second Gateway and of
#: the sink. The first Gateway keeps the ``DAS_GATEWAY_PORT`` variable of ``deploy/docker-compose.yml``.
GATEWAY_B_PORT_VAR = "DAS_GATEWAY_B_PORT"
SINK_HOST_PORT_VAR = "DAS_E2E_SINK_PORT"

#: The sink's Compose service name and the port it listens on inside the stack.
SINK_SERVICE = "webhook-sink"
SINK_PORT = 8099
SINK_URL = f"http://{SINK_SERVICE}:{SINK_PORT}/hook"

"""Names, keys and limits shared by the tenancy e2e stack's files and tests (das-multi-tenant task 12.1).

The Compose override, the e2e Gateway config and the tests all have to agree on these, so they live in one place and a
unit test (``tests/unit/test_e2e_tenancy_static.py``) checks each file against them. Everything here is test-only: the
keys are placeholders for a throw-away stack. The keys avoid the words ``tenant`` and ``operator``: the log redactor
masks any 8-character window of a key, and those words are field names in the Audit_Entry.
"""

from __future__ import annotations

PROJECT = "das-e2e-tenant"

#: team-a: three keys with different Scopes, a quota of two unfinished Requests for the whole tenant, and it may ask
#: for ``high`` priority.
TEAM_A_ADMIN = "e2e-team-a-admin-0123456789"  # submit, read, admin
TEAM_A_READER = "e2e-team-a-reader-0123456789"  # read
TEAM_A_WRITER = "e2e-team-a-writer-0123456789"  # submit
#: team-b: one key with every Scope, no tenant quota, and ``normal`` as its highest priority.
TEAM_B_ADMIN = "e2e-team-b-admin-0123456789"
#: A key no tenant names: the Default_Tenant, with the Scopes submit and read.
PLAIN_KEY = "e2e-plain-key-0123456789"
#: The Admin_API_Key: the Operator.
ROOT_KEY = "e2e-root-admin-0123456789"

#: ``key_fingerprint(key)``: the first 8 hex characters of its sha256. Checked by a unit test.
FINGERPRINTS = {
    TEAM_A_ADMIN: "7dee5a42",
    TEAM_A_READER: "4c6a9aa2",
    TEAM_A_WRITER: "7604eafb",
    TEAM_B_ADMIN: "db62a72f",
}

#: Keys the Gateway accepts as API keys (``DAS__API_KEYS``); the Operator key is separate (``DAS__ADMIN_API_KEYS``).
API_KEYS = (TEAM_A_ADMIN, TEAM_A_READER, TEAM_A_WRITER, TEAM_B_ADMIN, PLAIN_KEY)

#: The Tenant_Quota of team-a (unfinished Requests of the whole tenant).
TEAM_A_QUOTA = 2

#: Environment variable (in the env file the fixture writes) for the host port of the second Gateway. The first one
#: keeps the ``DAS_GATEWAY_PORT`` variable of ``deploy/docker-compose.yml``.
GATEWAY_B_PORT_VAR = "DAS_GATEWAY_B_PORT"

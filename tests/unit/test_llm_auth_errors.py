"""Authentication and authorization failures are their own kind of permanent error (das-smarter-agents task 2.1;
Req 2.4): a fallback model cannot fix a bad key, so the provider must be able to tell them apart."""

from __future__ import annotations

import anthropic
import botocore.exceptions as bexc
import httpx
import openai
import pytest

from das.llm.adapters._common import error_for_status
from das.llm.adapters.anthropic import classify_anthropic_error
from das.llm.adapters.bedrock import classify_bedrock_error
from das.llm.adapters.ollama import classify_ollama_error
from das.llm.adapters.openai import classify_openai_error
from das.llm.errors import AuthLLMError, LLMCallFailed, PermanentLLMError, RateLimitedLLMError, TransientLLMError


def response(status: int) -> httpx.Response:
    return httpx.Response(status, request=httpx.Request("POST", "http://provider.invalid/v1"), json={"error": "x"})


def client_error(code: str, status: int) -> bexc.ClientError:
    return bexc.ClientError(
        {"Error": {"Code": code, "Message": "nope"}, "ResponseMetadata": {"HTTPStatusCode": status}}, "Converse"
    )


# --------------------------------------------------------------------------- the type


def test_an_auth_error_is_a_permanent_error() -> None:
    assert issubclass(AuthLLMError, PermanentLLMError)
    assert not issubclass(AuthLLMError, TransientLLMError)


# --------------------------------------------------------------------------- shared status mapping


@pytest.mark.parametrize("status", [401, 403])
def test_401_and_403_are_auth_errors(status: int) -> None:
    assert isinstance(error_for_status("p", status, "denied"), AuthLLMError)


@pytest.mark.parametrize("status", [400, 402, 404, 409, 413, 422])
def test_other_client_errors_are_permanent_but_not_auth(status: int) -> None:
    err = error_for_status("p", status, "bad")
    assert isinstance(err, PermanentLLMError) and not isinstance(err, AuthLLMError)


def test_rate_limits_and_server_errors_keep_their_kinds() -> None:
    assert isinstance(error_for_status("p", 429, ""), RateLimitedLLMError)
    assert type(error_for_status("p", 503, "")) is TransientLLMError
    assert type(error_for_status("p", 408, "")) is TransientLLMError


# --------------------------------------------------------------------------- each adapter


def test_ollama_401_is_an_auth_error() -> None:
    exc = httpx.HTTPStatusError("denied", request=httpx.Request("POST", "http://o"), response=response(401))
    assert isinstance(classify_ollama_error(exc), AuthLLMError)


def test_openai_authentication_and_permission_errors_are_auth_errors() -> None:
    for exc in (
        openai.AuthenticationError("bad key", response=response(401), body=None),
        openai.PermissionDeniedError("no access", response=response(403), body=None),
    ):
        assert isinstance(classify_openai_error(exc), AuthLLMError)
    assert not isinstance(
        classify_openai_error(openai.NotFoundError("no model", response=response(404), body=None)), AuthLLMError
    )


def test_anthropic_authentication_and_permission_errors_are_auth_errors() -> None:
    for exc in (
        anthropic.AuthenticationError("bad key", response=response(401), body=None),
        anthropic.PermissionDeniedError("no access", response=response(403), body=None),
    ):
        assert isinstance(classify_anthropic_error(exc), AuthLLMError)
    assert not isinstance(
        classify_anthropic_error(anthropic.NotFoundError("no model", response=response(404), body=None)),
        AuthLLMError,
    )


@pytest.mark.parametrize(
    ("code", "status"),
    [
        ("AccessDeniedException", 403),
        ("UnrecognizedClientException", 403),
        ("ExpiredTokenException", 403),
        ("InvalidSignatureException", 403),
        ("UnauthorizedOperation", 401),
        ("SomethingElse", 403),  # the status alone is enough
    ],
)
def test_bedrock_auth_codes_and_statuses_are_auth_errors(code: str, status: int) -> None:
    assert isinstance(classify_bedrock_error(client_error(code, status)), AuthLLMError)


def test_bedrock_missing_credentials_are_auth_errors() -> None:
    assert isinstance(classify_bedrock_error(bexc.NoCredentialsError()), AuthLLMError)
    assert isinstance(classify_bedrock_error(bexc.PartialCredentialsError(provider="env", cred_var="x")), AuthLLMError)


@pytest.mark.parametrize(("code", "status"), [("ValidationException", 400), ("ResourceNotFoundException", 404)])
def test_other_bedrock_client_errors_are_permanent_but_not_auth(code: str, status: int) -> None:
    err = classify_bedrock_error(client_error(code, status))
    assert isinstance(err, PermanentLLMError) and not isinstance(err, AuthLLMError)


def test_bedrock_throttling_stays_rate_limited() -> None:
    assert isinstance(classify_bedrock_error(client_error("ThrottlingException", 429)), RateLimitedLLMError)


# --------------------------------------------------------------------------- the failure the provider raises


def test_a_call_failure_says_by_default_that_it_is_neither_permanent_nor_auth() -> None:
    failed = LLMCallFailed("p", "m", "reason", 3)
    assert (failed.permanent, failed.auth) == (False, False)
    assert LLMCallFailed("p", "m", "r", 1, permanent=True, auth=True).auth is True

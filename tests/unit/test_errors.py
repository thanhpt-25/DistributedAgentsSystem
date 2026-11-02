from __future__ import annotations

import pytest

from das.errors import (
    EXIT_CONFIG_ERROR,
    EXIT_DEPENDENCY_UNREACHABLE,
    EXIT_GENERIC_FAILURE,
    EXIT_REGISTRATION_FAILED,
    EXIT_TRANSPORT_ERROR,
    ConfigError,
    DependencyUnreachable,
    FieldError,
    RegistrationFailed,
    TransportIncomplete,
    TransportNotInstalled,
    exit_code_for,
)


@pytest.mark.parametrize(
    ("exc", "code"),
    [
        (ConfigError([FieldError("role", "field required")]), EXIT_CONFIG_ERROR),
        (TransportNotInstalled("kafka"), EXIT_TRANSPORT_ERROR),
        (TransportIncomplete("http", ["publish"]), EXIT_TRANSPORT_ERROR),
        (DependencyUnreachable("redis"), EXIT_DEPENDENCY_UNREACHABLE),
        (RegistrationFailed("worker-1"), EXIT_REGISTRATION_FAILED),
        (RuntimeError("boom"), EXIT_GENERIC_FAILURE),
    ],
)
def test_exit_code_mapping(exc: BaseException, code: int) -> None:
    assert exit_code_for(exc) == code


def test_exit_codes_are_distinct() -> None:
    codes = {EXIT_CONFIG_ERROR, EXIT_TRANSPORT_ERROR, EXIT_DEPENDENCY_UNREACHABLE,
             EXIT_REGISTRATION_FAILED}
    assert codes == {2, 3, 4, 5}


def test_config_error_lists_every_field_and_reason() -> None:
    err = ConfigError(
        [
            FieldError("limits.max_attempts", "must be between 1 and 10"),
            FieldError("llm.model", "field required"),
        ]
    )
    assert err.fields == ("limits.max_attempts", "llm.model")
    text = str(err)
    assert "limits.max_attempts: must be between 1 and 10" in text
    assert "llm.model: field required" in text


def test_config_error_names_source_and_reason() -> None:
    err = ConfigError(source="/etc/das/config.yaml", reason="file not found")
    assert "/etc/das/config.yaml" in str(err)
    assert "file not found" in str(err)
    assert err.errors == ()


def test_transport_errors_name_adapter_and_missing_ops() -> None:
    assert "kafka" in str(TransportNotInstalled("kafka"))
    err = TransportIncomplete("http", ["acknowledge", "request_reply"])
    assert err.missing == ("acknowledge", "request_reply")
    assert "http" in str(err)
    assert "acknowledge" in str(err) and "request_reply" in str(err)


def test_dependency_unreachable_names_dependency() -> None:
    assert "rabbitmq" in str(DependencyUnreachable("rabbitmq", "connection refused"))

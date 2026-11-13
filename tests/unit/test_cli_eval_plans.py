"""``das eval-plans`` (das-smarter-agents task 4.2; Phase 6 Req 4.1, 4.3, 4.4): the real Planner over a dataset, a JSON
report on stdout, and an exit code that says whether the ratios reached ``--min-pass``."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

import das.cli as cli
from das.errors import EXIT_CONFIG_ERROR
from das.llm.base import ModelParams
from das.llm.errors import PermanentLLMError
from das.llm.retry import LLMProvider
from tests.fakes.llm_adapter import FakeAdapter

ENV = {"DAS__LLM__API_KEY": "sk-test-0123456789abcdef"}
PARAMS = ModelParams(model="m-1", temperature=0.0, max_output_tokens=256)
GOOD = json.dumps({"jobs": [
    {"job_id": "a", "instruction": "find", "capabilities": ["search"]},
    {"job_id": "b", "instruction": "write", "capabilities": ["write"], "depends_on": ["a"]},
]})
ONE = json.dumps({"jobs": [{"job_id": "a", "instruction": "find", "capabilities": ["search"]}]})
BAD = json.dumps({"jobs": [{"job_id": "x", "instruction": "paint", "capabilities": ["paint"]}]})


def config(tmp_path: Path, **limits: object) -> Path:
    cfg = {
        "role": "manager", "host": "127.0.0.1",
        "transports": {"jobs": "rabbitmq", "control": "grpc"},
        "rabbitmq": {"url": "amqp://127.0.0.1:1/", "management_url": "http://127.0.0.1:1"},
        "redis": {"url": "redis://127.0.0.1:1/0"},
        "grpc": {"bind": "0.0.0.0:50051", "advertise_host": "127.0.0.1", "advertise_port": 50051},
        "llm": {"provider": "anthropic", "model": "m-1"},
        "limits": limits,
    }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return path


def case(prompt: str = "do it", **expect: object) -> str:
    return json.dumps({"prompt": prompt, "capabilities": ["search", "write"], **({"expect": expect} if expect else {})})


def dataset(tmp_path: Path, *lines: str) -> Path:
    path = tmp_path / "cases.jsonl"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def llm(*outcomes: object) -> tuple[LLMProvider, FakeAdapter]:
    adapter = FakeAdapter(list(outcomes))
    return LLMProvider(adapter, PARAMS, retry_count=0, timeout=1), adapter


def run(tmp_path: Path, provider: LLMProvider, data: Path, *extra: str, **limits: object) -> int:
    argv = ["eval-plans", "--config", str(config(tmp_path, **limits)), "--dataset", str(data), *extra]
    return cli.main(argv, ENV, eval_llm=provider)


# --------------------------------------------------------------------------- the report (Req 4.3, 4.4)


def test_a_run_prints_a_json_report_and_exits_zero(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    provider, adapter = llm(GOOD, ONE)
    code = run(tmp_path, provider, dataset(tmp_path, case("first"), case("second")))
    report = json.loads(capsys.readouterr().out)  # stdout is the report and nothing else
    assert code == 0 and len(adapter.calls) == 2
    assert [c["line"] for c in report["cases"]] == [1, 2]
    assert report["totals"]["cases"] == 2 and report["totals"]["valid_ratio"] == 1.0
    assert report["totals"]["total_tokens"] == 4  # the fake provider reports 2 tokens per completion


def test_each_prompt_reaches_the_planner_with_the_cases_own_capabilities(tmp_path: Path) -> None:
    provider, adapter = llm(GOOD)
    line = json.dumps({"prompt": "UNIQUE-PROMPT", "capabilities": ["search", "write"]})
    run(tmp_path, provider, dataset(tmp_path, line))
    text = adapter.calls[0].messages[0].content
    assert "UNIQUE-PROMPT" in text and "- search" in text and "- write" in text


def test_the_planning_retries_of_the_config_decide_how_often_a_bad_plan_is_corrected(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    provider, adapter = llm(BAD, BAD, BAD)
    run(tmp_path, provider, dataset(tmp_path, case()), planning_retries=2)
    report = json.loads(capsys.readouterr().out)
    assert len(adapter.calls) == 3 and report["cases"][0]["attempts"] == 3 and not report["cases"][0]["valid"]


# --------------------------------------------------------------------------- the exit code (Req 4.4)


def test_an_invalid_plan_does_not_fail_the_run_without_a_threshold(tmp_path: Path) -> None:
    provider, _ = llm(GOOD, BAD)
    assert run(tmp_path, provider, dataset(tmp_path, case(), case()), planning_retries=0) == 0  # min-pass defaults to 0


def test_a_threshold_the_valid_ratio_misses_exits_one_and_still_prints_the_report(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    provider, _ = llm(GOOD, BAD)
    code = run(tmp_path, provider, dataset(tmp_path, case(), case()), "--min-pass", "1", planning_retries=0)
    assert code == 1 and json.loads(capsys.readouterr().out)["totals"]["valid_ratio"] == 0.5


def test_a_threshold_the_expectation_ratio_misses_exits_one(tmp_path: Path) -> None:
    provider, _ = llm(ONE)
    data = dataset(tmp_path, case(min_jobs=2))  # valid, but one job
    assert run(tmp_path, provider, data, "--min-pass", "0.9") == 1


def test_a_threshold_that_is_reached_exits_zero(tmp_path: Path) -> None:
    provider, _ = llm(GOOD, GOOD)
    assert run(tmp_path, provider, dataset(tmp_path, case(min_jobs=2), case()), "--min-pass", "1") == 0


def test_a_failing_provider_scores_the_case_and_the_run_goes_on(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    provider, _ = llm(PermanentLLMError("down for good"), GOOD)
    code = run(tmp_path, provider, dataset(tmp_path, case(), case()), "--min-pass", "0.5")
    report = json.loads(capsys.readouterr().out)
    assert code == 0 and [c["valid"] for c in report["cases"]] == [False, True]
    assert "down for good" in report["cases"][0]["failures"][0]


# --------------------------------------------------------------------------- usage errors (Req 4.4)


def test_a_bad_dataset_line_is_a_usage_error_naming_the_line_and_the_planner_is_never_asked(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    provider, adapter = llm(GOOD)
    code = run(tmp_path, provider, dataset(tmp_path, case(), "", "{broken"))
    captured = capsys.readouterr()
    assert code == EXIT_CONFIG_ERROR and "line 3" in captured.err
    assert captured.out == "" and adapter.calls == []  # nothing was spent on a dataset that cannot be used


def test_a_missing_dataset_is_a_usage_error(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    provider, adapter = llm()
    assert run(tmp_path, provider, tmp_path / "nope.jsonl") == EXIT_CONFIG_ERROR
    assert "nope.jsonl" in capsys.readouterr().err and adapter.calls == []


def test_a_missing_config_exits_with_the_config_error_code_and_prints_no_report(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    provider, _ = llm()
    argv = ["eval-plans", "--config", str(tmp_path / "missing.yaml"), "--dataset", str(dataset(tmp_path, case()))]
    assert cli.main(argv, ENV, eval_llm=provider) == EXIT_CONFIG_ERROR
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("bad", ["-0.1", "1.5", "lots", "nan"])
def test_a_threshold_outside_zero_to_one_is_a_usage_error(tmp_path: Path, bad: str) -> None:
    provider, adapter = llm(GOOD)
    argv = ["eval-plans", "--config", str(config(tmp_path)), "--dataset", str(dataset(tmp_path, case())),
            "--min-pass", bad]
    assert cli.main(argv, ENV, eval_llm=provider) == 2 and adapter.calls == []


def test_both_paths_are_required() -> None:
    assert cli.main(["eval-plans"], ENV) == 2
    assert cli.main(["eval-plans", "--config", "x.yaml"], ENV) == 2


# --------------------------------------------------------------------------- closing what the command opened


class Closable(FakeAdapter):
    closed = False

    async def aclose(self) -> None:
        type(self).closed = True


def test_a_provider_the_command_builds_is_closed_afterwards(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    Closable.closed = False
    built = LLMProvider(Closable([GOOD]), PARAMS, retry_count=0, timeout=1)
    monkeypatch.setattr("das.eval.command.build_llm_provider", lambda config: built)
    argv = ["eval-plans", "--config", str(config(tmp_path)), "--dataset", str(dataset(tmp_path, case()))]
    assert cli.main(argv, ENV) == 0 and Closable.closed


def test_a_provider_that_was_handed_in_is_left_open(tmp_path: Path) -> None:
    Closable.closed = False
    provider = LLMProvider(Closable([GOOD]), PARAMS, retry_count=0, timeout=1)
    assert run(tmp_path, provider, dataset(tmp_path, case())) == 0
    assert not Closable.closed


def test_a_provider_without_a_close_method_is_fine(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    built, _ = llm(GOOD)  # FakeAdapter has no aclose
    monkeypatch.setattr("das.eval.command.build_llm_provider", lambda config: built)
    argv = ["eval-plans", "--config", str(config(tmp_path)), "--dataset", str(dataset(tmp_path, case()))]
    assert cli.main(argv, ENV) == 0


def test_the_parser_knows_the_command() -> None:
    args = cli.build_parser().parse_args(
        ["eval-plans", "--config", "c.yaml", "--dataset", "d.jsonl", "--min-pass", "0.8"]
    )
    assert (args.command, args.config, args.dataset, args.min_pass) == ("eval-plans", "c.yaml", "d.jsonl", 0.8)
    assert cli.build_parser().parse_args(["eval-plans", "--config", "c", "--dataset", "d"]).min_pass == 0.0

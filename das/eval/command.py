"""``das eval-plans``: run the real Planner over a dataset and report how it did (Phase 6 Req 4.1, 4.3, 4.4).

The report is JSON on stdout and nothing else is: logs go to stderr. The exit code is 0 when the valid ratio and the
expectation ratio both reach ``--min-pass`` (default 0, so a run without a threshold never fails on quality), 1 when
either falls short, and 2 for a dataset or configuration that cannot be used, before a single LLM call is made.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Callable, Mapping, Sequence
from typing import Any, TextIO

from das.config import load_config
from das.errors import EXIT_CONFIG_ERROR, EXIT_OK
from das.eval.plans import Case, CaseScore, DatasetError, load_dataset, run, summarize
from das.llm.factory import build_llm_provider
from das.llm.retry import LLMProvider
from das.manager.planner import Planner, PlannerConfig
from das.observability import configure_logging

#: The ratios did not reach ``--min-pass``.
EXIT_BELOW_THRESHOLD = 1
LOG_AGENT_ID = "eval-plans"


def min_pass_argument(value: str) -> float:
    """``--min-pass``: a ratio from 0 to 1."""
    try:
        ratio = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a number from 0 to 1") from None
    if not 0.0 <= ratio <= 1.0:  # also false for NaN
        raise argparse.ArgumentTypeError("must be a number from 0 to 1")
    return ratio


async def _evaluate(
    provider: LLMProvider, cfg: PlannerConfig, cases: Sequence[Case], *, close: bool
) -> list[CaseScore]:
    try:
        return await run(Planner(provider), cfg, cases)
    finally:
        if close:  # only what this command opened; a provider handed in stays the caller's
            await provider.aclose()


def eval_plans_main(
    args: argparse.Namespace,
    environ: Mapping[str, str],
    *,
    fail: Callable[[BaseException], int],
    llm: LLMProvider | None = None,
    out: TextIO | None = None,
    err: TextIO | None = None,
) -> int:
    """Run the command; ``fail`` maps a configuration or provider-setup error to its exit code, as startup does."""
    out = out if out is not None else sys.stdout
    err = err if err is not None else sys.stderr
    configure_logging(LOG_AGENT_ID, level=args.log_level, stream=err)

    try:
        cases = load_dataset(args.dataset)
    except DatasetError as exc:
        print(f"das: error: {exc}", file=err)
        return EXIT_CONFIG_ERROR
    try:
        config = load_config(args.config, environ)
        provider = llm if llm is not None else build_llm_provider(config)
    except Exception as exc:  # noqa: BLE001 - mapped to an exit code
        return fail(exc)

    limits: Any = config.limits
    cfg = PlannerConfig(max_jobs=int(limits.max_jobs), planning_retries=int(limits.planning_retries))
    report = summarize(asyncio.run(_evaluate(provider, cfg, cases, close=llm is None)))
    print(json.dumps(report.to_dict(), indent=2), file=out)
    return EXIT_OK if report.passes(args.min_pass) else EXIT_BELOW_THRESHOLD


__all__ = ["EXIT_BELOW_THRESHOLD", "eval_plans_main", "min_pass_argument"]

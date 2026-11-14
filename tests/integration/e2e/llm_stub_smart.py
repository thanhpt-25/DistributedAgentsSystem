"""Fake LLM for the Phase 6 end-to-end tests: ``llm_stub.py`` plus the behaviours the smarter-agents features need
(das-smarter-agents task 7.3).

A marker in the submitted prompt (``[[scenario=...]]``) picks what the Planner plans; markers in a Job's instruction
pick what the Worker's model does. Anything without a marker answers exactly as ``llm_stub.py`` does.

- ``[[scenario=structured]]``: the research Job has an ``output_schema``. The model first answers in prose, then, when
  asked to correct itself, with valid JSON: the one corrective completion of Phase 6 Req 1.5.
- ``[[scenario=replan]]``: the research Job carries ``[[fail]]`` and the model answers it with HTTP 400 (a permanent
  error), so the Request would fail; a replan keeps the Job that succeeded and replaces the rest.
- ``[[scenario=tool]]``: the research Job requires ``tool-calculator``. The model asks for ``calculator("6*7")``, then
  answers from the tool's result.
- The model name ``missing-model`` is answered with HTTP 404 (a permanent error that is not an authentication error),
  which a Worker configured with a fallback model must survive.

Standard library only; runs in a plain ``python:3.12-slim`` container next to ``llm_stub.py``.
"""

from __future__ import annotations

import json
import re
from http.server import ThreadingHTTPServer
from typing import Any

try:  # in the container both files sit in /stub
    import llm_stub as base
except ImportError:  # under pytest the package path is used
    from tests.integration.e2e import llm_stub as base

SCENARIO = re.compile(r"\[\[scenario=(\w+)\]\]")
REPLAN_MARK = "The request below was planned and executed, but some jobs failed"
JSON_MARK = "Reply with only a JSON value that matches this JSON Schema"
CORRECTION_MARK = "Your previous reply did not match"
MISSING_MODEL = "missing-model"
ANSWER_SCHEMA = {
    "type": "object",
    "properties": {"answer": {"type": "integer"}},
    "required": ["answer"],
    "additionalProperties": False,
}


def scenario_of(request: str) -> str | None:
    match = SCENARIO.search(request)
    return match.group(1) if match else None


def _summarize_job(prev: list[str], req: str, number: int) -> dict[str, Any]:
    return {"job_id": f"j{number}", "instruction": f"[summarize] {req}", "capabilities": ["summarize"],
            "depends_on": prev}


def first_plan(request: str) -> dict[str, Any] | None:
    """The plan for a marked prompt, or ``None`` for the base stub's one-Job-per-Capability plan."""
    scenario = scenario_of(request)
    if scenario == "structured":
        research = {"job_id": "j1", "instruction": f"[research] [[json]] {request}", "capabilities": ["research"],
                    "output_schema": ANSWER_SCHEMA}
        return {"jobs": [research, _summarize_job(["j1"], request, 2)]}
    if scenario == "replan":
        failing = {"job_id": "j1", "instruction": f"[research] [[fail]] {request}", "capabilities": ["research"]}
        fine = {"job_id": "j2", "instruction": f"[code-review] {request}", "capabilities": ["code-review"]}
        return {"jobs": [failing, fine, _summarize_job(["j1", "j2"], request, 3)]}
    if scenario == "tool":
        research = {"job_id": "j1", "instruction": f"[research] [[tool]] compute 6*7 {request}",
                    "capabilities": ["research", "tool-calculator"]}
        return {"jobs": [research, _summarize_job(["j1"], request, 2)]}
    return None


def succeeded_jobs(replan_prompt: str) -> list[dict[str, Any]]:
    """The Succeeded Job definitions of a replanning prompt, to be kept exactly."""
    _, _, rest = replan_prompt.partition("Succeeded jobs (keep exactly), with their results:\n")
    block, _, _ = rest.partition("\n\nFailed jobs (replace):")
    return [json.loads(line) for line in block.splitlines() if line.startswith('{"job_id"')]


def replan(replan_prompt: str) -> dict[str, Any]:
    """Keep what succeeded, replace the rest with a research Job and a summary that do not fail."""
    kept = succeeded_jobs(replan_prompt)
    request = base.request_text(replan_prompt) or "the request"
    clean = re.sub(r"\[\[scenario=\w+\]\]", "", request).strip()
    again = {"job_id": "r1", "instruction": f"[research] try again: {clean}", "capabilities": ["research"]}
    deps = [job["job_id"] for job in kept] + ["r1"]
    done = {"job_id": "r2", "instruction": f"[summarize] {clean}", "capabilities": ["summarize"], "depends_on": deps}
    return {"jobs": [*kept, again, done]}


def planner_reply(first_user_message: str) -> str:
    if first_user_message.startswith(REPLAN_MARK):
        return json.dumps(replan(first_user_message))
    plan = first_plan(base.request_text(first_user_message))
    return json.dumps(plan if plan is not None else base.build_plan(first_user_message))


def chat(body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    """The reply to one ``/api/chat`` request: ``(status, JSON body)``."""
    if body.get("model") == MISSING_MODEL:
        return 404, {"error": f"model '{MISSING_MODEL}' not found"}
    messages = body.get("messages") or []
    system = next((m.get("content", "") for m in messages if m.get("role") == "system"), "")
    users = [m.get("content", "") for m in messages if m.get("role") == "user"]
    if system.startswith(base.PLANNER_MARK):
        return 200, _ok(body, planner_reply(users[0] if users else ""))
    if system.startswith(base.WORKER_MARK):
        every_user = "\n".join(users)
        if "[[fail]]" in every_user:
            return 400, {"error": "scripted permanent failure"}
        if body.get("tools"):
            tool_results = [m.get("content", "") for m in messages if m.get("role") == "tool"]
            if not tool_results:
                call = {"function": {"name": "calculator", "arguments": {"expression": "6*7"}}}
                return 200, _ok(body, "", tool_calls=[call])
            return 200, _ok(body, f"The result is {tool_results[-1]}")
        if JSON_MARK in every_user:
            last = users[-1] if users else ""
            return 200, _ok(body, '{"answer": 42}' if last.startswith(CORRECTION_MARK) else "The answer is forty two")
    return 200, _ok(body, base.reply_for(messages))


def _ok(body: dict[str, Any], content: str, tool_calls: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    prompt_chars = sum(len(m.get("content", "")) for m in body.get("messages") or [])
    message: dict[str, Any] = {"role": "assistant", "content": content}
    if tool_calls:
        message["tool_calls"] = tool_calls
    return {
        "model": body.get("model", "fake-llm"), "message": message, "done": True,
        "prompt_eval_count": max(1, prompt_chars // 4), "eval_count": max(1, len(content) // 4),
    }


class Handler(base.Handler):
    def do_POST(self) -> None:  # noqa: N802 - http.server API
        if self.path != "/api/chat":
            self._send(404, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except ValueError:
            self._send(400, {"error": "invalid json"})
            return
        status, payload = chat(body)
        self._send(status, payload)


if __name__ == "__main__":
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("0.0.0.0", 11434), Handler).serve_forever()

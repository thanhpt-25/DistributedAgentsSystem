"""Ollama-compatible fake LLM server for the end-to-end tests (task 19.4).

Runs inside a plain ``python:3.12-slim`` container (standard library only) and
answers the two endpoints the ``ollama`` Provider_Adapter uses:

- ``GET /api/tags``  -> 200, used by the agents' LLM health check.
- ``POST /api/chat`` -> a non-streaming chat reply.

Replies depend on the system prompt the agent sends:

- Planner: a valid Job_Plan JSON built from the "Advertised capabilities" list
  in the planning prompt. One job per advertised Capability; a ``summarize``
  job (when advertised) depends on every other job, so plans form a real DAG.
  Each instruction carries the original request text, so markers in the
  prompt reach the workers.
- Worker: plain text. A ``[[delay=N]]`` marker in the instruction makes the
  reply wait N seconds, which keeps a Job in flight for the cancel and
  takeover tests.
- Anything else (the Summarizer): plain text naming the request.

No production code knows about this server: the agents just run the real
``ollama`` adapter against ``DAS__LLM__BASE_URL=http://llm-stub:11434``.
"""

from __future__ import annotations

import json
import re
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PLANNER_MARK = "You are the planner"
WORKER_MARK = "You are a worker agent"
DELAY_RE = re.compile(r"\[\[delay=(\d+(?:\.\d+)?)\]\]")
MAX_DELAY = 120.0


def advertised_capabilities(text: str) -> list[str]:
    """Capabilities listed under "Advertised capabilities:" in the planning prompt."""
    m = re.search(r"Advertised capabilities:\n((?:- [^\n]+\n?)+)", text)
    if not m:
        return []
    return [line[2:].strip() for line in m.group(1).splitlines() if line.startswith("- ")]


def request_text(text: str) -> str:
    _, sep, rest = text.rpartition("\nRequest:\n")
    return rest.strip() if sep else ""


def build_plan(planning_prompt: str) -> dict:
    caps = advertised_capabilities(planning_prompt)
    req = request_text(planning_prompt) or "the request"
    jobs = []
    others = [c for c in caps if c != "summarize"]
    for i, cap in enumerate(others, start=1):
        jobs.append({"job_id": f"j{i}", "instruction": f"[{cap}] {req}", "capabilities": [cap]})
    if "summarize" in caps:
        jobs.append(
            {
                "job_id": f"j{len(jobs) + 1}",
                "instruction": f"[summarize] {req}",
                "capabilities": ["summarize"],
                "depends_on": [j["job_id"] for j in jobs],
            }
        )
    return {"jobs": jobs}


def reply_for(messages: list[dict]) -> str:
    system = next((m.get("content", "") for m in messages if m.get("role") == "system"), "")
    users = [m.get("content", "") for m in messages if m.get("role") == "user"]
    if system.startswith(PLANNER_MARK):
        # The first user message is the planning prompt; corrections reuse it.
        return json.dumps(build_plan(users[0] if users else ""))
    last = users[-1] if users else ""
    if system.startswith(WORKER_MARK):
        m = DELAY_RE.search(last)
        if m:
            time.sleep(min(float(m.group(1)), MAX_DELAY))
        first_line = last.splitlines()[1] if len(last.splitlines()) > 1 else last
        return f"fake-llm worker output for: {first_line[:200]}"
    return "fake-llm final answer combining the job results."


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send(self, code: int, body: dict) -> None:
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802 - http.server API
        if self.path == "/api/tags":
            self._send(200, {"models": [{"name": "fake-llm"}]})
        else:
            self._send(404, {"error": "not found"})

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
        messages = body.get("messages") or []
        content = reply_for(messages)
        prompt_chars = sum(len(m.get("content", "")) for m in messages)
        self._send(
            200,
            {
                "model": body.get("model", "fake-llm"),
                "message": {"role": "assistant", "content": content},
                "done": True,
                "prompt_eval_count": max(1, prompt_chars // 4),
                "eval_count": max(1, len(content) // 4),
            },
        )

    def log_message(self, fmt: str, *args: object) -> None:
        print(f"llm-stub {self.command} {self.path}", flush=True)


if __name__ == "__main__":
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("0.0.0.0", 11434), Handler).serve_forever()

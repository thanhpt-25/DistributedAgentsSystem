# SDD progress: das-client-api pure core
Task 1: complete (no git; snapshot snap-1, review clean)
  Minor (for final review): retry_after only tested at rate 1.0; retry_after_header raises on inf/nan (config should bound rate); burst accepts True; TakeResult lacks class docstring.
Task 2: complete (no git; snapshot snap-2, review clean)
  Minor (for final review): Budget.from_json_obj Decimal(cost) raises InvalidOperation (not ValueError) on malformed str and accepts floats; decimal_text normalize uses ambient 28-digit precision; no tests for float/str max_cost rejection or non-int max_total_tokens in from_json_obj; BudgetStatus lacks class docstring.
Task 3: complete (no git; snapshot snap-3, review clean)
  Minor (for final review): EventType and EVENT_TYPES spell names twice (use get_args); created/reopened_event_data lack docstrings; terminal_event_type called only to validate in terminal_event_data; planned payload keeps submitted list order (consider sorted); idem_key_hash assumes pre-validated ASCII (document). Gateway task must pass the client's own Budget (pre-merge) to body_hash.
Task 4: complete (no git; snapshots snap-4/snap-4fix, review round 2 clean; round 1 fixed over-long timestamp crash + reject vectors, user-approved)
  Minor (for final review): no boundary test that a 19-digit timestamp is still parsed; "malformed timestamp" reject vector would fail via HMAC even without the format guard.
Final review: With fixes. Important: (1) verify() NaN now bypasses tolerance; (2) "malformed timestamp" reject vector masked by HMAC, need format-guard vectors + 19-digit/leading-zero accept vectors; (3) Budget.from_json_obj raises InvalidOperation / accepts float. Awaiting user decision. design.md:185 and :284 corrected by controller.
Final-review fixes: complete (review clean). Full check: 2091 passed / 3 pre-existing warnings, ruff/mypy/lint-imports clean. Task group 1 DONE.
  Minor (later): Budget.from_json_obj accepts non-canonical/huge Decimal strings ("1e999999999" -> later decimal.Overflow in to_json_obj; " 1.5 ", "1_0"); guard with decimal_text(max_cost) == cost.
Next: write plan for task group 2 (configuration).
# group 2: config
G2 Task 1: complete (review clean; 2.1 unticked until Task 2 lands settings.py)
  Minor (later): rate/burst/limits accept YAML booleans (max_cost rejects them); DAS__CLIENT_LIMITS__OVERRIDES from env gives "valid list" error, not "file only"; webhook secret names differing only by case silently collapse; override cannot clear a default budget (None = inherit); property test samples webhook_secrets only as scalar; client_api_errors skipped when pydantic already failed.
G2 Task 2: complete (review clean)
  Minor (later): override default_budget: {} clears the default budget (merged treats it as set, _budget -> None); undocumented/untested. max_active_requests inheritance untested.
Checkpoint 3: 2144 passed / 3 pre-existing warnings; ruff, mypy, lint-imports clean. Group 2 DONE.
# group 4: state store (snapshots in /tmp/das_sdd_snaps)
G4 Task 1: complete (review clean)
  Minor (group final review): rate_check.lua accepts burst=inf and HSETs before PEXPIRE fails (violates validate-before-write; unreachable via StateStore/config); PEXPIRE arg is a Lua number (real Redis %.14g -> exponent form >= 1e14 ms; unreachable with MIN_RATE); bad-arg tests miss nan/inf; test imports private scripts._read; keys.py docstrings cite "Req 3" not "Req X.Y".
G4 Task 2: complete (review clean)
  Minor (group final review): create_request.lua hardcodes request.created payload (pinned by test); _create silently retries unknown replies (only EXISTS should loop); assert used for invariant at store.py:510; ClientRequestSpec has no Python range checks (bad values -> ResponseError not ValueError); Req 3.7 lacks direct "no active key" assertion; empty Budget() stored as "{}" (group 7 Gateway must pass None).
G4 Task 3: complete (review clean)
  Minor (group final review): purge SREM covered only by planted-member unit test (Property 3 cannot reach it; judged adequate); delete_request reads client_fp before MULTI without WATCH (client_fp immutable; add comment); delete_request docstring cites only Req 3.6.
G4 Task 4: complete (review clean; commit_plan event-before-status ordering judged correct: keeps events 1:1 with transition log under mid-script fault)
  Minor (group final review): add comment in commit_plan.lua explaining event ordering; commit_plan repeats object check instead of a shared helper; check_budget_view accepts a JSON array; cancel malformed-budget test checks only status+events; new tests import fields from test_state_phase2_dlq (shared fixture would be cleaner).
  Follow-up recorded in tasks.md: 5.3 (Manager passes budget_view to finalize/fail_request) and 7.3 (Gateway cancel passes budget_view).
G4 Task 5: complete (round 1 found Critical stale-claim-after-reset bug, reproduced by controller; fixed with event_id fence; re-review clean; controller fixed 2 stale fence comments)
  Minor (group final review): fail() lease param in property test lacks type hint; claim_webhook scans at most 10 entries (documented); scripts don't enforce max_attempts (sender must, group 7 must test).
  Source of the 3 suite warnings: pytest-asyncio asyncio_default_fixture_loop_scope unset, from pytester sessions in tests/unit/test_require_docker_hook.py (pre-existing).
G4 final review: With fixes. User approved fixes + option (a) for both decisions (reopened event from Replay; idem record of a gone Request counts as absent). Spec updated by controller. Fix dispatched.
G4 final-review fixes: complete (review clean). Folded into tasks.md 7.3: cancel hardening on corrupt budget data, usage-fence docstring, prices/max_cost cancel test.
  Minor (later): Property 6 reaches each post-Replay branch in ~8/100 examples (weight replay higher); extra blank line after WebhookClaim in store.py.
# group 5: Manager budget enforcement (plan docs/superpowers/plans/2026-10-30-das-client-api-budget.md)
G5 Task 1: complete (review clean, Approved; 19 focused tests pass with -W error).
  Minor (later/final review): resume() re-emits PublishDispatch for in-flight Jobs and re-arms retry/publish timers on takeover in stop mode (Req 5.8 strictness; redelivery may spend uncounted tokens) -> decide in G5 Task 2; _never_published reads private w._snap_timer_keys; long one-liners machine.py:725, store.py:1477; Property 5 never draws ReplayRequested/UnackedDeliveryExhausted.

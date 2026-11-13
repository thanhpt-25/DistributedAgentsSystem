"""Property 3: the calculator returns the value a reference evaluation returns for the supported grammar and refuses
everything else without raising anything but ``ToolError`` (das-smarter-agents task 5.3; Phase 6 Req 8.3, 5.8).

The reference is Python's own ``eval`` over a namespace with no builtins, run only on text this module generates from
the grammar (never on arbitrary text). The generator keeps numbers and exponents small, so the reference never has to
compute something enormous and the calculator's size limits never come into play."""

from __future__ import annotations

import ast
import math
from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from das.tools.calculator import calculate
from das.tools.types import ToolError

REFERENCE_NAMESPACE: dict[str, Any] = {
    "__builtins__": {},
    "sqrt": math.sqrt, "abs": abs, "min": min, "max": max, "round": round, "floor": math.floor, "ceil": math.ceil,
    "sin": math.sin, "cos": math.cos, "tan": math.tan, "log": math.log, "log10": math.log10, "exp": math.exp,
    "pi": math.pi, "e": math.e,
}
REFUSED = object()


def reference(text: str) -> Any:
    """What a careful reference evaluation says: a finite real number, or REFUSED."""
    try:
        value = eval(text, REFERENCE_NAMESPACE)  # noqa: S307 - only ever called on text generated below
    except (ZeroDivisionError, ValueError, OverflowError):
        return REFUSED
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return REFUSED
    if isinstance(value, float) and not math.isfinite(value):
        return REFUSED
    return value


LEAVES = st.one_of(
    st.integers(0, 9).map(str),
    st.sampled_from(["0.5", "1.5", "2.25", "3.0", "pi", "e"]),
)
ONE_ARGUMENT = ["sqrt", "abs", "floor", "ceil", "sin", "cos", "tan", "exp", "round", "log", "log10"]
OPERATORS = st.sampled_from(["+", "-", "*", "/", "//", "%"])


def extend(inner: st.SearchStrategy[str]) -> st.SearchStrategy[str]:
    return st.one_of(
        st.tuples(inner, OPERATORS, inner).map(lambda t: f"({t[0]} {t[1]} {t[2]})"),
        st.tuples(inner, st.sampled_from(["0", "1", "2"])).map(lambda t: f"({t[0]} ** {t[1]})"),  # small exponents
        inner.map(lambda a: f"(abs({a}) ** 0.5)"),  # a fractional power of a non-negative number is always real
        inner.map(lambda a: f"(-{a})"),
        inner.map(lambda a: f"(+{a})"),
        st.tuples(st.sampled_from(ONE_ARGUMENT), inner).map(lambda t: f"{t[0]}({t[1]})"),
        st.tuples(st.sampled_from(["min", "max"]), inner, inner).map(lambda t: f"{t[0]}({t[1]}, {t[2]})"),
        inner.map(lambda a: f"round({a}, 2)"),
    )


EXPRESSIONS = st.recursive(LEAVES, extend, max_leaves=6)


@given(text=EXPRESSIONS)
@settings(deadline=None)
def test_the_calculator_agrees_with_the_reference_on_the_supported_grammar(text: str) -> None:
    expected = reference(text)
    if expected is REFUSED:
        try:
            calculate(text)
        except ToolError:
            return
        raise AssertionError(f"the reference refuses {text!r} but the calculator answered")
    assert ast.literal_eval(calculate(text)) == expected  # the same value...
    assert calculate(text) == (str(expected) if isinstance(expected, int) else repr(expected))  # ...in the same form


# --------------------------------------------------------------------------- everything else


ALLOWED_NODES = (
    ast.Expression, ast.BinOp, ast.UnaryOp, ast.Constant, ast.Name, ast.Call, ast.Load,
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod, ast.Pow, ast.UAdd, ast.USub,
)
FRAGMENTS = [
    "1", "2", "3.5", "x", "pi", "e", "sqrt", "abs", "min", "max", "(", ")", ",", "+", "-", "*", "/", "//", "%", "**",
    "__import__", "os", ".", "lambda", ":", "[", "]", "{", "}", "'a'", '"b"', "if", "else", "for", "in", "and", "or",
    "not", "==", "<", "=", ":=", ";", "\n", " ", "@", "<<", ">>", "&", "|", "^", "~", "1j", "True", "None", "...",
]
SNIPPETS = st.lists(st.sampled_from(FRAGMENTS), max_size=12).map(" ".join)


def outside_the_grammar(text: str) -> bool:
    """True when the text parses to something with a node the calculator must not evaluate."""
    try:
        tree = ast.parse(text.strip(), mode="eval")
    except (SyntaxError, ValueError, RecursionError, MemoryError):
        return False
    return any(not isinstance(node, ALLOWED_NODES) for node in ast.walk(tree))


@given(text=st.one_of(SNIPPETS, st.text(max_size=60)))
@settings(deadline=None, max_examples=400)
def test_any_text_is_answered_with_a_finite_number_or_a_tool_error_and_nothing_else(text: str) -> None:
    try:
        answer = calculate(text)
    except ToolError as refusal:
        assert str(refusal) and len(str(refusal)) < 300
        return
    value = ast.literal_eval(answer)
    assert isinstance(value, (int, float)) and not isinstance(value, bool)
    assert not (isinstance(value, float) and not math.isfinite(value))


@given(text=SNIPPETS)
@settings(deadline=None, max_examples=400)
def test_text_that_parses_to_anything_outside_the_arithmetic_grammar_is_refused(text: str) -> None:
    if not outside_the_grammar(text):
        return
    try:
        calculate(text)
    except ToolError:
        return
    raise AssertionError(f"{text!r} is outside the grammar but was answered")

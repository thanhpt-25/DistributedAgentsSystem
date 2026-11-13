"""The calculator tool (das-smarter-agents task 5.3; Phase 6 Req 5.7, 5.8): arithmetic through a safe parser, never
``eval``, with limits on length, exponent and size. A refusal is a ``ToolError`` whose text the model can read."""

from __future__ import annotations

import time

import pytest

from das.tools.calculator import MAX_EXPONENT, MAX_EXPRESSION_LENGTH, calculate
from das.tools.types import ToolError

# --------------------------------------------------------------------------- arithmetic


@pytest.mark.parametrize(
    ("expression", "result"),
    [
        ("2+3", "5"),
        ("2 + 3 * 4", "14"),
        ("(2 + 3) * 4", "20"),
        ("10 - 4 - 3", "3"),
        ("2 ** 3 ** 2", "512"),  # right to left, as Python
        ("-2 ** 2", "-4"),
        ("(-2) ** 2", "4"),
        ("+5", "5"),
        ("--5", "5"),
        ("7 // 2", "3"),
        ("-7 // 2", "-4"),
        ("7 % 3", "1"),
        ("-7 % 3", "2"),
        ("7 / 2", "3.5"),
        ("6 / 3", "2.0"),
        ("0.1 + 0.2", "0.30000000000000004"),
        ("1e3", "1000.0"),
        ("2 ** -1", "0.5"),
        ("2 ** 0.5", "1.4142135623730951"),
        ("1_000 + 1", "1001"),
        ("  42  ", "42"),
        ("0", "0"),
    ],
)
def test_arithmetic_gives_what_python_gives(expression: str, result: str) -> None:
    assert calculate(expression) == result


@pytest.mark.parametrize(
    ("expression", "result"),
    [
        ("sqrt(16)", "4.0"),
        ("abs(-3)", "3"),
        ("abs(-3.5)", "3.5"),
        ("min(3, 1, 2)", "1"),
        ("max(3, 1, 2)", "3"),
        ("round(2.5)", "2"),
        ("round(3.14159, 2)", "3.14"),
        ("floor(2.7)", "2"),
        ("ceil(2.1)", "3"),
        ("log10(1000)", "3.0"),
        ("exp(0)", "1.0"),
        ("sin(0)", "0.0"),
        ("cos(0)", "1.0"),
        ("tan(0)", "0.0"),
        ("log(1)", "0.0"),
        ("pi", "3.141592653589793"),
        ("e", "2.718281828459045"),
        ("2 * pi", "6.283185307179586"),
        ("sqrt(abs(-16)) + max(1, 2)", "6.0"),
        ("(sqrt)(4)", "2.0"),  # the same AST as sqrt(4)
    ],
)
def test_the_whitelisted_functions_and_constants(expression: str, result: str) -> None:
    assert calculate(expression) == result


# --------------------------------------------------------------------------- what it refuses (Req 5.8)


@pytest.mark.parametrize(
    "attack",
    [
        "__import__('os').system('echo hi')",
        "().__class__.__bases__[0].__subclasses__()",
        "open('/etc/passwd').read()",
        "eval('1+1')",
        "exec('x=1')",
        "compile('1', 'f', 'eval')",
        "lambda: 1",
        "(lambda x: x)(1)",
        "[x for x in range(3)]",
        "{1: 2}",
        "{1, 2}",
        "[1, 2]",
        "(1, 2)",
        "'text'",
        '"text"',
        "b'bytes'",
        "f'{1}'",
        "1 if True else 2",
        "1 and 2",
        "not 1",
        "1 < 2",
        "1 == 1",
        "1 in [1]",
        "x := 1",
        "x = 1",
        "x",
        "pi.real",
        "sqrt.__call__(4)",
        "math.sqrt(4)",
        "os",
        "True",
        "False",
        "None",
        "1j",
        "3 + 4j",
        "...",
        "1 @ 2",
        "1 << 2",
        "1 >> 2",
        "1 & 2",
        "1 | 2",
        "1 ^ 2",
        "~1",
        "sqrt(x=4)",
        "sqrt(*[4])",
        "max(*[1, 2])",
        "unknown(1)",
        "sqrt",
        "pi(1)",
        "sqrt(4)(1)",
        "await x",
        "yield 1",
        "1; 2",
        "1\n2",
        "import os",
        "",
        "   ",
        "(",
        "1 +",
        "1 2",
        "# just a comment",
    ],
)
def test_anything_outside_the_arithmetic_grammar_is_refused_with_a_message(attack: str) -> None:
    with pytest.raises(ToolError) as info:
        calculate(attack)
    assert str(info.value)  # the model is told something


def test_an_expression_that_is_not_text_is_refused() -> None:
    for bad in (None, 5, ["1+1"], {"e": "1"}):
        with pytest.raises(ToolError):
            calculate(bad)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- errors the model can read


@pytest.mark.parametrize(
    ("expression", "fragment"),
    [
        ("1 / 0", "zero"),
        ("1 // 0", "zero"),
        ("1 % 0", "zero"),
        ("0 ** -1", "zero"),
        ("sqrt(-1)", "domain"),
        ("log(0)", "domain"),
        ("log(-1)", "domain"),
        ("(-8) ** (1/3)", "real"),
        ("1e308 * 10", "finite"),
        ("1e999", "finite"),
        ("exp(1000)", "finite"),
        ("10.0 ** 400", "finite"),  # within the exponent limit, but a float cannot hold it
        ("sqrt(1, 2)", "argument"),
        ("min()", "argument"),
        ("round(1, 2, 3)", "argument"),
        ("round(1.5, 2.5)", "integer"),
        ("floor(1e308 * 10)", "finite"),
    ],
)
def test_a_calculation_that_cannot_be_done_says_why(expression: str, fragment: str) -> None:
    with pytest.raises(ToolError, match=fragment):
        calculate(expression)


# --------------------------------------------------------------------------- limits (Req 5.8)


def test_an_expression_over_the_length_limit_is_refused() -> None:
    assert calculate("1+" * 50 + "1") == "51"  # 101 characters is fine
    with pytest.raises(ToolError, match="long"):
        calculate("1+" * MAX_EXPRESSION_LENGTH + "1")


def test_an_exponent_over_the_limit_is_refused() -> None:
    assert calculate(f"2 ** {MAX_EXPONENT}").startswith("10715086071862673209484250490600018105614048117055336074")
    with pytest.raises(ToolError, match="exponent"):
        calculate(f"2 ** {MAX_EXPONENT + 1}")
    with pytest.raises(ToolError, match="exponent"):
        calculate("10 ** 100000")


def test_a_result_too_large_for_an_integer_is_refused_instead_of_computed() -> None:
    started = time.monotonic()
    for expression in ("9999 ** 1000", "99999999999 ** 999", "(10 ** 600) * (10 ** 600) * 10 ** 600"):
        with pytest.raises(ToolError, match="large"):
            calculate(expression)
    assert time.monotonic() - started < 1  # refused up front, not after a long computation


def test_a_huge_power_is_refused_before_it_is_built_not_after() -> None:
    """Unguarded, ``(10 ** 1000) ** 1000`` builds a 3-million-bit number (about a quarter of a second) and only then is
    refused; the guard refuses it in about a millisecond."""
    started = time.monotonic()
    with pytest.raises(ToolError, match="large"):
        calculate("(10 ** 1000) ** 1000")
    assert time.monotonic() - started < 0.15


def test_a_sum_that_grows_past_the_integer_limit_is_refused() -> None:
    """Each term is 2 ** 3498 (3499 bits) and allowed; two of them make 3500 bits, four make 3501."""
    term = "(2**1000)*(2**1000)*(2**1000)*(2**498)"  # no spaces: four terms must fit in 200 characters
    assert int(calculate(f"{term}+{term}")) == 2**3499  # 3500 bits: still allowed
    four = f"{term}+{term}+{term}+{term}"
    assert len(four) <= MAX_EXPRESSION_LENGTH
    with pytest.raises(ToolError, match="large"):
        calculate(four)


def test_a_keyword_argument_next_to_positional_ones_is_refused_not_ignored() -> None:
    with pytest.raises(ToolError, match="keyword"):
        calculate("sqrt(4, x=1)")


def test_a_function_name_used_as_a_value_says_to_call_it() -> None:
    with pytest.raises(ToolError, match="function"):
        calculate("sqrt + 1")


def test_a_long_literal_is_fine() -> None:
    assert calculate("9" * 150) == "9" * 150


def test_repeated_squaring_cannot_build_a_huge_number() -> None:
    expression = "((((((((((3 ** 9) ** 9) ** 9) ** 9) ** 9) ** 9) ** 9) ** 9) ** 9) ** 9)"
    with pytest.raises(ToolError, match="large"):
        calculate(expression)


def test_deeply_nested_operations_are_refused_without_crashing() -> None:
    # Parentheses add no nodes; a chain of unary minus does, and fits in the length limit.
    with pytest.raises(ToolError, match="deep"):
        calculate("-" * 150 + "1")
    assert calculate("-" * 20 + "1") == "1"


def test_the_message_of_a_refusal_never_contains_more_than_a_short_excerpt() -> None:
    with pytest.raises(ToolError) as info:
        calculate("__import__('os')" + " " * 100 + "#" + "x" * 5000)
    assert len(str(info.value)) < 300

"""The calculator tool: arithmetic through a safe parser, never ``eval`` (Phase 6 Req 5.8). Pure.

The expression is parsed with :mod:`ast` and walked by hand. Only these nodes are evaluated: numbers (``int`` and
``float``, not ``bool`` or complex), ``+ - * / // % **``, unary ``+`` and ``-``, a few math functions called by name,
and the constants ``pi`` and ``e``. Anything else is refused with a short message naming the kind of thing it was, never
repeating the text. Limits stop a small expression from doing a large amount of work: its length, its nesting, the size
of an exponent, and the size of an integer (checked *before* the multiplication or power that would build it).

A number comes back in the exact form Python prints it (``str`` for an integer, ``repr`` for a float), so what the model
reads is what ``ast.literal_eval`` reads back.
"""

from __future__ import annotations

import ast
import math
import operator
from collections.abc import Callable
from typing import Any, Final

from das.tools.types import ToolError

MAX_EXPRESSION_LENGTH: Final = 200
MAX_DEPTH: Final = 50
MAX_EXPONENT: Final = 1000
#: An integer result may use this many bits (about 10 to the power 1053): enough for any sum a model needs.
MAX_RESULT_BITS: Final = 3500
_MAX_NAME_SHOWN: Final = 30

_CONSTANTS: Final[dict[str, float]] = {"pi": math.pi, "e": math.e}


def _round(value: Any, digits: Any = None) -> Any:
    if digits is None:
        return round(value)
    if isinstance(digits, bool) or not isinstance(digits, int):
        raise ToolError("round digits must be an integer")
    return round(value, digits)


#: name -> (function, fewest arguments, most arguments)
_FUNCTIONS: Final[dict[str, tuple[Callable[..., Any], int, int]]] = {
    "sqrt": (math.sqrt, 1, 1),
    "abs": (abs, 1, 1),
    "min": (min, 1, 10),
    "max": (max, 1, 10),
    "round": (_round, 1, 2),
    "floor": (math.floor, 1, 1),
    "ceil": (math.ceil, 1, 1),
    "sin": (math.sin, 1, 1),
    "cos": (math.cos, 1, 1),
    "tan": (math.tan, 1, 1),
    "log": (math.log, 1, 1),
    "log10": (math.log10, 1, 1),
    "exp": (math.exp, 1, 1),
}

_BINARY: Final[dict[type[ast.operator], Callable[[Any, Any], Any]]] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
}


def _shown(name: str) -> str:
    return repr(name if len(name) <= _MAX_NAME_SHOWN else name[:_MAX_NAME_SHOWN] + "...")


def _checked(value: Any) -> Any:
    """A result the tool will hand back: a real, finite number whose integer part is not enormous."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ToolError("the result is not a real number")
    if isinstance(value, float) and not math.isfinite(value):
        raise ToolError("the result is not a finite number")
    if isinstance(value, int) and value.bit_length() > MAX_RESULT_BITS:
        raise ToolError("the result is too large")
    return value


def _power(base: Any, exponent: Any) -> Any:
    if isinstance(exponent, int) and abs(exponent) > MAX_EXPONENT:
        raise ToolError(f"the exponent is too large (at most {MAX_EXPONENT})")
    if isinstance(base, int) and isinstance(exponent, int) and exponent > 0:
        if abs(base).bit_length() * exponent > MAX_RESULT_BITS:  # refuse before building the number
            raise ToolError("the result would be too large")
    return base**exponent


def _binary(op: ast.operator, left: Any, right: Any) -> Any:
    if isinstance(op, ast.Pow):
        return _power(left, right)
    function = _BINARY.get(type(op))
    if function is None:
        raise ToolError(f"the operator {type(op).__name__} is not allowed in arithmetic")
    if isinstance(op, ast.Mult) and isinstance(left, int) and isinstance(right, int):
        if left.bit_length() + right.bit_length() > MAX_RESULT_BITS:  # refuse before building the number
            raise ToolError("the result would be too large")
    return function(left, right)


def _call(name: str, args: list[Any]) -> Any:
    entry = _FUNCTIONS.get(name)
    if entry is None:
        raise ToolError(f"unknown function {_shown(name)}")
    function, fewest, most = entry
    if not fewest <= len(args) <= most:
        count = f"{fewest}" if fewest == most else f"{fewest} to {most}"
        raise ToolError(f"{name} takes {count} argument(s)")
    return function(*args)


def _evaluate(node: ast.AST, depth: int) -> Any:
    if depth > MAX_DEPTH:
        raise ToolError("the expression is nested too deep")
    try:
        if isinstance(node, ast.Constant):
            if isinstance(node.value, bool) or not isinstance(node.value, (int, float)):
                raise ToolError("only real numbers are allowed")
            return _checked(node.value)
        if isinstance(node, ast.Name):
            if node.id in _CONSTANTS:
                return _CONSTANTS[node.id]
            if node.id in _FUNCTIONS:
                raise ToolError(f"{node.id} is a function: call it, as in {node.id}(x)")
            raise ToolError(f"unknown name {_shown(node.id)}")
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
            operand = _evaluate(node.operand, depth + 1)
            return _checked(-operand if isinstance(node.op, ast.USub) else +operand)
        if isinstance(node, ast.BinOp):
            left, right = _evaluate(node.left, depth + 1), _evaluate(node.right, depth + 1)
            return _checked(_binary(node.op, left, right))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.keywords:
                raise ToolError("keyword arguments are not allowed")
            return _checked(_call(node.func.id, [_evaluate(arg, depth + 1) for arg in node.args]))
        raise ToolError(f"{type(node).__name__} is not allowed in arithmetic")
    except ZeroDivisionError:
        raise ToolError("division by zero") from None
    except OverflowError:
        raise ToolError("the result is not a finite number") from None
    except ValueError:
        raise ToolError("math domain error: the argument is outside what the function accepts") from None


def calculate(expression: str) -> str:
    """The value of an arithmetic expression as text, or a :class:`ToolError` saying why not."""
    if not isinstance(expression, str):
        raise ToolError("the expression must be text")
    if len(expression) > MAX_EXPRESSION_LENGTH:
        raise ToolError(f"the expression is too long (at most {MAX_EXPRESSION_LENGTH} characters)")
    text = expression.strip()
    if not text:
        raise ToolError("the expression is empty")
    try:
        tree = ast.parse(text, mode="eval")
    except (SyntaxError, ValueError, RecursionError, MemoryError):
        raise ToolError("the expression is not valid arithmetic") from None
    value = _evaluate(tree.body, 0)
    return str(value) if isinstance(value, int) else repr(value)


__all__ = ["MAX_DEPTH", "MAX_EXPONENT", "MAX_EXPRESSION_LENGTH", "MAX_RESULT_BITS", "calculate"]

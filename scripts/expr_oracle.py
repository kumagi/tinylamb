#!/usr/bin/env python3
"""Independent Python oracle for tinylamb's scalar-expression fuzzer.

Reads Lisp-style S-expressions (one per line, from files or stdin) as
emitted by expression/expr_simplify_oracle.hpp (see ToSimplifySExpr) and
evaluates each under the *documented GoogleSQL scalar semantics* that
tinylamb implements -- NOT raw Python semantics. Every intentional
deviation from raw Python is marked [PY-DIFF] below.

Usage:
    printf '%s' '(add (i 1) (i 2))' | python3 scripts/expr_oracle.py
    python3 scripts/expr_oracle.py expr_oracle_fuzz-repro-*.test

The .test files carry `-- sexpr:` lines; the script extracts and checks
those, printing OK/ERROR/UNKNOWN per case. Compare its verdict with the
`-- reference:` line: any OK-vs-ERROR disagreement is a real bug in either
the engine or this script.

Output per case: `OK <value>`, `ERROR <message>`, or `UNKNOWN <reason>`.
UNKNOWN means this script does not model the case (e.g. float MOD); it is
a deliberate abstention, never a pass or fail.

Semantic mapping ([PY-DIFF] = differs from raw Python):
- NULL: typed ((n int|float|bool|string)); propagates through arithmetic,
  comparisons yield NULL, AND/OR/XOR follow Kleene three-valued logic.
- INT64: Python ints are unbounded, so add/sub/mul/neg overflow [PY-DIFF:
  raise ERROR instead of wrapping]; abs(INT64_MIN) is ERROR here while the
  C++ engine's std::abs is technically UB -- a known audit-only divergence.
- Division [PY-DIFF]: int/int is a *floating* division (17/5 -> 3.4);
  any division or MOD by zero raises ERROR (Python would give ZeroDivision
  for // but inf for float -- the engine raises in both cases).
- MOD [PY-DIFF]: sign follows the dividend (truncated division, like C);
  Python's % follows the divisor. Implemented as a - (trunc(a/b) * b).
- NaN: propagates through arithmetic; eq/ne/lt/... treat NaN as unordered
  (eq -> false, ne -> true, ordered -> false), matching IEEE.
- LIKE: SQL LIKE where % matches any run (possibly empty) and _ matches
  exactly one character; case-sensitive.
- IN: a hit returns TRUE even with NULLs present; else NULL if the target
  or any item is NULL, else FALSE.
- GREATEST/LEAST return NULL if any argument is NULL.
- Booleans are 1/0; TRUE/FALSE render as TRUE/FALSE, like the engine.
"""

import math
import re
import sys

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


class OracleError(Exception):
    pass


class Unknown(Exception):
    pass


# NULL is a singleton carrying its static type for rendering fidelity.
class Null:
    def __init__(self, type_name):
        self.type_name = type_name

    def __repr__(self):
        return "NULL"


def is_null(v):
    return isinstance(v, Null)


def check_int_range(v):
    if v < INT64_MIN or v > INT64_MAX:
        raise OracleError("integer overflow")
    return v


def to_float(v):
    if isinstance(v, bool):
        return float(v)
    return float(v)


def arith(op, a, b):
    if is_null(a) or is_null(b):
        return Null("float" if op == "div" else "int")
    ai = isinstance(a, int) and not isinstance(a, bool)
    bi = isinstance(b, int) and not isinstance(b, bool)
    if op == "mod":
        # Mirrors EvaluateBinary: NULL propagates; int/int uses truncated
        # division (sign follows the dividend); mixed int/double uses fmod
        # as a double (zero divisor raises); double/double raises
        # "unsupported binary operation".
        if ai and bi:
            if b == 0:
                raise OracleError("division by zero in MOD")
            q = abs(a) // abs(b)
            if (a < 0) != (b < 0):
                q = -q
            return check_int_range(a - q * b)
        if isinstance(a, float) and isinstance(b, float):
            raise OracleError("unsupported binary operation")
        if isinstance(a, bool) or isinstance(b, bool):
            raise OracleError("unsupported binary operation")
        import math
        if float(b) == 0.0:
            raise OracleError("division by zero")
        return math.fmod(float(a), float(b))
    if op == "div":
        # [PY-DIFF] GoogleSQL: integer division is floating.
        if (isinstance(b, (int, float)) and not isinstance(b, bool)
                and b == 0):
            raise OracleError("division by zero")
        return to_float(a) / to_float(b)
    if ai and bi:
        if op == "add":
            return check_int_range(a + b)
        if op == "sub":
            return check_int_range(a - b)
        if op == "mul":
            return check_int_range(a * b)
    x, y = to_float(a), to_float(b)
    if op == "add":
        return x + y
    if op == "sub":
        return x - y
    if op == "mul":
        return x * y
    raise OracleError("unknown arithmetic op: " + op)


def compare(op, a, b):
    if is_null(a) or is_null(b):
        return Null("bool")
    if isinstance(a, float) and isinstance(b, float):
        import math
        if math.isnan(a) or math.isnan(b):
            if op == "ne":
                return True
            return False
    if isinstance(a, str) or isinstance(b, str):
        if not (isinstance(a, str) and isinstance(b, str) and
                op in ("eq", "ne")):
            raise Unknown("non-equality string comparison is not modeled")
    if op == "eq":
        return a == b
    if op == "ne":
        return a != b
    if op == "lt":
        return a < b
    if op == "le":
        return a <= b
    if op == "gt":
        return a > b
    if op == "ge":
        return a >= b
    raise OracleError("unknown comparison op: " + op)


def truthy(v):
    if is_null(v):
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return len(v) > 0
    raise OracleError("non-boolean condition")


def logic(op, a, b):
    ta = truthy(a)
    if op == "not":
        return None if ta is None else (not ta)
    tb = truthy(b)
    if op == "and":
        if ta is False or tb is False:
            return False
        if ta is True and tb is True:
            return True
        return None
    if op == "or":
        if ta is True or tb is True:
            return True
        if ta is False and tb is False:
            return False
        return None
    if op == "xor":
        if ta is None or tb is None:
            return None
        return ta != tb
    raise OracleError("unknown logic op: " + op)


def like_match(value, pattern):
    # Iterative matcher for % (any run) and _ (exactly one char).
    if is_null(value) or is_null(pattern):
        return Null("bool")
    if not isinstance(value, str) or not isinstance(pattern, str):
        raise OracleError("LIKE requires strings")

    def rec(si, pi):
        while pi < len(pattern):
            c = pattern[pi]
            if c == "%":
                for k in range(len(value) - si, -1, -1):
                    if rec(si + k, pi + 1):
                        return True
                return False
            if si >= len(value):
                return False
            if c != "_" and c != value[si]:
                return False
            si += 1
            pi += 1
        return si == len(value)

    return rec(0, 0)


def eval_in(target, items):
    if not items:
        return False
    saw_null = is_null(target)
    for item in items:
        if is_null(item):
            saw_null = True
            continue
        if not is_null(target):
            try:
                if compare("eq", target, item) is True:
                    return True
            except Unknown:
                saw_null = True
    if saw_null:
        return Null("bool")
    return False


def eval_nullif(a, b):
    # Mirrors the engine (and BigQuery): only a TRUE equality yields NULL.
    # A NULL on either side is never TRUE, so NULLIF(x, NULL) is x and
    # NULLIF(NULL, x) is NULL.
    if is_null(a):
        return Null("int")
    if is_null(b):
        return a
    return Null("int") if compare("eq", a, b) is True else a




def eval_cast(tag, v):
    if is_null(v):
        kind = {"cast-int": "int", "cast-float": "float",
                "cast-bool": "bool"}.get(tag, "string")
        return Null(kind)
    if tag == "cast-int":
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, int):
            return check_int_range(v)
        if isinstance(v, float):
            import math
            if math.isnan(v) or math.isinf(v):
                raise OracleError("cannot cast NaN/Inf to INT64")
            # [PY-DIFF] the engine rounds half away from zero (std::round),
            # not Python's banker's rounding and not truncation.
            rounded = math.floor(v + 0.5) if v >= 0 else math.ceil(v - 0.5)
            if rounded >= 2**63 or rounded < -(2**63):
                raise OracleError("int overflow casting from float")
            return check_int_range(rounded)
        raise OracleError("cannot cast to INT64")
    if tag == "cast-float":
        if isinstance(v, bool):
            return float(v)
        if isinstance(v, (int, float)):
            return float(v)
        raise OracleError("cannot cast to FLOAT64")
    if tag == "cast-bool":
        t = truthy(v)
        return Null("bool") if t is None else t
    if tag == "cast-string":
        return render(v)
    raise OracleError("unknown cast: " + tag)


def eval_node(node):
    op = node[0]
    if op == "i":
        return check_int_range(node[1])
    if op == "f":
        return node[1]
    if op == "b":
        return node[1]
    if op == "n":
        return Null(node[1])
    if op == "s":
        return node[1]
    if op in ("add", "sub", "mul", "div", "mod"):
        return arith(op, eval_node(node[1]), eval_node(node[2]))
    if op in ("eq", "ne", "lt", "le", "gt", "ge"):
        r = compare(op, eval_node(node[1]), eval_node(node[2]))
        return r if is_null(r) else bool(r)
    if op in ("and", "or", "xor"):
        if op == "or":
            # Short-circuits like the engine: TRUE OR x is TRUE without
            # evaluating x, so errors in x never surface.
            left = eval_node(node[1])
            if truthy(left) is True:
                return True
            r = logic(op, left, eval_node(node[2]))
            return Null("bool") if r is None else bool(r)
        if op == "and":
            # Short-circuits like the engine: FALSE AND x is FALSE.
            left = eval_node(node[1])
            if truthy(left) is False:
                return False
            r = logic(op, left, eval_node(node[2]))
            return Null("bool") if r is None else bool(r)
        r = logic(op, eval_node(node[1]), eval_node(node[2]))
        return Null("bool") if r is None else bool(r)
    if op == "not":
        r = logic(op, eval_node(node[1]), None)
        return Null("bool") if r is None else bool(r)
    if op == "neg":
        a = eval_node(node[1])
        if is_null(a):
            return Null("int")
        if isinstance(a, bool):
            return -int(a)
        if isinstance(a, int):
            return check_int_range(-a)
        return -a
    if op in ("isnull", "isnotnull", "istrue", "isnottrue",
              "isfalse", "isnotfalse"):
        a = eval_node(node[1])
        if op == "isnull":
            return is_null(a)
        if op == "isnotnull":
            return not is_null(a)
        t = truthy(a)
        if t is None:
            # IS TRUE/FALSE over NULL is FALSE; IS NOT ... is TRUE.
            return op in ("isnottrue", "isnotfalse")
        if op == "istrue":
            return t is True
        if op == "isnottrue":
            return t is not True
        if op == "isfalse":
            return t is False
        if op == "isnotfalse":
            return t is not False
    if op in ("like", "nlike"):
        r = like_match(eval_node(node[1]), eval_node(node[2]))
        if is_null(r):
            return Null("bool")
        return bool(r) if op == "like" else (not r)
    if op == "in":
        r = eval_in(eval_node(node[1]),
                    [eval_node(item) for item in node[2:]])
        return r if is_null(r) else bool(r)
    if op == "case":
        # Lazy like the engine: conditions evaluate in order and only the
        # taken branch's value is evaluated, so errors in untaken branches
        # never surface.
        *branches, otherwise = node[1:]
        for branch in branches:
            if (not isinstance(branch, list) or len(branch) != 3 or
                    branch[0] != "pair"):
                raise OracleError("malformed case branch")
            if truthy(eval_node(branch[1])) is True:
                return eval_node(branch[2])
        return eval_node(otherwise)
    if op in ("abs",):
        a = eval_node(node[1])
        if is_null(a):
            return Null("int")
        if isinstance(a, bool):
            return int(a)
        if isinstance(a, int):
            if a == INT64_MIN:
                raise OracleError("integer overflow in ABS")
            return abs(a)
        return abs(a)
    if op in ("greatest", "least"):
        vals = [eval_node(a) for a in node[1:]]
        if any(is_null(v) for v in vals):
            return Null("int")
        try:
            return max(vals) if op == "greatest" else min(vals)
        except TypeError:
            raise OracleError("greatest/least over mixed types")
    if op == "coalesce":
        for a in node[1:]:
            v = eval_node(a)
            if not is_null(v):
                return v
        return Null("int")
    if op == "nullif":
        return eval_nullif(eval_node(node[1]), eval_node(node[2]))
    if op == "ifnull":
        a = eval_node(node[1])
        return eval_node(node[2]) if is_null(a) else a
    if op == "sign":
        a = eval_node(node[1])
        if is_null(a):
            return Null("int")
        if isinstance(a, int) and not isinstance(a, bool):
            return 1 if a > 0 else (-1 if a < 0 else 0)
        if isinstance(a, float):
            if math.isnan(a):
                return a
            return 1.0 if a > 0.0 else (-1.0 if a < 0.0 else 0.0)
        raise OracleError("SIGN requires numeric argument")
    if op in ("safe_add", "safe_subtract", "safe_multiply"):
        l = eval_node(node[1])
        r = eval_node(node[2])
        if is_null(l) or is_null(r):
            return Null("int")
        if (not isinstance(l, (int, float)) or isinstance(l, bool)
                or not isinstance(r, (int, float)) or isinstance(r, bool)):
            raise OracleError(op + " requires numeric arguments")
        if isinstance(l, int) and isinstance(r, int):
            res = (l + r if op == "safe_add" else l - r
                   if op == "safe_subtract" else l * r)
            # SAFE_* returns NULL on overflow instead of raising.
            return Null("int") if res > INT64_MAX or res < INT64_MIN else res
        lf, rf = to_float(l), to_float(r)
        res = (lf + rf if op == "safe_add" else lf - rf
               if op == "safe_subtract" else lf * rf)
        return Null("float") if math.isinf(res) or math.isnan(res) else res
    if op == "safe_negate":
        a = eval_node(node[1])
        if is_null(a):
            return Null("int")
        if isinstance(a, int) and not isinstance(a, bool):
            return Null("int") if a == INT64_MIN else -a
        if isinstance(a, float):
            return -a
        raise OracleError("SAFE_NEGATE requires numeric argument")
    if op in ("ceil", "ceiling", "floor"):
        a = eval_node(node[1])
        if is_null(a):
            return Null("float")
        if isinstance(a, int) and not isinstance(a, bool):
            return a
        if isinstance(a, float):
            return float(math.ceil(a) if op != "floor" else math.floor(a))
        raise OracleError(op + " requires numeric argument")
    if op in ("round", "trunc", "truncate"):
        a = eval_node(node[1])
        digits = eval_node(node[2]) if len(node) > 2 else 0
        if is_null(a) or is_null(digits):
            return Null("float")
        if not isinstance(digits, int) or isinstance(digits, bool):
            raise OracleError(op + " digits must be an integer")
        if isinstance(a, int) and not isinstance(a, bool):
            if digits >= 0:
                return a
            # [PY-DIFF] integer rounding is half away from zero at the
            # 10^|digits| scale; |digits| beyond int64 saturates the scale.
            trunc_digits = (INT64_MAX if digits == INT64_MIN else -digits)
            scale = 1
            for _ in range(trunc_digits):
                if scale > INT64_MAX // 10:
                    return 0
                scale *= 10
            q = abs(a) // scale
            rem = abs(a) % scale
            if op == "round" and rem >= scale // 2:
                q += 1
            res = q * scale
            if res > INT64_MAX:
                raise OracleError("integer overflow in " + op.upper())
            return -res if a < 0 else res
        if isinstance(a, float):
            if math.isnan(a):
                return a
            factor = 10.0**digits
            x = a * factor
            if op == "round":
                # std::round: half away from zero.
                res = math.copysign(math.floor(abs(x) + 0.5), x)
            else:
                res = math.trunc(x)
            return res / factor
        raise OracleError(op + " requires numeric argument")
    if op in ("fdiv", "fmod"):
        # Function-call DIV/MOD (see f-prefixed tokens in SerializeSExpr):
        # int/int is truncated C division/remainder, NOT the floating
        # division of binary "div".  Mixed numeric coerces to double:
        # fdiv truncates toward zero, fmod is fmod.
        l = eval_node(node[1])
        r = eval_node(node[2])
        if is_null(l) or is_null(r):
            return Null("int" if op == "fdiv" else "float")
        li = isinstance(l, int) and not isinstance(l, bool)
        ri = isinstance(r, int) and not isinstance(r, bool)
        if li and ri:
            if r == 0:
                raise OracleError("division by zero in " +
                                  ("DIV" if op == "fdiv" else "MOD"))
            if l == INT64_MIN and r == -1:
                raise OracleError("integer overflow in " +
                                  ("DIV" if op == "fdiv" else "'%'"))
            q = abs(l) // abs(r)
            if (l < 0) != (r < 0):
                q = -q
            return q if op == "fdiv" else l - q * r
        if op == "fmod" and isinstance(l, float) and isinstance(r, float):
            raise OracleError("unsupported binary operation")
        lf, rf = to_float(l), to_float(r)
        if rf == 0.0:
            raise OracleError("division by zero" +
                              (" in DIV" if op == "fdiv" else ""))
        if op == "fmod":
            return math.fmod(lf, rf)
        q = math.trunc(lf / rf)
        if (math.isnan(q) or math.isinf(q) or q < INT64_MIN
                or q > INT64_MAX):
            raise OracleError("DIV result out of range for INT64")
        return int(q)
    if op in ("ieee_divide", "safe_divide"):
        l = eval_node(node[1])
        r = eval_node(node[2])
        if is_null(l) or is_null(r):
            return Null("float")
        lf, rf = to_float(l), to_float(r)
        if rf == 0.0:
            if op == "safe_divide":
                return Null("float")
            if lf == 0.0:
                return float("nan")
            return math.copysign(float("inf"), lf)
        res = lf / rf
        if op == "safe_divide" and (math.isinf(res) or math.isnan(res)):
            return Null("float")
        return res
    if op == "sqrt":
        a = eval_node(node[1])
        if is_null(a):
            return Null("float")
        v = to_float(a)
        if v < 0.0:
            raise OracleError("SQRT of negative number")
        return math.sqrt(v)
    if op == "cbrt":
        a = eval_node(node[1])
        if is_null(a):
            return Null("float")
        # math.cbrt needs Python >= 3.11; fall back to libm via ctypes.
        try:
            return math.cbrt(to_float(a))
        except AttributeError:
            import ctypes
            libm = ctypes.CDLL("libm.so.6")
            libm.cbrt.restype = ctypes.c_double
            libm.cbrt.argtypes = [ctypes.c_double]
            return libm.cbrt(to_float(a))
    if op in ("ln", "log", "log10"):
        # [PY-DIFF] out-of-range inputs return -inf/NaN like libm, not
        # Python's ValueError.  log takes an optional base.
        a = eval_node(node[1])
        if is_null(a):
            return Null("float")
        v = to_float(a)
        if len(node) > 2:
            bv = eval_node(node[2])
            if is_null(bv):
                return Null("float")
            base = to_float(bv)
        else:
            base = None
        if op == "log10":
            res = math.log10(v) if v > 0 else (
                float("nan") if v < 0 else float("-inf"))
            return res
        logv = math.log(v) if v > 0 else (
            float("nan") if v < 0 else float("-inf"))
        if base is None:
            return logv
        if v == 1.0 and math.isinf(base):
            return float("nan")
        logb = math.log(base) if base > 0 else (
            float("nan") if base < 0 else float("-inf"))
        return logv / logb
    if op == "exp":
        a = eval_node(node[1])
        if is_null(a):
            return Null("float")
        v = to_float(a)
        try:
            res = math.exp(v)
        except OverflowError:
            res = float("inf")
        if math.isinf(res) and not math.isinf(v):
            raise OracleError("Floating point overflow in function: EXP")
        return res
    if op in ("cos", "sin", "tan", "acos", "asin", "atan", "cosh", "sinh",
              "tanh"):
        a = eval_node(node[1])
        if is_null(a):
            return Null("float")
        v = to_float(a)
        try:
            res = getattr(math, op)(v)
        except (ValueError, OverflowError):
            res = float("nan") if op != "atanh" else float("nan")
        return res
    if op in ("pow", "power"):
        l = eval_node(node[1])
        r = eval_node(node[2])
        if is_null(l) or is_null(r):
            return Null("float")
        lf, rf = to_float(l), to_float(r)
        if (lf < 0.0 and not math.isinf(lf) and not math.isnan(rf)
                and math.floor(rf) != rf):
            raise OracleError("Floating point error in function: POW")
        if lf == 0.0 and rf < 0.0 and not math.isinf(rf):
            raise OracleError("division by zero in POW")
        try:
            res = math.pow(lf, rf)
        except (ValueError, OverflowError):
            res = float("nan") if lf < 0 else float("inf")
        if math.isinf(res) and not math.isinf(lf) and not math.isinf(rf):
            raise OracleError("Floating point overflow in function: POW")
        return res
    if op in ("length", "octet_length", "byte_length", "char_length",
              "character_length"):
        a = eval_node(node[1])
        if is_null(a):
            return Null("int")
        if not isinstance(a, str):
            raise OracleError(op + " requires string argument")
        # length/byte_length count bytes; char_length counts code points.
        return len(a) if op in ("char_length",
                                "character_length") else len(a.encode())
    if op == "ascii":
        a = eval_node(node[1])
        if is_null(a):
            return Null("int")
        return a.encode()[0] if a else 0
    if op == "unicode":
        a = eval_node(node[1])
        if is_null(a):
            return Null("int")
        return ord(a[0]) if a else 0
    if op == "chr":
        a = eval_node(node[1])
        if is_null(a):
            return Null("string")
        if not isinstance(a, int) or isinstance(a, bool):
            raise OracleError("CHR requires an integer argument")
        if a < 0 or a > 0x10FFFF or 0xD800 <= a <= 0xDFFF:
            raise OracleError("CHR argument out of range")
        return chr(a)
    if op in ("substr", "substring"):
        s = eval_node(node[1])
        start = eval_node(node[2])
        length = eval_node(node[3]) if len(node) > 3 else None
        if is_null(s) or is_null(start) or is_null(length):
            return Null("string")
        if not isinstance(s, str):
            raise OracleError("SUBSTR argument type mismatch")
        if length is not None:
            if length < 0:
                raise OracleError("SUBSTR length cannot be negative")
            if length == 0:
                return ""
        if start > 0:
            idx = start - 1
        elif start < 0:
            idx = len(s) + start
        else:
            idx = 0
        idx = max(idx, 0)
        if idx >= len(s):
            return ""
        return s[idx:] if length is None else s[idx:idx + length]
    if op in ("upper", "lower"):
        a = eval_node(node[1])
        if is_null(a):
            return Null("string")
        # Engine applies C toupper/tolower per byte (ASCII-only).
        if op == "upper":
            return "".join(
                chr(ord(c) - 32) if "a" <= c <= "z" else c for c in a)
        return "".join(
            chr(ord(c) + 32) if "A" <= c <= "Z" else c for c in a)
    if op in ("trim", "ltrim", "rtrim"):
        s = eval_node(node[1])
        cutset = eval_node(node[2]) if len(node) > 2 else " \t\n\r\x0b\x0c"
        if is_null(s) or is_null(cutset):
            return Null("string")
        if op == "trim":
            return s.strip(cutset)
        return s.lstrip(cutset) if op == "ltrim" else s.rstrip(cutset)
    if op == "replace":
        s, frm, to = (eval_node(n) for n in node[1:4])
        if is_null(s) or is_null(frm) or is_null(to):
            return Null("string")
        return s if frm == "" else s.replace(frm, to)
    if op == "concat":
        vals = [eval_node(n) for n in node[1:]]
        if any(is_null(v) for v in vals):
            return Null("string")
        if not all(isinstance(v, str) for v in vals):
            raise OracleError("CONCAT currently requires string arguments")
        return "".join(vals)
    if op in ("starts_with", "ends_with"):
        s = eval_node(node[1])
        p = eval_node(node[2])
        if is_null(s) or is_null(p):
            return Null("bool")
        return s.startswith(p) if op == "starts_with" else s.endswith(p)
    if op in ("strpos", "instr"):
        hay = eval_node(node[1])
        needle = eval_node(node[2])
        if is_null(hay) or is_null(needle):
            return Null("int")
        # Engine searches bytes; encode both for multi-byte parity.
        pos = hay.encode().find(needle.encode())
        return 0 if pos < 0 else pos + 1
    if op in ("lpad", "rpad"):
        s = eval_node(node[1])
        target = eval_node(node[2])
        pad = eval_node(node[3]) if len(node) > 3 else " "
        if is_null(s) or is_null(target) or is_null(pad):
            return Null("string")
        if target < 0:
            raise OracleError(
                "Second argument (output size) for LPAD/RPAD cannot be "
                "negative")
        if target > 1000000:
            raise OracleError(
                "Output of LPAD/RPAD exceeds max allowed output size of 1MB")
        if target == 0:
            return ""
        if pad == "":
            raise OracleError("Pattern in LPAD/RPAD cannot be empty")
        if target <= len(s):
            return s[:target]
        needed = target - len(s)
        pad_str = pad * (needed // len(pad)) + pad[:needed % len(pad)]
        return pad_str + s if op == "lpad" else s + pad_str
    if op == "reverse":
        a = eval_node(node[1])
        if is_null(a):
            return Null("string")
        return a[::-1]
    if op == "repeat":
        s = eval_node(node[1])
        n = eval_node(node[2])
        if is_null(s) or is_null(n):
            return Null("string")
        if n < 0:
            raise OracleError(
                "Second argument (repeat count) for REPEAT cannot be "
                "negative")
        if n == 0 or s == "":
            return ""
        if len(s.encode()) * n > 1000000:
            raise OracleError(
                "Output of REPEAT exceeds max allowed output size of 1MB")
        return s * n
    if op in ("left", "right", "byte_left", "byte_right"):
        s = eval_node(node[1])
        n = eval_node(node[2])
        if is_null(s) or is_null(n):
            return Null("string")
        if n < 0:
            raise OracleError("Second argument (length) for " +
                              op.upper() + " cannot be negative")
        if n == 0:
            return ""
        return s[:n] if op.endswith("left") else s[max(0, len(s) - n):]
    if op in ("__bit_and", "__bit_or", "__bit_xor"):
        l = eval_node(node[1])
        r = eval_node(node[2])
        if is_null(l) or is_null(r):
            return Null("int")
        res = (l & r if op == "__bit_and" else l | r
               if op == "__bit_or" else l ^ r)
        # Wrap into signed int64 range.
        res &= (1 << 64) - 1
        return res - (1 << 64) if res > INT64_MAX else res
    if op in ("__shift_left", "__shift_right"):
        l = eval_node(node[1])
        r = eval_node(node[2])
        if is_null(l) or is_null(r):
            return Null("int")
        if r < 0:
            raise OracleError("Bitwise shift by negative offset.")
        if r >= 64:
            return 0
        ul = l & ((1 << 64) - 1)
        res = (ul << r) & ((1 << 64) - 1) if op == "__shift_left" else ul >> r
        return res - (1 << 64) if res > INT64_MAX else res
    if op in ("shl", "shr", "bitnot"):
        a = eval_node(node[1])
        b = eval_node(node[2]) if len(node) > 2 else None
        if is_null(a) or is_null(b):
            return Null("int")
        if op == "bitnot":
            return check_int_range(~a)
        # Binary << / >> mirror the __shift_* functions.
        if b < 0:
            raise OracleError("Bitwise shift by negative offset.")
        if b >= 64:
            return 0
        ua = a & ((1 << 64) - 1)
        res = (ua << b) & ((1 << 64) - 1) if op == "shl" else ua >> b
        return res - (1 << 64) if res > INT64_MAX else res
    if op.startswith("cast-"):
        return eval_cast(op, eval_node(node[1]))
    raise OracleError("unknown op: " + op)


def render_float(v):
    # Match C++ std::to_chars shortest round-trip used by
    # FormatSimplifyValue: integral doubles render without ".0".
    s = repr(v)
    if s.endswith(".0") and "e" not in s and "E" not in s:
        s = s[:-2]
    return s


def render(v):
    if is_null(v):
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        import math
        if math.isnan(v):
            return "nan"
        if math.isinf(v):
            return "inf" if v > 0 else "-inf"
        return render_float(v)
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)


TOKEN_RE = re.compile(r"\s*(\(|\)|'[^']*(?:''[^']*)*'|[^()\s]+)")


def tokenize(text):
    pos = 0
    tokens = []
    while pos < len(text):
        if text[pos].isspace():
            pos += 1
            continue
        m = TOKEN_RE.match(text, pos)
        if not m:
            raise OracleError("tokenize error at: " + text[pos:])
        tokens.append(m.group(1))
        pos = m.end()
    return tokens


def parse_operand(tokens, pos):
    # Parses one operand, wrapping bare atoms into typed literal nodes so
    # the evaluator never sees an ambiguous raw atom.
    tok = tokens[pos]
    if tok == "(":
        return parse_list(tokens, pos)
    if tok.startswith("'"):
        return ["s", tok[1:-1].replace("''", "'")], pos + 1
    if re.fullmatch(r"-?\d+", tok):
        return ["i", int(tok)], pos + 1
    if tok in ("nan", "inf", "-inf"):
        import math
        return ["f", {"nan": math.nan, "inf": math.inf,
                      "-inf": -math.inf}[tok]], pos + 1
    try:
        return ["f", float(tok)], pos + 1
    except ValueError:
        pass
    if tok == "true":
        return ["b", True], pos + 1
    if tok == "false":
        return ["b", False], pos + 1
    if tok in ("int", "float", "bool", "string"):
        return tok, pos + 1  # type-name payload of (n ...)
    raise OracleError("unexpected atom: " + tok)


def parse_list(tokens, pos):
    assert tokens[pos] == "("
    pos += 1
    if tokens[pos] == "(":
        # Branch pair (cond val) used by case: not an application.
        cond, pos = parse_operand(tokens, pos)
        val, pos = parse_operand(tokens, pos)
        if tokens[pos] != ")":
            raise OracleError("malformed branch pair")
        return ["pair", cond, val], pos + 1
    if tokens[pos] == ")":
        raise OracleError("expected operator")
    op = tokens[pos]
    pos += 1
    if op in ("i", "f", "b", "s", "n"):
        # Literal constructor: the payload is raw, not a typed operand.
        payload, pos = parse_literal_payload(tokens, pos, op)
        if tokens[pos] != ")":
            raise OracleError("malformed literal")
        return [op, payload], pos + 1
    items = [op]
    while tokens[pos] != ")":
        node, pos = parse_operand(tokens, pos)
        items.append(node)
    return items, pos + 1


def parse_literal_payload(tokens, pos, op):
    import math
    tok = tokens[pos]
    if op == "n":
        if tok not in ("int", "float", "bool", "string"):
            raise OracleError("bad null type: " + tok)
        return tok, pos + 1
    if op == "s":
        if not tok.startswith("'"):
            raise OracleError("bad string literal: " + tok)
        return tok[1:-1].replace("''", "'"), pos + 1
    if op == "b":
        if tok == "true":
            return True, pos + 1
        if tok == "false":
            return False, pos + 1
        raise OracleError("bad bool literal: " + tok)
    if op == "i":
        if re.fullmatch(r"-?\d+", tok):
            return int(tok), pos + 1
        raise OracleError("bad int literal: " + tok)
    if op == "f":
        if tok == "nan":
            return math.nan, pos + 1
        if tok == "inf":
            return math.inf, pos + 1
        if tok == "-inf":
            return -math.inf, pos + 1
        try:
            return float(tok), pos + 1
        except ValueError:
            raise OracleError("bad float literal: " + tok)
    raise OracleError("unknown literal: " + op)


def parse(tokens, pos=0):
    return parse_operand(tokens, pos)


def normalize(node):
    # The parser already emits typed nodes; kept as an identity hook.
    return node


def evaluate(text):
    node = normalize(parse(tokenize(text.strip()))[0])
    return render(eval_node(node))


def values_match(actual_rendered, expected):
    # Compares an evaluated rendering against a C++ reference rendering
    # (FormatSimplifyValue). Floats compare numerically so integral doubles
    # agree regardless of fixed/scientific rendering; TRUE/FALSE accept 1/0
    # because the engine stores booleans as INT64.
    if expected.startswith("THROW:"):
        return actual_rendered.startswith("ERROR")
    if not actual_rendered.startswith("OK "):
        return False
    actual = actual_rendered[3:]
    if actual == expected:
        return True
    bool_map = {"TRUE": "1", "FALSE": "0"}
    if bool_map.get(actual, actual) == expected:
        return True
    try:
        fa, fb = float(actual), float(expected)
        import math
        if math.isnan(fa) and math.isnan(fb):
            return True
        return fa == fb
    except ValueError:
        return False


def extract_sexprs(lines):
    cases = []
    for line in lines:
        line = line.rstrip("\n")
        if line.startswith("-- sexpr: "):
            cases.append(line[len("-- sexpr: "):])
        elif line and not line.startswith("--") and line.startswith("("):
            cases.append(line)
    return cases


def main(argv):
    lines = []
    if len(argv) > 1:
        for path in argv[1:]:
            with open(path) as f:
                lines.extend(f.readlines())
    else:
        lines = sys.stdin.readlines()
    # Batch cross-check mode: TSV lines "sexpr<TAB>expected" (e.g. dumped
    # from the C++ generator) are verified pairwise instead of printed.
    # Checked first: corpus lines start with a seed, not "(".
    paired = [line for line in lines
              if line and not line.startswith("--") and "\t" in line]
    if paired:
        bad = 0
        for line in paired:
            # Accepts "sexpr<TAB>expected" and "seed<TAB>sexpr<TAB>expected".
            parts = line.rstrip("\n").split("\t")
            if len(parts) == 3:
                _, sexpr, expected = parts
            elif len(parts) == 2:
                sexpr, expected = parts
            else:
                continue
            expected = expected.strip()
            try:
                verdict = "OK " + evaluate(sexpr)
            except Unknown as e:
                print("UNKNOWN\t" + sexpr + "\t" + str(e))
                continue
            except OracleError as e:
                verdict = "ERROR " + str(e)
            except Exception as e:  # noqa: BLE001
                print("UNKNOWN\t" + sexpr + "\tinternal: " + str(e))
                bad = 1
                continue
            if values_match(verdict, expected):
                print("MATCH\t" + sexpr)
            else:
                print("MISMATCH\t" + sexpr + "\texpected=" + expected +
                      "\tactual=" + verdict)
                bad = 1
        return bad
    cases = extract_sexprs(lines)
    if not cases:
        print("no S-expressions found", file=sys.stderr)
        return 2
    failed = 0
    for sexpr in cases:
        try:
            print("OK " + evaluate(sexpr))
        except Unknown as e:
            print("UNKNOWN " + str(e))
        except OracleError as e:
            print("ERROR " + str(e))
        except Exception as e:  # noqa: BLE001 -- oracle must not crash
            print("UNKNOWN internal: " + str(e))
            failed = 1
    return failed


if __name__ == "__main__":
    sys.exit(main(sys.argv))

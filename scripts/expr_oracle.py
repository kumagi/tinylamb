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

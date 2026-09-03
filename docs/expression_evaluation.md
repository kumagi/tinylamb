# Expression evaluation policy

## Canonical semantics

**`EvaluateBinary` / `Expression::Evaluate` on the AST is the semantic reference**
for scalar SQL expressions. Any fast path must produce the same `Value` results
for the rows and types it claims to support.

## Bytecode as the batch IR

Vectorized executors (`Selection`, `Projection`) compile supported expression
subtrees into a **`BytecodeProgram`** via `BytecodeCompiler`:

1. `ExpressionRewriter` constant-folds where possible.
2. Supported nodes become a stack program (`BytecodeOp` + typed operands).
3. `BytecodeProgram::EvaluateBatch` evaluates every row using **`EvaluateBinary`
   and `EvaluateUnary`** — the same helpers the AST path uses.

If bytecode compilation fails, the executor falls back to per-row
`Expression::Evaluate`.

### Short-circuit AND/OR (D7, `docs/design.md`)

`AND` and `OR` compile to control flow, not an eager binary op, so the VM
skips the right operand exactly when the AST evaluator does and the three
evaluation paths (AST / Bytecode / JIT) keep identical truth tables **and
identical error behaviour**:

```
AND:  <left>  JumpIfFalse -> end  <right>  BinaryInt64(AND)   end:
OR :  <left>  JumpIfTrue  -> end  <right>  BinaryInt64(OR)    end:
```

The jump peeks the stack (does not pop), so every path reaches `end` with one
value. NULL never jumps (three-valued logic evaluates the right operand
through `EvaluateBinary`). Consequently `FALSE AND rhs` and `TRUE OR rhs`
never evaluate `rhs`, so a division-by-zero or other error in `rhs` is
suppressed on exactly the rows the AST suppresses it on. The VM runs under a
program counter (previously a range-for) to support these jumps. The JIT
kernels do not compile AND/OR (they are comparison/projection/sum only), so
the short-circuit path never reaches the JIT. Coverage lives in
`differential_test.cpp` (`Evaluate_LogicalShortCircuitErrors_MatchAcrossPaths`)
and `bytecode_test.cpp` (`D7_*`).

**Bytecode is the authoritative description of what a batch kernel may assume**
about an expression (column slots, constants, operators, result type).

## JIT as a bytecode compiler

LLVM kernels (`JitInt64Kernels`) are **not a third semantic definition**. They
are optional native code for a narrow subset of bytecode programs:

| Kernel | Bytecode shape | Notes |
| --- | --- | --- |
| INT64 filter | `LoadColumn`, `LoadConstant`, `BinaryInt64` compare | Selection derives `column`, `constant`, and `op` from bytecode (`selection.cpp`). Used only when the batch has **no NULLs** in that column (see `docs/jit_profile.md`). |
| INT64 projection | linear `LoadColumn` + `BinaryInt64` multiply/add | Projection fast path |
| SUM | aggregate over INT64 column | Parallel aggregation |

Promotion rules (e.g. Selection waits until ~20M rows seen) live in executor
code and `docs/jit_profile.md`. **New JIT shapes must be justified against an
existing bytecode program**, not against ad-hoc AST rewrites.

## Three paths today (intentional, not three semantics)

| Path | Role |
| --- | --- |
| AST `Evaluate` | General scalar evaluation; relational executor, joins, uncached plans |
| Bytecode interpreter | Default batch path; defines JIT-eligible shapes |
| LLVM JIT | Native implementation of selected bytecode programs |

**Do not add a fourth interpretation.** When extending comparisons, dates, or
NULL rules, update `EvaluateBinary` first, then bytecode opcodes, then JIT only
if a matching bytecode pattern exists.

## Testing

`bytecode_test.SemanticsMatchAstAndJitOnInt64Compares` checks AST, bytecode, and
JIT filter kernels agree on simple INT64 column-vs-constant compares (non-NULL
rows for JIT). Add similar tests when new bytecode→JIT mappings are introduced.

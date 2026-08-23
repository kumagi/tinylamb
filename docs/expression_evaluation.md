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

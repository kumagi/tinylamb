# `expression/` — Layer 7 three-tier evaluation (AST / Bytecode / JIT)

`Expression::Evaluate` on the AST is the **semantic reference**. Bytecode and
JIT are fast paths only — never introduce divergent behavior.
`differential_test.cpp` pins parity over operators × NULL combinations ×
edge cases (div-by-zero, `INT64_MIN/-1`, overflow, type mismatch).

## Key files

- `expression.hpp` (`ExpressionBase` + `Expression = shared_ptr` +
  `ColumnValueExp/BinaryExpressionExp/…` factories), `evaluation_context.hpp`
  (DB abstraction — depends on this, not on `database/`), one file pair per
  operator: `binary/unary/aggregate/case/in/function_call/query/interval/
  array/cast/lambda/window/named/column_value`, plus `sql_udf.*`.
- `bytecode.{hpp,cpp}` — stack VM (`kLoadColumn/kLoadConstant/
  kBinaryInt64/Double/Varchar/Date/kUnary…/kJumpIfFalse/kJumpIfTrue/kJump`).
  Short-circuit jumps preserve three-valued AND/OR + error conditions;
  unsupported shapes return `nullopt`/false and fall back to AST
  (env var `TINYLAMB_DISABLE_BYTECODE` to disable).
- `jit.{hpp,cpp}` — optional LLVM ORC (`TINYLAMB_HAS_LLVM`), narrow non-NULL
  INT64 kernels only (`Filter/Project/Sum` + overflow-checked variants that
  throw like the AST); process-lifetime function-pointer cache.
- `rewrite.{hpp,cpp}` (`ExpressionPattern/Rule/RuleSet/Rewriter`,
  `ExpressionRuleSet::Default`: folding, boolean identity, De Morgan,
  comparison canonicalization), `proto_text.*`/`proto_schema.*`
  (serialization), `expr_simplify_oracle.*` (fuzz oracle).

## Rules for new operators

AST first, then Bytecode, then (only if INT64-narrow) JIT. Run
`differential_test`, `bytecode_test`, `jit_test`, `rewrite_test`,
`expression_test`, `function_call_test`.

Note: `expression/bytecode.* -> executor/data_chunk.hpp` is an allowlisted
edge (S6/A1 separation candidate) — do not add more.

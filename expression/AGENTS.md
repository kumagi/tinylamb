# `expression/` — Layer 7 three-tier evaluation (AST / Bytecode / JIT)

`Expression::Evaluate` on the AST is the **semantic reference**. Bytecode and
JIT are fast paths only — never introduce divergent behavior.
`differential_test.cpp` pins parity over operators × NULL combinations ×
edge cases (div-by-zero, `INT64_MIN/-1`, overflow, type mismatch).

Value-context nuance: `bytecode.cpp` deliberately drops `MakeValueRules`
(boolean-identity and other truthiness simplifications, ~14 rules) from the
bytecode rewrite set. Those rules are sound only in predicate context; applying
them to a value-producing expression would return a normalized boolean where
the AST returns the original value. Excluding them is what keeps bytecode
values identical to AST values — it is not a divergence from the reference.

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
  INT64 kernels (`Filter/Project/Sum` + `CompileProjectionChecked` /
  `CompileSumChecked` overflow-checked variants flagging wrapped lanes for
  AST-identical overflow errors); runs target-aware LLVM O3 middle-end
  (`TINYLAMB_JIT_DISABLE_OPT=1` disables, `TINYLAMB_JIT_DUMP_DIR` dumps IR/object
  files); process-lifetime function-pointer cache.
- `collation.{hpp,cpp}` — the *static* collation an expression asks for:
  `CollationOfExpression` (a `COLLATE(x, 'spec')` call; everything else is
  kNone) and `CollationOfOrdering` (one collated key makes the whole ORDER BY
  case-insensitive; combine single keys with `ResolveCollation`). The plan
  and the sort operators read the key's collation from here; the
  *value*-resolved collation is
  `type/collation.hpp`. `COLLATE()` itself lives in the function-call
  evaluator and resolves the spec through `CollationOfSpec`, the same rule
  the batch evaluator uses.
- `rewrite.{hpp,cpp}` (`ExpressionPattern/Rule/RuleSet/Rewriter`,
  `ExpressionRuleSet::Default`: folding, boolean identity, De Morgan,
  comparison canonicalization), `proto_text.*`/`proto_schema.*`
  (serialization), `expr_simplify_oracle.*` (fuzz oracle).

## Rules for new operators

AST first, then Bytecode, then (only if INT64-narrow) JIT. Run
`differential_test`, `bytecode_test`, `jit_test`, `rewrite_test`,
`expression_test`, `function_call_test`.

Note: `expression/bytecode.* -> executor/data_chunk.hpp` is an allowlisted
edge. `BytecodeColumnView`/`BytecodeSink` are currently coupled to `DataChunk`'s
types, so this is a live dependency rather than a merely hypothetical one; it
remains a separation candidate for a future refactor. Do not add more.

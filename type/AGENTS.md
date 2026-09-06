# `type/` — Layer 2 value/row/schema representation

`Value` tagged-union + `Row` + `Schema` + `Column`. Everything above
serializes through here; depends only on `common/`.

## Key files

- `value.hpp`/`value.cpp` (+ `value_type.hpp`, zero-dependency enum header
  also included from `common/`) — `ValueType{kNull/kInt64/kVarChar/kDouble/
  kDate/kArray}`, type-safe operators, `Serialize` / memcomparable encoding
  (index-key ordering).
- `row.{hpp,cpp}` — `Row{vector<Value>}`: `Serialize`/`Deserialize` (needs a
  `Schema`), `TryPeekInteger` (INT64/DATE fast path), `Extract`/`operator+`
  (join concatenation).
- `schema.{hpp,cpp}`, `column.{hpp,cpp}`, `column_name.{hpp,cpp}` —
  `Schema{name, columns}` owns columns (deep copy; easy dangling diagnosis),
  `Offset(ColumnName)` lookup, `operator+` for join output. `ColumnName`
  handles qualified names (`users.id`).
- `constraint.{hpp,cpp}` (`kNotNull/kUnique/kPrimaryKey`), `function.hpp`
  (catalog function signatures), `date.*`, `interval.*`.

## Before reading code here

Flow is `Schema` define → `Row(Value…)` build → `RowPage` serialize →
`Schema` interpret. `Row::Deserialize` without the matching `Schema` is
meaningless. `Value` semantics is what `expression/differential_test.cpp`
pins across AST/Bytecode/JIT.

Note: `type/column.hpp -> page/row_position.hpp` (constants only) is an
allowlisted edge in `scripts/check_layering.py`.

Test: `./build/value_test`, `row_test`, `schema_test`, `constraint_test`.

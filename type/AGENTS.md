# `type/` — Layer 2 value/row/schema representation

`Value` tagged-union + `Row` + `Schema` + `Column`. Everything above
serializes through here; depends only on `common/`.

## Key files

- `value.hpp`/`value.cpp` (+ `value_type.hpp`, zero-dependency enum header
  also included from `common/`) — `ValueType{kNull/kInt64/kVarChar/kDouble/
  kDate/kArray}`, type-safe operators, `Serialize` / memcomparable encoding
  (index-key ordering), `CompareForOrderBy` (the ORDER BY total order).
- `collation.{hpp,cpp}` — **the comparison contract for ordering**:
  `Collation{kNone/kBinary/kCaseInsensitive}` decoded from `Value`'s tag
  (tag 3 is the unsigned marker, not a collation), `ResolveCollation`
  (either side folds, matching the `=` predicate),
  `ApplyCollation`/`ApplyKeyCollation` (ASCII-only fold, the one fold rule
  `expression::FoldCase` and `CanonicalDistinctValue` delegate to),
  `CompareVarCharForOrderBy` (the shared ENUM rank + fold), 
  `CompareWithCollation`/`CompareUnderCollation`/`EqualUnderCollation`
  (peer equality with DISTINCT's NaN/-0.0 canonicalization) and
  `SortKeyOrder` + `MakeSortKeyOrder` (NULL placement, direction, collation —
  what every sort-shaped operator resolves its keys through). No locale
  collation: a non-`binary` spec folds, non-ASCII keeps byte order.
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

Test: `./build/value_test`, `collation_test`, `row_test`, `schema_test`,
`constraint_test`.

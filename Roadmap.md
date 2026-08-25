# GoogleSQL Compliance Implementation Roadmap

Comprehensive, granular implementation roadmap to achieve 100% pass rate on all GoogleSQL compliance/conformance test cases (excluding differential privacy) in `tinylamb`.

---

## ⚠️ Core Architectural Principles & Invariants

> ### 1. Strict Prohibition of Ad-hoc Workarounds
> **Never write quick-and-dirty hacks, special-case string matchers, or isolated shortcuts just to pass a test case.**
> Every operator, expression, type, function, and query shape must be implemented cleanly according to the formal GoogleSQL specification and standard relational algebra. The implementation must be robust and general enough to support future Cascades query optimization, vectorized/morsel execution, LLVM JIT code generation, and distributed query execution.
>
> ### 2. Strict Layered Dependency Discipline
> All additions must respect the 12-layer DAG documented in [`ARCHITECTURE.md`](ARCHITECTURE.md) and mechanically enforced by `python3 scripts/check_layering.py`. Never introduce upward dependency violations.
>
> ### 3. Canonical Expression Semantics (Ground Truth Invariant)
> When introducing scalar functions or operators:
> - **AST (`Expression::Evaluate` / `EvaluateBinary`) is the semantic reference (Ground Truth)**.
> - **Bytecode VM (`BytecodeProgram`)** must be updated for batch execution.
> - **Differential tests (`expression/differential_test.cpp`)** must verify semantic equivalence between AST and Bytecode/JIT.
>
> ### 4. Zero Regressions in Production Workloads
> Existing production paths (PostgreSQL wire-protocol server, 22 TPC-H queries, TPC-C transactions, MVCC recovery) must remain 100% green at all times.

---

## Granular Implementation Checklist

---

### [x] Phase 0: Test Harness & Conformance Infrastructure
- [x] Fix `#` comment line skipping in `.test` file parser (`query/googlesql_compliance_file.cpp`)
- [x] Fix multi-line / duplicate `[name=...]` and trailing metadata inclusion into SQL query text
- [x] Parse `[required_features]` and `[required_feature]` tags robustly
- [x] Parse `[prepare_database]` sections as multi-statement setup sequences
- [x] Support `[unknown_order]` result comparison with unordered multiset matching
- [x] Support floating-point comparison with epsilon tolerance (NaN, Infinity, precision diffs)
- [x] Support STRUCT tokenization and recursive structural equality in `ComplianceValueMatches`
- [x] Support ARRAY tokenization and element-by-element equality in `ComplianceValueMatches`
- [x] Support NULL token matching (`NULL`, `null`)
- [x] Support string literal unquoting and escape sequences in compliance output matcher
- [x] Support expected error substring matching for negative test cases (`ERROR: ...`)
- [x] Add automated script `scripts/compliance_summary.py` to report per-file and per-case pass rates

---

### [x] Phase 1: Scalar Operators, Boolean Predicates & Core Function Library

#### 1.1 Boolean & NULL Predicates
- [x] `IS TRUE` operator in AST visitor and scalar engine
- [x] `IS NOT TRUE` operator
- [x] `IS FALSE` operator
- [x] `IS NOT FALSE` operator
- [x] `IS NULL` operator
- [x] `IS NOT NULL` operator
- [x] `IS UNKNOWN` operator
- [x] `IS NOT UNKNOWN` operator
- [x] Three-valued boolean logic for `AND` / `OR` with NULL operands
- [x] Strict NULL propagation in `NOT` (handling `NOT NULL` evaluating to NULL vs literal NULL argument rejection)

#### 1.2 Comparison & Pattern Matching Operators
- [x] `BETWEEN ... AND ...` operator
- [x] `NOT BETWEEN ... AND ...` operator
- [x] `LIKE` pattern matching with `%` and `_` wildcards
- [x] `NOT LIKE` pattern matching
- [x] `LIKE ANY (...)` and `LIKE ALL (...)` quantification
- [x] `IN (val1, val2, ...)` with static constant lists
- [x] `NOT IN (val1, val2, ...)` with static constant lists
- [x] `DISTINCT FROM` / `IS NOT DISTINCT FROM` (NULL-safe equality)

#### 1.3 Control Flow & Conditional Functions
- [x] `IF(condition, true_expr, false_expr)` function
- [x] `COALESCE(expr1, expr2, ...)` function (short-circuiting first non-null)
- [x] `NULLIF(expr1, expr2)` function
- [x] `IFNULL(expr, default_expr)` function
- [x] Simple `CASE expr WHEN val THEN ... ELSE ... END` expression
- [x] Searched `CASE WHEN cond THEN ... ELSE ... END` expression

#### 1.4 String & JSON Functions
- [x] `CONCAT(s1, s2, ...)` function
- [x] `SUBSTR(str, pos[, len])` / `SUBSTRING`
- [x] `LENGTH(str)` / `CHAR_LENGTH(str)`
- [x] `BYTE_LENGTH(str)`
- [x] `UPPER(str)`
- [x] `LOWER(str)`
- [x] `TRIM(str[, chars])`
- [x] `LTRIM(str[, chars])`
- [x] `RTRIM(str[, chars])`
- [x] `STARTS_WITH(str, prefix)`
- [x] `ENDS_WITH(str, suffix)`
- [x] `STRPOS(str, substr)` / `INSTR`
- [x] `REPLACE(str, from, to)`
- [x] `REPEAT(str, n)`
- [x] `REVERSE(str)`
- [x] `LPAD(str, len[, pad])`
- [x] `RPAD(str, len[, pad])`
- [x] `SPLIT(str[, delimiter])`
- [x] `REGEXP_CONTAINS(str, pattern)`
- [x] `REGEXP_EXTRACT(str, pattern)`
- [x] `REGEXP_REPLACE(str, pattern, replacement)`
- [x] `REGEXP_MATCH(str, pattern)`
- [x] `REGEXP_INSTR(str, pattern, ...)`
- [x] `REGEXP_EXTRACT_ALL(str, pattern)`
- [x] `SOUNDEX(str)`
- [x] `TRANSLATE(str, from, to)`
- [x] `FORMAT(fmt, ...)`
- [x] `JSON_EXTRACT`, `JSON_QUERY`, `JSON_VALUE`, `JSON_EXTRACT_SCALAR`, `JSON_EXTRACT_ARRAY`, `JSON_EXTRACT_STRING_ARRAY`, `TO_JSON_STRING`

#### 1.5 Math & Numeric Functions
- [x] `ABS(x)`
- [x] `SIGN(x)`
- [x] `ROUND(x[, n])`
- [x] `TRUNC(x[, n])` / `TRUNCATE`
- [x] `MOD(x, y)`
- [x] `POW(x, y)` / `POWER`
- [x] `SQRT(x)`
- [x] `CBRT(x)`
- [x] `CEIL(x)` / `CEILING`
- [x] `FLOOR(x)`
- [x] `GREATEST(x1, x2, ...)`
- [x] `LEAST(x1, x2, ...)`
- [x] `LN(x)` / Natural logarithm
- [x] `LOG(x[, base])`
- [x] `LOG10(x)`
- [x] `EXP(x)`
- [x] `ACOS(x)`, `ASIN(x)`, `ATAN(x)`, `ATAN2(y, x)`
- [x] `COS(x)`, `SIN(x)`, `TAN(x)`
- [x] `COSH(x)`, `SINH(x)`, `TANH(x)`
- [x] `IEEE_DIVIDE(x, y)`
- [x] `SAFE_DIVIDE(x, y)`, `SAFE_ADD`, `SAFE_SUBTRACT`, `SAFE_MULTIPLY`, `SAFE_NEGATE`

#### 1.6 Type Casting & Conversion Functions
- [x] `CAST(x AS INT64)`
- [x] `CAST(x AS DOUBLE)` / `FLOAT64`
- [x] `CAST(x AS STRING)`
- [x] `CAST(x AS BOOL)`
- [x] `CAST(x AS DATE)`
- [x] `CAST(x AS TIMESTAMP)`
- [x] `SAFE_CAST(x AS TargetType)`
- [x] Hexadecimal literal conversion: `0x...` integers


#### 1.7 Date & Time Functions
- [ ] `CURRENT_DATE([timezone])`
- [ ] `CURRENT_TIMESTAMP()`
- [ ] `DATE(year, month, day)` constructor
- [ ] `DATE_ADD(date, INTERVAL n unit)`
- [ ] `DATE_SUB(date, INTERVAL n unit)`
- [ ] `DATE_DIFF(date1, date2, unit)`
- [ ] `DATE_TRUNC(date, unit)`
- [ ] `EXTRACT(part FROM date_or_timestamp)` for `YEAR`, `MONTH`, `DAY`, `DAYOFWEEK`, `DAYOFYEAR`, `QUARTER`, `HOUR`, `MINUTE`, `SECOND`
- [ ] `FORMAT_DATE(format_string, date)`
- [ ] `PARSE_DATE(format_string, date_string)`

---

### [ ] Phase 2: DDL, Database Preparation & DML Execution

#### 2.1 Multi-Statement & Setup Execution
- [ ] Multi-statement execution support in `SqlEngine` for `[prepare_database]` blocks
- [ ] Clean isolation and cleanup between compliance test runs

#### 2.2 `CREATE TABLE AS SELECT` (CTAS)
- [ ] AST resolution of `CREATE TABLE table_name AS SELECT ...`
- [ ] Infer column names and types from logical plan schema
- [ ] Physical table creation in `Database` catalog
- [ ] Batch row insertion from plan executor into newly created table
- [ ] Transaction commit and catalog sync for CTAS

#### 2.3 Explicit `CREATE TABLE` DDL
- [ ] Parse explicit column definitions (`column_name TYPE [NOT NULL] [DEFAULT expr]`)
- [ ] Support all scalar column types (`INT64`, `DOUBLE`, `STRING`, `BOOL`, `DATE`, `TIMESTAMP`)
- [ ] Support primary key column annotations (`PRIMARY KEY`)
- [ ] Create table metadata in system catalog

#### 2.4 DML Statements (`INSERT`, `UPDATE`, `DELETE`)
- [ ] Multi-row bulk `INSERT INTO table VALUES (...), (...), ...`
- [ ] `INSERT INTO table (col1, col2) VALUES (...)` with column reordering
- [ ] `INSERT INTO table SELECT ...` (insert from query)
- [ ] `UPDATE table SET col = expr WHERE condition`
- [ ] `DELETE FROM table WHERE condition`
- [ ] DML `RETURNING` clause support (`INSERT/UPDATE/DELETE ... RETURNING col1, col2`)

---

### [ ] Phase 3: Complex Data Types (ARRAY & STRUCT) and UNNEST

#### 3.1 Composite Type Representation in Type System
- [ ] Introduce `Value::Array` representing dynamic vector of homogenous `Value`s
- [ ] Introduce `Value::Struct` representing ordered tuple of named/unnamed `Value`s
- [ ] Type serialization/deserialization for `ARRAY` and `STRUCT`
- [ ] Nullability and empty array semantics (`[]` vs `NULL`)

#### 3.2 Array & Struct Literal AST Visitor
- [ ] Untyped array literal: `[elem1, elem2, ...]`
- [ ] Typed array literal: `ARRAY<TYPE>[elem1, elem2, ...]`
- [ ] Explicit STRUCT literal: `STRUCT(val1 AS name1, val2 AS name2)`
- [ ] Anonymous struct literal: `(val1, val2, ...)`

#### 3.3 Accessor Operators & Element Extraction
- [ ] Dot-notation struct field accessor: `struct_expr.field_name`
- [ ] Positional struct field accessor: `struct_expr.1`
- [ ] 0-based array subscript: `array_expr[OFFSET(n)]`
- [ ] 1-based array subscript: `array_expr[ORDINAL(n)]`
- [ ] Safe array offset accessor: `array_expr[SAFE_OFFSET(n)]`
- [ ] Safe array ordinal accessor: `array_expr[SAFE_ORDINAL(n)]`

#### 3.4 Array Construction & Manipulation Functions
- [ ] `ARRAY_CONCAT(arr1, arr2, ...)`
- [ ] `ARRAY_LENGTH(arr)`
- [ ] `ARRAY_TO_STRING(arr, delimiter[, null_text])`
- [ ] `GENERATE_ARRAY(start, end[, step])`
- [ ] `GENERATE_DATE_ARRAY(start_date, end_date[, INTERVAL n unit])`
- [ ] `ARRAY_REVERSE(arr)`

#### 3.5 UNNEST Operator & Relational Flattening
- [ ] AST translation for `FROM UNNEST(array_expr) [AS alias]`
- [ ] `UnnestPlan` relational plan node in `plan/`
- [ ] `UnnestExecutor` physical iterator in `executor/`
- [ ] `WITH OFFSET [AS offset_alias]` support in `UNNEST`
- [ ] Lateral/Correlated join with `UNNEST`: `FROM table t, UNNEST(t.array_col) a`
- [ ] `LEFT JOIN UNNEST(...) ON ...` outer unnesting semantics

#### 3.6 Collection Aggregation
- [ ] `ARRAY_AGG(expr [ORDER BY ...] [LIMIT ...])` aggregate function
- [ ] `ARRAY_CONCAT_AGG(arr [ORDER BY ...] [LIMIT ...])` aggregate function
- [ ] `STRING_AGG(str[, delimiter] [ORDER BY ...] [LIMIT ...])` aggregate function

---

### [ ] Phase 4: Set Operations, Advanced Subqueries & Query Scoping

#### 4.1 Set Operations
- [ ] `UNION ALL` with type coercion across multiple branches
- [ ] `UNION DISTINCT` with duplicate elimination
- [ ] `INTERSECT DISTINCT` relational operator and executor
- [ ] `INTERSECT ALL` relational operator and executor
- [ ] `EXCEPT DISTINCT` relational operator and executor
- [ ] `EXCEPT ALL` relational operator and executor
- [ ] `CORRESPONDING` column-name matching for set operations
- [ ] Parenthesized set operation subtrees: `(SELECT ...) UNION ALL (SELECT ...)`

#### 4.2 Advanced CTE (`WITH`) Scoping & Recursion
- [ ] Multi-statement CTE resolution (`WITH q1 AS (...), q2 AS (...) SELECT ...`)
- [ ] Scalar subquery with local `WITH` clause: `SELECT (WITH q AS (...) SELECT x FROM q)`
- [ ] `IN` subquery with local `WITH` clause: `WHERE x IN (WITH q AS (...) SELECT y FROM q)`
- [ ] Forward reference prevention and shadowing rules in nested CTEs
- [ ] `WITH RECURSIVE` fixed-point iteration for recursive CTEs

#### 4.3 Correlated Subqueries & Query Expressions
- [ ] Multi-column `IN` subqueries: `WHERE (a, b) IN (SELECT x, y FROM t)`
- [ ] `EXISTS` and `NOT EXISTS` subqueries with deep correlation
- [ ] Scalar subqueries returning 0 rows (evaluated to `NULL`)
- [ ] Scalar subqueries returning >1 row (runtime cardinality check error)
- [ ] `QUALIFY` clause for filtering over windowed expressions
- [ ] `GROUP BY ALL` automatic grouping column derivation
- [ ] `GROUPING SETS`, `ROLLUP`, and `CUBE` multidimensional aggregation

---

### [ ] Phase 5: Analytic & Window Functions

#### 5.1 Window Execution Infrastructure
- [ ] Window specification AST node (`PARTITION BY`, `ORDER BY`, frame clauses)
- [ ] `WindowPlan` logical plan node in `plan/`
- [ ] `WindowExecutor` streaming/partition-buffered physical operator in `executor/`
- [ ] Frame specification parser: `ROWS BETWEEN ... AND ...`
- [ ] Frame specification parser: `RANGE BETWEEN ... AND ...`
- [ ] Frame bounds: `UNBOUNDED PRECEDING`, `n PRECEDING`, `CURRENT ROW`, `n FOLLOWING`, `UNBOUNDED FOLLOWING`

#### 5.2 Ranking & Distribution Functions
- [ ] `ROW_NUMBER() OVER (...)`
- [ ] `RANK() OVER (...)`
- [ ] `DENSE_RANK() OVER (...)`
- [ ] `PERCENT_RANK() OVER (...)`
- [ ] `CUME_DIST() OVER (...)`
- [ ] `NTILE(num_buckets) OVER (...)`

#### 5.3 Value & Navigation Window Functions
- [ ] `LEAD(expr[, offset[, default_expr]]) OVER (...)`
- [ ] `LAG(expr[, offset[, default_expr]]) OVER (...)`
- [ ] `FIRST_VALUE(expr) OVER (...)`
- [ ] `LAST_VALUE(expr) OVER (...)`
- [ ] `NTH_VALUE(expr, n) OVER (...)`
- [ ] `NULLS FIRST` / `NULLS LAST` ordering modifier in window definitions

#### 5.4 Windowed Aggregates
- [ ] `SUM(expr) OVER (...)` sliding / expanding frame evaluation
- [ ] `COUNT(*) OVER (...)` and `COUNT(expr) OVER (...)`
- [ ] `AVG(expr) OVER (...)`
- [ ] `MIN(expr) OVER (...)`
- [ ] `MAX(expr) OVER (...)`

---

### [ ] Phase 6: Extended Types & Advanced Extensions

#### 6.1 JSON Data Type & Functions
- [ ] Native `Value::Json` representation
- [ ] `JSON_VALUE(json_expr, json_path)`
- [ ] `JSON_QUERY(json_expr, json_path)`
- [ ] `JSON_EXTRACT(json_expr, json_path)`
- [ ] `JSON_EXTRACT_SCALAR(json_expr, json_path)`
- [ ] `TO_JSON_STRING(value)`
- [ ] `PARSE_JSON(json_string)`

#### 6.2 Precise Decimals & Binary Data
- [ ] `NUMERIC` (128-bit fixed-point decimal, 38 digits precision, 9 scale)
- [ ] `BIGNUMERIC` (256-bit fixed-point decimal, 76 digits precision, 38 scale)
- [ ] `BYTES` literal support: `b"..."` and `B'...'`
- [ ] Binary encoding/decoding: `TO_HEX`, `FROM_HEX`, `TO_BASE64`, `FROM_BASE64`
- [ ] Hash functions: `MD5`, `SHA1`, `SHA256`, `SHA512`, `FARM_FINGERPRINT`

#### 6.3 Timezone & Civil Time
- [ ] `TIMESTAMP WITH TIME ZONE` support
- [ ] `TIME` (time of day without date)
- [ ] `DATETIME` (civil date and time)
- [ ] `STRING(timestamp, timezone)` formatting
- [ ] `TIMESTAMP(date_or_string, timezone)` parsing

#### 6.4 Table-Valued Functions (TVF) & Advanced Syntaxes
- [ ] Table-Valued Function (TVF) invocation syntax
- [ ] Time-window TVFs: `TUMBLE(...)`, `HOP(...)`
- [ ] PIVOT / UNPIVOT relational operators
- [ ] Pipe syntax operators (`|> WHERE ... |> AGGREGATE ...`)

---

## Tracking & Verification Workflow

1. **Baseline Measurement**: Run `./build/googlesql_compliance_test` to capture existing failures.
2. **Feature Implementation**:
   - Implement functionality in the lowest appropriate architectural layer (`type/`, `expression/`, `executor/`, `plan/`, `query/`).
   - Run `python3 scripts/check_layering.py` to confirm 0 architectural layering violations.
   - Add differential test cases in `expression/differential_test.cpp` to verify AST ↔ Bytecode ↔ JIT equivalence.
3. **Validation & Checkoff**:
   - Run the corresponding `.test` files in `googlesql_compliance_test` to verify 100% test pass.
   - Run regression tests (`ctest --test-dir build --output-on-failure`).
   - Check off the completed feature item (`[x]`) in this roadmap.

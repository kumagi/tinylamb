# 第70章 式書き換え — フレームワークと自明な Rule の一括解説

- 状態: draft / 執筆基準リビジョン: `3880673` (2026-09-12)

`ExpressionRuleSet` は、スカラー式を直接書き換える最下層の Rule セットです。
この章ではフレームワークと基本的な 35 本を説明し、適用条件の複雑な 38 本は
第 2 部の個別章へ分けます。

## 70-1. スカラー式を直接変換する Rule

式書き換え Rule は、論理同値 Rule と異なり、メモ構造を介さずに動作します。
論理同値 Rule がメモに同値な関係代数式を追加するのに対し、式書き換え Rule は
スカラー式（AST の `Expression`）を受け取り、等価なスカラー式を返します。
したがって、メモ、同値 Group、コスト計算のいずれにも依存しません。

定義は `expression/rewrite.cpp` の `ExpressionRuleSet::Default()` にあります。
基準リビジョンでは `built.Add` を 74 回呼び出しますが、`abs_of_abs` が同名で
2 回登録されています。`Add` は同名登録を上書きするため（70-2 節）、実効的な
登録数は 73 本です。本章で 35 本、第 2 部で 38 本を扱い、合計 73 本が
`00-index.md` の一覧と一致します。

式書き換えは、オプティマイザの初期処理以外でも実行されます。実行経路は次の
3 箇所です。

1. `plan/optimizer.cpp` の `Optimizer::Optimize` 冒頭（第 1 章 1-5 節（1））: ORDER BY キー、SELECT 式、WHERE 述語、外部結合の ON 条件の正規化。
2. `expression/bytecode.cpp` の `BytecodeCompiler::Compile`: バイトコード生成直前の簡約。
3. `executor/detail/scan_filter.cpp` の `TryCompileSimpleCompare`: スキャン事前フィルタの構築。

SELECT 句と ORDER BY キーには、`inner_join_not_null_inference` を除外した
`value_context_rules` を適用します（第 1 章 1-5 節（1））。WHERE 述語、
ON 条件、バイトコード生成、スキャン事前フィルタでは、
`ContainsNotOfOrderedDoubleComparison` が真となる木に限り、`not_comparison` を
除外した `NotComparisonFreeRules()` を適用します（70-3（b）節）。

この最下層での正規化は、上位の論理最適化を補助します。例えば、
`canonicalize_comparison` は定数を右辺に集め、`or_of_ranges_to_in` は等値 OR を
IN リストにまとめます。その結果、上位の論理 Rule やインデックスアクセスの
パターンに照合しやすくなります。

## 70-2. フレームワーク — Pattern、Rule、RuleSet、Rewriter

式書き換えのフレームワークは、`expression/rewrite.hpp` に定義された 4 つの
部品（`ExpressionPattern`、`ExpressionRule`、`ExpressionRuleSet`、
`ExpressionRewriter`）で構成されます。

まず照合パターンを表現する `ExpressionPattern` を確認します。

```cpp
class ExpressionPattern {
 public:
  static ExpressionPattern Any(std::string capture = {});
  static ExpressionPattern Type(TypeTag type, std::string capture = {});
  static ExpressionPattern Binary(std::optional<BinaryOperation> operation,
                                  ExpressionPattern left,
                                  ExpressionPattern right,
                                  std::string capture = {});
  static ExpressionPattern Unary(std::optional<UnaryOperation> operation,
                                  ExpressionPattern child,
                                  std::string capture = {});
  // ...(省略: Match と private メンバ — 型・二項/単項演算子の optional
  // 指定、子パターンのリスト children_、キャプチャ名。パターンは immutable
  // な値で、Rule はリライタや他 Rule に依存しない)...
};
```

関係演算子を対象とする第 30 章の `Pattern` と比べると、2 つの違いがあります。
1 点目は、照合対象がスカラー式であり、キャプチャする値も部分式であることです。
`ExpressionBindings` は、名前から `Expression` への写像です。2 点目は、
演算子指定が `std::optional` であり、任意の演算子に一致する条件を `nullopt` で
表すことです。

構築用 DSL である `expression_dsl` には、`Any`、`Is`（型指定）、`Binary`、
`AnyBinary`、`Unary`、`AnyUnary` の 6 関数があります。いずれもファクトリ
メソッドへ直接委譲します。`ExpressionPattern::Match` は、同じキャプチャ名が
複数回現れた場合に `Same`（ノード種別と `ToString()` の完全一致）を要求します。
一致しなければ照合に失敗します。この仕組みにより、`and_idempotent` のような
「左右に同じ部分式が現れる」規則は、`Any("x")` を左右に指定するだけで
記述できます。

個々の規則を保持する `ExpressionRule` は、規則名、照合パターン、書き換え関数の
3 点に加え、`root_only` フラグを保持します。設計方針 D6
（`docs/design.md`）では、「述語を追加する規則はルートの論理積集合にのみ
適用する」と定めています。述語を追加する書き換えを AND の内側で無制限に
適用すると、推論した述語が新たな AND を生成し続け、不動点探索が停止しない
ためです。

複数の規則を束ねる `ExpressionRuleSet` は、内部に
`std::vector<ExpressionRule>` を保持する値型です。`Add` は内部で
`Remove(rule.Name())` を実行してから、新しい規則を追加します。したがって、
同名規則の再登録は上書きとして機能します。`abs_of_abs` の 2 回の登録は、
この動作に依存しています。

これらの規則を実行するのが `ExpressionRewriter` です。`ExpressionRewriter` は
RuleSet への参照とパス数の上限 `pass_limit_` を保持します。書き換え処理の
中核である `TryRewrite` の実装は次のとおりです（`expression/rewrite.cpp`）。

```cpp
StatusOr<Expression> ExpressionRewriter::TryRewrite(
    const Expression& expression) const {
  Expression current = expression;
  // D6 (docs/design.md): the pass cap is a safety net, not a rejection
  // mechanism.  When a rule set oscillates past the cap, returning the last
  // stable form keeps a valid query runnable: the result still preserves
  // the input's semantics (every accepted rewrite is meaning-preserving),
  // while throwing here turned "optimizer did not reach a fixed point" into
  // a runtime failure for the whole statement.
  const size_t pass_limit = pass_limit_ == 0 ? 32 : pass_limit_;
  for (size_t pass = 0; pass < pass_limit; ++pass) {
    ASSIGN_OR_RETURN(Expression, next, TryRewriteOnce(current, 0));
    if (Same(current, next)) {
      return next;
    }
    current = std::move(next);
  }
  // ...(収束しなかった場合は LOG(ERROR) を1回記録し、current（直前の安定形）を返す)
  return current;
}
```

書き換えは、1 パス（`TryRewriteOnce` の 1 回の実行）ごとに式全体を走査し、
`Same` による不動点判定が成立するまで繰り返します。パス数の上限は 32 回で、
無限ループを防ぐ安全弁として機能します。上限に達してもクエリ処理は中断せず、
直前の式（`current`）を返します。適用済みの各書き換えは意味を保存するため、
この式も実行できます。

1 パスを担う `TryRewriteOnce` はボトムアップで進みます。まず、すべての子ノードを
再帰的に書き換え、その後で親ノードの Rule を登録順に評価します。変換式を
最初に返した Rule を採用し、次の処理へ移ります。
`rule.root_only() && depth != 0` を満たす Rule は評価しません。

## 70-3. 型と安全性の境界 — 2 つの分割

式書き換え Rule は、入力スキーマの情報を持たずに実行されます。定数判定では、
空のスキーマを `TryEvaluate(Row(), Schema())` に渡します。この設計により任意の
場所から呼び出せますが、式の静的型が未確定であることに注意が必要です。
tinylamb では、この問題を 2 通りの方法で扱います。

### (a) 型確定が必要な算術書き換えの分離 — `RewriteTypedArithmetic`

型情報がなければ安全性を保証できない算術変形は、汎用 RuleSet から切り離し、
専用の関数として実装しています（`expression/rewrite.hpp`）。

```cpp
// Applies arithmetic rewrites that require resolved input-column types.
// Keeping these separate from the schema-free rule set prevents type-changing
// rewrites such as DOUBLE-column multiplication by an integer zero.
[[nodiscard]] Expression RewriteTypedArithmetic(const Expression& expression,
                                                const Schema& input_schema);
```

`RewriteTypedArithmetic` は `TryRewrite` の完了後、入力スキーマが確定した
オプティマイザの文脈（SELECT 句、WHERE 句、外部結合の ON 条件）から個別に
呼び出します。対象は、`-1 * x` の単項マイナス化、`x + x` から `x * 2` への
変換、加減算の定数再結合、`x * 0` の畳み込みです。

とくに `x * 0` の畳み込みには型による制限が課されています（`expression/rewrite.cpp`）。

```cpp
  try {
    // Floating-point x * 0 is not generally zero: NaN and infinities must
    // be preserved. Restrict this rewrite to resolved integer arithmetic.
    if (current->ResultType(input_schema).GetType() != TypeTag::kBigInt ||
        (*value)->ResultType(input_schema).GetType() != TypeTag::kBigInt) {
      return current;
    }
```

浮動小数点数の `x * 0.0` は、`x` が NaN や無限大の場合に `0.0` を返しません。この式を無条件にゼロへ変換すると結果が変わるため、オペランドが静的に `INT64` だと分かる場合に限定します。また、列 `x` が NULL なら式全体も NULL になるという SQL の三値論理を保つため、単純な `0` ではなく `CASE WHEN x IS NULL THEN CAST(NULL AS INT64) ELSE CAST(0 AS INT64) END` へ展開します。

### (b) NaN と NOT — 型ブラインドな Rule の境界

スキーマを持たない RuleSet では、浮動小数点数に固有の意味を見落とす可能性が
あります。`not_comparison` がその例です。二値論理では `NOT(x < y)` を
`x >= y` へ変形できますが、IEEE 754 の浮動小数点演算では成り立ちません。
順序比較は、オペランドのいずれかが NaN なら常に FALSE を返します。そのため、
`x` が NaN のとき、`NOT(NaN < y)` は TRUE ですが、変形後の
`NaN >= y` は FALSE です。

tinylamb は、この境界を明示的な判定関数と代替 RuleSet で処理します
（`expression/rewrite.hpp`）。

```cpp
// True when the tree contains NOT(ordered-comparison) whose schema-typed
// operands can be IEEE NaN.  NOT(x < y) only equals x >= y when NaN cannot
// occur: the AST reference evaluates every ordered NaN comparison to FALSE,
// so NOT(NaN < x) is TRUE while the negated form is FALSE.  The rewrite
// rules are type-blind, so callers (BytecodeCompiler, scan pre-filtering)
// use this check to swap in NotComparisonFreeRules().
bool ContainsNotOfOrderedDoubleComparison(const Expression& expression,
                                          const Schema& schema);
// Default() minus the NaN-unsound not_comparison rule.
const ExpressionRuleSet& NotComparisonFreeRules();
```

呼び出し側（バイトコードコンパイラやスキャン事前フィルタ）はスキーマ情報を
持つため、対象の式木に「DOUBLE 型となり得るオペランドを含む順序比較の NOT」が
あるかを判定できます。該当する構造があれば、`Default()` から
`not_comparison` を除外した `NotComparisonFreeRules()` で書き換えます。

## 70-4. 自明な Rule 35 本の一括解説

`Default()` に含まれる基本的な 35 本の Rule を、意味に基づいて 6 つの
グループに分類します。表の「guard」列は、変換関数が適用を拒否して
`Expression{}`（空の式）を返す条件です。

### 定数畳み込み（4 本）

二項演算の定数畳み込みを担う `fold_binary` の実装は以下のとおりです（`expression/rewrite.cpp`）。

```cpp
    built.Add(ExpressionRule(
        "fold_binary",
        AnyBinary(Is(TypeTag::kConstantValue, "left"),
                  Is(TypeTag::kConstantValue, "right")),
        [](const Expression& expression, const ExpressionBindings&) {
          if (StatusOr<Value> folded = expression->TryEvaluate(Row(), Schema());
              folded.HasValue()) {
            return ConstantValueExp(folded.MoveValue());
          }
          return Expression{};
        }));
```

`fold_binary` は左右の辺がどちらも定数ノードである式を捉え、式全体の
`TryEvaluate` を試みます。評価に成功した場合だけ、得られた値を定数ノードとして
返します。ゼロ除算（`1 / 0`）のように実行時エラーとなる式は、`TryEvaluate` が
失敗するため書き換えず、実行時に同じエラーを発生させます。

| Rule | パターン → 書き換え | guard（適用除外条件） |
|---|---|---|
| `fold_binary` | 両辺が定数の二項演算 → `TryEvaluate` で評価した定数 | 評価失敗（ゼロ除算など） |
| `fold_unary` | 子ノードが定数の単項演算 → `TryEvaluate` で評価した定数 | 評価失敗 |
| `fold_in` | すべてのオペランドが定数の `IN` 式 → 評価した定数 | 非定数のオペランドが 1 つでも存在する場合 |
| `fold_function` | 引数がすべてリテラル（定数または INTERVAL）の関数呼出し → 評価した定数 | 関数の揮発性分類（`GetFunctionVolatility`）が `kImmutable` 以外（`RAND` や `NOW` など） |

### IN と CASE の正規化（6 本）

単一要素の `IN` 式を等値比較に変換する `singleton_in` では、NULL 定数の扱いに
注意が必要です（`expression/rewrite.cpp`）。

```cpp
          if (IsConstant(in.list_.front()) &&
              in.list_.front()->AsConstantValue().GetValue().IsNull()) {
            return Expression{};
          }
```

`x IN (NULL)` を単純に `x = NULL` へ変換することは、三値論理では正しく
ありません。このケースは、第 2 部の `in_single_null` が扱います。そのため、
`singleton_in` はリスト要素が NULL 定数の場合に適用を拒否します。

| Rule | パターン → 書き換え | guard（適用除外条件） |
|---|---|---|
| `singleton_in` | 要素が 1 つの `IN` 式（`x IN (c)`）→ 等値比較 `x = c` | リスト要素が NULL 定数 |
| `dedupe_in_list` | `IN` 式のリスト要素から重複を除去（順序維持） | 重複が存在しない場合 |
| `empty_in_list` | リストが空の `IN` 式 → FALSE 定数 | 左辺の子式が非 NULL の静的定数でない場合（左辺が NULL のときは三値論理により NULL を返す必要があるため） |
| `simplify_case` | CASE 式から定数 WHEN 条件を整理（定数 TRUE に達した時点で ELSE に繰り上げ、後続を枝刈り） | 定数の WHEN 条件が 1 つも存在しない場合 |
| `uniform_case_result` | すべての WHEN 分岐および ELSE の結果が同一値の CASE 式 → その共通結果 | 各 WHEN 条件が `ExpressionCannotThrow`（評価時に例外を発生させない安全判定）かつ `SafeToReduceEvaluationCount`（副作用を含まない）を満たさない場合 |
| `if_to_case` | 3 引数の `IF(c, t, e)` → `CASE WHEN c THEN t ELSE e END` | なし（構文の正規化） |

### 比較とブール論理の正規化（6 本）

論理演算の恒等式を処理する `boolean_identity` では、短絡評価と例外発生の順序が制約となります（`expression/rewrite.cpp`）。

```cpp
          // The AST reference short-circuits only when the LEFT operand
          // already decides the result.  Folding decided-by-left to a
          // constant is always safe; folding `x AND FALSE` / `x OR TRUE`
          // drops x and must not erase a throw it could raise
          // (oracle-found: `CAST(NaN AS INT64) != -inf OR TRUE`).
```

`FALSE AND x` や `TRUE OR x` のように左辺によって結果が確定する場合は無条件で畳み込めますが、`x AND FALSE` や `x OR TRUE` のように左辺 `x` を消去する変形では、`x` の評価に伴う実行時例外が消失する危険があります。そのため、`ExpressionCannotThrow(left)` が真である場合にのみ適用が許可されます。

| Rule | パターン → 書き換え | guard（適用除外条件） |
|---|---|---|
| `canonicalize_comparison` | 左辺が定数、右辺が非定数の二項比較 → 演算子を反転して定数を右辺に移動（`5 > x` → `x < 5`） | 左辺が非定数、または右辺も定数 |
| `boolean_identity` | 真偽定数を含む AND / OR の簡約（`TRUE AND x` → `x` など） | 残存式が非ブール型、または消去される左辺式が例外を送出する可能性がある場合 |
| `double_negation` | `NOT(NOT(x))` → `x` | `x` が静的に非ブール型 |
| `and_idempotent` / `or_idempotent` | 同一式の論理積・論理和（`x AND x` / `x OR x`）→ `x` | `x` が静的に非ブール型、または `SafeToReduceEvaluationCount` が偽（評価回数を減らせない副次的作用を含む） |
| `canonicalize_boolean` | ブール評価式と 0 / 1 との比較（`expr = 1` など）→ 式そのものまたは `NOT expr` | 定数が NULL または 0 / 1 以外の整数 |

### 算術恒等式（10 本）

加算の恒等式 `identity_add_zero` には、最適化の品質と厳密な型付けの兼ね合いが存在します（`expression/rewrite.cpp`）。

```cpp
          // NOTE: intentional deviation from the AST ground truth (kept for
          // optimizer quality, pinned by the optimizer_arithmetic compliance
          // file): the identities fire on numeric/int columns even though
          // the AST types e.g. `a + 0.0` as double.  Known IEEE gap: a
          // -0.0 survivor is normalized to +0.0.
```

AST の厳密な評価基準では、`a + 0.0` は DOUBLE 型へ昇格します。一方、この
Rule はインデックス照合を維持するため、整数型の列にも適用されます。この挙動は
コンプライアンステストで固定された既知の仕様差です。

| Rule | パターン → 書き換え | guard（適用除外条件） |
|---|---|---|
| `identity_add_zero` | `x + 0` または `0 + x` → `x` | 残存側が静的に非数値型 |
| `identity_subtract_zero` | `x - 0` → `x` | 右辺がゼロ定数でない、または左辺が静的に非数値型 |
| `identity_multiply_one` | `x * 1` または `1 * x` → `x` | 残存側が静的に非数値型 |
| `identity_divide_one` | `x / 1` → `x` | 右辺が定数 1 でない、または左辺が静的に非数値型 |
| `canonicalize_add_negative_constant` | `x + (-c)`（右辺が負の整数定数）→ `x - c` | 左辺が静的に非数値型、または定数が `INT64_MIN`（反転時にオーバーフローするため） |
| `canonicalize_subtract_negative_constant` | `x - (-c)` → `x + c` | 同上 |
| `multiply_by_negative_one` | `x * (-1)` または `(-1) * x` → `-x` | 定数が `-1` 以外、または残存側が静的に非数値型 |
| `combine_repeated_addend` | `x + x` → `x * 2` | 静的に非数値型、または評価回数の削減が不安全 |
| `double_negation_arithmetic` | `-(-x)` → `x` | 子ノードが静的に非数値型、または子ノードが `INT64_MIN` 定数 |
| `double_bitwise_negation` | `~(~x)` → `x` | 静的に非数値型、DOUBLE 型、または型未確定 |

### 定数の再結合（4 本）

`(x + 1) + 2` を `x + 3` へ変形する再結合は、中間結果におけるオーバーフローの発生有無を変化させる可能性があります。

```cpp
// expression/rewrite.cpp
// Reassociating (x + a) + b -> x + (a + b) can elide the intermediate
// x + a overflow, changing throw-vs-value.
```

したがって、再結合 Rule 群では定数の符号が一致していることを条件とし、中間オーバーフローの消失によって計算結果やエラー挙動が変わらないことを保証しています。

| Rule | パターン → 書き換え | guard（適用除外条件） |
|---|---|---|
| `reassociate_add_constants` | `(x + a) + b` → `x + (a + b)` | 定数 `a` と `b` の符号が一致しない、または `a + b` の加算自体がオーバーフローする場合 |
| `reassociate_subtract_constants` | `(x - a) - b` → `x - (a + b)` | 同上 |
| `reassociate_subtract_add_constants` | `(x - a) + b` → `x + (b - a)` | `(a <= 0) == (b >= 0)` が不成立、または定数計算がオーバーフローする場合 |
| `reassociate_add_subtract_constants` | `(x + a) - b` → `x + (a - b)` | `(a >= 0) == (b <= 0)` が不成立、または定数計算がオーバーフローする場合 |

### 関数と配列の平坦化（5 本）

多重にネストした同名関数呼び出しを展開し、引数リストを平坦化します。

| Rule | パターン → 書き換え | guard（適用除外条件） |
|---|---|---|
| `abs_of_abs` | `ABS(ABS(x))` → `ABS(x)` | 引数の構造が合致しない場合 |
| `concat_flatten` | `CONCAT(CONCAT(a, b), c)` → `CONCAT(a, b, c)` | 変化がない、または結果の引数が 2 個未満 |
| `array_flatten_optimization` | ネストした `ARRAY_CONCAT` の展開および空配列リテラルの除去 | 引数の構造が合致しない場合 |
| `pow_identities` | `POW(x, 1)` → `CAST(x AS FLOAT64)` | 指数が定数 1 でない場合（戻り値を FLOAT64 型に固定するため CAST を付与） |
| `greatest_least_fold` | `GREATEST(GREATEST(a, b), c)` → `GREATEST(a, b, c)` | 引数が空、または結果の引数が 2 個未満 |

これら 35 本の自明な Rule に共通する原則は、「式を消去または変形する際、その式が本来発生させるはずだった実行時例外や副作用を消失させてはならない」という点です。`ExpressionCannotThrow` や `SafeToReduceEvaluationCount` による事後条件判定は、この意味論的整合性を保証するために設けられています。

## 70-5. 個別に解説する 38 本の Rule

残る 38 本の Rule（ド・モルガンの法則、吸収則、共通積因子の抽出など）は、
適用条件や意味の検証を個別に説明する必要があります。各 Rule は、第 2 部の
`part2-rules/expression/` で解説します。

- NOT の展開（6 本）: `de_morgan.md`, `not_comparison.md`, `not_like.md`, `not_not_like.md`, `not_is_null.md`, `not_is_not_null.md`
- ブール論理の簡約（9 本）: `xor_boolean_identity.md`, `xor_to_or_and_not.md`, `absorption_and.md`, `absorption_and_reversed.md`, `absorption_or.md`, `absorption_or_reversed.md`, `boolean_filter_pullup.md`, `distribute_or_over_and_budgeted.md`, `boolean_eq_true_false_three_valued.md`
- 文字列照合と正規表現（3 本）: `like_equality.md`, `not_like_equality.md`, `regexp_prefix_extraction.md`
- NULL と三値論理（5 本）: `is_null_of_null_check.md`, `is_not_null_of_null_check.md`, `contradiction_from_null_eq.md`, `in_single_null.md`, `not_in_null_semantics.md`
- 型変換と関数特性（10 本）: `collapse_nested_identical_cast.md`, `nullif_to_case.md`, `coalesce_and_nullif_simplification.md`, `deterministic_function_cse.md`, `function_volatility_classification.md`, `nondeterministic_barrier.md`, `safe_divide_rewrite.md`, `datetime_and_string_fold_extent.md`, `json_path_constant_fold.md`, `numeric_widening_cast.md`
- 述語推論と因子展開（5 本）: `factor_or_common_and.md`, `or_of_ranges_to_in.md`, `interval_normalize.md`, `predicate_pushdown_case.md`, `inner_join_not_null_inference.md`

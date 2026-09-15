# 第70章 式書き換え — フレームワークと自明な Rule の一括解説

- 状態: draft / 執筆基準リビジョン: `3880673` (2026-09-12)
- 位置づけ: 3層のRuleセット（第1章 1-4節）のうち最下層に位置する式書き換え（`ExpressionRuleSet`）のフレームワークを解説し、自明な35本のRuleを一括して整理します。非自明な残り38本は第2部 `part2-rules/expression/` の個別章で扱います（70-5節）。パターンの構文は第30章の `cascades::dsl` と対比しながら確認します。

## 70-1. 3層Ruleの最下層 — 最適化の入口

式書き換えRuleは、論理同値Ruleと異なりメモ構造を介さずに動作します。論理同値Ruleがメモに同値な関係代数式を追加するのに対し、式書き換えRuleはスカラー式（ASTの `Expression`）を入力として受け取り、等価なスカラー式を返します。したがって、メモ、同値グループ、コスト計算のいずれにも依存しません。定義は `expression/rewrite.cpp` の `ExpressionRuleSet::Default()` に置かれています。基準リビジョンでは74回の `built.Add` を呼び出しますが、`abs_of_abs` が同名で2回登録されているため（`Add` は同名登録を上書きする仕様、70-2節）、実効的な登録数は73本です。このうち本章で35本、第2部で38本を扱い、合計73本が `00-index.md` の一覧と整合しています。

式書き換えが実行される契機は、オプティマイザの初期処理に留まりません。実行経路は以下の3箇所に存在します。

1. `plan/optimizer.cpp` の `Optimizer::Optimize` 冒頭（第1章 1-5節 (1)）: ORDER BYキー、SELECT式、WHERE述語、外部結合のON条件の正規化。
2. `expression/bytecode.cpp` の `BytecodeCompiler::Compile`: バイトコード生成直前の簡約。
3. `executor/detail/scan_filter.cpp` の `TryCompileSimpleCompare`: スキャン事前フィルタの構築。

SELECT句とORDER BYキーに対しては、`inner_join_not_null_inference` を除外した `value_context_rules` を適用します（第1章 1-5節 (1)）。さらにWHERE述語、ON条件、バイトコード生成、スキャン事前フィルタでは、`ContainsNotOfOrderedDoubleComparison` が真となる木に限り、`not_comparison` を除外した `NotComparisonFreeRules()` を適用します（70-3(b)節）。

この最下層での正規化は、上位の論理最適化を補助する役割を果たします。たとえば `canonicalize_comparison` によって定数を右辺に集めたり、`or_of_ranges_to_in` によって等値ORをINリストにまとめたりすることで、上位の論理Ruleやインデックスアクセスのパターン照合が成立しやすくなります。

## 70-2. フレームワーク — Pattern、Rule、RuleSet、Rewriter

式書き換えのフレームワークは、`expression/rewrite.hpp` に定義された4つの部品（`ExpressionPattern`、`ExpressionRule`、`ExpressionRuleSet`、`ExpressionRewriter`）で構成されます。

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

関係演算子を対象とする第30章の `Pattern` と比較すると、2つの違いがあります。第1に、照合対象がスカラー式であるため、キャプチャされる値も部分式（`ExpressionBindings` は名前から `Expression` への写像）となります。第2に、演算子指定が `std::optional` となっており、任意の演算子に一致させる条件を `nullopt` で表現します。

構築用DSLである `expression_dsl` には、`Any`、`Is`（型指定）、`Binary`、`AnyBinary`、`Unary`、`AnyUnary` の6関数が用意されています。これらはファクトリメソッドへの直接の委譲です。`ExpressionPattern::Match` の照合処理では、同一のキャプチャ名が複数回現れた場合に `Same`（ノード種別と `ToString()` の完全一致）を要求します。一致しない場合は照合失敗となります。この仕組みにより、`and_idempotent` のような「左右に同一の部分式が現れる」規則は、`Any("x")` を左右に指定するだけで記述できます。

個々の規則を保持する `ExpressionRule` は、規則名、照合パターン、書き換え関数の3点に加え、`root_only` フラグを保持します。設計方針（D6、`docs/design.md`）では「述語を追加する規則はルートの論理積集合にのみ適用する」と定めています。述語を追加する書き換えをANDの内側で無制限に適用すると、推論された述語が新たなANDを生成し続け、不動点探索が停止しなくなるためです。

複数の規則を束ねる `ExpressionRuleSet` は、内部に `std::vector<ExpressionRule>` を保持する値型です。`Add` を呼び出すと、内部で `Remove(rule.Name())` を実行してから新しい規則を追加します。したがって、同名規則の再登録は上書きとして機能します。`abs_of_abs` が2回登録されている実装は、この上書き動作に依存しています。

これらの規則を実行エンジンとして駆動するのが `ExpressionRewriter` です。`ExpressionRewriter` はRuleSetへの参照と探索パス上限 `pass_limit_` を保持します。書き換え処理の中核である `TryRewrite` の実装は以下のとおりです（`expression/rewrite.cpp`）。

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

書き換えは、1パス（`TryRewriteOnce` の1回実行）ごとに式全体を走査し、`Same` による不動点判定が成立するまで繰り返されます。規定のパス上限（32回）は無限ループを防ぐ安全弁であり、上限を超過してもクエリ処理をエラーで中断しません。適用される各書き換えは意味論を保存しているため、不動点に達しなかった場合でも直前の安定形（`current`）をそのまま返すことで、安全に実行を継続できます。

1パスを担う `TryRewriteOnce` はボトムアップで進行します。まずすべての子ノードを再帰的に書き換えた後、親ノードに対して登録順にRuleを評価します。最初に変換式を返したRuleが採用され、即座に次の処理へ移ります。この際、`rule.root_only() && depth != 0` の条件を満たすRuleは評価をスキップします。

## 70-3. 型と安全性の境界 — 2つの分割

式書き換えRuleは、入力スキーマの情報を持たずに実行されます（空のスキーマ `TryEvaluate(Row(), Schema())` を渡して定数判定を行う設計）。スキーマ非依存の設計は任意の場面で呼び出せる利点を持つ反面、式の静的型が未確定であるために生じる安全上の境界が存在します。tinylambでは、この問題を2通りの手法で切り分けています。

### (a) 型確定が必要な算術書き換えの分離 — `RewriteTypedArithmetic`

型情報が確定しなければ安全性を保証できない算術変形は、汎用RuleSetから切り離し、専用の関数として実装されています（`expression/rewrite.hpp`）。

```cpp
// Applies arithmetic rewrites that require resolved input-column types.
// Keeping these separate from the schema-free rule set prevents type-changing
// rewrites such as DOUBLE-column multiplication by an integer zero.
[[nodiscard]] Expression RewriteTypedArithmetic(const Expression& expression,
                                                const Schema& input_schema);
```

`RewriteTypedArithmetic` は、`TryRewrite` が完了した後に、入力スキーマが確定したオプティマイザの文脈（SELECT句、WHERE句、外部結合ON条件）から個別に呼び出されます。対象となる変形は、`-1 * x` の単項マイナス化、`x + x` から `x * 2` への変換、加減算の定数再結合、そして `x * 0` の畳み込みです。

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

浮動小数点数における `x * 0.0` は、NaNや無限大の入力に対して `0.0` にならないため、無条件のゼロへの変形は意味論を破壊します。そのため、この変形はオペランドが静的に `INT64` であると確定している場合に限定されます。さらに、列 `x` が NULL の場合に式全体が NULL を返さなければならないSQLの3値論理を維持するため、単純な `0` ではなく `CASE WHEN x IS NULL THEN CAST(NULL AS INT64) ELSE CAST(0 AS INT64) END` へと展開されます。

### (b) NaN と NOT — 型ブラインドな Rule の境界

スキーマを持たないRuleSetでは、浮動小数点数特有の意味論を見落とす危険が生じます。顕著な例が `not_comparison` です。二値論理においては `NOT(x < y)` を `x >= y` へ変形することは自明ですが、IEEE 754の浮動小数点演算においては成り立ちません。順序比較はオペランドのいずれかがNaNである場合に常にFALSEを返すため、`x` がNaNのとき `NOT(NaN < y)` はTRUEとなる一方、変形後の `NaN >= y` はFALSEに評価されます。

tinylambはこの安全性の境界を明示的な判定関数と代替RuleSetによって制御しています（`expression/rewrite.hpp`）。

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

呼び出し側（バイトコードコンパイラやスキャン事前フィルタ）はスキーマ情報を保持しているため、対象の式木に「DOUBLE型となり得るオペランドを含む順序比較のNOT」が存在するかを判定します。該当する構造が検出された場合、`Default()` から `not_comparison` を除外した `NotComparisonFreeRules()` を選択して書き換えを実行します。

## 70-4. 自明な Rule 35 本の一括解説

`Default()` に含まれる自明な35本のRuleを、意味論に基づく6つのグループに分類して整理します。表中の「guard」列は、変換関数が適用を拒絶して `Expression{}`（空の式）を返す条件を示します。

### 定数畳み込み（4本）

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

`fold_binary` は左右両辺が定数ノードである式を捕捉し、式全体の `TryEvaluate` を試みます。評価に成功した場合にのみ、得られた値を定数ノードとして返します。ゼロ除算（`1 / 0`）のように評価時にエラーを発生させる式は、`TryEvaluate` が失敗するため書き換えを行わず、実行時の例外処理を温存します。

| Rule | パターン → 書き換え | guard（適用除外条件） |
|---|---|---|
| `fold_binary` | 両辺が定数の二項演算 → `TryEvaluate` で評価した定数 | 評価失敗（ゼロ除算など） |
| `fold_unary` | 子ノードが定数の単項演算 → `TryEvaluate` で評価した定数 | 評価失敗 |
| `fold_in` | すべてのオペランドが定数の `IN` 式 → 評価した定数 | 非定数のオペランドが1つでも存在する場合 |
| `fold_function` | 引数がすべてリテラル（定数またはINTERVAL）の関数呼出し → 評価した定数 | 関数の揮発性分類（`GetFunctionVolatility`）が `kImmutable` 以外（`RAND` や `NOW` など） |

### IN と CASE の正規化（6本）

単一要素の `IN` 式を等値比較に変換する `singleton_in` では、NULL定数の扱いに注意が必要です（`expression/rewrite.cpp`）。

```cpp
          if (IsConstant(in.list_.front()) &&
              in.list_.front()->AsConstantValue().GetValue().IsNull()) {
            return Expression{};
          }
```

`x IN (NULL)` を単純に `x = NULL` に変換することは、三値論理の意味論上正しくありません（このケースは第2部の `in_single_null` が個別に引き受けます）。そのため、`singleton_in` はリスト要素がNULL定数の場合には適用を拒否します。

| Rule | パターン → 書き換え | guard（適用除外条件） |
|---|---|---|
| `singleton_in` | 要素が1つの `IN` 式（`x IN (c)`）→ 等値比較 `x = c` | リスト要素がNULL定数 |
| `dedupe_in_list` | `IN` 式のリスト要素から重複を除去（順序維持） | 重複が存在しない場合 |
| `empty_in_list` | リストが空の `IN` 式 → FALSE定数 | 左辺の子式が非NULLの静的定数でない場合（左辺がNULLのときは三値論理によりNULLを返さなければならないため） |
| `simplify_case` | CASE式から定数WHEN条件を整理（定数TRUEに達した時点でELSEに繰り上げ、後続を枝刈り） | 定数のWHEN条件が1つも存在しない場合 |
| `uniform_case_result` | すべてのWHEN分岐およびELSEの結果が同一値のCASE式 → その共通結果 | 各WHEN条件が `ExpressionCannotThrow`（評価時に例外を発生させない安全判定）かつ `SafeToReduceEvaluationCount`（副作用を含まない）を満たさない場合 |
| `if_to_case` | 3引数の `IF(c, t, e)` → `CASE WHEN c THEN t ELSE e END` | なし（構文の正規化） |

### 比較とブール論理の正規化（6本）

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
| `boolean_identity` | 真偽定数を含むAND/ORの簡約（`TRUE AND x` → `x` など） | 残存式が非ブール型、または消去される左辺式が例外を送出する可能性がある場合 |
| `double_negation` | `NOT(NOT(x))` → `x` | `x` が静的に非ブール型 |
| `and_idempotent` / `or_idempotent` | 同一式の論理積・論理和（`x AND x` / `x OR x`）→ `x` | `x` が静的に非ブール型、または `SafeToReduceEvaluationCount` が偽（評価回数を減らせない副次的作用を含む） |
| `canonicalize_boolean` | ブール評価式と0/1定数との比較（`expr = 1` など）→ 式そのものまたは `NOT expr` | 定数がNULLまたは0/1以外の整数 |

### 算術恒等式（10本）

加算の恒等式 `identity_add_zero` には、最適化の品質と厳密な型付けの兼ね合いが存在します（`expression/rewrite.cpp`）。

```cpp
          // NOTE: intentional deviation from the AST ground truth (kept for
          // optimizer quality, pinned by the optimizer_arithmetic compliance
          // file): the identities fire on numeric/int columns even though
          // the AST types e.g. `a + 0.0` as double.  Known IEEE gap: a
          // -0.0 survivor is normalized to +0.0.
```

ASTの厳密な評価基準では `a + 0.0` はDOUBLE型へと昇格しますが、このRuleはオプティマイザのインデックス照合効率を維持するため、整数型列に対しても適用されます。この挙動はコンプライアンステストによって固定された既知の仕様逸脱です。

| Rule | パターン → 書き換え | guard（適用除外条件） |
|---|---|---|
| `identity_add_zero` | `x + 0` または `0 + x` → `x` | 残存側が静的に非数値型 |
| `identity_subtract_zero` | `x - 0` → `x` | 右辺がゼロ定数でない、または左辺が静的に非数値型 |
| `identity_multiply_one` | `x * 1` または `1 * x` → `x` | 残存側が静的に非数値型 |
| `identity_divide_one` | `x / 1` → `x` | 右辺が1定数でない、または左辺が静的に非数値型 |
| `canonicalize_add_negative_constant` | `x + (-c)`（右辺が負の整数定数）→ `x - c` | 左辺が静的に非数値型、または定数が `INT64_MIN`（反転時にオーバーフローするため） |
| `canonicalize_subtract_negative_constant` | `x - (-c)` → `x + c` | 同上 |
| `multiply_by_negative_one` | `x * (-1)` または `(-1) * x` → `-x` | 定数が `-1` 以外、または残存側が静的に非数値型 |
| `combine_repeated_addend` | `x + x` → `x * 2` | 静的に非数値型、または評価回数の削減が不安全 |
| `double_negation_arithmetic` | `-(-x)` → `x` | 子ノードが静的に非数値型、または子ノードが `INT64_MIN` 定数 |
| `double_bitwise_negation` | `~(~x)` → `x` | 静的に非数値型、DOUBLE型、または型未確定 |

### 定数の再結合（4本）

`(x + 1) + 2` を `x + 3` へ変形する再結合は、中間結果におけるオーバーフローの発生有無を変化させる可能性があります。

```cpp
// expression/rewrite.cpp
// Reassociating (x + a) + b -> x + (a + b) can elide the intermediate
// x + a overflow, changing throw-vs-value.
```

したがって、再結合Rule群では定数の符号が一致していることを条件とし、中間オーバーフローの消失によって計算結果やエラー挙動が変わらないことを保証しています。

| Rule | パターン → 書き換え | guard（適用除外条件） |
|---|---|---|
| `reassociate_add_constants` | `(x + a) + b` → `x + (a + b)` | 定数 `a` と `b` の符号が一致しない、または `a + b` の加算自体がオーバーフローする場合 |
| `reassociate_subtract_constants` | `(x - a) - b` → `x - (a + b)` | 同上 |
| `reassociate_subtract_add_constants` | `(x - a) + b` → `x + (b - a)` | `(a <= 0) == (b >= 0)` が不成立、または定数計算がオーバーフローする場合 |
| `reassociate_add_subtract_constants` | `(x + a) - b` → `x + (a - b)` | `(a >= 0) == (b <= 0)` が不成立、または定数計算がオーバーフローする場合 |

### 関数と配列の平坦化（5本）

多重にネストした同名関数呼び出しを展開し、引数リストを平坦化します。

| Rule | パターン → 書き換え | guard（適用除外条件） |
|---|---|---|
| `abs_of_abs` | `ABS(ABS(x))` → `ABS(x)` | 引数の構造が合致しない場合 |
| `concat_flatten` | `CONCAT(CONCAT(a, b), c)` → `CONCAT(a, b, c)` | 変化がない、または結果の引数が2個未満 |
| `array_flatten_optimization` | ネストした `ARRAY_CONCAT` の展開および空配列リテラルの除去 | 引数の構造が合致しない場合 |
| `pow_identities` | `POW(x, 1)` → `CAST(x AS FLOAT64)` | 指数が1定数でない場合（戻り値の型をFLOAT64に固定するためのCASTを付与） |
| `greatest_least_fold` | `GREATEST(GREATEST(a, b), c)` → `GREATEST(a, b, c)` | 引数が空、または結果の引数が2個未満 |

これら35本の自明なRuleに共通する原則は、「式を消去または変形する際、その式が本来発生させるはずだった実行時例外や副作用を消失させてはならない」という点です。`ExpressionCannotThrow` や `SafeToReduceEvaluationCount` による事後条件判定は、この意味論的整合性を担保するために設けられています。

## 70-5. 第2部の個別章への橋渡し（38本）

残る38本のRule（ド・モルガンの法則、吸収則、共通積因子の抽出など）は、複雑な適用条件や詳細な意味論的検証を要するため、第2部 `part2-rules/expression/` の個別章で詳細に解説します。

- NOTの展開（6本）: `de_morgan.md`, `not_comparison.md`, `not_like.md`, `not_not_like.md`, `not_is_null.md`, `not_is_not_null.md`
- ブール論理の簡約（9本）: `xor_boolean_identity.md`, `xor_to_or_and_not.md`, `absorption_and.md`, `absorption_and_reversed.md`, `absorption_or.md`, `absorption_or_reversed.md`, `boolean_filter_pullup.md`, `distribute_or_over_and_budgeted.md`, `boolean_eq_true_false_three_valued.md`
- 文字列照合と正規表現（3本）: `like_equality.md`, `not_like_equality.md`, `regexp_prefix_extraction.md`
- NULLと三値論理（5本）: `is_null_of_null_check.md`, `is_not_null_of_null_check.md`, `contradiction_from_null_eq.md`, `in_single_null.md`, `not_in_null_semantics.md`
- 型変換と関数特性（10本）: `collapse_nested_identical_cast.md`, `nullif_to_case.md`, `coalesce_and_nullif_simplification.md`, `deterministic_function_cse.md`, `function_volatility_classification.md`, `nondeterministic_barrier.md`, `safe_divide_rewrite.md`, `datetime_and_string_fold_extent.md`, `json_path_constant_fold.md`, `numeric_widening_cast.md`
- 述語推論と因子展開（5本）: `factor_or_common_and.md`, `or_of_ranges_to_in.md`, `interval_normalize.md`, `predicate_pushdown_case.md`, `inner_join_not_null_inference.md`

## 70-6. まとめ

- 式書き換えは最下層に位置し、関係代数メモを介さずスカラー式を直接変形する。オプティマイザ、バイトコードコンパイラ、スキャン事前フィルタで共有され、正規化によって上位Ruleの照合を支援する。
- フレームワークは `ExpressionPattern`、`ExpressionRule`、`ExpressionRuleSet`、`ExpressionRewriter` の4要素で構成される。ボトムアップ走査によって不動点に達するまで適用され、反復上限超過時は直前の安定形を返却することで実行可能性を担保する（D6）。
- 静的型が必要な算術最適化は `RewriteTypedArithmetic` に分離され、浮動小数点のNaN不整合を避けるため `ContainsNotOfOrderedDoubleComparison` による動的なRuleSet切り替え（`NotComparisonFreeRules`）を行う。
- 自明な35本のRule群では、評価短絡や式の消去に伴う例外消失・副作用消失を防ぐため、`ExpressionCannotThrow` や `SafeToReduceEvaluationCount` を用いた網羅的なガードが設定されている。


/-!
SQL predicates can return TRUE, FALSE, or UNKNOWN. The equivalence proofs and
counterexamples identify which Boolean rewrites preserve three-valued results and which
filters may pass rows.
-/

/-
# Step 1: SQL 三値論理（TRUE / FALSE / NULL）とスカラー式書き換えの検証

C++ プログラマのための超ざっくり Lean 4 読み方ガイド：
- `inductive` : C++ の `enum class` や `std::variant`（代数的データ型）
- `def`       : 通常の関数定義（`match ... with` は網羅性チェック付きの `switch`）
- `Prop`      : 「真偽を証明したい命題」の型（`a = b` や `∀ x, ...` など）
- `theorem`   : コンパイル時に **全入力パターンで絶対に正しいこと** が検査される定理。
                もし 1 パターンでも反例があれば、**コンパイルエラー**になります！
- `by ...`    : 証明スクリプト（タクティク）。
                `cases x` は `x` の全ケースで場合分け、`rfl` は「両辺を計算すると同じ値になる」、
                `decide` は有限パターンの総当たり自動判定です。
-/

namespace TinylambOpt

/-- SQL の三値論理型（TRUE / FALSE / UNKNOWN(=NULL)）。
    `deriving DecidableEq, Repr` は C++ で `operator==` とデバッグ出力を自動生成するイメージです。 -/
inductive SqlBool where
  | tTrue  : SqlBool
  | tFalse : SqlBool
  | tNull  : SqlBool
  deriving DecidableEq, Repr

namespace SqlBool

/-- SQL の `NOT`（Kleene の三値否定）: NULL を反転しても NULL のまま。 -/
def not : SqlBool → SqlBool
  | .tTrue  => .tFalse
  | .tFalse => .tTrue
  | .tNull  => .tNull

/-- SQL の `AND`: 片方が FALSE なら相手が NULL でも FALSE になる（ここが直感とズレやすい急所！）。 -/
def and : SqlBool → SqlBool → SqlBool
  | .tFalse, _       => .tFalse
  | _,       .tFalse => .tFalse
  | .tTrue,  .tTrue  => .tTrue
  | _,       _       => .tNull

/-- SQL の `OR`: 片方が TRUE なら相手が NULL でも TRUE になる。 -/
def or : SqlBool → SqlBool → SqlBool
  | .tTrue, _      => .tTrue
  | _,      .tTrue => .tTrue
  | .tFalse, .tFalse => .tFalse
  | _,      _      => .tNull

/-- SQL の `IS TRUE` 演算子: NULL を FALSE に潰して二値化する。 -/
def isTrue : SqlBool → SqlBool
  | .tTrue => .tTrue
  | _      => .tFalse

/-- SQL の `IS NOT TRUE` 演算子: FALSE または NULL のとき TRUE。 -/
def isNotTrue : SqlBool → SqlBool
  | .tTrue => .tFalse
  | _      => .tTrue

/-- `WHERE` 句や `JOIN ON` 句でのフィルタ判定:
    SQL では `tTrue` の行だけを通し、`tFalse` と `tNull` は両方とも落とす！ -/
def inWhere : SqlBool → Bool
  | .tTrue => true
  | _      => false

end SqlBool

/-!
## 1-1. `expression/rewrite.cpp` の基本ブール書き換えルールの証明

まずは `ExpressionRuleSet::Default()` に入っている基本的な規則が、
三値論理（`tNull` 含む）でも**完全に値を保存する**ことを 1 行で証明してみます。
-/

/-- `double_negation`: `NOT (NOT x)` は `x` が NULL でも常に `x` と完全一致する。 -/
theorem double_negation_sound (x : SqlBool) :
    x.not.not = x := by
  cases x <;> rfl

/-- `and_idempotent` / `or_idempotent`: `x AND x = x`, `x OR x = x`。 -/
theorem and_idempotent_sound (x : SqlBool) : x.and x = x := by
  cases x <;> rfl

theorem or_idempotent_sound (x : SqlBool) : x.or x = x := by
  cases x <;> rfl

/-- `de_morgan`: 三値論理でもド・モルガンの法則 `NOT (a AND b) = (NOT a) OR (NOT b)` は成立する！
    （3 × 3 = 9 通りすべてを `cases a <;> cases b <;> rfl` でコンパイラが網羅検証） -/
theorem de_morgan_and_sound (a b : SqlBool) :
    (a.and b).not = (a.not).or (b.not) := by
  cases a <;> cases b <;> rfl

theorem de_morgan_or_sound (a b : SqlBool) :
    (a.or b).not = (a.not).and (b.not) := by
  cases a <;> cases b <;> rfl

/-- `absorption_and`: 吸収則 `a AND (a OR b) = a` は実は三値論理でも成り立つか？
    （人間が頭で考えると `a = NULL, b = FALSE` あたりで不安になりますが、Lean なら即座に検証可能！） -/
theorem absorption_and_sound (a b : SqlBool) :
    a.and (a.or b) = a := by
  cases a <;> cases b <;> rfl

/-- `WHERE` フィルタにおける `AND` / `OR` の分解法則:
    三値論理のままでは `NULL` がありますが、`inWhere`（`tTrue` かどうか）を通すと
    通常の `Bool` の `&&` / `||` に綺麗に落ちます（`merge_selections` ルールの根拠）。 -/
theorem and_inWhere (a b : SqlBool) :
    (a.and b).inWhere = (a.inWhere && b.inWhere) := by
  cases a <;> cases b <;> rfl

theorem or_inWhere (a b : SqlBool) :
    (a.or b).inWhere = (a.inWhere || b.inWhere) := by
  cases a <;> cases b <;> rfl

/-!
## 1-2. 三値論理で「うっかり書き換えるとバグる」罠の検出！

逆に、二値論理（C++ の `bool`）の感覚で Optimizer ルールを書くと壊れる代表例です。
Lean では「成り立たない（`¬ ∀ ...`）」こと自体も定理として証明できます。
-/

/-- 罠1（排中律の崩壊）: `x OR (NOT x)` を `TRUE` に畳み込むルールを書くとバグる！
    理由: `x = tNull` のとき `tNull OR tNull = tNull ≠ tTrue`。 -/
theorem excluded_middle_unsound :
    ¬ (∀ x : SqlBool, x.or x.not = .tTrue) := by
  intro h
  have hNull := h .tNull
  -- `hNull` は `.tNull = .tTrue` となり矛盾
  cases hNull

/-- NULL 許容スカラー値: `none` が SQL の `NULL`、`some v` が非 NULL 値 `v` を表します。 -/
abbrev SqlVal (α : Type) := Option α

/-- SQL の `=` 比較: どちらかが `NULL` なら（たとえ `NULL = NULL` でも！）結果は `tNull`。 -/
def sqlEq {α : Type} [DecidableEq α] (a b : SqlVal α) : SqlBool :=
  match a, b with
  | some x, some y => if x = y then .tTrue else .tFalse
  | _,      _      => .tNull

/-- 3 値 `IN` リスト評価。 -/
def evalInList {α : Type} [DecidableEq α] (x : SqlVal α) : List (SqlVal α) → SqlBool
  | [] => .tFalse
  | [y] => sqlEq x y
  | y :: ys => (sqlEq x y).or (evalInList x ys)

theorem empty_in_is_false {α : Type} [DecidableEq α] (x : SqlVal α) :
    evalInList x [] = .tFalse := rfl

theorem evalInList_cons {α : Type} [DecidableEq α] (x y : SqlVal α)
    (ys : List (SqlVal α)) :
    evalInList x (y :: ys) = (sqlEq x y).or (evalInList x ys) := by
  cases ys with
  | nil => simp only [evalInList]; cases sqlEq x y <;> rfl
  | cons => rfl

/-- SQL の `IS NOT DISTINCT FROM`（NULL 安全等価）: `NULL` 同士も `tTrue` になる。 -/
def sqlIsNotDistinctFrom {α : Type} [DecidableEq α] (a b : SqlVal α) : SqlBool :=
  match a, b with
  | none,   none   => .tTrue
  | some x, some y => if x = y then .tTrue else .tFalse
  | _,      _      => .tFalse

/-- 罠2（自己等価 `x = x`）: `comparison_self_predicates` で `x = x` を無条件に `TRUE` にするとバグる！
    `x = NULL` のとき `sqlEq none none = .tNull` になるため、`NOT NULL` 保証が必須。 -/
theorem self_eq_unsound_without_not_null :
    ¬ (∀ x : SqlVal Int, sqlEq x x = .tTrue) := by
  intro h
  have hNull := h none
  cases hNull

/-- 一方、`x` が非 NULL（`x ≠ none`）だと証明できるなら `x = x` は常に `tTrue`！ -/
theorem self_eq_sound_when_not_null {α : Type} [DecidableEq α]
    (x : SqlVal α) (hNotNull : x ≠ none) :
    sqlEq x x = .tTrue := by
  cases x with
  | none => contradiction
  | some v => simp [sqlEq]

/-!
## 1-3. IEEE 754 `NaN` と `not_comparison` の罠（`70-expression-rewrite.md` §70-3(b)）

`tinylamb` では DOUBLE 列を含むとき `NOT (x < y) -> x >= y`（`not_comparison`）を無効化しています。
この理由も 10 行で形式化できます。
-/

/-- 簡易浮動小数点モデル: 通常の整数値または `nan`。 -/
inductive FloatVal where
  | finite : Int → FloatVal
  | nan    : FloatVal
  deriving DecidableEq, Repr

def floatLt : FloatVal → FloatVal → SqlBool
  | .finite x, .finite y => if x < y then .tTrue else .tFalse
  | _,         _         => .tFalse  -- IEEE 754: NaN との順序比較は常に FALSE

def floatGe : FloatVal → FloatVal → SqlBool
  | .finite x, .finite y => if x ≥ y then .tTrue else .tFalse
  | _,         _         => .tFalse  -- IEEE 754: NaN >= y も FALSE！

/-- `NOT (x < y) = (x >= y)` は `NaN` があると崩壊する！
    `x = nan` のとき左辺は `NOT FALSE = TRUE`、右辺は `FALSE`。 -/
theorem not_comparison_unsound_on_nan :
    ¬ (∀ x y : FloatVal, (floatLt x y).not = floatGe x y) := by
  intro h
  have hNan := h .nan (.finite 0)
  cases hNan

/-!
## 1-4. `or_to_union`（`docs/opt/part2-rules/logical/or_to_union.md`）の三値論理核定理

`Selection(p0 OR p1)` を `UNION ALL` に分解するとき、単純に `p0` と `p1` で分けると両方を満たす行が 2 回出ます。
かといって第 2 枝を `p1 AND (NOT p0)` にすると、`p0 = NULL, p1 = TRUE` の行で
`p1 AND (NOT NULL) = TRUE AND NULL = NULL` となって行が消滅する大バグになります！

`tinylamb` の `or_to_union` は第 2 枝を
  `IS_TRUE(p1) AND IS_NOT_TRUE(IS_TRUE(p0))`
と構成しています。これが **どんな三値論理の組み合わせでも排他的かつ漏れがない** ことを検証します。
-/

/-- 悪い書き換え案: 第 2 枝を `p1 AND NOT p0` にした場合、`p0 = NULL, p1 = TRUE` の行が消えるバグの証明！ -/
theorem naive_or_split_loses_null_row :
    let p0 := SqlBool.tNull
    let p1 := SqlBool.tTrue
    (p0.or p1).inWhere = true ∧
    p0.inWhere = false ∧
    (p1.and p0.not).inWhere = false := by
  decide

/-- `tinylamb` の `or_to_union` 第 0 枝述語: `IS_TRUE(p0)` -/
def orBranch0 (p0 : SqlBool) : SqlBool :=
  p0.isTrue

/-- `tinylamb` の `or_to_union` 第 1 枝述語: `IS_TRUE(p1) AND IS_NOT_TRUE(IS_TRUE(p0))` -/
def orBranch1 (p0 p1 : SqlBool) : SqlBool :=
  p1.isTrue.and p0.isTrue.isNotTrue

/-- 核定理1（排他性）: 第 0 枝と第 1 枝の両方を同時に通過する行は絶対に存在しない。 -/
theorem or_branches_disjoint (p0 p1 : SqlBool) :
    ¬ ((orBranch0 p0).inWhere = true ∧ (orBranch1 p0 p1).inWhere = true) := by
  cases p0 <;> cases p1 <;> decide

/-- 核定理2（完全被覆と多重度 1 保存）:
    元の `p0 OR p1` を通過した回数を `0 / 1` で数えると、
    第 0 枝の通過回数（`0 / 1`）＋ 第 1 枝の通過回数（`0 / 1`）と **全 9 パターンで完全一致**する！ -/
theorem or_branches_exact_count (p0 p1 : SqlBool) :
    ((p0.or p1).inWhere.toNat) =
      (orBranch0 p0).inWhere.toNat + (orBranch1 p0 p1).inWhere.toNat := by
  cases p0 <;> cases p1 <;> rfl

end TinylambOpt

-- C++ references to this model or its proof.
-- expression/rewrite.cpp
-- plan/cascades.cpp
-- plan/cascades_predicate_rules.cpp

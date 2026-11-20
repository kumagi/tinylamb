import TinylambOpt.BagRel

/-
# Step 3: バグ発見と「さらにアグレッシブな書き換えルール」の発見
## ケーススタディ: `outer_to_anti_join`（`plan/cascades.cpp`）

SQL で頻出するアンチ結合イディオム：
```sql
SELECT ... FROM L LEFT JOIN R ON p(L, R) WHERE R.col IS NULL
```
現行の `outer_to_anti_join` は、右列の NOT NULL 宣言、または ON 述語がその列の NULL を拒否することを検査します。左側だけを参照する残余連言は Selection として保持します。テストは `OuterToAntiJoinFiresOnNullableJoinKeyRejectedByOn` と `OuterToAntiJoinPreservesLeftResidualConjunct` です。

ここでは Lean を使って：
1. まず「無条件に AntiJoin 化すると本当に壊れるケース（反例）」を確認し、
2. 証明が詰まる箇所を観察することで、**カタログ制約が一切なくても（CTE やサブクエリでも！）安全に発火でき、
   さらに残余フィルタまで左入力へ押し込める「よりアグレッシブな書き換え定理」** を導出・機械証明します！
-/

namespace TinylambOpt

/-- `LeftOuterJoin` の出力タプル `(x, optR)` に対する `R.col IS NULL` の意味論。
    - マッチせず NULL パディングされた行 `(x, none)` では R の全列が NULL なので `tTrue`
    - マッチした行 `(x, some y)` では `col y` が元々 `none`（NULL）だったときだけ `tTrue` -/
def rightColIsNull {L R Val : Type} (col : R → SqlVal Val) : (L × Option R) → SqlBool
  | (_, none)   => .tTrue
  | (_, some y) =>
    match col y with
    | none   => .tTrue
    | some _ => .tFalse

/-- `AntiJoin` の結果（左テーブルの生存行）に右側 NULL パディング `(x, none)` を付与する関数
    （`LeftOuterJoin` の出力スキーマ `L × Option R` と型を揃えて厳密比較するため）。 -/
def padRightNull {L R : Type} (rows : Rel L) : Rel (L × Option R) :=
  rows.map (fun x => (x, none))

/-!
## 3-1. なぜ無条件だとバグるのか？（Lean による 2 つの反例証明）
-/

/-- 右テーブルの 2 列スキーマ例: 結合キー `id` とメモ列 `note`（どちらも NULL 許容）。 -/
structure RightRow where
  id   : SqlVal Int
  note : SqlVal Int
  deriving DecidableEq, Repr

/-- **反例 A（結合キーではない NULL 許容列を `IS NULL` した場合）**:
    `ON l = r.id WHERE r.note IS NULL` のとき、`r = { id = 1, note = NULL }` は結合にマッチしつつ
    `r.note IS NULL` も通過する！ これを `AntiJoin` にすると行が消えるバグになる。 -/
theorem bug_when_testing_non_join_nullable_col :
    let l : Rel Int := [1]
    let r : Rel RightRow := [{ id := some 1, note := none }]
    let onPred := fun (x : Int) (y : RightRow) => sqlEq (some x) y.id
    select (rightColIsNull RightRow.note) (leftOuterJoin onPred l r) ≠
    padRightNull (antiJoin onPred l r) := by
  decide

/-- **反例 B（結合キー自身でも `IS NOT DISTINCT FROM` で結合した場合）**:
    `ON l IS NOT DISTINCT FROM r WHERE r IS NULL` のとき、`l = NULL` と `r = NULL` がマッチしてしまうため、
    やはり無条件に `AntiJoin` にすると行が消えるバグになる。 -/
theorem bug_when_join_allows_null_match :
    let l : Rel (SqlVal Int) := [none]
    let r : Rel (SqlVal Int) := [none]
    let onPred := fun (x y : SqlVal Int) => sqlIsNotDistinctFrom x y
    select (rightColIsNull id) (leftOuterJoin onPred l r) ≠
    padRightNull (antiJoin onPred l r) := by
  decide

/-!
## 3-2. 証明から逆算する「本質的な安全条件」の発見！

上の反例 A・B がなぜ起きたかを Lean の証明ゴールで見ると、必要な条件はたった 1 行に集約されます：

> **「`r` の中の行 `y` が結合条件 `onPred x y` を通過した（`= tTrue`）ならば、`col y` は `none` ではない」**

これを満たすルートは 2 つあります：
1. **現行 `tinylamb` の条件 (`CatalogNotNull`)**: そもそもテーブル `r` 内の全行で `col y ≠ none`
2. **ON 述語による条件 (`JoinRejectsNullOnCol`)**:
   テーブル `r` に `NULL` が何百万行入っていても、**結合条件 `onPred` 自体が `col y = none` を弾く**
   （例：`ON L.id = R.id` で `WHERE R.id IS NULL` を書いた場合、`sqlEq _ none = .tNull` なので絶対にマッチしない！）
   ※しかも `tinylamb` には既に `ExpressionRejectsNullsOnColumn`（`plan/cascades.cpp`）が実装済み！
-/

/-- 条件1（現行）: カタログ制約等により、右リレーション `r` 内で `col` が一切 NULL を取らない。 -/
def CatalogNotNull {R Val : Type} (r : Rel R) (col : R → SqlVal Val) : Prop :=
  ∀ y ∈ r, col y ≠ none

/-- 条件2（新発見）: 結合述語 `onPred` が `col` に関して Null-Rejecting である
    （`col y = none` の行は決して `onPred x y = tTrue` にならない）。 -/
def JoinRejectsNullOnCol {L R Val : Type}
    (onPred : L → R → SqlBool) (col : R → SqlVal Val) : Prop :=
  ∀ (x : L) (y : R), col y = none → (onPred x y).inWhere = false

/-- 典型例の確認: 通常の等値結合 `ON f(x) = R.col` は、カタログ制約ゼロでも常に `JoinRejectsNullOnCol` を満たす！ -/
theorem equiJoin_always_rejects_null_on_col {L R Val : Type} [DecidableEq Val]
    (lhsExpr : L → SqlVal Val) (col : R → SqlVal Val) :
    JoinRejectsNullOnCol (fun x y => sqlEq (lhsExpr x) (col y)) col := by
  intro x y hNull
  simp only [sqlEq, hNull]
  cases lhsExpr x <;> rfl

/-- 核補題: `CatalogNotNull` または `JoinRejectsNullOnCol` の**どちらか一方**でも成り立てば、
    結合にマッチした右行 `y ∈ r` が `rightColIsNull col (x, some y)` を通過することは絶対にない。 -/
private theorem matched_row_never_passes_is_null {L R Val : Type}
    (onPred : L → R → SqlBool) (col : R → SqlVal Val) (r : Rel R)
    (hSafe : CatalogNotNull r col ∨ JoinRejectsNullOnCol onPred col)
    (x : L) (y : R) (hyMem : y ∈ r) (hMatch : (onPred x y).inWhere = true) :
    (rightColIsNull col (x, some y)).inWhere = false := by
  have hNotNull : col y ≠ none := by
    cases hSafe with
    | inl hCat => exact hCat y hyMem
    | inr hRej =>
      intro hNull
      have hFalse := hRej x y hNull
      rw [hFalse] at hMatch
      contradiction
  cases hCol : col y with
  | none => contradiction
  | some v => simp [rightColIsNull, hCol, SqlBool.inWhere]

/-!
## 3-3. アグレッシブ版 `outer_to_anti_join`（残余フィルタ押し込み統合版）の完全証明！

さらに欲張って、現行ルールが保持する左側の残余連言を一般化し、
`WHERE R.col IS NULL AND q(L, R)`（残余フィルタ `q` の混在）も一気に解決します。

`R.col IS NULL` を生き残る行はすべて `(x, none)`（右テーブル側が全列 NULL）なので、
残余フィルタ `q(L, R)` の中の `R` 参照をすべて `none`（NULL）に置き換えた `fun x => q x none` を
**`AntiJoin` の左入力 `L` へ事前プッシュダウン**しても結果が 100% 一致することを証明します！
-/

/-- 補助補題: 2段フィルタ＋map を `flatMap` の1パス形式に書き換える。 -/
private theorem filter_filter_map_eq_flatMap {α β : Type}
    (p1 p2 : α → Bool) (f : α → β) :
    ∀ l : List α, ((l.filter p1).filter p2).map f =
      l.flatMap (fun x => if p1 x && p2 x then [f x] else [])
  | [] => rfl
  | x :: xs => by
    have ih := filter_filter_map_eq_flatMap p1 p2 f xs
    cases h1 : p1 x <;> cases h2 : p2 x <;>
      simp only [List.filter, List.flatMap_cons, List.map, h1, h2,
                 Bool.false_and, Bool.true_and, Bool.false_eq_true, ↓reduceIte,
                 List.nil_append, List.cons_append, ih]

/-- **主定理 (`outer_to_anti_join_aggressive`)**:
    `CatalogNotNull r col` **または** `JoinRejectsNullOnCol onPred col` が成り立つとき、
    残余フィルタ `q(L, R)` が付いていても：
      `Selection(R.col IS NULL AND q(L, R), LeftOuterJoin(L, R, onPred))`
    は、残余フィルタの R 側を NULL 評価して左入力へ押し込んだ AntiJoin：
      `padRightNull(AntiJoin(Selection(q(L, NULL), L), R, onPred))`
    と **Bag 多重度・行順序まで含めて完全に一致（`=`）する**！ -/
theorem outer_to_anti_join_aggressive {L R Val : Type}
    (onPred : L → R → SqlBool)
    (col : R → SqlVal Val)
    (q : L → Option R → SqlBool)
    (l : Rel L) (r : Rel R)
    (hSafe : CatalogNotNull r col ∨ JoinRejectsNullOnCol onPred col) :
    select (fun lr => (rightColIsNull col lr).and (q lr.1 lr.2)) (leftOuterJoin onPred l r) =
    padRightNull (antiJoin onPred (select (fun x => q x none) l) r) := by
  -- 両辺を `l.flatMap (fun x => ...)` に揃えて、左の各行 `x` ごとの一致に帰着させる
  simp only [select, leftOuterJoin, padRightNull, antiJoin, List.filter_flatMap,
             filter_filter_map_eq_flatMap]
  congr 1
  funext x
  -- `x` にマッチする `r` の行リストが「空（0件）」か「1件以上」かで場合分け
  cases hMatches : r.filter (fun y => (onPred x y).inWhere) with
  | nil =>
    -- 【ケース1: マッチ 0 件】 NULL 補完行 `(x, none)` が生成され、`q x none` だけで合否が決まる
    have hAny : r.any (fun y => (onPred x y).inWhere) = false := by
      simpa [List.filter_eq_nil_iff] using hMatches
    rw [hAny]
    simp only [List.isEmpty_nil, ↓reduceIte, List.filter, rightColIsNull]
    cases q x none <;> rfl
  | cons y0 ys =>
    -- 【ケース2: マッチ 1 件以上】 マッチした全行で `rightColIsNull` が `false` になるため両方とも `[]`
    have hAny : r.any (fun y => (onPred x y).inWhere) = true := by
      have hy0MemFilter : y0 ∈ r.filter (fun y => (onPred x y).inWhere) := by
        simp [hMatches]
      rw [List.mem_filter] at hy0MemFilter
      exact List.any_eq_true.mpr ⟨y0, hy0MemFilter.1, hy0MemFilter.2⟩
    have hAllFalse : ∀ y ∈ (y0 :: ys),
        (((rightColIsNull col (x, some y)).and (q x (some y))).inWhere) = false := by
      intro y hy
      have hyInFilter : y ∈ r.filter (fun z => (onPred x z).inWhere) := by
        simpa [hMatches] using hy
      rw [List.mem_filter] at hyInFilter
      have hNullFalse :=
        matched_row_never_passes_is_null onPred col r hSafe x y hyInFilter.1 hyInFilter.2
      rw [and_inWhere, hNullFalse, Bool.false_and]
    simp only [List.isEmpty_cons, Bool.false_eq_true, ↓reduceIte, hAny, Bool.not_true,
               Bool.and_false, List.filter_eq_nil_iff, List.mem_map, forall_exists_index,
               and_imp, forall_apply_eq_imp_iff₂]
    exact fun a ha => by simp [hAllFalse a ha]

/-- 特に残余フィルタが無い単純形（`WHERE R.col IS NULL` のみ）の系（Corollary）:
    `CascadesTest.OuterToAntiJoinRefusesNullableNullTestedColumn` のクエリ
    `SELECT ... FROM t1 LEFT JOIN t2 ON t1.id = t2.id WHERE t2.id IS NULL`
    は、`t2.id` が nullable でも AntiJoin と完全一致することがここから直ちに従う！ -/
theorem outer_to_anti_join_equi_nullable_sound {L R Val : Type} [DecidableEq Val]
    (lhsExpr : L → SqlVal Val) (col : R → SqlVal Val)
    (l : Rel L) (r : Rel R) :
    let onPred := fun x y => sqlEq (lhsExpr x) (col y)
    select (rightColIsNull col) (leftOuterJoin onPred l r) =
    padRightNull (antiJoin onPred l r) := by
  intro onPred
  have hRej := equiJoin_always_rejects_null_on_col lhsExpr col
  have hMain := outer_to_anti_join_aggressive
    onPred col (fun _ _ => .tTrue) l r (Or.inr hRej)
  have hAndTrue : ∀ b : SqlBool, b.and .tTrue = b := by
    intro b; cases b <;> rfl
  have hSelectTrue : select (fun (_ : L) => SqlBool.tTrue) l = l := by
    simp [select, SqlBool.inWhere]
  simpa [hAndTrue, hSelectTrue] using hMain

end TinylambOpt

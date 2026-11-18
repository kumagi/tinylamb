import TinylambOpt.ThreeValued

/-
# Step 2: Bag（多重集合）関係代数と Cascades 論理ルールの証明

SQL のテーブルは集合（Set）ではなく **Bag（多重集合 = 重複行あり）** です。
「同じ行が 1 行出るか 2 行出るか」まで一致しないと SQL オプティマイザとしてはバグになります。

ここでは：
- テーブル `Rel Row` を `List Row`（重複ありの行リスト）として定義し、
- 「出力順は問わないが、すべての行の出現回数（多重度）が完全一致すること」を
  標準ライブラリの順列同値 **`List.Perm`（`BagEq`）** で表します。
-/

namespace TinylambOpt

/-- 重複行を許すリレーション（テーブル）表現。 -/
abbrev Rel (Row : Type) := List Row

/-- Bag 等価性: `r1` と `r2` が行の並び替えを除いて同じ重複度を持つこと（`r1 ~ r2`）。 -/
abbrev BagEq {Row : Type} (r1 r2 : Rel Row) : Prop :=
  List.Perm r1 r2

/-- `Selection(p, r)`（`WHERE p`）: 三値論理で `tTrue` になる行だけを残す。 -/
def select {Row : Type} (p : Row → SqlBool) (r : Rel Row) : Rel Row :=
  r.filter (fun row => (p row).inWhere)

/-- `UnionAll(r1, r2)`（`UNION ALL`）: 重複排除せずそのまま連結する。 -/
def unionAll {Row : Type} (r1 r2 : Rel Row) : Rel Row :=
  r1 ++ r2

/-- `Project(f, r)`（`SELECT f(row)`）: 各行を射影する（重複は維持）。 -/
def project {Row Out : Type} (f : Row → Out) (r : Rel Row) : Rel Out :=
  r.map f

/-- `InnerJoin(onPred, l, r)`:
    `LeftOuterJoin` と出力スキーマ `L × Option R` を揃えるため、マッチした右行を `some y` で包みます。 -/
def innerJoin {L R : Type} (onPred : L → R → SqlBool) (l : Rel L) (r : Rel R) :
    Rel (L × Option R) :=
  l.flatMap fun x =>
    (r.filter (fun y => (onPred x y).inWhere)).map (fun y => (x, some y))

/-- `LeftOuterJoin(onPred, l, r)`:
    左の各行 `x` について、マッチする右行があれば `(x, some y)` をすべて出力し、
    1 行もマッチしなければ NULL 補完行 `(x, none)` を 1 行出力する。 -/
def leftOuterJoin {L R : Type} (onPred : L → R → SqlBool) (l : Rel L) (r : Rel R) :
    Rel (L × Option R) :=
  l.flatMap fun x =>
    let matches_ := r.filter (fun y => (onPred x y).inWhere)
    if matches_.isEmpty then
      [(x, none)]
    else
      matches_.map (fun y => (x, some y))

/-- `AntiJoin(onPred, l, r)`:
    左の各行 `x` のうち、右に `onPred x y = tTrue` となる行 `y` が **1 つも存在しない** ものだけを残す。 -/
def antiJoin {L R : Type} (onPred : L → R → SqlBool) (l : Rel L) (r : Rel R) :
    Rel L :=
  l.filter fun x => !(r.any (fun y => (onPred x y).inWhere))

/-!
## 2-1. `or_to_union`（`plan/cascades.cpp`）の Bag 多重度保存の完全証明

`Selection(p0 OR p1, r)` と
`UnionAll(Selection(IS_TRUE(p0), r), Selection(IS_TRUE(p1) AND IS_NOT_TRUE(IS_TRUE(p0)), r))`
が、**任意の行型 `Row`・任意の行数・任意の重複行・任意の三値述語 `p0, p1`** に対して
Bag 等価（`BagEq`）になることを帰納法で証明します！
-/

/-- 補助補題: 各行がちょうど片方のフィルタだけを通るなら、`filter q (r) ++ filter q' (r)` は `filter p (r)` の順列になる。 -/
private theorem filter_partition_perm {Row : Type}
    (p q1 q2 : Row → Bool)
    (hDisjoint : ∀ x, ¬ (q1 x = true ∧ q2 x = true))
    (hCover : ∀ x, p x = (q1 x || q2 x)) :
    ∀ r : List Row, BagEq (r.filter p) (r.filter q1 ++ r.filter q2)
  | [] => List.Perm.nil
  | x :: xs => by
    have ih := filter_partition_perm p q1 q2 hDisjoint hCover xs
    have hCov := hCover x
    have hDis := hDisjoint x
    cases h1 : q1 x <;> cases h2 : q2 x
    · -- q1 = false, q2 = false => どのフィルタも通らない
      simp [h1, h2] at hCov
      simpa [List.filter, hCov, h1, h2] using ih
    · -- q1 = false, q2 = true => 右側の枝（第 1 枝）だけを通る
      simp [h1, h2] at hCov
      simp only [List.filter, hCov, h1, h2]
      -- `x :: filter p xs ~ filter q1 xs ++ (x :: filter q2 xs)` を示す
      exact (ih.cons x).trans List.perm_middle.symm
    · -- q1 = true, q2 = false => 左側の枝（第 0 枝）だけを通る
      simp [h1, h2] at hCov
      simp only [List.filter, hCov, h1, h2, List.cons_append]
      exact ih.cons x
    · -- q1 = true, q2 = true => 排他性 `hDisjoint` によりあり得ない！
       exfalso
       exact hDis ⟨h1, h2⟩

/-- **定理 (`or_to_union` 正当性)**:
    `tinylamb` の排他ガード付き `UNION ALL` 分解は、三値論理の NULL を含めて
    どんなテーブルに対しても Bag の行多重度を 100% 保存する。 -/
theorem or_to_union_preserves_bag {Row : Type}
    (p0 p1 : Row → SqlBool) (r : Rel Row) :
    BagEq
      (select (fun row => (p0 row).or (p1 row)) r)
      (unionAll
        (select (fun row => orBranch0 (p0 row)) r)
        (select (fun row => orBranch1 (p0 row) (p1 row)) r)) := by
  apply filter_partition_perm
  · intro row
    exact or_branches_disjoint (p0 row) (p1 row)
  · intro row
    dsimp only
    cases p0 row <;> cases p1 row <;> rfl

/-!
## 2-2. `outer_to_inner_join_on_null_rejecting_filter` の完全証明

直上の `WHERE q` が右テーブルの NULL 補完行 `(x, none)` を必ず落とす（Null-Rejecting: `∀ x, (q x none).inWhere = false`）とき、
`Selection(q, LeftOuterJoin(L, R, onPred))` は `Selection(q, InnerJoin(L, R, onPred))` と**並び順まで含めて完全一致**します！
-/

/-- 述語 `q` が右側リレーションに対して Null-Rejecting であることの意味論的定義:
    右側が `none`（NULL パディング）なら、左側の値 `x` が何であれ `WHERE q` を通過しない。 -/
def NullRejectingOnRight {L R : Type} (q : L → Option R → SqlBool) : Prop :=
  ∀ x : L, (q x none).inWhere = false

/-- **定理 (`outer_to_inner_join_on_null_rejecting_filter` 正当性)**:
    `q` が `NullRejectingOnRight` を満たすなら、LEFT OUTER JOIN を INNER JOIN に降格しても出力行リストは完全一致する。 -/
theorem outer_to_inner_join_sound {L R : Type}
    (onPred : L → R → SqlBool)
    (q : L → Option R → SqlBool)
    (hReject : NullRejectingOnRight q)
    (l : Rel L) (r : Rel R) :
    select (fun lr => q lr.1 lr.2) (leftOuterJoin onPred l r) =
    select (fun lr => q lr.1 lr.2) (innerJoin onPred l r) := by
  simp only [select, leftOuterJoin, innerJoin, List.filter_flatMap]
  congr 1
  funext x
  cases hMatch : r.filter (fun y => (onPred x y).inWhere) with
  | nil =>
    -- マッチ 0 件のとき: LeftOuterJoin は `[(x, none)]` を出すが、`hReject x` によりフィルタで消える！
    simp [List.isEmpty, hReject x]
  | cons y ys =>
    -- マッチ 1 件以上のとき: LeftOuterJoin も InnerJoin も同じ `map` を出す
    simp [List.isEmpty]

/-- 反例の検証: `COALESCE(r.val, 0) = 0` のように NULL を値に変えてしまう述語は
    `NullRejectingOnRight` を満たさないため、INNER JOIN に降格すると結果が変わってしまう！ -/
theorem coalesce_filter_breaks_outer_to_inner :
    let l : Rel Int := [1]
    let r : Rel Int := []
    let onPred := fun (x y : Int) => sqlEq (some x) (some y)
    -- `COALESCE(r, 0) = 0`: 右が `none` でも `0 = 0` で `tTrue` になってしまう述語
    let q := fun (_ : Int) (ry : Option Int) =>
      sqlEq (some (ry.getD 0)) (some 0)
    select (fun lr => q lr.1 lr.2) (leftOuterJoin onPred l r) ≠
    select (fun lr => q lr.1 lr.2) (innerJoin onPred l r) := by
  decide

end TinylambOpt

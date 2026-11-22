import Std.Tactic
import TinylambOpt.BagRel

/-
# LEFT JOIN の `ON p OR q` を排他的な 2 マッチ枝 + 1 未マッチ枝に分解する

C++ 対応 (`plan/cascades.cpp` の `or_to_union` / LEFT JOIN の NULL 補完):
  * 枝0: `p` が TRUE の行対（`orBranch0 p = IS_TRUE(p)`）。
  * 枝1: `q` が TRUE かつ `p` が TRUE でない行対
    (`orBranch1 p q = IS_TRUE(q) AND IS_NOT_TRUE(IS_TRUE(p))`)。
    このガードは `plan/cascades.cpp` の `or_to_union` が使うものと同じで、
    `p = UNKNOWN, q = TRUE` の行を落とさない（素朴な `q AND NOT p` は落とす）。
  * 未マッチ枝: 各左行について「全右行を調べ」`p OR q` が 1 件も TRUE でない場合だけ
    `(x, none)` を 1 行出す（`LeftOuterJoin` の NULL 補完）。
十分条件: 述語は決定的な関数であること（C++ の式評価も同じ行・同じスナップショットでは決定的）。
三値の `p, q` 自体は任意で、NULL を含めてもよい。 -/

namespace TinylambOpt.OuterOrBranches

/-- 参照評価: `ON p OR q` の LEFT OUTER JOIN。 -/
def refLOJ {L R : Type} (p q : L → R → SqlBool) (l : Rel L) (r : Rel R) : Rel (L × Option R) :=
  leftOuterJoin (fun x y => (p x y).or (q x y)) l r

/-- 最適化評価: 2 つのマッチ枝（排他的）と未マッチ枝に分けて連結したもの。 -/
def optLOJ {L R : Type} (p q : L → R → SqlBool) (l : Rel L) (r : Rel R) : Rel (L × Option R) :=
  l.flatMap fun x =>
    let b0 := r.filter (fun y => (orBranch0 (p x y)).inWhere)
    let b1 := r.filter (fun y => (orBranch1 (p x y) (q x y)).inWhere)
    let matched := b0 ++ b1
    if matched.isEmpty then
      [(x, none)]
    else
      matched.map (fun y => (x, some y))

/-- 補助: `Perm` なら `isEmpty` も `map` も同じなので、NULL 補完の分岐も `Perm`。 -/
private theorem lojRowOfPerm {L R : Type} (x : L) {b b' : List R} (h : b.Perm b') :
    (if b.isEmpty then [(x, none)] else b.map (fun y => (x, some y))).Perm
      (if b'.isEmpty then [(x, none)] else b'.map (fun y => (x, some y))) := by
  cases b with
  | nil =>
      cases b' with
      | nil => rfl
      | cons z zs =>
          have hlen := h.length_eq
          simp at hlen
  | cons y ys =>
      cases b' with
      | nil =>
          have hlen := h.length_eq
          simp at hlen
      | cons z zs =>
          change (List.map (fun w => (x, some w)) (y :: ys)).Perm
                 (List.map (fun w => (x, some w)) (z :: zs))
          exact h.map (fun w => (x, some w))

/-- 各行ごとの Bag 等価性。`or_to_union_preserves_bag` を右行リストにそのまま使う。 -/
private theorem lojRowSound {L R : Type} (p q : L → R → SqlBool) (x : L) (r : Rel R) :
    ((if (r.filter (fun y => ((p x y).or (q x y)).inWhere)).isEmpty then [(x, none)]
      else (r.filter (fun y => ((p x y).or (q x y)).inWhere)).map (fun y => (x, some y))).Perm
     (if ((r.filter (fun y => (orBranch0 (p x y)).inWhere)) ++
          (r.filter (fun y => (orBranch1 (p x y) (q x y)).inWhere))).isEmpty then [(x, none)]
      else ((r.filter (fun y => (orBranch0 (p x y)).inWhere)) ++
          (r.filter (fun y => (orBranch1 (p x y) (q x y)).inWhere))).map (fun y => (x, some y)))) := by
  have hBag := or_to_union_preserves_bag (fun y : R => p x y) (fun y : R => q x y) r
  unfold select unionAll at hBag
  simpa [orBranch0] using lojRowOfPerm x hBag

/-- 補助: 各行が `Perm` なら `flatMap` 全体も `Perm`。 -/
private theorem flatMap_perm_of_perm {L Out : Type} (l : List L) (f g : L → List Out)
    (h : ∀ x, (f x).Perm (g x)) : (l.flatMap f).Perm (l.flatMap g) := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.flatMap_cons]
      exact (h x).append ih

/-- **定理 (`outer_or_branches_sound`)**:
    `ON p OR q` の LEFT JOIN は、枝0 (`p` TRUE)・枝1 (`q` TRUE かつ `p` 非 TRUE)・
    未マッチ枝（`p OR q` が 0 件の左行を NULL 補完）に分解しても
    Bag 多重度（NULL 補完行を含む）が完全に保存される。 -/
theorem outer_or_branches_sound {L R : Type}
    (p q : L → R → SqlBool) (l : Rel L) (r : Rel R) :
    BagEq (refLOJ p q l r) (optLOJ p q l r) := by
  unfold refLOJ optLOJ leftOuterJoin
  apply flatMap_perm_of_perm
  intro x
  exact lojRowSound p q x r

/-- 境界例: `p = UNKNOWN, q = TRUE` の行対は枝1 を通過し、元の `p OR q` も通過する。
    `p = TRUE, q = TRUE` は枝0 のみ。両枝の排他性は既存の `or_branches_disjoint` が担う。 -/
theorem boundary_branch_examples :
    ((SqlBool.tNull.or SqlBool.tTrue).inWhere = true) ∧
    (orBranch0 SqlBool.tNull).inWhere = false ∧
    (orBranch1 SqlBool.tNull SqlBool.tTrue).inWhere = true ∧
    (orBranch1 SqlBool.tTrue SqlBool.tTrue).inWhere = false ∧
    ((SqlBool.tTrue.or SqlBool.tTrue).inWhere = true) ∧
    (orBranch0 SqlBool.tTrue).inWhere = true := by
  decide

end TinylambOpt.OuterOrBranches

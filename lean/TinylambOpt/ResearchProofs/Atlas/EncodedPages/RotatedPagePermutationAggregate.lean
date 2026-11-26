import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `RotatedPage.PermutationAggregate` — `∀ c, ((rows layout c).map Prod.snd).Perm ((rows
  LinearOrdinal.layout c).map Prod.snd)`
-/

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.RotatedPagePermutationAggregate

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RotatedPage

set_option maxHeartbeats 0
set_option maxRecDepth 10000

-- =====================================================================
-- Generic list helpers
-- =====================================================================

private theorem perm_cons_snoc {α : Type} (a : α) (l : List α) :
    (a :: l).Perm (l ++ [a]) := by
  induction l with
  | nil => simp
  | cons b bs ih =>
      exact (List.Perm.swap b a bs).trans (ih.cons b)

private theorem perm_append_comm {α : Type} (l₁ l₂ : List α) :
    (l₁ ++ l₂).Perm (l₂ ++ l₁) := by
  induction l₁ with
  | nil => simp
  | cons a as ih =>
      have h1 : (a :: (as ++ l₂)).Perm ((as ++ l₂) ++ [a]) := perm_cons_snoc a (as ++ l₂)
      have h2 : ((as ++ l₂) ++ [a]).Perm ((l₂ ++ as) ++ [a]) :=
        ih.append (List.Perm.refl [a])
      have h3' : (l₂ ++ (as ++ [a])).Perm (l₂ ++ (a :: as)) :=
        List.Perm.append (List.Perm.refl l₂) (perm_cons_snoc a as).symm
      have h3 : ((l₂ ++ as) ++ [a]).Perm (l₂ ++ (a :: as)) := by
        simpa [List.append_assoc] using h3'
      exact h1.trans (h2.trans h3)

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem filterMap_some_pair_map_snd {α : Type} (l : List α) (f : α → Int) :
    (l.filterMap (fun i => some (i, f i))).map Prod.snd = l.map f := by
  rw [filterMap_some_eq_map]
  simp

private def valuesFrom (c : Config) (start cnt : Nat) : List Int :=
  (List.range cnt).map (fun k => c.base + c.step * ((start + k : Nat) : Int))

private theorem valuesFrom_split (c : Config) (s t : Nat) :
    valuesFrom c 0 (s + t) = valuesFrom c 0 s ++ valuesFrom c s t := by
  unfold valuesFrom
  rw [List.range_add]
  simp only [List.map_append, List.map_map]
  congr 1
  apply List.map_congr_left
  intro k hk
  simp [Int.natCast_add, Int.mul_add]

-- =====================================================================
-- RotatedPage.PermutationAggregate
-- =====================================================================

theorem sound_PermutationAggregate :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.RotatedPage.PermutationAggregate.Goal := by
  intro c
  by_cases hzero : c.n = 0
  · simp [rows, hzero]
  · have hnpos : 0 < c.n := by omega
    let s := c.split % c.n
    have hslt : s < c.n := by dsimp [s]; exact Nat.mod_lt _ hnpos
    have hsle : s ≤ c.n := Nat.le_of_lt hslt
    have hsum : (c.n - s) + s = c.n := Nat.sub_add_cancel hsle
    have hsum' : s + (c.n - s) = c.n := Nat.add_sub_of_le hsle
    have hrowsRot : (rows layout c).map Prod.snd =
        valuesFrom c s (c.n - s) ++ valuesFrom c 0 s := by
      unfold rows
      simp only [layout, Option.map_some]
      rw [filterMap_some_pair_map_snd]
      change (List.range c.n).map (fun i => c.base + c.step * (((i + s) % c.n : Nat) : Int)) =
        valuesFrom c s (c.n - s) ++ valuesFrom c 0 s
      unfold valuesFrom
      have hrange : List.range c.n =
          List.range (c.n - s) ++ (List.range s).map (fun j => c.n - s + j) := by
        calc
          List.range c.n = List.range ((c.n - s) + s) := congrArg List.range hsum.symm
          _ = List.range (c.n - s) ++ (List.range s).map (fun j => c.n - s + j) := List.range_add
      rw [hrange]
      simp only [List.map_append, List.map_map]
      congr 1
      · apply List.map_congr_left
        intro i hi
        have hi' : i < c.n - s := List.mem_range.mp hi
        have hlt : i + s < c.n := by omega
        have hm : (i + s) % c.n = i + s := Nat.mod_eq_of_lt hlt
        rw [hm]
        congr 2
        omega
      · apply List.map_congr_left
        intro j hj
        have hj' : j < s := List.mem_range.mp hj
        have heq : c.n - s + j + s = c.n + j := by omega
        have hmod : (c.n - s + j + s) % c.n = j := by
          rw [heq, Nat.add_mod_left, Nat.mod_eq_of_lt (by omega : j < c.n)]
        change c.base + c.step * ((((c.n - s + j) + s) % c.n : Nat) : Int) =
          c.base + c.step * (((0 + j : Nat) : Int))
        rw [hmod]
        simp
    have hrowsLin' :
        (rows TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal.layout c).map Prod.snd =
          valuesFrom c 0 c.n := by
      unfold rows
      simp only [TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal.layout, Option.map_some]
      rw [filterMap_some_pair_map_snd]
      unfold valuesFrom
      simp [Nat.zero_add]
    have hsplit : valuesFrom c 0 c.n = valuesFrom c 0 s ++ valuesFrom c s (c.n - s) := by
      unfold valuesFrom
      rw [congrArg List.range hsum'.symm, List.range_add]
      simp only [List.map_append, List.map_map]
      congr 1
      apply List.map_congr_left
      intro k hk
      simp [Int.natCast_add, Int.mul_add]
    rw [hrowsRot, hrowsLin', hsplit]
    exact perm_append_comm _ _

end TinylambOpt.ResearchProofs.Atlas.EncodedPages.RotatedPagePermutationAggregate

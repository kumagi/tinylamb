import TinylambOpt.ResearchGoals.November26.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false


namespace TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q14
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26.Tpcds

private def comb (a b : Option Int) : Option Int :=
  match a, b with
  | some x, some y => some (x + y)
  | some x, none => some x
  | none, some y => some y
  | none, none => none

private theorem view (Z : List (Option Int)) :
    nsum Z = if (Z.filterMap id).isEmpty = true then none else some (Z.filterMap id).sum := rfl

private theorem nsum_append (X Y : List (Option Int)) :
    nsum (X ++ Y) = comb (nsum X) (nsum Y) := by
  rw [view, view, view, List.filterMap_append]
  by_cases hX : (X.filterMap id).isEmpty = true
  · have hXnil : X.filterMap id = [] := List.isEmpty_iff.mp hX
    by_cases hY : (Y.filterMap id).isEmpty = true
    · have hYnil : Y.filterMap id = [] := List.isEmpty_iff.mp hY
      have hXY : (X.filterMap id ++ Y.filterMap id).isEmpty = true := by
        rw [List.isEmpty_iff, List.append_eq_nil_iff]
        exact ⟨hXnil, hYnil⟩
      simp [hX, hY, hXY, hXnil, hYnil, comb]
    · have hXY : ¬ ((X.filterMap id ++ Y.filterMap id).isEmpty = true) := by
        intro h
        have hnil : X.filterMap id ++ Y.filterMap id = [] := List.isEmpty_iff.mp h
        exact hY (List.isEmpty_iff.mpr (List.append_eq_nil_iff.mp hnil).2)
      simp [hX, hY, hXY, hXnil, comb, List.sum_append]
  · by_cases hY : (Y.filterMap id).isEmpty = true
    · have hYnil : Y.filterMap id = [] := List.isEmpty_iff.mp hY
      have hXY : ¬ ((X.filterMap id ++ Y.filterMap id).isEmpty = true) := by
        intro h
        have hnil : X.filterMap id ++ Y.filterMap id = [] := List.isEmpty_iff.mp h
        exact hX (List.isEmpty_iff.mpr (List.append_eq_nil_iff.mp hnil).1)
      simp [hX, hY, hXY, hYnil, comb, List.sum_append]
    · have hXY : ¬ ((X.filterMap id ++ Y.filterMap id).isEmpty = true) := by
        intro h
        have hnil : X.filterMap id ++ Y.filterMap id = [] := List.isEmpty_iff.mp h
        exact hX (List.isEmpty_iff.mpr (List.append_eq_nil_iff.mp hnil).1)
      have hsum : (X.filterMap id ++ Y.filterMap id).sum =
          (X.filterMap id).sum + (Y.filterMap id).sum := List.sum_append
      simp [hX, hY, hXY, comb, hsum]

private theorem nsum_singleton (z : Option Int) : nsum [z] = z := by
  rw [view]
  cases z <;> simp

private theorem nsum_map_nsum :
    ∀ (xs : List (List (Option Int))), nsum (xs.map nsum) = nsum xs.flatten := by
  intro xs
  induction xs with
  | nil => rfl
  | cons g gs ih =>
      show nsum (nsum g :: gs.map nsum) = nsum (g ++ gs.flatten)
      rw [← List.singleton_append, nsum_append, nsum_singleton, ih, ← nsum_append]

theorem sound : TinylambOpt.ResearchGoals.November26.Tpcds.Q14.Goal := by
  intro cells threshold kept
  exact nsum_map_nsum kept
end TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q14

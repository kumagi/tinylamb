import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-!
# Goal_PartitionedAggregateStates_MonthCaseZeroAndNullState

A monthly nullable conditional sum can append a single zero witness when any
other month occurred: the zero does not change the sum but makes the nullable
state defined exactly when the left-hand CASE arm would be defined.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_MonthCaseZeroAndNullState

open TinylambOpt.ResearchGoals.QueryRewrites

private theorem filterMap_map_id {α β : Type} (xs : List α) (f : α → Option β) :
    (xs.map f).filterMap id = xs.filterMap f := by
  induction xs with
  | nil => simp
  | cons x xs ih => cases f x <;> simp [ih, List.filterMap_cons]

private theorem sum_filterMap_eq_of_nsum_eq {xs ys : List (Option Int)} (h : nsum xs = nsum ys) :
    (xs.filterMap id).sum = (ys.filterMap id).sum := by
  unfold nsum at h
  by_cases hx : (xs.filterMap id).isEmpty
  · rw [List.isEmpty_iff] at hx
    by_cases hy : (ys.filterMap id).isEmpty
    · rw [List.isEmpty_iff] at hy
      simp [hx, hy]
    · simp [hx, hy] at h
  · by_cases hy : (ys.filterMap id).isEmpty
    · rw [List.isEmpty_iff] at hy
      simp [hx, hy] at h
    · simp [hx, hy] at h ⊢
      exact h

private theorem nsum_cons_congr (x : Option Int) {xs ys : List (Option Int)} (h : nsum xs = nsum ys) :
    nsum (x :: xs) = nsum (x :: ys) := by
  cases x with
  | none => simpa [nsum] using h
  | some v =>
      have hs := sum_filterMap_eq_of_nsum_eq h
      simp [nsum, hs]

private theorem nsum_eq_sum_of_mem_some (xs : List (Option Int)) (hmem : ∃ x ∈ xs, x.isSome) :
    nsum xs = some ((xs.filterMap id).sum) := by
  unfold nsum
  have hne : (xs.filterMap id) ≠ [] := by
    intro hnil
    rw [List.filterMap_eq_nil_iff] at hnil
    rcases hmem with ⟨x, hx, hs⟩
    cases x with
    | none => simp at hs
    | some a => have hh := hnil (some a) hx; simp at hh
  simp [hne]

theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.MonthCaseZeroAndNullState.Goal := by
  intro rs month
  dsimp only [TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.MonthCaseZeroAndNullState.Goal]
  induction rs with
  | nil => simp [nsum, forKey]
  | cons r rs ih =>
      rcases r with ⟨m, v⟩
      have ih' := ih
      simp [forKey] at ih'
      by_cases hm : m == month
      · have hmeq : m = month := by simpa using hm
        by_cases ho : rs.any (fun r => r.1 != month)
        · simpa [hmeq, ho, forKey] using nsum_cons_congr v ih'
        · simpa [hmeq, ho, forKey] using nsum_cons_congr v ih'
      · have hmne : m ≠ month := by simpa using hm
        have hsum := sum_filterMap_eq_of_nsum_eq ih'
        rw [filterMap_map_id] at hsum
        have hleft : nsum (List.map (fun x : Nat × Option Int =>
              if x.1 = month then x.2 else some 0) ((m, v) :: rs)) =
            some ((List.filterMap id (List.map (fun x : Nat × Option Int =>
              if x.1 = month then x.2 else some 0) ((m, v) :: rs))).sum) := by
          apply nsum_eq_sum_of_mem_some
          refine ⟨some 0, ?_, by simp⟩
          simp [hmne]
        have hright : nsum (forKey ((m, v) :: rs) month ++
              if ((m, v) :: rs).any (fun r => r.1 != month) then [some 0] else []) =
            some ((List.filterMap id (forKey ((m, v) :: rs) month ++
              if ((m, v) :: rs).any (fun r => r.1 != month) then [some 0] else [])).sum) := by
          apply nsum_eq_sum_of_mem_some
          refine ⟨some 0, ?_, by simp⟩
          simp [forKey, hmne]
        simp [hmne, forKey] at hleft hright hsum ⊢
        by_cases ho : rs.any (fun r => r.1 != month)
        · simp [ho] at hsum ⊢
          rw [hleft, hright]
          simp [hsum]
        · simp [ho] at hsum ⊢
          rw [hleft, hright]
          simp [hsum]

end TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_MonthCaseZeroAndNullState

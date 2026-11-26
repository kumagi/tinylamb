import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
# Goal_KeyRoutedJoinStates_SegmentOrderClosureBeforeLineProbe

Filtering line keys by the closure of eligible customers through eligible
orders preserves exact join multiplicities: a line order whose weight is
nonzero must have at least one eligible customer occurrence, so it belongs to
the support set.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_SegmentOrderClosureBeforeLineProbe

open TinylambOpt.ResearchGoals.QueryRewrites

private theorem sum_counts_eq_zero {α : Type} [BEq α] [LawfulBEq α] (xs ys : List α) :
    (xs.map (fun x => ys.count x)).sum = 0 ↔ ∀ x ∈ xs, ¬ x ∈ ys := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      constructor
      · intro h y hy
        cases hy with
        | head =>
            have hx0 : ys.count x = 0 := by omega
            exact List.count_eq_zero.mp hx0
        | tail y hy => exact (ih.mp (by omega)) y hy
      · intro h
        have hx0 : ys.count x = 0 := List.count_eq_zero.mpr (h x (by simp))
        have hrest : ∀ y ∈ xs, ¬ y ∈ ys := by
          intro y hy
          exact h y (by simp [hy])
        simp [hx0, ih.mpr hrest]

theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.SegmentOrderClosureBeforeLineProbe.Goal := by
  intro customers orders lines
  dsimp only [TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.SegmentOrderClosureBeforeLineProbe.Goal]
  let p : Nat → Bool := fun o => (orders.filter (fun r => customers.contains r.2)).map Prod.fst |>.contains o
  let weight : Nat → Nat := fun o => ((forKey orders o).map customers.count).sum
  have hweight_zero : ∀ o, p o = false → weight o = 0 := by
    intro o ho
    exact sum_counts_eq_zero (forKey orders o) customers |>.mpr (by
      intro c hc hcmem
      apply Bool.eq_false_iff.mp ho
      have hmem : o ∈ (orders.filter (fun r => customers.contains r.2)).map Prod.fst := by
        apply List.mem_map.mpr
        refine ⟨(o, c), ?_, rfl⟩
        simp only [List.mem_filter]
        exact ⟨by simpa [forKey] using hc, by simpa using hcmem⟩
      simpa [p] using hmem)
  change (List.map weight (List.filter p lines)).sum = (List.map weight lines).sum
  induction lines with
  | nil => simp
  | cons l ls ih =>
      by_cases hp : p l = true
      · simp [hp, ih]
      · have hp' : p l = false := by simpa using hp
        have hw : weight l = 0 := hweight_zero l hp'
        simp [hp', hw, ih]

end TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_SegmentOrderClosureBeforeLineProbe

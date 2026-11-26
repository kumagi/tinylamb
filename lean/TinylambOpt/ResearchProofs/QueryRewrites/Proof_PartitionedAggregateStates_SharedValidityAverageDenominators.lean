import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

namespace TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_SharedValidityAverageDenominators

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-!
Sharing one average denominator across four nullable measures is safe when every
selected row has the same validity mask for all measures. The exact numerators are
unchanged and the valid-row counts coincide, so AVG states may be reused.
-/

set_option maxHeartbeats 0

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-- Equal validity masks imply equal numbers of non-NULL values in filterMap. -/
theorem filterMap_length_of_isSome_eq {α : Type} (f g : α → Option Int) (xs : List α)
    (h : ∀ x ∈ xs, (f x).isSome = (g x).isSome) :
    (xs.filterMap f).length = (xs.filterMap g).length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have hx := h x (by simp)
      have htail : ∀ y ∈ xs, (f y).isSome = (g y).isSome := by
        intro y hy
        exact h y (by simp [hy])
      have ih' := ih htail
      cases hf : f x <;> cases hg : g x
      · simpa [hf, hg] using ih'
      · simp [hf, hg] at hx
      · simp [hf, hg] at hx
      · simpa [hf, hg] using ih'

theorem sound : SharedValidityAverageDenominators.Goal := by
  intro rs hmask j
  have hlen : (rs.filterMap (fun r => r j)).length = (rs.filterMap (fun r => r 0)).length :=
    filterMap_length_of_isSome_eq (fun r => r j) (fun r => r 0) rs (by
      intro r hr
      exact hmask r hr j)
  simp [hlen]
end TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_SharedValidityAverageDenominators

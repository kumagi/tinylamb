import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

/-!
# SharedDimensionFilterBeforeTimeSplit

Restricting both time-window counts to the same eligible household/page
rows preserves both counts. This models sharing a dimension filter across
split time bands before applying per-band hour predicates.
-/

namespace Nov26.Proof_SparseSetAndWindowEvaluation_SharedDimensionFilterBeforeTimeSplit

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.SharedDimensionFilterBeforeTimeSplit

private theorem countWhere_shared (facts : List (Nat × Bool)) (hour : Nat → Nat) (lo : Nat) :
    countWhere ((facts.filter Prod.snd).map Prod.fst) (fun k => decide (lo ≤ hour k ∧ hour k ≤ lo + 1)) =
      countWhere facts (fun x => x.2 && decide (lo ≤ hour x.1 ∧ hour x.1 ≤ lo + 1)) := by
  unfold countWhere
  have h :
      ((facts.filter Prod.snd).map Prod.fst).filter (fun k => decide (lo ≤ hour k ∧ hour k ≤ lo + 1))
      =
      (facts.filter (fun x => x.2 && decide (lo ≤ hour x.1 ∧ hour x.1 ≤ lo + 1))).map Prod.fst := by
    rw [List.filter_map]
    simp only [Function.comp, List.filter_filter]
    congr 1
    apply List.filter_congr
    intro x hx
    cases hs : x.2 <;> simp [hs]
  rw [h, List.length_map]

theorem sound : Goal := by
  intro facts hour
  simp only [countWhere_shared]

#print axioms sound

end Nov26.Proof_SparseSetAndWindowEvaluation_SharedDimensionFilterBeforeTimeSplit

import TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2

/-!
# Deviation-band filter before the neighbor join (TPC-DS Q57)

The `keep` predicate inspects only the current month row, so filtering current months
before expanding each row with its previous/next neighbors yields the same ordered
sequence of triples as expanding first and filtering afterward. The executor may
prune whole month partitions before the self-join lookups.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q57_DeviationBandFilterBeforeNeighborJoinGoal

open TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : Q57_DeviationBandFilterBeforeNeighborJoinGoal := by
  intro months keep neighbors
  induction months with
  | nil => rfl
  | cons m months ih =>
    have hhead : ((neighbors m.1).map (fun p => (m, p))).filter (fun t => keep t.1) =
        if keep m then (neighbors m.1).map (fun p => (m, p)) else [] := by
      rw [List.filter_map]
      have hpred : ((fun t : (Nat × Int) × (Int × Int) => keep t.1) ∘
          (fun p : Int × Int => (m, p))) = (fun _ => keep m) := rfl
      rw [hpred]
      by_cases hm : keep m = true
      · have htrue : (neighbors m.1).filter (fun _ : Int × Int => true) = neighbors m.1 :=
          List.filter_eq_self.mpr (fun _ _ => rfl)
        simp [hm, htrue]
      · simp [hm]
    by_cases hm : keep m = true <;>
      simp [List.flatMap_cons, List.filter_append, hm, hhead, ih]

end TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q57_DeviationBandFilterBeforeNeighborJoinGoal

import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_ConstantSoldDateShiftedThresholds

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-! Inside a constant sold-date run, each ship-date bucket boundary is just the
original lag boundary translated by the sold date. The rewrite therefore shifts
the stored threshold list once per run and classifies ship dates directly. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-- Translating every boundary by `s` preserves the count of boundaries below
`x - s`: `b < x - s` iff `s + b < x`. -/
theorem countP_shifted (bs : List Int) (s x : Int) :
    bs.countP (fun b => b < x - s) = (bs.map (s + ·)).countP (fun b => b < x) := by
  induction bs with
  | nil => rfl
  | cons b bs ih =>
      simp only [List.countP_cons, List.map_cons]
      rw [ih]
      by_cases h : b < x - s
      · have h' : s + b < x := by omega
        simp [h, h']
      · have h' : ¬ s + b < x := by omega
        simp [h, h']

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.ConstantSoldDateShiftedThresholds.Goal := by
  intro sold shipped
  exact countP_shifted [30, 60, 90, 120] sold shipped
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_ConstantSoldDateShiftedThresholds

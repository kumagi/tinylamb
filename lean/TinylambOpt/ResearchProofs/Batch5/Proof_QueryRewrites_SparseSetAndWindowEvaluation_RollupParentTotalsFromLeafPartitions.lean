import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation
import TinylambOpt.ResearchProofs.QueryRewrites.NullableSum

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.SparseSetAndWindowEvaluation.Goal_RollupParentTotalsFromLeafPartitions

open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation


/-!
# RollupParentTotalsFromLeafPartitions

Merging leaf subtotals by class preserves the nullable overall subtotal, with a
real NULL class treated as its own grouping key. The executor can preaggregate
leaf partitions before rolling them up, including when every value is NULL.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

open TinylambOpt.ResearchGoals.QueryRewrites (nsum)
open TinylambOpt.ResearchProofs.QueryRewrites.NullableSum (nsum_group_by_key)

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.RollupParentTotalsFromLeafPartitions.Goal := by
  intro leaves
  have h := nsum_group_by_key (fun _ : Option Nat => true) leaves
  have hk : (leaves.map Prod.fst).eraseDups.filter (fun _ => true) =
      (leaves.map Prod.fst).eraseDups :=
    List.filter_eq_self.mpr (by intro k hk; rfl)
  have hr : leaves.filter (fun _ : Option Nat × Option Int => true) = leaves :=
    List.filter_eq_self.mpr (by intro r hr; rfl)
  rw [hk, hr] at h
  exact h
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.SparseSetAndWindowEvaluation.Goal_RollupParentTotalsFromLeafPartitions

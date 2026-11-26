import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates
import TinylambOpt.ResearchProofs.CloudBI.Common

namespace TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_PriceBandBypassesAddressPredicate

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-! Partitioning by the price arm of a disjunction preserves the bag of selected
postal-group inputs under nullable prices: rows that pass the price arm come
first, followed by address-only rows that fail the price arm. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchProofs.CloudBI.Helpers

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.PriceBandBypassesAddressPredicate.Goal := by
  intro rs
  dsimp
  let high : Bool × Option Int → Bool := fun r => r.2.any (500 < ·)
  let addr : Bool × Option Int → Bool := fun r => !high r && r.1
  have hdisj : ∀ x ∈ rs, (high x && addr x) = false := by
    intro x _
    dsimp [addr]
    cases h : high x <;> simp [h]
  have hpred : ∀ x : Bool × Option Int, (x.1 || high x) = (high x || addr x) := by
    intro x
    dsimp [addr]
    cases h : high x <;> cases x.1 <;> simp [h]
  have hfilter : rs.filter (fun r => r.1 || high r) = rs.filter (fun r => high r || addr r) := by
    apply List.filter_congr
    intro x _
    exact hpred x
  change (rs.filter (fun r => r.1 || high r)).Perm ((rs.filter high) ++ rs.filter (fun r => !high r && r.1))
  rw [hfilter]
  exact (filter_disjoint_perm high addr rs hdisj).symm
end TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_PriceBandBypassesAddressPredicate

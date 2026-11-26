import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-!
# Goal_PartitionedAggregateStates_PriceBandBypassesAddressPredicate

Registers the proof of the PriceBandBypassesAddressPredicate goal in PartitionedAggregateStates: the modeled rewrite
preserves the exact result, so SQL execution may apply it.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_PriceBandBypassesAddressPredicate
open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.PriceBandBypassesAddressPredicate
theorem sound : Goal := by
  intro rs
  dsimp [Goal]
  let high := fun (r : Bool × Option Int) => r.2.any (500 < ·)
  change (rs.filter (fun r => r.1 || high r)).Perm ((rs.filter high) ++ rs.filter (fun r => !high r && r.1))
  induction rs with
  | nil => simp [high]
  | cons r rs ih =>
      by_cases hh : high r = true
      · -- r goes to high partition
        have : (r.1 || high r) = true := by simp [hh]
        simp [List.filter, hh, this, ih, high]
      · have hhf : high r = false := by cases h : high r <;> simp_all
        by_cases ha : r.1 = true
        · -- goes to right partition
          simp only [List.filter, hhf, ha, Bool.or_false, Bool.true_and, ite_true, Bool.false_eq_true, ite_false]
          exact (List.Perm.cons r ih).trans (List.Perm.symm (List.perm_middle))
        · have haf : r.1 = false := by cases h : r.1 <;> simp_all
          have hor : (r.1 || high r) = false := by simp [haf, hhf]
          have hnot : (!high r && r.1) = false := by simp [haf]
          simp [List.filter, hor, hhf, hnot, haf, ih, high]
end TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_PriceBandBypassesAddressPredicate

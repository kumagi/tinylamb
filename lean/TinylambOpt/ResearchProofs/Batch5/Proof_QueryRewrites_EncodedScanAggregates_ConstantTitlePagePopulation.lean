import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.EncodedScanAggregates.Goal_ConstantTitlePagePopulation

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
Constant-title population: when every survivor has the same certified title,
the title lookup can be replaced by a survivor count. This preserves the
modeled grouped population while avoiding repeated title probes.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-- When every surviving row has the same certified nonempty title, the
title lookup can be replaced by a count of the survivor mask. -/
theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.ConstantTitlePagePopulation.Goal := by
  intro titles label hne hall
  induction titles with
  | nil => simp [TinylambOpt.ResearchGoals.QueryRewrites.countWhere]
  | cons r rs ih =>
      have hr : r.1 = label := hall r (by simp)
      have hrs : ∀ q ∈ rs, q.1 = label := by
        intro q hq
        exact hall q (by simp [hq])
      have hih := ih hrs
      have hih' :
          List.filterMap (fun x : String × Bool => if x.2 = true ∧ ¬ x.1 = "" then some x.1 else none) rs =
            List.replicate (List.filter Prod.snd rs).length label := by
        simpa [Bool.and_eq_true, TinylambOpt.ResearchGoals.QueryRewrites.countWhere] using hih
      cases r with
      | mk s b =>
          have hs : s = label := hr
          subst s
          cases b <;> simp [hne, TinylambOpt.ResearchGoals.QueryRewrites.countWhere, hih', List.replicate_succ]
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.EncodedScanAggregates.Goal_ConstantTitlePagePopulation

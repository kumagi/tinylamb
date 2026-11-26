import TinylambOpt.ResearchGoals.QueryRewrites.Common
import TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.AggregateReplay.Goal_LateDistinctAfterLocalLimit

open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay


/-! Scratch proof: local top-k truncation before DISTINCT can crowd out a
    distinct value that appears in the global top-k of the deduplicated stream. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

/-- One run [0,0,1] with k=2: local best is [0,0] so the deduplicated local
    stream has only [0], while the deduplicated global stream has [0,1]. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay.LateDistinctAfterLocalLimit.Goal := by
  intro h
  have hbad := h 2 ([[0, 0, 1]] : List (List Nat))
  exact (by native_decide : ¬ best 2 (([[0, 0, 1]] : List (List Nat)).flatMap (best 2)).eraseDups =
    best 2 (([0, 0, 1] : List Nat).eraseDups)) hbad
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.AggregateReplay.Goal_LateDistinctAfterLocalLimit

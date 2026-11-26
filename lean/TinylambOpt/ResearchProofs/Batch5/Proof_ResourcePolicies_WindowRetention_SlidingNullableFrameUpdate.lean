import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WindowRetention.Goal_SlidingNullableFrameUpdate

open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention


/-!
An exact running sum plus a non-NULL count distinguishes an empty nullable frame
from a frame whose sum is zero. Sliding the frame therefore only subtracts the
expired payload when it was non-NULL, and emits NULL exactly when the updated
count is zero.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention.SlidingNullableFrameUpdate.Goal := by
  intro expired entered middle
  cases expired with
  | none =>
      cases entered with
      | none =>
          simp [TinylambOpt.ResearchGoals.QueryRewrites.nsum]
      | some f =>
          simp [TinylambOpt.ResearchGoals.QueryRewrites.nsum]
  | some e =>
      cases entered with
      | none =>
          simp [TinylambOpt.ResearchGoals.QueryRewrites.nsum]
          split <;> simp_all <;> omega
      | some f =>
          simp [TinylambOpt.ResearchGoals.QueryRewrites.nsum]
          omega
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WindowRetention.Goal_SlidingNullableFrameUpdate

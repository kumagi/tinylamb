import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WindowRetention.Goal_SkipNullsBeforeFrameBoundary

open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention


/-!
Compacting NULL payloads before applying a physical ROWS frame changes which
values lie inside the frame. The counterexample retains a non-NULL row from
before the morsel cut that lies outside the one-preceding-row frame, producing a
non-NULL SUM where the physical frame yields NULL.
-/

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention.SkipNullsBeforeFrameBoundary.Goal := by
  intro h
  have hEq := h [some (0 : Int), none] [none] 1 0 (by simp)
  simp [TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention.frame,
    TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention.tailRows,
    TinylambOpt.ResearchGoals.QueryRewrites.nsum] at hEq
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WindowRetention.Goal_SkipNullsBeforeFrameBoundary

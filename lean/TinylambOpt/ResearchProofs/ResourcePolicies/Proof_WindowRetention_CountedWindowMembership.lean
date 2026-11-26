import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.WindowRetention.Goal_CountedWindowMembership

open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention


/-!
A frequency decrement for the expired key together with an increment for the
entering key reconstructs distinct membership of the next sliding window when
the expired key is counted exactly once. This is the reference-count invariant
that a bounded DISTINCT-window state must maintain.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention.CountedWindowMembership.Goal := by
  intro x y k middle
  by_cases hxk : x = k <;> by_cases hyk : y = k
  all_goals
    have hky : (k = y) = (y = k) := propext ⟨fun h => h.symm, fun h => h.symm⟩
    simp [List.mem_eraseDups, hxk, hyk, beq_iff_eq, hky]
end TinylambOpt.ResearchProofs.ResourcePolicies.WindowRetention.Goal_CountedWindowMembership

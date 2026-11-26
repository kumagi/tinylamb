import TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.WaitQueue.Goal_CanceledHeadNeedsRebatch

open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue


/-! Canceling a selected writer head leaves the following reader in the queue;
the scheduler must rebatch that exposed head instead of reusing the old turn.
Concrete counterexample. -/

namespace Scratch.CanceledHeadNeedsRebatch

open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue.CanceledHeadNeedsRebatch.Goal := by
  unfold TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue.CanceledHeadNeedsRebatch.Goal
  intro h
  let t : Ticket := ⟨0, 0, true⟩
  let ts : List Ticket := [⟨0, 1, false⟩]
  have hbad := h t ts (by decide) (by decide)
  have hnot : turn (cancel t.owner t.generation (t :: ts)) ≠
      cancel t.owner t.generation (turn (t :: ts)) := by decide
  exact hnot hbad

end Scratch.CanceledHeadNeedsRebatch
end TinylambOpt.ResearchProofs.ResourcePolicies.WaitQueue.Goal_CanceledHeadNeedsRebatch

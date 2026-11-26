import TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WaitQueue.Goal_BypassElapsedTimeBound

open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue


/-! Counting scheduler turns does not bound elapsed time: a late reader with a
slow completion time raises the cohort's retirement time. Concrete counterexample. -/

namespace Scratch.BypassElapsedTimeBound

open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue.BypassElapsedTimeBound.Goal := by
  unfold TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue.BypassElapsedTimeBound.Goal
  intro h
  have hbad := h [1] 2 (by decide)
  have hnot : ¬ (([1] ++ [2]).max?).getD 0 ≤ ([1].max?).getD 0 := by decide
  exact hnot hbad

end Scratch.BypassElapsedTimeBound
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WaitQueue.Goal_BypassElapsedTimeBound

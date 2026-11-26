import TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WaitQueue.Goal_ReaderBypassLatency

open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue


/-! A reader joining an already waiting reader cohort does not add a scheduler
turn ahead of the writer; the writer still waits one turn. -/

namespace Scratch.ReaderBypassLatency

open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue.ReaderBypassLatency.Goal := by
  intro active writer newcomer tail ha hw hn hne1 hne2
  have ha' : active.writer = false := by cases h : active.writer <;> simp_all
  have hn' : newcomer.writer = false := by cases h : newcomer.writer <;> simp_all
  unfold waitTurns
  simp [ha', hw, hn', hne1, hne2]

end Scratch.ReaderBypassLatency
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WaitQueue.Goal_ReaderBypassLatency

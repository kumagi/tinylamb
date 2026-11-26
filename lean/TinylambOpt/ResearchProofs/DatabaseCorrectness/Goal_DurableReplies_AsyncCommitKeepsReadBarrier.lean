import TinylambOpt.ResearchGoals.DatabaseCorrectness.DurableReplies

/-! An asynchronous writer still cannot acknowledge while a read dependency
    points at an undurable WAL end. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.DurableReplies.Goal_AsyncCommitKeepsReadBarrier

open TinylambOpt.ResearchGoals.DatabaseCorrectness.DurableReplies

theorem sound : AsyncCommitKeepsReadBarrier.Goal := by
  intro t d x hx hdx
  have hall : t.observed.all (fun y => decide (y ≤ d)) = false := by
    rw [List.all_eq_false]
    refine ⟨x, hx, ?_⟩
    simp
    omega
  simp [sequentialWaits, hall]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.DurableReplies.Goal_AsyncCommitKeepsReadBarrier

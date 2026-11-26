import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_CommitBatches_LargestCompletionWakesPrefix
open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches
open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches.LargestCompletionWakesPrefix

theorem refute : ¬ Goal := by
  intro h
  let ws : List Write := [⟨0, 1⟩, ⟨2, 1⟩]
  have hne : ws ≠ [] := by simp [ws]
  have hmax : (ws.map (fun w => w.start + w.count)).max? = some 3 := by native_decide
  have := h ws 3 hne hmax
  revert this
  native_decide
end Nov26.Proof_CommitBatches_LargestCompletionWakesPrefix

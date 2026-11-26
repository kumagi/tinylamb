import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_WorkerPeaksAreSimultaneous

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission


set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Refutation of the claim that independently measured worker high-water marks
must be reachable in the same sample. Two workers whose peaks occur in different
rows have a combined peak below the sum of their individual peaks, so summing
per-worker maxima over-reserves scratch memory for an admission test. -/

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.WorkerPeaksAreSimultaneous.Goal := by
  intro h
  have hb := h [(1, 0), (0, 1)]
  simp at hb
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_WorkerPeaksAreSimultaneous

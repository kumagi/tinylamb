import TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_EvictionWriteback_EqualPayloadSkipsWalFence
open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback
theorem refute : ¬ EqualPayloadSkipsWalFence.Goal := by
  intro h
  let d : Image := ⟨1, 1, 5, 42⟩
  let s : Slot := ⟨⟨1, 2, 10, 42⟩, some d, false, 0⟩
  have h' := h s d (by simp [s]) (by simp [s, d]) (by simp [s, d])
  simp [d, s] at h'
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_EvictionWriteback_EqualPayloadSkipsWalFence

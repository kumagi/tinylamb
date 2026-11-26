import TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_EvictionWriteback_MaxRevisionFlushCoalescing
open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback
theorem refute : ¬ MaxRevisionFlushCoalescing.Goal := by
  intro h
  let b : Image := ⟨1, 2, 50, 1⟩
  let a : Image := ⟨1, 1, 10, 2⟩
  let s : Slot := ⟨b, none, true, 0⟩
  have h' := h s a b 30 rfl rfl (by decide) (by decide)
  simp [finish, s, a, b] at h'
end Nov26.Proof_EvictionWriteback_MaxRevisionFlushCoalescing

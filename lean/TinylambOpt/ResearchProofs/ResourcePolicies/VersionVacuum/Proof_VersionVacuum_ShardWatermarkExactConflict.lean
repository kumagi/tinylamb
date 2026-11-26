import TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_VersionVacuum_ShardWatermarkExactConflict
open TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum
theorem refute : ¬ ShardWatermarkExactConflict.Goal := by
  intro h
  let evicted : List (Nat × Nat) := [(0, 10)]
  have h' := h evicted 5 1 (by decide)
  rcases h' with ⟨t, ht, hlt⟩
  simp [evicted] at ht
end Nov26.Proof_VersionVacuum_ShardWatermarkExactConflict

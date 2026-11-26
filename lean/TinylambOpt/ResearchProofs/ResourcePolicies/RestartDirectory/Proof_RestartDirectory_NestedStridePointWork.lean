import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_RestartDirectory_NestedStridePointWork
open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
theorem sound : NestedStridePointWork.Goal := by
  intro coarse fine i hfine hcoarse hdvd
  unfold anchor
  have hmod : i % fine = i % coarse % fine := by
    rcases hdvd with ⟨k, hk⟩
    rw [hk, Nat.mod_mul_right_mod]
  have hle : i % coarse % fine ≤ i % coarse := Nat.mod_le _ _
  have hf : i / fine * fine + i % fine = i := by have h := Nat.div_add_mod i fine; rwa [Nat.mul_comm] at h
  have hc : i / coarse * coarse + i % coarse = i := by have h := Nat.div_add_mod i coarse; rwa [Nat.mul_comm] at h
  have hf2 : i - i / fine * fine = i % fine := by omega
  have hc2 : i - i / coarse * coarse = i % coarse := by omega
  omega
end Nov26.Proof_RestartDirectory_NestedStridePointWork

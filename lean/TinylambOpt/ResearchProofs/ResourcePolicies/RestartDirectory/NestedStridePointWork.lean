import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

namespace TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.NestedStridePointWork

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

theorem goal : NestedStridePointWork.Goal := by
  intro coarse fine i hf hc hd
  simp only [NestedStridePointWork.Goal, anchor]
  have hle_mod : i % fine ≤ i % coarse := by
    have hmod : i % fine = i % coarse % fine := by
      rcases hd with ⟨k, hk⟩
      subst hk
      rw [Nat.mul_comm]
      rw [Nat.mod_mod_of_dvd i (Nat.dvd_mul_left fine k)]
    rw [hmod]
    exact Nat.mod_le _ _
  have hdf : i / fine * fine ≤ i := Nat.div_mul_le_self i fine
  have hdc : i / coarse * coarse ≤ i := Nat.div_mul_le_self i coarse
  -- i % s = i - i/s*s
  have hrf : i - i / fine * fine = i % fine := by
    have hdm : fine * (i / fine) + i % fine = i := Nat.div_add_mod i fine
    have hmc : fine * (i / fine) = i / fine * fine := Nat.mul_comm _ _
    rw [hmc] at hdm
    omega
  have hrc : i - i / coarse * coarse = i % coarse := by
    have hdm : coarse * (i / coarse) + i % coarse = i := Nat.div_add_mod i coarse
    have hmc : coarse * (i / coarse) = i / coarse * coarse := Nat.mul_comm _ _
    rw [hmc] at hdm
    omega
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.NestedStridePointWork

import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.RestartDirectory.Goal_NestedStridePointWork

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory


/-!
When a fine restart stride divides a coarse restart stride, every coarse
boundary is also a fine boundary, so a point lookup cannot need more frames
after refining the directory.

The proof only compares restart-boundary positions in the same length stream;
it does not change any decoded value.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_NestedStridePointWork

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

theorem sound : NestedStridePointWork.Goal := by
  intro coarse fine i hfine hcoarse hdiv
  obtain ⟨k, hk⟩ := hdiv
  subst coarse
  have hle_i : i / (fine * k) * (fine * k) ≤ i := Nat.div_mul_le_self i (fine * k)
  have hdvd : fine ∣ i / (fine * k) * (fine * k) := by
    refine ⟨i / (fine * k) * k, ?_⟩
    ac_rfl
  have hmul : fine * ((i / (fine * k) * (fine * k)) / fine) =
      i / (fine * k) * (fine * k) := by
    have h := Nat.div_mul_cancel hdvd
    simpa [Nat.mul_comm] using h
  have hdivmono : (i / (fine * k) * (fine * k)) / fine ≤ i / fine :=
    Nat.div_le_div_right hle_i
  have hle_anchor : i / (fine * k) * (fine * k) ≤ i / fine * fine := by
    rw [← hmul]
    simpa [Nat.mul_comm] using Nat.mul_le_mul_left fine hdivmono
  unfold anchor
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_NestedStridePointWork
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.RestartDirectory.Goal_NestedStridePointWork

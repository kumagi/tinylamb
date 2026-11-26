import TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

/-! A concave quadratic is bounded at both endpoints but can exceed the
    signed width at an interior point, so endpoint certificates are unsound. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_GuardCertificates_EndpointsCertifyCheckedSquareDifference

open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

theorem refute :
    ¬ TinylambOpt.ResearchGoals.JitKernels.GuardCertificates.EndpointsCertifyCheckedSquareDifference.Goal := by
  intro h
  let d : Domain := { lo := 0, hi := 10, modulus := 1, residue := 0 }
  have hmod : 0 < d.modulus := by simp [d]
  have hinside : inside d (5 : Int) := by simp [inside, d]
  have hf1 : fits 4 (first d * (10 - first d)) := by
    simp [fits, first, d]
  have hf2 : fits 4 (last d * (10 - last d)) := by
    simp [fits, last, d]
  have hh := h d 10 4 (5 : Int) hmod hinside hf1 hf2
  have hbad : ¬ (-(2^4:Int) ≤ (25:Int) ∧ (25:Int) < (2^4:Int)) := by native_decide
  exact hbad (by simpa [fits] using hh)

end TinylambOpt.ResearchProofs.JitKernels.Goal_GuardCertificates_EndpointsCertifyCheckedSquareDifference

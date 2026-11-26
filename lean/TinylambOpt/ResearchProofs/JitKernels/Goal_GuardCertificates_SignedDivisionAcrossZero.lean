import TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

-- Refutation: a congruence class modulo 2^k does not make truncating and floored division agree.

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_GuardCertificates_SignedDivisionAcrossZero

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.GuardCertificates.SignedDivisionAcrossZero.Goal := by
  intro h
  let d : TinylambOpt.ResearchGoals.JitKernels.GuardCertificates.Domain := ⟨(-10 : Int), 10, 2, 1⟩
  have hmod : d.modulus = 2 ^ 1 := by native_decide
  have hin' : (-10 : Int) ≤ -1 ∧ -1 ≤ 10 ∧ ((-1 : Int) - 1) % (2 : Int) = 0 := by native_decide
  have hin : TinylambOpt.ResearchGoals.JitKernels.GuardCertificates.inside d (-1 : Int) := by
    simpa [TinylambOpt.ResearchGoals.JitKernels.GuardCertificates.inside, d] using hin'
  have c := h d (-1 : Int) 1 hmod hin
  exact (by native_decide : ¬ ((-1 : Int).tdiv (2 ^ 1) = (-1 : Int) / (2 ^ 1))) c

end TinylambOpt.ResearchProofs.JitKernels.Goal_GuardCertificates_SignedDivisionAcrossZero

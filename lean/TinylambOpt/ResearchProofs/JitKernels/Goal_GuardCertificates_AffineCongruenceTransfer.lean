import TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_GuardCertificates_AffineCongruenceTransfer

open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates


theorem sound : AffineCongruenceTransfer.Goal := by
  intro d m c x hmne hm hx
  have hdiv : (d.modulus : Int) ∣ x - d.residue := Int.dvd_of_emod_eq_zero hx.2.2
  rcases hdiv with ⟨t, ht⟩
  apply Int.emod_eq_zero_of_dvd
  have hlin : m * x + c - (m * d.residue + c) = m * (x - d.residue) := by
    rw [Int.mul_sub]
    omega
  rcases Int.natAbs_eq m with hmabs | hmabs
  · have hna : ((m.natAbs : Nat) : Int) = m := hmabs.symm
    refine ⟨t, ?_⟩
    rw [hlin, ht, Int.natCast_mul, hna, Int.mul_assoc]
  · have hna : ((m.natAbs : Nat) : Int) = -m := by omega
    refine ⟨-t, ?_⟩
    rw [hlin, ht, Int.natCast_mul, hna, Int.mul_assoc]
    rw [Int.mul_neg, Int.neg_mul_neg]
end TinylambOpt.ResearchProofs.JitKernels.Goal_GuardCertificates_AffineCongruenceTransfer

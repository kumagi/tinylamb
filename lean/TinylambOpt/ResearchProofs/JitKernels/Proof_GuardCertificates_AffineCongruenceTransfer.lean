import TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

/-! SOUND: A fused affine expression `m*x + c` of a class-typed value inherits the
multiplied modulus: `x ≡ d.residue (mod d.modulus)` implies
`m*x + c ≡ m*d.residue + c (mod |m| * d.modulus)`, since `m*x - m*d.residue =
m*(d.modulus*q) = (m*d.modulus)*q` is a multiple of `|m|*d.modulus`. Downstream
divisibility tests on the fused value can run before evaluation. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.GuardCertificates_AffineCongruenceTransfer
open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

/-- Affine images of congruence classes carry the multiplied modulus. -/
theorem sound : AffineCongruenceTransfer.Goal := by
  intro d m c x hm0 hm hx
  have hc : (x - d.residue) % (d.modulus:Int) = 0 := hx.2.2
  have hdecomp : (x - d.residue) % (d.modulus:Int)
      + ((x - d.residue) / (d.modulus:Int)) * (d.modulus:Int) = x - d.residue :=
    Int.emod_add_ediv_mul _ _
  rw [hc] at hdecomp
  obtain ⟨q, hq⟩ : ∃ q, x - d.residue = q * (d.modulus:Int) :=
    ⟨(x - d.residue) / (d.modulus:Int), by omega⟩
  have h1 : m*x + c - (m*d.residue + c) = m * (q * (d.modulus:Int)) := by
    rw [← hq, Int.mul_sub]; omega
  rw [h1]
  by_cases hmsign : (0:Int) ≤ m
  · have hn : (m.natAbs : Int) = m := Int.natAbs_of_nonneg hmsign
    refine Int.emod_eq_zero_of_dvd ⟨q, ?_⟩
    simp only [Int.natCast_mul, hn, Int.mul_comm, Int.mul_left_comm, Int.mul_assoc]
  · have hn : (m.natAbs : Int) = -m := by
      rw [← Int.natAbs_neg, Int.natAbs_of_nonneg (by omega)]
    refine Int.emod_eq_zero_of_dvd ⟨-q, ?_⟩
    simp only [Int.natCast_mul, hn, Int.neg_mul, Int.mul_neg, Int.neg_neg,
      Int.mul_comm, Int.mul_left_comm, Int.mul_assoc]

end TinylambOpt.ResearchProofs.JitKernels.GuardCertificates_AffineCongruenceTransfer

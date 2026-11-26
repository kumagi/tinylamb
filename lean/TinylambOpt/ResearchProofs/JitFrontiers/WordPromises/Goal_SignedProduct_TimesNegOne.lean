import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedProduct_TimesNegOne

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises


private theorem tn_mp (x : W)
    (ht : (((x * (-1 : W)).toInt == x.toInt * (-1 : Int)) = true)) :
    x ≠ (0x8000000000000000 : W) := by
  by_cases hx : x = (0x8000000000000000 : W)
  · subst x
    have hbad : ((((0x8000000000000000 : W) * (-1 : W)).toInt ==
        (0x8000000000000000 : W).toInt * (-1 : Int)) = false) := by decide
    rw [hbad] at ht
    contradiction
  · exact hx

private theorem tn_mpr (x : W) (hxne : x ≠ (0x8000000000000000 : W)) :
    ((x * (-1 : W)).toInt == x.toInt * (-1 : Int)) = true := by
  have hxneint : x.toInt ≠ -(2^63 : Int) := by
    intro heq
    apply hxne
    apply BitVec.toInt_inj.mp
    rw [heq]
    decide
  have hnot : ¬ BitVec.smulOverflow x (-1 : W) = true := by
    simp [BitVec.smulOverflow]
    have hb1 := BitVec.le_two_mul_toInt (x := x)
    have hb2 := BitVec.two_mul_toInt_lt (x := x)
    omega
  have heq := BitVec.toInt_mul_of_not_smulOverflow hnot
  rw [heq]
  simp

private theorem tn_goal (x : W) :
    (((x * (-1 : W)).toInt == x.toInt * (-1 : Int)) = true) ↔
      x ≠ (0x8000000000000000 : W) :=
  ⟨tn_mp x, tn_mpr x⟩


theorem sound : SignedProduct.TimesNegOne.Goal := by
  intro x
  simpa [SignedProduct.op] using tn_goal x

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedProduct_TimesNegOne

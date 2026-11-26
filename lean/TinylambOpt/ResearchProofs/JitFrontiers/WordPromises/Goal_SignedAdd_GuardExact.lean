import TinylambOpt.ResearchGoals.JitFrontiers

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedAdd_GuardExact

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises
open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises.SignedAdd

theorem not_eq_true' {b : Bool} : ¬(b = true) → b = false := by
  cases h : b <;> simp_all

theorem h2 (x y : W) : signed (x.toInt + y.toInt) = !x.saddOverflow y := by
  unfold signed BitVec.saddOverflow
  rw [Bool.and_eq_decide, Bool.or_eq_decide, ← decide_not]
  rw [show (2:Int)^(64-1) = (2:Int)^63 from rfl,
    show (-(2^63):Int) = -((2:Int)^63) from rfl]
  refine decide_eq_decide.mpr ?_
  simp only [decide_eq_true_eq]
  omega

theorem h1 (x y : W) : negative ((~~~(x ^^^ y)) &&& (x ^^^ (x+y))) = x.saddOverflow y := by
  unfold negative
  rw [BitVec.saddOverflow_eq]
  have e : ∀ (v : W), v.msb = v.getLsbD 63 := fun _ => rfl
  rw [e (x+y), e x, e y]
  rw [BitVec.getLsbD_and, BitVec.getLsbD_not, BitVec.getLsbD_xor,
    BitVec.getLsbD_xor, BitVec.getLsbD_add (by omega : (63:Nat) < 64)]
  generalize hc : BitVec.carry 63 x y false = C
  generalize ha : x.getLsbD 63 = A
  generalize hb : y.getLsbD 63 = B
  cases A <;> cases B <;> cases C <;> decide

theorem sound : SignedAdd.GuardExact.Goal := by
  intro x y
  simp only [op]
  rw [h1, h2]

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedAdd_GuardExact

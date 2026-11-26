import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.BV64
open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

private theorem bitwise_or_eq_xor_iff (a b : Nat) :
    (Nat.bitwise (· ^^ ·) a b = Nat.bitwise (· || ·) a b) ↔ Nat.bitwise (· && ·) a b = 0 := by
  rw [Nat.eq_iff_testBit_eq, Nat.eq_iff_testBit_eq (m := 0)]
  constructor
  · intro h i
    rw [Nat.testBit_bitwise (by decide : (fun p q : Bool => p && q) false false = false)]
    rw [Nat.zero_testBit]
    have hi := h i
    rw [Nat.testBit_bitwise (by decide : (fun p q : Bool => p ^^ q) false false = false),
      Nat.testBit_bitwise (by decide : (fun p q : Bool => p || q) false false = false)] at hi
    have key : ∀ p q : Bool, ((p ^^ q) = (p || q)) → ((p && q) = false) := by decide
    exact key _ _ hi
  · intro h i
    rw [Nat.testBit_bitwise (by decide : (fun p q : Bool => p ^^ q) false false = false),
      Nat.testBit_bitwise (by decide : (fun p q : Bool => p || q) false false = false)]
    have hi := h i
    rw [Nat.testBit_bitwise (by decide : (fun p q : Bool => p && q) false false = false),
      Nat.zero_testBit] at hi
    have key : ∀ p q : Bool, ((p && q) = false) → ((p ^^ q) = (p || q)) := by decide
    exact key _ _ hi

theorem or_eq_xor_iff {x y : W} : (x ^^^ y = x ||| y) ↔ (x &&& y = 0) := by
  have key := bitwise_or_eq_xor_iff x.toNat y.toNat
  rw [← BitVec.toNat_inj, BitVec.toNat_xor, BitVec.toNat_or]
  have h0 : (x &&& y = (0:W)) ↔ (x &&& y).toNat = 0 := by
    constructor
    · intro h; exact congrArg BitVec.toNat h
    · intro h; exact BitVec.toNat_inj.mp h
  rw [h0, BitVec.toNat_and]
  exact key

theorem and_eq_zero_iff_beq {x y : W} :
    ((x ^^^ y) == (x ||| y)) = ((x &&& y) == 0) := by
  rw [Bool.beq_eq_decide_eq, Bool.beq_eq_decide_eq]
  exact decide_eq_decide.mpr or_eq_xor_iff

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.BV64

import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

/-!
The cheap subtract-and-mask borrow test can have false-positive lanes above an
active borrow, but every such lane is preceded by a real zero byte. Therefore a
short-tail miss check over a live prefix agrees with the exact zero-byte mask:
SQL execution may use the cheaper test to skip scalar equality checks without
changing whether the prefix is reported as a miss.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_BorrowMasks_ActivePrefixMissProof
open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks
open TinylambOpt.ResearchGoals.SwarDigitBallot

abbrev W := BitVec 64
abbrev B := BitVec 8

private def yc : W := ~~~ones + (1 : W)

def carryAfter : Nat → (Nat → Bool) → (Nat → Bool) → Bool → Bool
  | 0, _, _, c => c
  | n+1, xb, yb, c => (xb n).atLeastTwo (yb n) (carryAfter n xb yb c)

theorem carry_eq_from (s k : Nat) (x y : W) (c : Bool) :
    BitVec.carry (s + k) x y c =
      carryAfter k (fun j => x.getLsbD (s + j)) (fun j => y.getLsbD (s + j)) (BitVec.carry s x y c) := by
  induction k with
  | zero => simp [carryAfter]
  | succ k ih =>
      rw [Nat.add_succ, BitVec.carry_succ, ih]
      rfl

theorem getLsbD_extract8 {x : W} {s i : Nat} (hi : i < 8) :
    (x.extractLsb' s 8).getLsbD i = x.getLsbD (s + i) := by
  rw [BitVec.getLsbD_extractLsb']
  simp [hi]

theorem carryAfter_extract8 (s : Nat) (x y : W) (c : Bool) :
    carryAfter 8 (fun j => x.getLsbD (s + j)) (fun j => y.getLsbD (s + j)) c =
      carryAfter 8 (fun j => (x.extractLsb' s 8).getLsbD j) (fun j => (y.extractLsb' s 8).getLsbD j) c := by
  repeat' rw [carryAfter]
  all_goals simp [getLsbD_extract8]

theorem carryAfter_extract7 (s : Nat) (x y : W) (c : Bool) :
    carryAfter 7 (fun j => x.getLsbD (s + j)) (fun j => y.getLsbD (s + j)) c =
      carryAfter 7 (fun j => (x.extractLsb' s 8).getLsbD j) (fun j => (y.extractLsb' s 8).getLsbD j) c := by
  repeat' rw [carryAfter]
  all_goals simp [getLsbD_extract8]

theorem carry8_eq_nat (b y : B) (c : Bool) :
    carryAfter 8 (fun j => b.getLsbD j) (fun j => y.getLsbD j) c =
      decide (b.toNat + y.toNat + c.toNat ≥ 256) := by
  revert b y c
  native_decide

theorem carry7_eq_nat (b y : B) (c : Bool) :
    carryAfter 7 (fun j => b.getLsbD j) (fun j => y.getLsbD j) c =
      decide ((b &&& 0x7f).toNat + (y &&& 0x7f).toNat + c.toNat ≥ 128) := by
  revert b y c
  native_decide

theorem yc_extract (i : Fin 8) :
    yc.extractLsb' (8 * i.val) 8 = if i.val = 0 then (0xff : B) else (0xfe : B) := by
  revert i
  native_decide

theorem yc_lsb7 (i : Fin 8) : yc.getLsbD (8 * i.val + 7) = true := by
  revert i
  native_decide

theorem high_lsb (p : Fin 64) : high.getLsbD p.val = decide (p.val % 8 = 7) := by
  revert p
  native_decide

theorem carry_next (x : W) (i : Nat) (hi : i < 8) :
    BitVec.carry (8 * (i + 1)) x yc false =
      decide ((x.extractLsb' (8 * i) 8).toNat +
        (if i = 0 then 255 else 254) + (BitVec.carry (8 * i) x yc false).toNat ≥ 256) := by
  rw [show 8 * (i + 1) = 8 * i + 8 by omega]
  rw [carry_eq_from]
  rw [carryAfter_extract8]
  rw [yc_extract ⟨i, hi⟩]
  rw [carry8_eq_nat]
  by_cases h0 : i = 0 <;> simp [h0]

theorem carry_bit7 (x : W) (i : Nat) (hi : i < 8) :
    BitVec.carry (8 * i + 7) x yc false =
      decide (((x.extractLsb' (8 * i) 8) &&& 0x7f).toNat +
        (if i = 0 then 127 else 126) + (BitVec.carry (8 * i) x yc false).toNat ≥ 128) := by
  rw [carry_eq_from]
  rw [carryAfter_extract7]
  rw [yc_extract ⟨i, hi⟩]
  rw [carry7_eq_nat]
  by_cases h0 : i = 0 <;> simp [h0]

def bstep (borrow : Bool) (b : B) : Bool := (b == 0) || ((b == 1) && borrow)

def borrow (x : W) (i : Nat) : Bool :=
  if i = 0 then false else !(BitVec.carry (8 * i) x yc false)

theorem not_eq_iff_eq_not {a b : Bool} : (!a = b) ↔ (a = !b) := by
  cases a <;> cases b <;> decide

theorem not_carry_first (b : B) :
    !(decide (b.toNat + 255 + 0 ≥ 256)) = bstep false b := by
  revert b
  native_decide

theorem not_carry_later (b : B) (c : Bool) :
    !(decide (b.toNat + 254 + c.toNat ≥ 256)) = bstep (!c) b := by
  revert b c
  native_decide

theorem borrow_succ (x : W) (i : Nat) (hi : i < 8) :
    borrow x (i + 1) = bstep (borrow x i) (x.extractLsb' (8 * i) 8) := by
  by_cases h0 : i = 0
  · subst i
    simp only [borrow, ite_eq_left rfl, Nat.mul_zero, Nat.zero_add]
    rw [carry_next x 0 (by omega)]
    simp
    exact not_eq_iff_eq_not.mp (by simpa using not_carry_first (x.extractLsb' 0 8))
  · simp only [borrow, ite_eq_right h0]
    rw [carry_next x i hi]
    simp [h0]
    exact not_eq_iff_eq_not.mp (by
      simpa [h0] using not_carry_later (x.extractLsb' (8 * i) 8) (BitVec.carry (8 * i) x yc false))

theorem fast_mark_first (b : B) :
    ((b.getLsbD 7 ^^ (true ^^ decide ((b &&& 0x7f).toNat + 127 + 0 ≥ 128))) && !(b.getLsbD 7)) =
      bstep false b := by
  revert b
  native_decide

theorem fast_mark_later (b : B) (c : Bool) :
    ((b.getLsbD 7 ^^ (true ^^ decide ((b &&& 0x7f).toNat + 126 + c.toNat ≥ 128))) && !(b.getLsbD 7)) =
      bstep (!c) b := by
  revert b c
  native_decide

theorem fast_lsb (x : W) (i : Nat) (hi : i < 8) :
    (fast x).getLsbD (8 * i + 7) = bstep (borrow x i) (x.extractLsb' (8 * i) 8) := by
  have hp : 8 * i + 7 < 64 := by omega
  have hhigh : high.getLsbD (8 * i + 7) = true := by
    have := high_lsb ⟨8 * i + 7, hp⟩
    have hm : (8 * i + 7) % 8 = 7 := by omega
    simpa [hm] using this
  have hyc : yc.getLsbD (8 * i + 7) = true := by
    exact yc_lsb7 ⟨i, hi⟩
  have hx : x.getLsbD (8 * i + 7) = (x.extractLsb' (8 * i) 8).getLsbD 7 := by
    exact (getLsbD_extract8 (x := x) (s := 8 * i) (i := 7) (by omega)).symm
  have hcarry : BitVec.carry (8 * i + 7) x yc false =
      decide (((x.extractLsb' (8 * i) 8) &&& 0x7f).toNat +
        (if i = 0 then 127 else 126) + (BitVec.carry (8 * i) x yc false).toNat ≥ 128) := by
    exact carry_bit7 x i hi
  unfold fast
  rw [BitVec.getLsbD_and, BitVec.getLsbD_and]
  rw [@BitVec.getLsbD_sub 64 (8 * i + 7) hp x ones]
  change ((x.getLsbD (8 * i + 7) ^^
      (yc.getLsbD (8 * i + 7) ^^ BitVec.carry (8 * i + 7) x yc false)) &&
      (~~~x).getLsbD (8 * i + 7) && high.getLsbD (8 * i + 7)) =
    bstep (borrow x i) (x.extractLsb' (8 * i) 8)
  rw [hyc, hhigh, hcarry]
  rw [BitVec.getLsbD_not]
  simp only [hp, decide_true, Bool.true_and, Bool.and_true]
  rw [hx]
  by_cases h0 : i = 0
  · subst i
    simp only [borrow, ite_true, Bool.not_false, Nat.mul_zero, Nat.zero_add,
      BitVec.carry_zero, Bool.toNat_false]
    exact fast_mark_first (x.extractLsb' 0 8)
  · simp only [borrow, h0]
    exact fast_mark_later (x.extractLsb' (8 * i) 8) (BitVec.carry (8 * i) x yc false)

theorem low7_extract (i : Fin 8) : low7.extractLsb' (8 * i.val) 8 = (0x7f : B) := by
  revert i
  native_decide

theorem low7_lsb7 (i : Fin 8) : low7.getLsbD (8 * i.val + 7) = false := by
  revert i
  native_decide

theorem low7_lsb (i : Fin 8) (j : Fin 8) :
    low7.getLsbD (8 * i.val + j.val) = decide (j.val < 7) := by
  revert i j
  native_decide

theorem low7_8_lsb (j : Fin 8) : (0x7f : B).getLsbD j.val = decide (j.val < 7) := by
  revert j
  native_decide

theorem extract_and_low7 (x : W) (i : Nat) (hi : i < 8) :
    (x &&& low7).extractLsb' (8 * i) 8 = (x.extractLsb' (8 * i) 8) &&& 0x7f := by
  apply BitVec.eq_of_getLsbD_eq
  intro j hj
  rw [getLsbD_extract8 (x := x &&& low7) (s := 8 * i) (i := j) hj]
  rw [BitVec.getLsbD_and]
  rw [low7_lsb ⟨i, hi⟩ ⟨j, hj⟩]
  rw [BitVec.getLsbD_and]
  rw [low7_8_lsb ⟨j, hj⟩]
  rw [getLsbD_extract8 (x := x) (s := 8 * i) (i := j) hj]

theorem low_carry_false (b : B) (c : Bool) :
    decide ((b &&& 0x7f).toNat + 127 + c.toNat ≥ 256) = false := by
  revert b c
  native_decide

theorem carry_add_low_next (x : W) (i : Nat) (hi : i < 8)
    (hc : BitVec.carry (8 * i) (x &&& low7) low7 false = false) :
    BitVec.carry (8 * (i + 1)) (x &&& low7) low7 false = false := by
  rw [show 8 * (i + 1) = 8 * i + 8 by omega]
  rw [carry_eq_from, hc]
  rw [carryAfter_extract8]
  rw [low7_extract ⟨i, hi⟩]
  rw [extract_and_low7 x i hi]
  rw [carry8_eq_nat]
  simpa using low_carry_false (x.extractLsb' (8 * i) 8) false

theorem carry_add_low_byte (x : W) : ∀ i : Nat, i ≤ 8 →
    BitVec.carry (8 * i) (x &&& low7) low7 false = false
  | 0, _ => by simp
  | i + 1, h => by
      exact carry_add_low_next x i (by omega) (carry_add_low_byte x i (by omega))

theorem carry_add_low_bit7 (x : W) (i : Nat) (hi : i < 8) :
    BitVec.carry (8 * i + 7) (x &&& low7) low7 false =
      decide (((x.extractLsb' (8 * i) 8) &&& 0x7f).toNat + 127 ≥ 128) := by
  rw [carry_eq_from]
  rw [show BitVec.carry (8 * i) (x &&& low7) low7 false = false from carry_add_low_byte x i (by omega)]
  rw [carryAfter_extract7]
  rw [low7_extract ⟨i, hi⟩]
  rw [extract_and_low7 x i hi]
  rw [carry7_eq_nat]
  simp [Nat.and_assoc]

theorem zero_mark_finite (b : B) :
    (!(decide ((b &&& 0x7f).toNat + 127 ≥ 128) || b.getLsbD 7)) = (b == 0) := by
  revert b
  native_decide

theorem zero_lsb (x : W) (i : Nat) (hi : i < 8) :
    (zeroBytes x).getLsbD (8 * i + 7) = (x.extractLsb' (8 * i) 8 == 0) := by
  have hp : 8 * i + 7 < 64 := by omega
  have hx : x.getLsbD (8 * i + 7) = (x.extractLsb' (8 * i) 8).getLsbD 7 := by
    symm
    exact getLsbD_extract8 (x := x) (s := 8 * i) (i := 7) (by omega)
  have hcarry := carry_add_low_bit7 x i hi
  unfold zeroBytes
  rw [BitVec.getLsbD_not]
  simp only [hp, decide_true, Bool.true_and]
  rw [BitVec.getLsbD_or, BitVec.getLsbD_or]
  rw [@BitVec.getLsbD_add 64 (8 * i + 7) hp (x &&& low7) low7]
  rw [BitVec.getLsbD_and]
  have hlow : low7.getLsbD (8 * i + 7) = false := by
    exact low7_lsb7 ⟨i, hi⟩
  rw [hlow]
  rw [hcarry, hx]
  simp only [Bool.false_xor, Bool.xor_false, Bool.and_false, Bool.or_false]
  exact zero_mark_finite (x.extractLsb' (8 * i) 8)

theorem bstep_true {borrow : Bool} {b : B} (h : bstep borrow b = true) :
    (b == 0) = true ∨ ((b == 1) = true ∧ borrow = true) := by
  revert borrow b
  native_decide

theorem borrow_witness (x : W) : ∀ i : Nat, i < 8 → borrow x i = true →
    ∃ j : Nat, j < i ∧ (x.extractLsb' (8 * j) 8 == 0) = true
  | 0, hi, h => by simp [borrow] at h
  | i + 1, hi, h => by
      have hi8 : i < 8 := by omega
      have hs : bstep (borrow x i) (x.extractLsb' (8 * i) 8) = true := by
        rw [← borrow_succ x i hi8]
        exact h
      rcases bstep_true hs with hzero | ⟨hone, hb⟩
      · exact ⟨i, by omega, hzero⟩
      · rcases borrow_witness x i hi8 hb with ⟨j, hj, hz⟩
        exact ⟨j, by omega, hz⟩

theorem marked_any_true {m : W} {n : Nat} :
    (marked m).any (· < n) = true ↔ ∃ i, i < n ∧ i ∈ marked m := by
  constructor
  · intro h
    rcases List.any_eq_true.mp h with ⟨i, hi, hp⟩
    exact ⟨i, by simpa using hp, hi⟩
  · rintro ⟨i, hi, him⟩
    exact List.any_eq_true.mpr ⟨i, him, by simpa using hi⟩

theorem mem_marked {m : W} {i : Nat} :
    i ∈ marked m ↔ i < 8 ∧ m.getLsbD (8 * i + 7) = true := by
  unfold marked
  constructor
  · intro h
    rcases List.mem_filter.mp h with ⟨hi, hb⟩
    exact ⟨List.mem_range.mp hi, hb⟩
  · rintro ⟨hi, hb⟩
    exact List.mem_filter.mpr ⟨List.mem_range.mpr hi, hb⟩

theorem sound : TinylambOpt.ResearchGoals.JitKernels.BorrowMasks.ActivePrefixMissProof.Goal := by
  intro x n
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro hf
    rw [marked_any_true] at hf ⊢
    rcases hf with ⟨i, hin, hifast⟩
    rcases mem_marked.mp hifast with ⟨hi8, hbit⟩
    have hstep : bstep (borrow x i) (x.extractLsb' (8 * i) 8) = true := by
      rw [← fast_lsb x i hi8]
      exact hbit
    rcases bstep_true hstep with hzero | ⟨hone, hb⟩
    · refine ⟨i, hin, mem_marked.mpr ⟨hi8, ?_⟩⟩
      rw [zero_lsb x i hi8]
      exact hzero
    · rcases borrow_witness x i hi8 hb with ⟨j, hji, hz⟩
      refine ⟨j, by omega, mem_marked.mpr ⟨by omega, ?_⟩⟩
      rw [zero_lsb x j (by omega)]
      exact hz
  · intro hz
    rw [marked_any_true] at hz ⊢
    rcases hz with ⟨i, hin, hizero⟩
    rcases mem_marked.mp hizero with ⟨hi8, hbit⟩
    have hzbyte : (x.extractLsb' (8 * i) 8 == 0) = true := by
      rw [← zero_lsb x i hi8]
      exact hbit
    refine ⟨i, hin, mem_marked.mpr ⟨hi8, ?_⟩⟩
    rw [fast_lsb x i hi8]
    unfold bstep
    rw [hzbyte]
    simp

end TinylambOpt.ResearchProofs.JitKernels.Goal_BorrowMasks_ActivePrefixMissProof

import TinylambOpt.ResearchGoals.SwarDigitBallot

namespace TinylambOpt.ResearchProofs.SwarDigitBallot

open TinylambOpt.ResearchGoals.SwarDigitBallot

/-!
Soundness of the SWAR 8-byte ASCII-digit ballot: every intermediate of
`ballot` is analysed per byte lane (lane `k` = bits `8k .. 8k+7`).  The two
masked additions (and the `<<< 3`) never carry across a lane boundary, so
lane `k` of the result depends only on lane `k` of the input, and it equals
`0x80` exactly when that byte is an ASCII digit (`48 ≤ b ≤ 57`); all other
bits of the lane are 0.

All word-level reasoning is lifted to plain `Nat` via `Z := x.toNat`; none
of the intermediate values overflow `2^64`, so `BitVec.toNat` lemmas bridge
the two worlds exactly.
-/

/-- The repeated-byte constants of `ballot`, as plain `Nat`s. -/
private def hi64 : Nat := 0x8080808080808080
private def low7N : Nat := 0x7f7f7f7f7f7f7f7f
private def f0N : Nat := 0xf0f0f0f0f0f0f0f0
private def n0N : Nat := 0x0f0f0f0f0f0f0f0f
private def c3N : Nat := 0x3030303030303030
private def c6N : Nat := 0x0606060606060606
private def c1N : Nat := 0x1010101010101010

/-- `lane n k` is byte `k` of `n`. -/
private def lane (n k : Nat) : Nat := n / 2 ^ (8 * k) % 2 ^ 8

/-- `ballot` lifted to `Nat`; the masked additions and the shift never
overflow `2^64`, so `(ballot x).toNat = ballotN x.toNat`. -/
private def fZ (Z : Nat) : Nat := (Z &&& f0N) ^^^ c3N
private def fT (Z : Nat) : Nat := ((fZ Z &&& low7N) + low7N) ||| fZ Z ||| low7N
private def fH (Z : Nat) : Nat := (2 ^ 64 - 1) - fT Z
private def fB (Z : Nat) : Nat := (((Z &&& n0N) + c6N) &&& c1N) * 8
private def ballotN (Z : Nat) : Nat := fH Z &&& ((2 ^ 64 - 1) - fB Z) &&& hi64

------------------------------------------------------------------------
-- small helpers
------------------------------------------------------------------------

private theorem k8_cases (k : Nat) (h : k < 8) :
    k = 0 ∨ k = 1 ∨ k = 2 ∨ k = 3 ∨ k = 4 ∨ k = 5 ∨ k = 6 ∨ k = 7 := by omega

private theorem lane_lt256 (n k : Nat) : lane n k < 256 := by
  unfold lane
  exact Nat.mod_lt _ (Nat.two_pow_pos 8)

/-- bit `j` of a number below `2^b` is `false` whenever `b ≤ j`. -/
private theorem testBit_zero_of_lt (x b i : Nat) (hx : x < 2 ^ b) (hbi : b ≤ i) :
    Nat.testBit x i = false := by
  rw [Nat.testBit_eq_decide_div_mod_eq,
    Nat.div_eq_of_lt (Nat.lt_of_lt_of_le hx (Nat.pow_le_pow_right Nat.zero_lt_two hbi)),
    Nat.zero_mod]
  exact decide_eq_false (by omega)

private theorem lane_and (a b k : Nat) :
    lane (a &&& b) k = lane a k &&& lane b k := by
  unfold lane
  rw [Nat.and_div_two_pow, Nat.and_mod_two_pow]

private theorem lane_or (a b k : Nat) :
    lane (a ||| b) k = lane a k ||| lane b k := by
  unfold lane
  rw [Nat.or_div_two_pow, Nat.or_mod_two_pow]

private theorem lane_xor (a b k : Nat) :
    lane (a ^^^ b) k = lane a k ^^^ lane b k := by
  unfold lane
  rw [Nat.xor_div_two_pow, Nat.xor_mod_two_pow]

/-- bit `8*k + b` of a word is bit `b` of its lane `k`. -/
private theorem testBit_lane (n k b : Nat) (hb : b < 8) :
    Nat.testBit n (8 * k + b) = Nat.testBit (lane n k) b := by
  unfold lane
  rw [Nat.add_comm, Nat.testBit_mod_two_pow, decide_eq_true hb, Nat.testBit_div_two_pow,
    Bool.true_and]

/-- lane of a 64-bit complement is the byte complement. -/
private theorem lane_not64 (x : Nat) (hx : x < 2 ^ 64) (k : Nat) (hk : k < 8) :
    lane ((2 ^ 64 - 1) - x) k = 255 - lane x k := by
  apply Nat.eq_of_testBit_eq
  intro j
  rcases Nat.lt_or_ge j 8 with hj | hj
  · have e1 : Nat.testBit (lane ((2 ^ 64 - 1) - x) k) j = !Nat.testBit x (8 * k + j) := by
      unfold lane
      rw [Nat.testBit_mod_two_pow, decide_eq_true hj, Nat.testBit_div_two_pow,
        show (2 ^ 64 - 1) - x = 2 ^ 64 - (x + 1) from by omega,
        Nat.testBit_two_pow_sub_succ hx,
        decide_eq_true (by omega : j + 8 * k < 64)]
      rw [show j + 8 * k = 8 * k + j from by omega, Bool.true_and]
      rfl
    have e2 : Nat.testBit (lane x k) j = Nat.testBit x (8 * k + j) :=
      (testBit_lane x k j hj).symm
    have e3 : Nat.testBit (255 - lane x k) j = !Nat.testBit (lane x k) j := by
      have hm : lane x k < 2 ^ 8 := lane_lt256 x k
      have hs : 255 - lane x k = 2 ^ 8 - (lane x k + 1) := by omega
      rw [hs, Nat.testBit_two_pow_sub_succ hm, decide_eq_true hj, Bool.true_and]
    rw [e1, e3, e2]
  · rw [testBit_zero_of_lt _ 8 j (lane_lt256 _ _) (by omega),
        testBit_zero_of_lt (255 - lane x k) 8 j (by
          have := lane_lt256 x k
          omega) (by omega)]

/-- carry-free lane addition. -/
private theorem lane_add (a b k : Nat)
    (h : a % 2 ^ (8 * k) + b % 2 ^ (8 * k) < 2 ^ (8 * k)) :
    lane (a + b) k = (lane a k + lane b k) % 2 ^ 8 := by
  unfold lane
  have hq : (a + b) / 2 ^ (8 * k) = a / 2 ^ (8 * k) + b / 2 ^ (8 * k) := by
    have ha := Nat.mod_add_div a (2 ^ (8 * k))
    have hb := Nat.mod_add_div b (2 ^ (8 * k))
    have e1 : a + b = (a % 2 ^ (8 * k) + b % 2 ^ (8 * k))
        + 2 ^ (8 * k) * (a / 2 ^ (8 * k)) + 2 ^ (8 * k) * (b / 2 ^ (8 * k)) := by
      omega
    rw [e1, Nat.add_assoc, ← Nat.mul_add,
      Nat.add_mul_div_left _ _ (Nat.two_pow_pos _), Nat.div_eq_of_lt h,
      Nat.zero_add]
  rw [hq, Nat.add_mod]

/-- carry-free lane shift-left-by-3 (`· 8`). -/
private theorem lane_mul8 (a k : Nat)
    (h : a % 2 ^ (8 * k) * 8 < 2 ^ (8 * k)) :
    lane (a * 8) k = (8 * lane a k) % 2 ^ 8 := by
  unfold lane
  have hq : a * 8 / 2 ^ (8 * k) = 8 * (a / 2 ^ (8 * k)) := by
    have ha := Nat.mod_add_div a (2 ^ (8 * k))
    have e1 : a * 8 = (a % 2 ^ (8 * k) + 2 ^ (8 * k) * (a / 2 ^ (8 * k))) * 8 := by
      rw [ha]
    have e2 : a * 8 = a % 2 ^ (8 * k) * 8
        + 2 ^ (8 * k) * (a / 2 ^ (8 * k) * 8) := by
      rw [e1, Nat.add_mul, Nat.mul_assoc]
    rw [e2, Nat.add_mul_div_left _ _ (Nat.two_pow_pos _), Nat.div_eq_of_lt h]
    rw [Nat.zero_add, Nat.mul_comm (a / 2 ^ (8 * k)) 8]
  rw [hq]
  rw [Nat.mul_mod, Nat.mod_eq_of_lt (show (8 : Nat) < 2 ^ 8 by decide)]

------------------------------------------------------------------------
-- lane behaviour of the constants (per-lane case splits)
------------------------------------------------------------------------

private theorem lc_f0 (k : Nat) (hk : k < 8) : lane f0N k = 240 := by
  rcases k8_cases k hk with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;> rfl

private theorem lc_c3 (k : Nat) (hk : k < 8) : lane c3N k = 48 := by
  rcases k8_cases k hk with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;> rfl

private theorem lc_low7 (k : Nat) (hk : k < 8) : lane low7N k = 127 := by
  rcases k8_cases k hk with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;> rfl

private theorem lc_n0 (k : Nat) (hk : k < 8) : lane n0N k = 15 := by
  rcases k8_cases k hk with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;> rfl

private theorem lc_c6 (k : Nat) (hk : k < 8) : lane c6N k = 6 := by
  rcases k8_cases k hk with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;> rfl

private theorem lc_c1 (k : Nat) (hk : k < 8) : lane c1N k = 16 := by
  rcases k8_cases k hk with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;> rfl

private theorem lc_hi (k : Nat) (hk : k < 8) : lane hi64 k = 128 := by
  rcases k8_cases k hk with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;> rfl

/-- `low7` bytes (`0x7f`) plus `low7` never carry out of a lane. -/
private theorem cf_low7 (k : Nat) (hk : k < 8) :
    low7N % 2 ^ (8 * k) * 2 < 2 ^ (8 * k) := by
  rcases k8_cases k hk with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;> decide

/-- the low-nibble mask plus `0x06` never carries out of a lane. -/
private theorem cf_n0c6 (k : Nat) (hk : k < 8) :
    n0N % 2 ^ (8 * k) + c6N % 2 ^ (8 * k) < 2 ^ (8 * k) := by
  rcases k8_cases k hk with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;> decide

/-- the `0x10` mask times 8 never carries out of a lane. -/
private theorem cf_c1 (k : Nat) (hk : k < 8) :
    c1N % 2 ^ (8 * k) * 8 < 2 ^ (8 * k) := by
  rcases k8_cases k hk with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;> decide

------------------------------------------------------------------------
-- bit / arithmetic helpers
------------------------------------------------------------------------

private theorem and_two_pow_bit (X n : Nat) :
    X &&& 2 ^ n = if X.testBit n then 2 ^ n else 0 := by
  cases h : X.testBit n with
  | true =>
    show X &&& 2 ^ n = 2 ^ n
    apply Nat.eq_of_testBit_eq
    intro j
    rw [Nat.testBit_and, Nat.testBit_two_pow]
    by_cases hj : j = n
    · rw [show j = n from hj, h]
      rfl
    · rw [decide_eq_false (fun hh => hj hh.symm)]
      simp
  | false =>
    show X &&& 2 ^ n = 0
    apply Nat.eq_of_testBit_eq
    intro j
    rw [Nat.testBit_and, Nat.zero_testBit, Nat.testBit_two_pow]
    by_cases hj : j = n
    · rw [show j = n from hj, h]
      rfl
    · rw [decide_eq_false (fun hh => hj hh.symm)]
      simp

private theorem sub255_bit7 (m : Nat) :
    Nat.testBit (255 - m) 7 = decide (m ≤ 127) := by
  by_cases hc : m ≤ 127
  · exact (Nat.testBit_eq_decide_div_mod_eq).trans
      ((decide_eq_true (by omega : (255 - m) / 2 ^ 7 % 2 = 1)).trans
        (decide_eq_true hc).symm)
  · exact (Nat.testBit_eq_decide_div_mod_eq).trans
      ((decide_eq_false (by omega : (255 - m) / 2 ^ 7 % 2 ≠ 1)).trans
        (decide_eq_false hc).symm)

private theorem t127_iff (u : Nat) (hu : u < 256) :
    (((u % 128) + 127) ||| u) ||| 127 ≤ 127 ↔ u = 0 := by
  constructor
  · intro h
    have h1 : u % 128 + 127 ≤ (((u % 128) + 127) ||| u) ||| 127 :=
      Nat.le_trans Nat.left_le_or Nat.left_le_or
    have h2 : u ≤ (((u % 128) + 127) ||| u) ||| 127 :=
      Nat.le_trans Nat.right_le_or Nat.left_le_or
    have h3 : u % 128 + 127 ≤ 127 := Nat.le_trans h1 h
    have h4 : u ≤ 127 := Nat.le_trans h2 h
    omega
  · intro h
    subst h
    decide

private theorem and240 (Y : Nat) (hY : Y < 256) : Y &&& 240 = 16 * (Y / 16) := by
  apply Nat.eq_of_testBit_eq
  intro j
  have h240 : Nat.testBit 240 j = (decide (4 ≤ j) && decide (j - 4 < 4)) := by
    show Nat.testBit (15 * 2 ^ 4) j = _
    rw [Nat.testBit_mul_two_pow]
    have : (15 : Nat) = 2 ^ 4 - 1 := rfl
    rw [this, Nat.testBit_two_pow_sub_one]
  rcases Nat.lt_or_ge j 4 with hj | hj
  · have hr : Nat.testBit (16 * (Y / 16)) j = false := by
      show Nat.testBit (2 ^ 4 * (Y / 16)) j = _
      rw [Nat.testBit_two_pow_mul, decide_eq_false (by omega : ¬ 4 ≤ j)]
      rfl
    rw [Nat.testBit_and, h240, hr]
    rw [decide_eq_false (by omega : ¬ (4 : Nat) ≤ j)]
    simp
  · rcases Nat.lt_or_ge j 8 with hj2 | hj2
    · have hr : Nat.testBit (16 * (Y / 16)) j
          = (decide (4 ≤ j) && Nat.testBit (Y / 16) (j - 4)) := by
        show Nat.testBit (2 ^ 4 * (Y / 16)) j = _
        rw [Nat.testBit_two_pow_mul]
      have hdiv : Nat.testBit (Y / 16) (j - 4) = Nat.testBit Y j := by
        rw [show (16 : Nat) = 2 ^ 4 from rfl, Nat.testBit_div_two_pow]
        rw [Nat.sub_add_cancel (by omega : 4 ≤ j)]
      rw [hr, hdiv, Nat.testBit_and, h240]
      rw [decide_eq_true (by omega : 4 ≤ j), decide_eq_true (by omega : j - 4 < 4)]
      simp
    · have hL : Nat.testBit (Y &&& 240) j = false := by
        rw [Nat.testBit_and, h240]
        rw [decide_eq_false (by omega : ¬ (j - 4 < 4))]
        simp
      have hR : Nat.testBit (16 * (Y / 16)) j = false := by
        show Nat.testBit (2 ^ 4 * (Y / 16)) j = _
        rw [Nat.testBit_two_pow_mul, decide_eq_true (by omega : (4 : Nat) ≤ j),
          Bool.true_and]
        exact testBit_zero_of_lt (Y / 16) 4 (j - 4) (by omega) (by omega)
      rw [hL, hR]

------------------------------------------------------------------------
-- per-lane description of `ballotN`
------------------------------------------------------------------------

private theorem lane_ballot (Z : Nat) (hZ : Z < 2 ^ 64) (k : Nat) (hk : k < 8) :
    lane (ballotN Z) k =
      if 48 ≤ lane Z k ∧ lane Z k ≤ 57 then 128 else 0 := by
  let Y := lane Z k
  have hY : Y = lane Z k := rfl
  have hY256 : Y < 256 := lane_lt256 Z k
  -- lane of the high-nibble residual
  have lz : lane (fZ Z) k = (Y &&& 240) ^^^ 48 := by
    rw [fZ, lane_xor, lane_and, lc_f0 k hk, lc_c3 k hk, ← hY]
  have hz256 : (Y &&& 240) ^^^ 48 < 256 := by
    have h1 : Y &&& 240 < 2 ^ 8 :=
      Nat.and_lt_two_pow Y (by decide : (240 : Nat) < 2 ^ 8)
    have h2 : (48 : Nat) < 2 ^ 8 := by decide
    have := Nat.xor_lt_two_pow h1 h2
    exact this
  -- lane of the zero-byte detector
  have lsz : lane (fZ Z &&& low7N) k = ((Y &&& 240) ^^^ 48) % 128 := by
    rw [lane_and, lz, lc_low7 k hk]
    exact Nat.and_two_pow_sub_one_eq_mod _ 7
  have hcT : (fZ Z &&& low7N) % 2 ^ (8 * k) + low7N % 2 ^ (8 * k)
      < 2 ^ (8 * k) := by
    have le1 : (fZ Z &&& low7N) % 2 ^ (8 * k) ≤ low7N % 2 ^ (8 * k) := by
      rw [Nat.and_mod_two_pow]
      exact Nat.and_le_right
    have := cf_low7 k hk
    omega
  have lsum : lane ((fZ Z &&& low7N) + low7N) k
      = ((Y &&& 240) ^^^ 48) % 128 + 127 := by
    have hlt : ((Y &&& 240) ^^^ 48) % 128 + 127 < 2 ^ 8 := by
      have : ((Y &&& 240) ^^^ 48) % 128 < 128 := Nat.mod_lt _ (by decide)
      omega
    rw [lane_add _ _ _ hcT, lsz, lc_low7 k hk, Nat.mod_eq_of_lt hlt]
  have hT64 : fT Z < 2 ^ 64 := by
    have hz64 : fZ Z < 2 ^ 64 := by
      rw [fZ]
      exact Nat.xor_lt_two_pow
        (Nat.lt_of_le_of_lt Nat.and_le_right (by decide : f0N < 2 ^ 64))
        (by decide : c3N < 2 ^ 64)
    have hs64 : (fZ Z &&& low7N) + low7N < 2 ^ 64 := by
      have : (fZ Z &&& low7N) + low7N ≤ low7N + low7N :=
        Nat.add_le_add_right Nat.and_le_right low7N
      exact Nat.lt_of_le_of_lt this (by decide : low7N + low7N < 2 ^ 64)
    rw [fT]
    exact Nat.or_lt_two_pow (Nat.or_lt_two_pow hs64 hz64)
      (by decide : low7N < 2 ^ 64)
  have lT : lane (fT Z) k
      = (((Y &&& 240) ^^^ 48) % 128 + 127) ||| ((Y &&& 240) ^^^ 48) ||| 127 := by
    rw [fT, lane_or, lane_or, lsum, lz, lc_low7 k hk]
  have lH : lane (fH Z) k
      = 255 - ((((Y &&& 240) ^^^ 48) % 128 + 127) ||| ((Y &&& 240) ^^^ 48) ||| 127) := by
    rw [fH, lane_not64 (fT Z) hT64 k hk, lT]
  -- lane of the bad-lane detector
  have ln0 : lane (Z &&& n0N) k = Y % 16 := by
    rw [lane_and, lc_n0 k hk, ← hY]
    exact Nat.and_two_pow_sub_one_eq_mod Y 4
  have hcB : (Z &&& n0N) % 2 ^ (8 * k) + c6N % 2 ^ (8 * k) < 2 ^ (8 * k) := by
    have le1 : (Z &&& n0N) % 2 ^ (8 * k) ≤ n0N % 2 ^ (8 * k) := by
      rw [Nat.and_mod_two_pow]
      exact Nat.and_le_right
    have := cf_n0c6 k hk
    omega
  have lsumB : lane ((Z &&& n0N) + c6N) k = Y % 16 + 6 := by
    have hlt : Y % 16 + 6 < 2 ^ 8 := by
      have : Y % 16 < 16 := Nat.mod_lt _ (by decide)
      omega
    rw [lane_add _ _ _ hcB, ln0, lc_c6 k hk, Nat.mod_eq_of_lt hlt]
  have lBm : lane (((Z &&& n0N) + c6N) &&& c1N) k = (Y % 16 + 6) &&& 16 := by
    rw [lane_and, lsumB, lc_c1 k hk]
  have hc1 : (((Z &&& n0N) + c6N) &&& c1N) % 2 ^ (8 * k) * 8
      < 2 ^ (8 * k) := by
    have le1 : (((Z &&& n0N) + c6N) &&& c1N) % 2 ^ (8 * k)
        ≤ c1N % 2 ^ (8 * k) := by
      rw [Nat.and_mod_two_pow]
      exact Nat.and_le_right
    have := cf_c1 k hk
    omega
  have hb64 : fB Z < 2 ^ 64 := by
    rw [fB]
    have : ((Z &&& n0N) + c6N) &&& c1N ≤ c1N := Nat.and_le_right
    exact Nat.lt_of_le_of_lt (Nat.mul_le_mul_right 8 this)
      (by decide : c1N * 8 < 2 ^ 64)
  have lB : lane (fB Z) k = 8 * ((Y % 16 + 6) &&& 16) := by
    rw [fB, lane_mul8 _ _ hc1, lBm]
    have : (Y % 16 + 6) &&& 16 ≤ 16 := Nat.and_le_right
    rw [Nat.mod_eq_of_lt]
    omega
  -- assemble
  rw [ballotN, lane_and, lane_and, lH, lane_not64 (fB Z) hb64 k hk, lB, lc_hi k hk]
  rw [← hY]
  let T := (((Y &&& 240) ^^^ 48) % 128 + 127) ||| ((Y &&& 240) ^^^ 48) ||| 127
  let B := 8 * ((Y % 16 + 6) &&& 16)
  rw [show (128 : Nat) = 2 ^ 7 from rfl]
  rw [and_two_pow_bit ((255 - T) &&& (255 - B)) 7]
  have hTb : T ≤ 255 := by
    have hmod : ((Y &&& 240) ^^^ 48) % 128 < 128 := Nat.mod_lt _ (by decide)
    have h1 : (((Y &&& 240) ^^^ 48) % 128 + 127) ||| ((Y &&& 240) ^^^ 48) < 2 ^ 8 :=
      Nat.or_lt_two_pow (by omega) hz256
    have h2 : T < 2 ^ 8 :=
      Nat.or_lt_two_pow h1 (by decide : (127 : Nat) < 2 ^ 8)
    omega
  have hBb : B ≤ 255 := by
    have hx : (Y % 16 + 6) &&& 16 ≤ 16 := Nat.and_le_right
    omega
  rw [Nat.testBit_and, sub255_bit7, sub255_bit7]
  -- collapse the disjunction of conditions to the digit test
  have hmask : (Y % 16 + 6) &&& 16 =
      if Nat.testBit (Y % 16 + 6) 4 then 16 else 0 := by
    show (Y % 16 + 6) &&& 2 ^ 4 = if Nat.testBit (Y % 16 + 6) 4 then 2 ^ 4 else 0
    exact and_two_pow_bit (Y % 16 + 6) 4
  have hmain : (T ≤ 127 ∧ B ≤ 127) ↔ (48 ≤ Y ∧ Y ≤ 57) := by
    constructor
    · rintro ⟨h1, h2⟩
      have hz : (Y &&& 240) ^^^ 48 = 0 := (t127_iff _ hz256).mp h1
      have hxeq : Y &&& 240 = 48 := by
        have : Y &&& 240 = (Y &&& 240) ^^^ 48 ^^^ 48 := by
          simp [Nat.xor_assoc]
        rw [hz] at this
        simpa using this
      have hbit : Nat.testBit (Y % 16 + 6) 4 = false := by
        have h2e : 8 * ((Y % 16 + 6) &&& 16) ≤ 127 := h2
        cases htb : Nat.testBit (Y % 16 + 6) 4 with
        | true =>
          have hx : (Y % 16 + 6) &&& 16 = 16 := by
            show (Y % 16 + 6) &&& 2 ^ 4 = 2 ^ 4
            rw [and_two_pow_bit (Y % 16 + 6) 4, htb]
            rfl
          omega
        | false => rfl
      have hlo : Y % 16 < 10 := by
        rcases Nat.lt_or_ge (Y % 16) 10 with hlt | hge
        · exact hlt
        · exfalso
          have h1 : (Y % 16 + 6) / 2 ^ 4 % 2 = 1 := by omega
          have hbit' : Nat.testBit (Y % 16 + 6) 4 = true := by
            rw [Nat.testBit_eq_decide_div_mod_eq]
            exact decide_eq_true h1
          rw [hbit] at hbit'
          simp at hbit'
      have h240 : Y &&& 240 = 16 * (Y / 16) := and240 Y hY256
      have ham : Y / 16 * 16 + Y % 16 = Y := Nat.div_add_mod' Y 16
      exact ⟨by omega, by omega⟩
    · intro h
      have h240 : Y &&& 240 = 16 * (Y / 16) := and240 Y hY256
      have hd : Y / 16 = 3 := by omega
      have hxeq : Y &&& 240 = 48 := by omega
      have hz : (Y &&& 240) ^^^ 48 = 0 := by
        rw [hxeq]
        exact Nat.xor_self 48
      have h1 : T ≤ 127 := by
        show (((Y &&& 240) ^^^ 48) % 128 + 127) ||| ((Y &&& 240) ^^^ 48) ||| 127 ≤ 127
        rw [hz]
        decide
      have hlo : Y % 16 ≤ 9 := by omega
      have hbit : Nat.testBit (Y % 16 + 6) 4 = false := by
        rw [Nat.testBit_eq_decide_div_mod_eq]
        exact decide_eq_false (by omega)
      have h2 : B ≤ 127 := by
        have hx : (Y % 16 + 6) &&& 16 = 0 := by rw [hmask, hbit]; rfl
        show 8 * ((Y % 16 + 6) &&& 16) ≤ 127
        rw [hx]
        omega
      exact ⟨h1, h2⟩
  have hdec :
      (decide (T ≤ 127) && decide (B ≤ 127)) = decide (48 ≤ Y ∧ Y ≤ 57) := by
    rcases hmain with ⟨fwd, bwd⟩
    by_cases hT' : T ≤ 127 <;> by_cases hB' : B ≤ 127
    · rw [decide_eq_true hT', decide_eq_true hB',
        decide_eq_true (fwd ⟨hT', hB'⟩)]
      rfl
    · rw [decide_eq_true hT', decide_eq_false hB',
        decide_eq_false (fun hd => hB' (bwd hd).2)]
      rfl
    · rw [decide_eq_false hT', decide_eq_true hB',
        decide_eq_false (fun hd => hT' (bwd hd).1)]
      rfl
    · rw [decide_eq_false hT', decide_eq_false hB',
        decide_eq_false (fun hd => hT' (bwd hd).1)]
      rfl
  rw [hdec]
  by_cases hd : 48 ≤ Y ∧ Y ≤ 57
  · rw [decide_eq_true hd]
    rw [if_pos hd]
    rfl
  · rw [decide_eq_false hd]
    rw [if_neg hd]
    rfl

private theorem bit_ballot (Z : Nat) (hZ : Z < 2 ^ 64) (k b : Nat)
    (hk : k < 8) (hb : b < 8) :
    Nat.testBit (ballotN Z) (8 * k + b)
      = (b == 7 && decide (48 ≤ lane Z k ∧ lane Z k ≤ 57)) := by
  rw [testBit_lane _ _ _ hb, lane_ballot Z hZ k hk]
  split
  · rename_i hd
    rw [decide_eq_true hd]
    rcases k8_cases b hb with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;> rfl
  · rename_i hd
    rw [decide_eq_false hd]
    rw [Nat.zero_testBit]
    simp

private theorem scalar_lane (x : W) (k : Nat) :
    scalar x k = decide (48 ≤ lane x.toNat k ∧ lane x.toNat k ≤ 57) := by
  show decide (48 ≤ (x.extractLsb' (8 * k) 8).toNat
      ∧ (x.extractLsb' (8 * k) 8).toNat ≤ 57)
    = decide (48 ≤ lane x.toNat k ∧ lane x.toNat k ≤ 57)
  rw [BitVec.extractLsb'_toNat, Nat.shiftRight_eq_div_pow]
  rfl

------------------------------------------------------------------------
-- BitVec ↔ Nat bridge
------------------------------------------------------------------------

private theorem bridge (x : W) : (ballot x).toNat = ballotN x.toNat := by
  have hc2 : low7.toNat = low7N := rfl
  have hc1 : high.toNat = hi64 := rfl
  have hc3 : ((0xf0f0f0f0f0f0f0f0 : W)).toNat = f0N := rfl
  have hc4 : ((0x3030303030303030 : W)).toNat = c3N := rfl
  have hc5 : ((0x0f0f0f0f0f0f0f0f : W)).toNat = n0N := rfl
  have hc6 : ((0x0606060606060606 : W)).toNat = c6N := rfl
  have hc7 : ((0x1010101010101010 : W)).toNat = c1N := rfl
  have hz : ((x &&& 0xf0f0f0f0f0f0f0f0) ^^^ 0x3030303030303030).toNat
      = fZ x.toNat := by
    rw [BitVec.toNat_xor, BitVec.toNat_and, hc3, hc4]
    rfl
  have hs64 : (fZ x.toNat &&& low7N) + low7N < 2 ^ 64 := by
    have : (fZ x.toNat &&& low7N) ≤ low7N := Nat.and_le_right
    exact Nat.lt_of_le_of_lt (Nat.add_le_add_right this low7N)
      (by decide : low7N + low7N < 2 ^ 64)
  have hsum : ((((x &&& 0xf0f0f0f0f0f0f0f0) ^^^ 0x3030303030303030) &&& low7)
      + low7).toNat = (fZ x.toNat &&& low7N) + low7N := by
    rw [BitVec.toNat_add]
    have e : (((x &&& 0xf0f0f0f0f0f0f0f0) ^^^ 0x3030303030303030) &&& low7).toNat
        = fZ x.toNat &&& low7N := by
      rw [BitVec.toNat_and, hz, hc2]
    rw [e, hc2, Nat.mod_eq_of_lt hs64]
  have hT : ((((((x &&& 0xf0f0f0f0f0f0f0f0) ^^^ 0x3030303030303030) &&& low7)
      + low7) ||| ((x &&& 0xf0f0f0f0f0f0f0f0) ^^^ 0x3030303030303030)) ||| low7).toNat
      = fT x.toNat := by
    rw [BitVec.toNat_or, BitVec.toNat_or, hsum, hz, hc2]
    rfl
  have hm : (zeroBytes ((x &&& 0xf0f0f0f0f0f0f0f0) ^^^ 0x3030303030303030)).toNat
      = fH x.toNat := by
    rw [zeroBytes, BitVec.toNat_not, hT]
    rfl
  have hadd2 : ((x &&& 0x0f0f0f0f0f0f0f0f) + 0x0606060606060606).toNat
      = (x.toNat &&& n0N) + c6N := by
    rw [BitVec.toNat_add, BitVec.toNat_and, hc5, hc6, Nat.mod_eq_of_lt]
    have : (x.toNat &&& n0N) ≤ n0N := Nat.and_le_right
    exact Nat.lt_of_le_of_lt (Nat.add_le_add_right this c6N)
      (by decide : n0N + c6N < 2 ^ 64)
  have hbad : ((((x &&& 0x0f0f0f0f0f0f0f0f) + 0x0606060606060606)
      &&& 0x1010101010101010) <<< 3).toNat = fB x.toNat := by
    rw [BitVec.toNat_shiftLeft]
    have e : (((x &&& 0x0f0f0f0f0f0f0f0f) + 0x0606060606060606)
        &&& 0x1010101010101010).toNat = ((x.toNat &&& n0N) + c6N) &&& c1N := by
      rw [BitVec.toNat_and, hadd2, hc7]
    rw [e, Nat.shiftLeft_eq]
    have hle : ((x.toNat &&& n0N) + c6N) &&& c1N ≤ c1N := Nat.and_le_right
    have hb : (((x.toNat &&& n0N) + c6N) &&& c1N) * 2 ^ 3 < 2 ^ 64 :=
      Nat.lt_of_le_of_lt (Nat.mul_le_mul_right (2 ^ 3) hle)
        (by decide : c1N * 2 ^ 3 < 2 ^ 64)
    rw [Nat.mod_eq_of_lt hb]
    rfl
  have hbn : (~~~((((x &&& 0x0f0f0f0f0f0f0f0f) + 0x0606060606060606)
      &&& 0x1010101010101010) <<< 3)).toNat = (2 ^ 64 - 1) - fB x.toNat := by
    rw [BitVec.toNat_not, hbad]
  have hball : ballot x =
      (zeroBytes ((x &&& 0xf0f0f0f0f0f0f0f0) ^^^ 0x3030303030303030)
        &&& ~~~((((x &&& 0x0f0f0f0f0f0f0f0f) + 0x0606060606060606)
          &&& 0x1010101010101010) <<< 3))
        &&& high := rfl
  rw [hball, BitVec.toNat_and, BitVec.toNat_and, hm, hbn, hc1]
  rfl

------------------------------------------------------------------------

theorem sound : TinylambOpt.ResearchGoals.SwarDigitBallot.Goal := by
  intro x i hi
  have hZ : x.toNat < 2 ^ 64 := x.isLt
  have hb : i % 8 < 8 := Nat.mod_lt _ (by decide)
  have hk : i / 8 < 8 := by omega
  have hsum : 8 * (i / 8) + i % 8 = i := by omega
  have h1 : (ballot x).getLsbD i = Nat.testBit (ballotN x.toNat) i := by
    show Nat.testBit (ballot x).toNat i = _
    rw [bridge x]
  have h2 := bit_ballot x.toNat hZ (i / 8) (i % 8) hk hb
  rw [hsum] at h2
  rw [h1, h2, ← scalar_lane x]

end TinylambOpt.ResearchProofs.SwarDigitBallot

import Std.Tactic
-- C++ counterpart: expression/jit.cpp (see issues/README.md).

/-
  TinylambOpt/LikeShiftAnd.lean

  Shift-And substring search for a nonempty byte literal, word-at-a-time.

  Model (binary bytes only; this is NOT UTF-8 / collation aware):
    * needle : List Byte, Byte = Nat, with needle.length > 0 and needle.length
      bounded by the total word width w * k (covers the 63/64/65 boundary by
      choosing w = 64, k = 1 or 2);
    * IDEAL state: a function Nat -> Bool.  Bit j means "the first j+1 bytes
      of the needle equal the suffix of the consumed input ending here".
      The update is  next(j) = (if j = 0 then true else state(j-1)) && (needle[j] == b),
      i.e. left-shift, start-bit injection at bit 0, and AND with the byte mask;
    * WORD machine: k words of w bits each (List (BitVec w)).  `shiftWords`
      shifts inside each word and carries the top bit of word c into bit 0 of
      word c+1; `maskWords` ANDs with the per-byte mask and zeroes every bit
      at index >= needle.length (unused high bits are always clear).

  The main theorem connects the two independently defined machines: after
  processing any input from the all-zero (row-start reset) state, the machine's
  top bit is set iff the needle ends the input.  Two boundary theorems pin the
  traps from the issue: repeated bytes, and a state carried across a row end,
  which manufactures a match that the correctly reset machine rejects.
-/

namespace TinylambOpt.LikeShiftAnd

abbrev Byte := Nat

def packNat (f : Nat → Bool) : Nat → Nat
  | 0 => 0
  | W + 1 => (if f W then 2 ^ W else 0) ||| packNat f W

theorem packNat_testBit (f : Nat → Bool) (W i : Nat) :
    (packNat f W).testBit i = if i < W then f i else false := by
  induction W with
  | zero => simp [packNat]
  | succ W ih =>
      simp only [packNat]
      rw [Nat.testBit_or, ih]
      by_cases hfW : f W
      · simp only [hfW, ↓reduceIte]
        by_cases hiW : i < W
        · have hpow : (2 ^ W).testBit i = false := by
            simp [Nat.testBit_two_pow, show W ≠ i by omega]
          rw [hpow, Bool.false_or, ite_eq_left hiW, ite_eq_left (show i < W + 1 by omega)]
        · by_cases hiEq : i = W
          · subst i
            rw [ite_eq_right (by omega : ¬ W < W), ite_eq_left (by omega : W < W + 1)]
            have hp : (2 ^ W).testBit W = true := by simp
            rw [hp, Bool.true_or, hfW]
          · have hpow : (2 ^ W).testBit i = false := by
              simp [Nat.testBit_two_pow, show W ≠ i by omega]
            rw [hpow, Bool.false_or, ite_eq_right hiW, ite_eq_right (show ¬ i < W + 1 by omega)]
      · simp only [hfW]; rw [ite_eq_right (by decide : ¬ (false = true)), Nat.zero_testBit, Bool.false_or]
        by_cases hiW : i < W
        · rw [ite_eq_left hiW, ite_eq_left (show i < W + 1 by omega)]
        · by_cases hiEq : i = W
          · subst i
            rw [ite_eq_left (by omega : W < W + 1)]; simp [hfW]
          · rw [ite_eq_right hiW, ite_eq_right (show ¬ i < W + 1 by omega)]

theorem pack_getLsbD (W i : Nat) (hi : i < W) (f : Nat → Bool) :
    (BitVec.ofNat W (packNat f W)).getLsbD i = f i := by
  rw [BitVec.getLsbD_ofNat, packNat_testBit]
  simp [hi]

theorem pack_unpack (W : Nat) (x : BitVec W) :
    BitVec.ofNat W (packNat x.getLsbD W) = x := by
  apply BitVec.eq_of_getLsbD_eq
  intro i hi
  rw [BitVec.getLsbD_ofNat, packNat_testBit]
  simp [hi]

def bitAt (w : Nat) : List (BitVec w) → Nat → Bool
  | [], _ => false
  | word :: rest, i => if _h : i < w then word.getLsbD i else bitAt w rest (i - w)

def wordShiftOne (w : Nat) (start : Bool) (word : BitVec w) : BitVec w :=
  BitVec.ofNat w (packNat (fun i =>
    if i < w then (if i = 0 then start else word.getLsbD (i - 1)) else false) w)

theorem wordShiftOne_bit (w i : Nat) (hi : i < w) (start : Bool) (word : BitVec w) :
    (wordShiftOne w start word).getLsbD i = if i = 0 then start else word.getLsbD (i - 1) := by
  unfold wordShiftOne
  rw [pack_getLsbD w i hi, ite_eq_left hi]

def shiftWords (w : Nat) (hw : 1 ≤ w) (start : Bool) :
    List (BitVec w) → List (BitVec w) × Bool
  | [] => ([], start)
  | word :: rest =>
      let shifted : BitVec w := wordShiftOne w start word
      let carry : Bool := word.getLsbD (w - 1)
      let (rest', cout) := shiftWords w hw carry rest
      (shifted :: rest', cout)

theorem shiftWords_length (w : Nat) (hw : 1 ≤ w) (start : Bool) (ws : List (BitVec w)) :
    (shiftWords w hw start ws).1.length = ws.length := by
  induction ws generalizing start with
  | nil => rfl
  | cons word rest ih => simp [shiftWords, ih]

theorem bitAt_shiftWords (w : Nat) (hw : 1 ≤ w) (start : Bool)
    (ws : List (BitVec w)) (i : Nat) (hi : i < w * ws.length) :
    bitAt w (shiftWords w hw start ws).1 i =
      if i = 0 then start else bitAt w ws (i - 1) := by
  induction ws generalizing i start with
  | nil => simp at hi
  | cons word rest ih =>
      have hs : w * (word :: rest).length = w * rest.length + w := by simp [Nat.mul_succ]
      change (if h : i < w then (wordShiftOne w start word).getLsbD i
              else bitAt w (shiftWords w hw (word.getLsbD (w - 1)) rest).1 (i - w)) =
             (if i = 0 then start else bitAt w (word :: rest) (i - 1))
      by_cases hiw : i < w
      · rw [dite_eq_left hiw, wordShiftOne_bit w i hiw start word]
        by_cases hi0 : i = 0
        · subst i
          rw [ite_eq_left rfl]
          change start = if 0 = 0 then start else _
          rfl
        · rw [ite_eq_right hi0]
          have hpred : i - 1 < w := by omega
          have hp : bitAt w (word :: rest) (i - 1) = word.getLsbD (i - 1) := by
            rw [bitAt, dite_eq_left hpred]
          rw [hp, ite_eq_right hi0]
      · rw [dite_eq_right hiw]
        have hwlen : i - w < w * rest.length := by omega
        rw [ih (word.getLsbD (w - 1)) (i - w) hwlen]
        by_cases hi0 : i = 0
        · omega
        · rw [ite_eq_right hi0]
          by_cases hlt : i - 1 < w
          · have hi_eq_w : i = w := by omega
            subst i
            rw [ite_eq_left (show w - w = 0 by omega)]
            have hw1 : w - 1 < w := by omega
            have hp : bitAt w (word :: rest) (w - 1) = word.getLsbD (w - 1) := by
              rw [bitAt, dite_eq_left hw1]
            rw [hp]
          · have hge : w ≤ i - 1 := by omega
            have hsub : i - 1 - w = i - w - 1 := by
              rw [Nat.sub_sub, Nat.sub_sub, Nat.add_comm 1 w]
            have hp : bitAt w (word :: rest) (i - 1) = bitAt w rest (i - w - 1) := by
              rw [bitAt, dite_eq_right hlt, hsub]
            rw [hp, ite_eq_right (show i - w ≠ 0 by omega)]

def wordMaskOne (needle : List Byte) (w : Nat) (b : Byte) (c : Nat) (word : BitVec w) : BitVec w :=
  BitVec.ofNat w (packNat (fun r =>
    if hg : c * w + r < needle.length then
      (if hr : r < w then word.getLsbD r else false) && (needle[c*w+r] == b)
    else false) w)

theorem wordMaskOne_bit (needle : List Byte) (w : Nat) (b : Byte) (c : Nat)
    (word : BitVec w) (i : Nat) (hi : i < w) :
    (wordMaskOne needle w b c word).getLsbD i =
      if hg : c * w + i < needle.length then word.getLsbD i && (needle[c*w+i] == b) else false := by
  unfold wordMaskOne
  rw [pack_getLsbD w i hi, dite_eq_left hi]

def maskWords (needle : List Byte) (w : Nat) (b : Byte) :
    Nat → List (BitVec w) → List (BitVec w)
  | _, [] => []
  | c, word :: rest => wordMaskOne needle w b c word :: maskWords needle w b (c+1) rest

theorem maskWords_length (needle : List Byte) (w : Nat) (b : Byte) (c : Nat) (ws : List (BitVec w)) :
    (maskWords needle w b c ws).length = ws.length := by
  induction ws generalizing c with
  | nil => rfl
  | cons word rest ih => simp [maskWords, ih]

theorem bitAt_maskWords (needle : List Byte) (w : Nat) (b : Byte) (c : Nat)
    (ws : List (BitVec w)) (i : Nat) (hi : i < w * ws.length) :
    bitAt w (maskWords needle w b c ws) i =
      if hg : c * w + i < needle.length then
        bitAt w ws i && (needle[c*w+i] == b)
      else false := by
  induction ws generalizing i c with
  | nil => simp at hi
  | cons word rest ih =>
      have hs : w * (word :: rest).length = w * rest.length + w := by simp [Nat.mul_succ]
      change (if h : i < w then (wordMaskOne needle w b c word).getLsbD i
              else bitAt w (maskWords needle w b (c+1) rest) (i-w)) =
             (if hg : c*w+i < needle.length then bitAt w (word::rest) i && needle[c*w+i] == b else false)
      by_cases hiw : i < w
      · rw [dite_eq_left hiw, wordMaskOne_bit needle w b c word i hiw]
        have hp : bitAt w (word :: rest) i = word.getLsbD i := by rw [bitAt, dite_eq_left hiw]
        rw [hp]
      · rw [dite_eq_right hiw]
        have hiw_rest : i - w < w * rest.length := by omega
        rw [ih (c + 1) (i - w) hiw_rest]
        have hs2 : (c + 1) * w = c * w + w := by
          rw [show c + 1 = Nat.succ c by omega, Nat.succ_mul]
        have hiwle : w ≤ i := by omega
        have hindex : (c + 1) * w + (i - w) = c * w + i := by omega
        rw [hindex]
        have hp : bitAt w (word :: rest) i = bitAt w rest (i - w) := by
          rw [bitAt, dite_eq_right hiw]
        rw [hp]

def EndsHere (l s : List Byte) : Prop := ∃ pre, s = pre ++ l

theorem take_succ_eq (needle : List Byte) (j : Nat) (hj : j < needle.length) :
    needle.take (j + 1) = needle.take j ++ [needle[j]] := by
  rw [List.take_succ]
  have hopt : needle[j]? = some needle[j] := List.getElem?_eq_getElem hj
  simp [hopt]

theorem endsHere_cons_char (needle s : List Byte) (j : Nat) (hj : j < needle.length) (b : Byte) :
    EndsHere (needle.take (j + 1)) (s ++ [b]) ↔
      needle[j] = b ∧ (j = 0 ∨ EndsHere (needle.take j) s) := by
  constructor
  · rintro ⟨pre, hEq⟩
    have htake := take_succ_eq needle j hj
    have hEq3 : s ++ [b] = (pre ++ needle.take j) ++ [needle[j]] := by
      calc
        s ++ [b] = pre ++ needle.take (j + 1) := hEq
        _ = pre ++ (needle.take j ++ [needle[j]]) := by rw [htake]
        _ = (pre ++ needle.take j) ++ [needle[j]] := by rw [List.append_assoc]
    have hlast := congrArg List.getLast? hEq3
    rw [List.getLast?_concat] at hlast
    rw [List.getLast?_concat] at hlast
    simp at hlast
    have hbj : needle[j] = b := hlast.symm
    have hEq4 : s ++ [b] = (pre ++ needle.take j) ++ [b] := by simpa [hbj] using hEq3
    have hs : s = pre ++ needle.take j := List.append_cancel_right hEq4
    refine ⟨hbj, ?_⟩
    by_cases hj0 : j = 0
    · exact Or.inl hj0
    · exact Or.inr ⟨pre, hs⟩
  · rintro ⟨hbj, hprev⟩
    have htake := take_succ_eq needle j hj
    by_cases hj0 : j = 0
    · subst j
      refine ⟨s, ?_⟩
      rw [htake, hbj]
      simp
    · rcases hprev with hzero | ⟨pre, hs⟩
      · omega
      · refine ⟨pre, ?_⟩
        rw [hs, htake, hbj, List.append_assoc]

def idealStepN (needle : List Byte) (state : Nat → Bool) (b : Byte) : Nat → Bool :=
  fun i => if hi : i < needle.length then
    (if i = 0 then true else state (i - 1)) && (needle[i] == b)
  else false

theorem idealStep_spec (needle : List Byte) (s : List Byte) (state : Nat → Bool)
    (inv : ∀ i, state i = true ↔ i < needle.length ∧ EndsHere (needle.take (i + 1)) s)
    (b : Byte) :
    ∀ i, idealStepN needle state b i = true ↔
      i < needle.length ∧ EndsHere (needle.take (i + 1)) (s ++ [b]) := by
  intro i
  unfold idealStepN
  by_cases hi : i < needle.length
  · rw [dite_eq_left hi]
    simp only [Bool.and_eq_true]
    constructor
    · rintro ⟨hshift, hmatch⟩
      have hbeq : needle[i] = b := by simpa using hmatch
      by_cases hi0 : i = 0
      · subst i
        refine ⟨hi, ?_⟩
        exact (endsHere_cons_char needle s 0 (by omega) b).2 ⟨by simpa using hbeq, Or.inl rfl⟩
      · have hs' : state (i - 1) = true := by simpa [hi0] using hshift
        have hprev := (inv (i - 1)).1 hs'
        have hidx : (i - 1) + 1 = i := by omega
        have hprev' : EndsHere (needle.take i) s := by simpa [hidx] using hprev.2
        refine ⟨hi, ?_⟩
        exact (endsHere_cons_char needle s i hi b).2 ⟨hbeq, Or.inr hprev'⟩
    · rintro ⟨_, hend⟩
      have hc := (endsHere_cons_char needle s i hi b).1 hend
      rcases hc with ⟨hbeq, hprev⟩
      have hmatch : needle[i] == b := by simpa [hbeq]
      constructor
      · by_cases hi0 : i = 0
        · simp [hi0]
        · rcases hprev with hzero | ⟨pre, hs⟩
          · omega
          · have hpred : (i - 1) < needle.length := by omega
            have hidx : (i - 1) + 1 = i := by omega
            have hprev2 : EndsHere (needle.take i) s := ⟨pre, hs⟩
            have hinv : state (i - 1) = true :=
              (inv (i - 1)).2 ⟨hpred, by simpa [hidx] using hprev2⟩
            simpa [hi0] using hinv
      · exact hmatch
  · rw [dite_eq_right hi]
    constructor
    · intro h; simp at h
    · rintro ⟨hcontra, _⟩; omega

def idealScanN (needle : List Byte) (state : Nat → Bool) : List Byte → Nat → Bool
  | [], i => state i
  | b :: bs, i => idealScanN needle (idealStepN needle state b) bs i

theorem idealScanN_spec (needle : List Byte) (pre input : List Byte) (state : Nat → Bool) (i : Nat)
    (inv : ∀ j, state j = true ↔ j < needle.length ∧ EndsHere (needle.take (j + 1)) pre) :
    idealScanN needle state input i = true ↔
      i < needle.length ∧ EndsHere (needle.take (i + 1)) (pre ++ input) := by
  induction input generalizing pre state i with
  | nil =>
      simpa [idealScanN] using inv i
  | cons b bs ih =>
      change idealScanN needle (idealStepN needle state b) bs i = true ↔
        i < needle.length ∧ EndsHere (needle.take (i + 1)) (pre ++ b :: bs)
      have inv' : ∀ j, idealStepN needle state b j = true ↔
          j < needle.length ∧ EndsHere (needle.take (j + 1)) (pre ++ [b]) := by
        intro j
        exact idealStep_spec needle pre state inv b j
      have h := ih (pre ++ [b]) (idealStepN needle state b) i inv'
      simpa [List.append_assoc] using h

theorem ideal_top_bit_match (needle : List Byte) (hne : needle ≠ []) (input : List Byte) :
    idealScanN needle (fun _ => false) input (needle.length - 1) = true ↔
      ∃ pre, input = pre ++ needle := by
  have hinit : ∀ j, (fun _ : Nat => false) j = true ↔
      j < needle.length ∧ EndsHere (needle.take (j + 1)) ([] : List Byte) := by
    intro j
    constructor
    · intro h; contradiction
    · rintro ⟨hj, pre, hEq⟩
      have hlen : (needle.take (j + 1)).length = 0 := by
        have hc := congrArg List.length hEq
        simp at hc
        omega
      have hmin : min (j + 1) needle.length = j + 1 := Nat.min_eq_left (by omega)
      have hpos : 0 < (needle.take (j + 1)).length := by
        rw [List.length_take, hmin]
        omega
      omega
  have h := idealScanN_spec needle [] input (fun _ => false) (needle.length - 1) hinit
  have hnpos : 0 < needle.length := List.length_pos_iff.mpr hne
  have hidx : needle.length - 1 + 1 = needle.length := by omega
  rw [h]
  constructor
  · rintro ⟨_, hmatch⟩
    simpa [EndsHere, hidx, List.take_length] using hmatch
  · intro hmatch
    refine ⟨by omega, ?_⟩
    simpa [EndsHere, hidx, List.take_length] using hmatch


def machineStep (needle : List Byte) (w : Nat) (hw : 1 ≤ w)
    (words : List (BitVec w)) (b : Byte) : List (BitVec w) :=
  maskWords needle w b 0 (shiftWords w hw true words).1

theorem machineStep_bit (needle : List Byte) (w : Nat) (hw : 1 ≤ w)
    (words : List (BitVec w)) (b : Byte) (i : Nat)
    (hi : i < w * words.length) :
    bitAt w (machineStep needle w hw words b) i =
      idealStepN needle (bitAt w words) b i := by
  unfold machineStep idealStepN
  have hlen : (shiftWords w hw true words).1.length = words.length := shiftWords_length w hw true words
  have hi' : i < w * (shiftWords w hw true words).1.length := by simpa [hlen] using hi
  rw [bitAt_maskWords needle w b 0 (shiftWords w hw true words).1 i hi']
  rw [bitAt_shiftWords w hw true words i hi]
  simp only [Nat.zero_mul, Nat.zero_add]

theorem maskWords_high_zero (needle : List Byte) (w : Nat) (b : Byte) (c : Nat)
    (ws : List (BitVec w)) (i : Nat) (hi : needle.length ≤ c * w + i) :
    bitAt w (maskWords needle w b c ws) i = false := by
  induction ws generalizing i c with
  | nil => rfl
  | cons word rest ih =>
      by_cases hiw : i < w
      · change (if h : i < w then (wordMaskOne needle w b c word).getLsbD i
                  else bitAt w (maskWords needle w b (c+1) rest) (i-w)) = false
        rw [dite_eq_left hiw, wordMaskOne_bit needle w b c word i hiw, dite_eq_right (by omega)]
      · change (if h : i < w then (wordMaskOne needle w b c word).getLsbD i
                  else bitAt w (maskWords needle w b (c+1) rest) (i-w)) = false
        rw [dite_eq_right hiw]
        apply ih
        have hs2 : (c + 1) * w = c * w + w := by
          rw [show c + 1 = Nat.succ c by omega, Nat.succ_mul]
        omega

theorem machineStep_high_zero (needle : List Byte) (w : Nat) (hw : 1 ≤ w)
    (words : List (BitVec w)) (b : Byte) (i : Nat) (hi : needle.length ≤ i) :
    bitAt w (machineStep needle w hw words b) i = false := by
  unfold machineStep
  apply maskWords_high_zero
  simpa using hi

theorem bitAt_zeroWords (w k i : Nat) : bitAt w (List.replicate k (0#w)) i = false := by
  induction k generalizing i with
  | zero => rfl
  | succ k ih =>
      change bitAt w ((0#w) :: List.replicate k (0#w)) i = false
      rw [bitAt]
      by_cases hiw : i < w
      · rw [dite_eq_left hiw]; simp
      · rw [dite_eq_right hiw, ih (i - w)]

theorem machineScan_bit (needle : List Byte) (w : Nat) (hw : 1 ≤ w)
    (words : List (BitVec w)) (hfit : needle.length ≤ w * words.length)
    (input : List Byte) (i : Nat) (hi : i < w * words.length) :
    bitAt w (input.foldl (fun st b => machineStep needle w hw st b) words) i =
      idealScanN needle (bitAt w words) input i := by
  induction input generalizing words with
  | nil => rfl
  | cons b bs ih =>
      have hstep (j : Nat) : bitAt w (machineStep needle w hw words b) j =
          idealStepN needle (bitAt w words) b j := by
        by_cases hj : j < w * words.length
        · exact machineStep_bit needle w hw words b j hj
        · rw [machineStep_high_zero needle w hw words b j (by omega)]
          unfold idealStepN
          rw [dite_eq_right (by omega)]
      change bitAt w (bs.foldl (fun st b => machineStep needle w hw st b)
        (machineStep needle w hw words b)) i =
          idealScanN needle (idealStepN needle (bitAt w words) b) bs i
      rw [show idealStepN needle (bitAt w words) b =
          bitAt w (machineStep needle w hw words b) by funext j; exact (hstep j).symm]
      have hlen1 := shiftWords_length w hw true words
      have hlen2 := maskWords_length needle w b 0 (shiftWords w hw true words).1
      have hfit' : needle.length ≤ w * (machineStep needle w hw words b).length := by
        simpa [machineStep, hlen1, hlen2] using hfit
      have hi' : i < w * (machineStep needle w hw words b).length := by
        simpa [machineStep, hlen1, hlen2] using hi
      exact ih (machineStep needle w hw words b) hfit' hi'

theorem like_shift_and_sound (needle : List Byte) (hne : needle ≠ [])
    (w k : Nat) (hw : 1 ≤ w) (hfit : needle.length ≤ w * k) (input : List Byte) :
    let st := input.foldl (fun st b => machineStep needle w hw st b)
      (List.replicate k (0#w))
    bitAt w st (needle.length - 1) = true ↔ ∃ pre, input = pre ++ needle := by
  dsimp
  have hi : needle.length - 1 < w * k := by
    have hnpos : 0 < needle.length := List.length_pos_iff.mpr hne
    omega
  have hi0 : needle.length - 1 < w * (List.replicate k (0#w)).length := by
    simpa using hi
  have hfit0 : needle.length ≤ w * (List.replicate k (0#w)).length := by
    simpa using hfit
  rw [machineScan_bit needle w hw (List.replicate k (0#w)) hfit0 input
    (needle.length - 1) hi0]
  rw [show bitAt w (List.replicate k (0#w)) = (fun _ : Nat => false) by
    funext i; exact bitAt_zeroWords w k i]
  exact ideal_top_bit_match needle hne input

theorem repeated_byte_run :
    bitAt 8 ([7, 7, 7].foldl (fun st b => machineStep [7, 7] 8 (by decide) st b)
      (List.replicate 1 (0 : BitVec 8))) 1 = true := by
  decide

theorem no_reset_spurious_match :
    let needle : List Byte := [1, 2]
    let w := 8
    let hw : 1 ≤ w := by decide
    let row1 : List Byte := [1]
    let row2 : List Byte := [2]
    let carry := row1.foldl (fun st b => machineStep needle w hw st b) (List.replicate 1 (0 : BitVec w))
    let withCarry := row2.foldl (fun st b => machineStep needle w hw st b) carry
    let afterReset := row2.foldl (fun st b => machineStep needle w hw st b) (List.replicate 1 (0 : BitVec w))
    bitAt w withCarry 1 = true ∧ bitAt w afterReset 1 = false := by
  decide

theorem boundary_widths :
    (List.replicate 63 (0 : Byte)).length ≤ 64 * 1 ∧
    (List.replicate 64 (0 : Byte)).length ≤ 64 * 1 ∧
    (List.replicate 65 (0 : Byte)).length ≤ 64 * 2 := by
  decide


end TinylambOpt.LikeShiftAnd

import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

/-!
**Proved: two crossing machine words decode every packed field up to width 64.**
For any canonical byte stream (`∀ b ∈ bs, b < 256`), any width `w ≤ 64` and any
lane `i`, a 16-byte window load `load bs w i 16` equals the reference decoder
`takeAt bs w i`: the field's bits always fit in the `shift + w ≤ 8 + 64` bits
covered by sixteen bytes, and both sides reduce to the same arithmetic
extraction `(fromLittle bs / 2^(i*w)) % 2^w` of the field from the whole
stream. SQL execution may let a JIT decoder fetch each ≤ 64-bit packed column
value with two unaligned 64-bit loads plus a shift/mask, instead of the
byte-at-a-time loop, without changing decoded query results.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PackedFieldLoads_TwoWordCrossingDecode
open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
open TinylambOpt.ResearchGoals.Compression.Wire

/-- Unfolding clauses for the little-endian byte decoder. -/
private theorem fromLittle_nil : fromLittle [] = 0 := rfl
private theorem fromLittle_cons (b : Nat) (bs : Bytes) :
    fromLittle (b :: bs) = b + 256 * fromLittle bs := rfl

/-- Reading the first `m` bytes decodes to the value modulo `256^m`. -/
private theorem fromLittle_take_mod (bs : Bytes) (h : ∀ b ∈ bs, b < 256) :
    ∀ m : Nat, fromLittle (bs.take m) = fromLittle bs % 256 ^ m := by
  induction bs with
  | nil => intro m; rw [List.take_nil, fromLittle_nil, Nat.zero_mod]
  | cons b t ih =>
    intro m
    cases m with
    | zero =>
      rw [List.take_zero, fromLittle_nil, Nat.pow_zero, Nat.mod_one]
    | succ m =>
      have hb : b < 256 := h b List.mem_cons_self
      have hE : fromLittle t % 256 ^ m < 256 ^ m :=
        Nat.mod_lt _ (Nat.pow_pos (by omega : (0 : Nat) < 256))
      have h1 : fromLittle ((b :: t).take (m + 1)) = b + 256 * (fromLittle t % 256 ^ m) := by
        rw [List.take_succ_cons, fromLittle_cons,
          ih (fun x hx => h x (List.mem_cons_of_mem b hx)) m]
      have h2 : fromLittle (b :: t) % 256 ^ (m + 1) = b + 256 * (fromLittle t % 256 ^ m) := by
        have hd : fromLittle t = 256 ^ m * (fromLittle t / 256 ^ m) +
            fromLittle t % 256 ^ m := (Nat.div_add_mod _ _).symm
        have h3 : 256 * (256 ^ m * (fromLittle t / 256 ^ m)) =
            (256 ^ m * 256) * (fromLittle t / 256 ^ m) := by
          rw [← Nat.mul_assoc, Nat.mul_comm 256 (256 ^ m)]
        conv => lhs; rw [fromLittle_cons, Nat.pow_succ, hd, Nat.mul_add, h3,
          Nat.add_left_comm b (256 ^ m * 256 * (fromLittle t / 256 ^ m)) _,
          Nat.add_comm (256 ^ m * 256 * (fromLittle t / 256 ^ m)) _,
          Nat.add_mul_mod_self_left]
        refine Nat.mod_eq_of_lt ?_
        omega
      rw [h1, h2]

/-- Dropping `s` leading bytes decodes to the value divided by `256^s`. -/
private theorem fromLittle_drop_div (bs : Bytes) (h : ∀ b ∈ bs, b < 256) :
    ∀ s : Nat, fromLittle (bs.drop s) = fromLittle bs / 256 ^ s := by
  induction bs with
  | nil => intro s; rw [List.drop_nil, fromLittle_nil, Nat.zero_div]
  | cons b t ih =>
    intro s
    cases s with
    | zero => rw [List.drop_zero, Nat.pow_zero, Nat.div_one]
    | succ s =>
      have hb : b < 256 := h b List.mem_cons_self
      have h1 : (b + 256 * fromLittle t) / 256 = fromLittle t := by
        rw [Nat.add_mul_div_left _ _ (by omega : (0 : Nat) < 256),
          Nat.div_eq_of_lt hb, Nat.zero_add]
      rw [List.drop_succ_cons,
        ih (fun x hx => h x (List.mem_cons_of_mem b hx)) s, fromLittle_cons,
        Nat.pow_succ, Nat.mul_comm (256 ^ s) 256, ← Nat.div_div_eq_div_mul, h1]

/-- Base-`b` digits split at position `s`: `X % b^(s+t)` accumulates the low
digits without carry because each digit block is below its radix power. -/
private theorem mod_pow_add (X b s t : Nat) (hb : 0 < b) :
    X % b ^ (s + t) = X % b ^ s + b ^ s * ((X / b ^ s) % b ^ t) := by
  have hs : 0 < b ^ s := Nat.pow_pos hb
  have ht : 0 < b ^ t := Nat.pow_pos hb
  have h1 : X % b ^ s < b ^ s := Nat.mod_lt _ hs
  have h2 : (X / b ^ s) % b ^ t < b ^ t := Nat.mod_lt _ ht
  have h1' : X = b ^ s * (X / b ^ s) + X % b ^ s := (Nat.div_add_mod X (b ^ s)).symm
  have h2' : X / b ^ s = b ^ t * (X / b ^ s / b ^ t) + (X / b ^ s) % b ^ t :=
    (Nat.div_add_mod (X / b ^ s) (b ^ t)).symm
  have key : b ^ s * (b ^ t * (X / b ^ s / b ^ t) + (X / b ^ s) % b ^ t) + X % b ^ s = X := by
    rw [← h2']
    exact h1'.symm
  have hdec : X = b ^ (s + t) * (X / b ^ s / b ^ t) +
      (X % b ^ s + b ^ s * ((X / b ^ s) % b ^ t)) := by
    rw [Nat.pow_add, Nat.mul_assoc, ← Nat.add_assoc,
      Nat.add_comm (b ^ s * (b ^ t * (X / b ^ s / b ^ t))) _, Nat.add_assoc,
      ← Nat.mul_add, Nat.add_comm _ _, key]
  have hbnd : X % b ^ s + b ^ s * ((X / b ^ s) % b ^ t) < b ^ (s + t) := by
    have hB : b ^ s * ((X / b ^ s) % b ^ t) + b ^ s ≤ b ^ s * b ^ t := by
      have := Nat.mul_le_mul_left (b ^ s)
        (by omega : (X / b ^ s) % b ^ t + 1 ≤ b ^ t)
      rwa [Nat.mul_add, Nat.mul_one] at this
    rw [Nat.pow_add]
    omega
  conv => lhs; rw [hdec]
  rw [Nat.add_comm (b ^ (s + t) * (X / b ^ s / b ^ t)) _,
    Nat.add_mul_mod_self_left]
  exact Nat.mod_eq_of_lt hbnd

/-- Shifting after masking equals masking after shifting (general radix). -/
private theorem shift_mod_pow (X b shift w : Nat) (hb : 0 < b) :
    (X % b ^ (shift + w)) / b ^ shift = (X / b ^ shift) % b ^ w := by
  have hp : 0 < b ^ shift := Nat.pow_pos hb
  rw [mod_pow_add X b shift w hb, Nat.add_mul_div_left _ _ hp,
    Nat.div_eq_of_lt (Nat.mod_lt _ hp), Nat.zero_add]

/-- `2^(i*w)` factors into the byte-aligned part and the intra-byte phase. -/
private theorem pow_shift_split (i w : Nat) :
    2 ^ (i * w) = 2 ^ (8 * (i * w / 8)) * 2 ^ (i * w % 8) := by
  have hs2 : 8 * (i * w / 8) + i * w % 8 = i * w := by
    have := Nat.div_add_mod (i * w) 8
    omega
  calc 2 ^ (i * w) = 2 ^ (8 * (i * w / 8) + i * w % 8) := congrArg (2 ^ ·) hs2.symm
    _ = 2 ^ (8 * (i * w / 8)) * 2 ^ (i * w % 8) := Nat.pow_add 2 _ _

/-- A byte window starting at offset `s` of length `m` decodes to the `s`-shifted,
`256^m`-masked part of the whole stream value. -/
private theorem window_eq (bs : Bytes) (s m : Nat) (hb : ∀ b ∈ bs, b < 256) :
    fromLittle ((bs.drop s).take m) = fromLittle bs / 256 ^ s % 256 ^ m := by
  rw [fromLittle_take_mod _ (fun b hbd => hb b (List.mem_of_mem_drop hbd)),
    fromLittle_drop_div bs hb]

/-- Once the requested bits lie inside the `n`-byte window, masking to the
window before shifting is a no-op for the extracted field. -/
private theorem extract_step (Y shift w n : Nat) (h : shift + w ≤ 8 * n) :
    (Y % 2 ^ (8 * n) / 2 ^ shift) % 2 ^ w = (Y / 2 ^ shift) % 2 ^ w := by
  have hd : 2 ^ (shift + w) ∣ 2 ^ (8 * n) := Nat.pow_dvd_pow 2 h
  have h2 : (0 : Nat) < 2 := by decide
  rw [← shift_mod_pow _ 2 _ _ h2, ← shift_mod_pow _ 2 _ _ h2,
    Nat.mod_mod_of_dvd _ hd]

/-- An `n`-byte load whose field fits the window extracts the field directly
from the whole-stream value. -/
private theorem load_eq_extract (bs : Bytes) (w i n : Nat) (hb : ∀ b ∈ bs, b < 256)
    (hwin : i * w % 8 + w ≤ 8 * n) :
    load bs w i n = (fromLittle bs / 2 ^ (i * w)) % 2 ^ w := by
  have h8 : ∀ (k : Nat), 256 ^ k = 2 ^ (8 * k) := by
    intro k
    rw [show (256 : Nat) = 2 ^ 8 from rfl, Nat.pow_mul]
  unfold load
  rw [window_eq bs (i * w / 8) n hb, h8, h8, extract_step _ (i * w % 8) w n hwin,
    pow_shift_split i w, ← Nat.div_div_eq_div_mul]

/-- The reference window decoder equals the whole-stream bit extraction. -/
private theorem takeAt_eq_extract (bs : Bytes) (w i : Nat) (hb : ∀ b ∈ bs, b < 256) :
    takeAt bs w i = (fromLittle bs / 2 ^ (i * w)) % 2 ^ w := by
  have hw : i * w % 8 + w ≤ 8 * byteCount (i * w % 8 + w) := by
    unfold byteCount
    omega
  show (fromLittle ((bs.drop (i * w / 8)).take (byteCount (i * w % 8 + w))) /
    2 ^ (i * w % 8)) % 2 ^ w = (fromLittle bs / 2 ^ (i * w)) % 2 ^ w
  exact load_eq_extract bs w i _ hb hw

theorem sound : TwoWordCrossingDecode.Goal := by
  intro bs w i hw hb
  have hmod : i * w % 8 < 8 := Nat.mod_lt _ (by decide : (0 : Nat) < 8)
  have h16 : i * w % 8 + w ≤ 8 * 16 := by omega
  rw [load_eq_extract bs w i 16 hb h16, takeAt_eq_extract bs w i hb]

end TinylambOpt.ResearchProofs.JitKernels.PackedFieldLoads_TwoWordCrossingDecode

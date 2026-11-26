import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

/-!
**Proved: zero padding leaves masked loads unchanged.** Appending eight zero
bytes to a packed stream does not change `load ... w i 8` for any lane: both
window decodes agree with the same whole-stream extraction, because the padded
high bytes contribute only multiples of `256^len` which vanish modulo `2^w`
after the shift (indeed `w` need not even be bounded here). SQL execution may
let the buffer manager keep a slack region of zeroed bytes after a packed page
so decoders can unconditionally issue their widest 8-byte load at the last
lane, without changing any decoded value or the stored page length.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PackedFieldLoads_ZeroPaddingMaskedWord
open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem fromLittle_nil : fromLittle [] = 0 := rfl
private theorem fromLittle_cons (b : Nat) (bs : Bytes) :
    fromLittle (b :: bs) = b + 256 * fromLittle bs := rfl

/-- Reading the first `m` bytes decodes to the value modulo `256^m`. -/

private theorem fromLittle_append (l₁ l₂ : Bytes) :
    fromLittle (l₁ ++ l₂) = fromLittle l₁ + 256 ^ l₁.length * fromLittle l₂ := by
  induction l₁ with
  | nil => rw [List.nil_append, fromLittle_nil, List.length_nil, Nat.pow_zero,
      Nat.one_mul, Nat.zero_add]
  | cons b t ih =>
    have h : 256 * (256 ^ t.length * fromLittle l₂) =
        256 ^ t.length * 256 * fromLittle l₂ := by
      rw [← Nat.mul_assoc, Nat.mul_comm 256 (256 ^ t.length)]
    rw [List.cons_append, fromLittle_cons, fromLittle_cons, ih,
      List.length_cons, Nat.pow_succ, Nat.mul_add, h]
    omega

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

private theorem window_eq (bs : Bytes) (s m : Nat) (hb : ∀ b ∈ bs, b < 256) :
    fromLittle ((bs.drop s).take m) = fromLittle bs / 256 ^ s % 256 ^ m := by
  rw [fromLittle_take_mod _ (fun b hbd => hb b (List.mem_of_mem_drop hbd)),
    fromLittle_drop_div bs hb]

/-- Once the requested bits lie inside the `n`-byte window, masking to the
window before shifting is a no-op for the extracted field. -/

private theorem fromLittle_zeros (zs : Bytes) (hz : ∀ z ∈ zs, z = 0) :
    fromLittle zs = 0 := by
  induction zs with
  | nil => rfl
  | cons b t ih =>
    rw [fromLittle_cons, hz b List.mem_cons_self,
      ih (fun z hmem => hz z (List.mem_cons_of_mem b hmem)), Nat.mul_zero, Nat.add_zero]

private theorem little_bytes (n k : Nat) : ∀ b ∈ little n k, b < 256 := by
  intro b hb
  rw [little, List.mem_map] at hb
  obtain ⟨i, _, rfl⟩ := hb
  exact Nat.mod_lt _ (by decide : (0 : Nat) < 256)

theorem zeroPad : ZeroPaddingMaskedWord.Goal := by
  intro w xs i hi
  unfold load
  have hz : ∀ z ∈ List.replicate 8 0, z = 0 := fun z hz => (List.mem_replicate.mp hz).2
  have hc : ∀ b ∈ pack w xs, b < 256 :=
    little_bytes (word w xs) (byteCount (xs.length * w))
  have hc2 : ∀ b ∈ pack w xs ++ List.replicate 8 0, b < 256 := by
    intro e he
    rw [List.mem_append] at he
    cases he with
    | inl h => exact hc e h
    | inr h =>
      rw [List.mem_replicate] at h
      rcases h with ⟨-, rfl⟩
      omega
  rw [window_eq _ _ 8 hc2, window_eq _ _ 8 hc, fromLittle_append,
    fromLittle_zeros _ hz, Nat.mul_zero, Nat.add_zero]

end TinylambOpt.ResearchProofs.JitKernels.PackedFieldLoads_ZeroPaddingMaskedWord

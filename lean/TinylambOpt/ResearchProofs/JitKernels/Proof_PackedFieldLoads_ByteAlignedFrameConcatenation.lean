import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

/-!
**Proved: byte-aligned packed frames concatenate without re-encoding.** If the
first column's payload `word w a` occupies an exact number `q` of bytes
(`8 ∣ a.length * w`), then `pack w (a ++ b) = pack w a ++ pack w b`: the packed
integer splits as `word w a + 256^q * word w b` with `word w a < 256^q`, and
`little` of such a split is the concatenation of the two digit streams. SQL
execution may store packed columns page-by-page padded to whole bytes and let
scanners concatenate the byte streams of successive pages (or rows groups)
without re-packing across boundaries, preserving every decoded value.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PackedFieldLoads_ByteAlignedFrameConcatenation
open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem word_nil (w : Nat) : word w [] = 0 := rfl
private theorem word_cons (w x : Nat) (xs : List Nat) :
    word w (x :: xs) = x % 2 ^ w + 2 ^ w * word w xs := rfl

private theorem word_append (w : Nat) (l₁ l₂ : List Nat) :
    word w (l₁ ++ l₂) = word w l₁ + 2 ^ (w * l₁.length) * word w l₂ := by
  induction l₁ with
  | nil =>
    rw [List.nil_append, word_nil, List.length_nil, Nat.mul_zero, Nat.pow_zero,
      Nat.one_mul, Nat.zero_add]
  | cons x t ih =>
    rw [List.cons_append, word_cons, ih, word_cons, List.length_cons, Nat.mul_add,
      ← Nat.mul_assoc, ← Nat.pow_add, Nat.add_comm w (w * t.length), ← Nat.mul_succ,
      ← Nat.add_assoc]

private theorem word_lt (w : Nat) (l : List Nat) : word w l < 2 ^ (w * l.length) := by
  induction l with
  | nil => rw [word_nil, List.length_nil, Nat.mul_zero, Nat.pow_zero]; omega
  | cons x t ih =>
    have h1 : x % 2 ^ w < 2 ^ w :=
      Nat.mod_lt _ (Nat.pow_pos (by omega : (0 : Nat) < 2))
    have h4 : 1 ≤ 2 ^ (w * t.length) - word w t := by omega
    have h5 : 2 ^ w ≤ 2 ^ w * (2 ^ (w * t.length) - word w t) := by
      have h5' := Nat.mul_le_mul_left (2 ^ w) h4
      rw [Nat.mul_one (2 ^ w)] at h5'
      exact h5'
    have h3 : 2 ^ (w * (t.length + 1)) = 2 ^ w * 2 ^ (w * t.length) := by
      rw [Nat.mul_succ, Nat.pow_add, Nat.mul_comm]
    have hB : word w t + (2 ^ (w * t.length) - word w t) = 2 ^ (w * t.length) := by
      omega
    have h6 : 2 ^ w * 2 ^ (w * t.length) =
        2 ^ w * word w t + 2 ^ w * (2 ^ (w * t.length) - word w t) := by
      rw [← Nat.mul_add, hB]
    rw [word_cons, List.length_cons, h3, h6]
    omega

private theorem low_digit (v w i q : Nat) (hv : v < 256 ^ q) (hi : i < q) :
    (v + 256 ^ q * w) / 256 ^ i % 256 = v / 256 ^ i % 256 := by
  obtain ⟨d, rfl⟩ : ∃ d, q = i + (d + 1) := ⟨q - i - 1, by omega⟩
  have hp : 256 ^ (i + (d + 1)) = 256 ^ i * (256 ^ d * 256) := by
    rw [Nat.pow_add, Nat.pow_succ]
  have hpos : 0 < (256 : Nat) ^ i := Nat.pow_pos (by decide : (0 : Nat) < 256)
  rw [hp, Nat.mul_assoc, Nat.add_mul_div_left _ _ hpos]
  rw [show 256 ^ d * 256 * w = 256 * (256 ^ d * w) from
    by rw [Nat.mul_comm (256 ^ d) 256, ← Nat.mul_assoc],
    Nat.add_mul_mod_self_left]

private theorem high_digit (v w i q : Nat) (hv : v < 256 ^ q) :
    (v + 256 ^ q * w) / 256 ^ (q + i) % 256 = w / 256 ^ i % 256 := by
  have hq : 0 < (256 : Nat) ^ q := Nat.pow_pos (by decide : (0 : Nat) < 256)
  have hdiv : (v + 256 ^ q * w) / 256 ^ q = w := by
    rw [Nat.add_comm, Nat.mul_add_div hq, Nat.div_eq_of_lt hv, Nat.add_zero]
  rw [Nat.pow_add, ← Nat.div_div_eq_div_mul, hdiv]

private theorem little_split (v q w k : Nat) (hv : v < 256 ^ q) :
    little (v + 256 ^ q * w) (q + k) = little v q ++ little w k := by
  have h1 : (List.range q).map (fun i => (v + 256 ^ q * w) / 256 ^ i % 256) =
      (List.range q).map (fun i => v / 256 ^ i % 256) :=
    List.map_congr_left (fun i hi => low_digit v w i q hv (List.mem_range.mp hi))
  have h2 : (List.map (q + ·) (List.range k)).map
      (fun i => (v + 256 ^ q * w) / 256 ^ i % 256) =
      (List.range k).map (fun i => w / 256 ^ i % 256) := by
    rw [List.map_map]
    exact List.map_congr_left (fun i _ => high_digit v w i q hv)
  rw [little, little, little, List.range_add, List.map_append, h1, h2]

theorem byteAligned : ByteAlignedFrameConcatenation.Goal := by
  intro w a b hdvd
  obtain ⟨q, hq⟩ := hdvd
  unfold pack
  have hlen : (a ++ b).length = a.length + b.length := List.length_append
  have h256 : 256 ^ q = 2 ^ (a.length * w) := by
    rw [hq, Nat.pow_mul]
  have hw' : word w (a ++ b) = word w a + 256 ^ q * word w b := by
    rw [word_append, Nat.mul_comm w a.length, ← h256]
  have hbc1 : byteCount (a.length * w) = q := by
    unfold byteCount
    rw [hq]
    rw [show 8 * q + 7 = 7 + 8 * q from by omega,
      Nat.add_mul_div_left _ _ (by decide : (0 : Nat) < 8)]
    omega
  have hbc2 : byteCount ((a.length + b.length) * w) = q + byteCount (b.length * w) := by
    unfold byteCount
    have h : (a.length + b.length) * w = 8 * q + b.length * w := by
      rw [Nat.add_mul, hq]
    rw [h, show 8 * q + b.length * w + 7 = b.length * w + 7 + 8 * q from by omega,
      Nat.add_mul_div_left _ _ (by decide : (0 : Nat) < 8)]
    omega
  have hv : word w a < 256 ^ q := by
    rw [h256, Nat.mul_comm a.length w]
    exact word_lt w a
  rw [hlen, hw', hbc2,
    little_split (word w a) q (word w b) (byteCount (b.length * w)) hv, hbc1]

end TinylambOpt.ResearchProofs.JitKernels.PackedFieldLoads_ByteAlignedFrameConcatenation

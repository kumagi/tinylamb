import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_IndependentHits_WireLookup

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.IndependentHits

/-! `wireLookup` for `IndependentHits.model`: a rank packed into the minimal
word width is recovered byte-for-byte by `get`, and the decoder then inverts
it. The ranking/decoding facts are generic in the machine; only the final
theorem is instantiated at `IndependentHits.model`. -/

private abbrev hiCount (m : Machine) (n q : Nat) : Nat :=
  ((m.step q true).map (count m n)).getD 0

private theorem count_succ (m : Machine) (n q : Nat) :
    count m (n + 1) q = lowCount m n q + hiCount m n q := by
  simp [count, lowCount, hiCount]

/-! A rank is strictly below the number of accepted words of that length. -/
private theorem rank_bound (m : Machine) : ∀ (xs : List Bool) (q r : Nat),
    rank m q xs = some r → r < count m xs.length q
  | [], q, r, h => by
    unfold rank at h
    by_cases hacc : m.accept q = true
    · simp [hacc] at h
      subst h
      simp [count, hacc]
    · have hacc' : m.accept q = false := by cases h' : m.accept q <;> simp_all
      simp [hacc'] at h
  | b :: bs, q, r, h => by
    have hlen : (b :: bs).length = bs.length + 1 := by simp
    rw [hlen, count_succ]
    unfold rank at h
    cases hs : m.step q b with
    | none => simp [hs] at h
    | some q' =>
      simp [hs] at h
      cases hr : rank m q' bs with
      | none => simp [hr] at h
      | some tail =>
        simp [hr, Option.bind_some] at h
        subst h
        have ih := rank_bound m bs q' tail hr
        cases b with
        | false =>
          simp [lowCount, hs] at ih ⊢
          omega
        | true =>
          simp [lowCount, hiCount, hs] at ih ⊢
          omega

/-! Ranking then unranking recovers the bitmap. -/
private theorem decoder_all (m : Machine) : decoder m := by
  intro q xs r hrank
  induction xs generalizing q r with
  | nil =>
    unfold rank at hrank
    by_cases hacc : m.accept q = true
    · simp [hacc] at hrank
      subst hrank
      unfold unrank
      simp [hacc]
    · have hacc' : m.accept q = false := by cases h' : m.accept q <;> simp_all
      simp [hacc'] at hrank
  | cons b bs ih =>
    unfold rank at hrank
    cases hs : m.step q b with
    | none => simp [hs] at hrank
    | some q' =>
      simp [hs] at hrank
      cases ht : rank m q' bs with
      | none => simp [ht] at hrank
      | some tail =>
        simp [ht, Option.bind_some] at hrank
        subst hrank
        have ih' := ih q' tail ht
        have hbound := rank_bound m bs q' tail ht
        have hlen : (b :: bs).length = bs.length + 1 := by simp
        unfold unrank
        simp only [hlen]
        cases b with
        | false =>
          have hlt : ¬ (lowCount m bs.length q ≤ tail) := by
            simp [lowCount, hs] at hbound ⊢
            omega
          have hdec : decide (lowCount m bs.length q ≤ tail) = false := by simp [hlt]
          simp [hdec, hs, ih', Option.bind_some]
        | true =>
          have hle : lowCount m bs.length q ≤ tail + lowCount m bs.length q := by omega
          have hdec : decide (lowCount m bs.length q ≤ tail + lowCount m bs.length q) = true := by simp [hle]
          simp [hdec, hs, Option.bind_some]
          have : tail + lowCount m bs.length q - lowCount m bs.length q = tail := by omega
          simp [this, ih']

/-! Wire facts: packing a single rank then reading lane 0 returns the rank. -/
private theorem fromLittle_append (xs : List Nat) (b : Nat) :
    fromLittle (xs ++ [b]) = fromLittle xs + 256 ^ xs.length * b := by
  induction xs with
  | nil => simp [fromLittle]
  | cons x xs ih =>
    simp only [List.cons_append, fromLittle, List.length_cons]
    rw [ih, Nat.pow_succ, Nat.mul_add, Nat.mul_assoc, Nat.mul_comm,
      Nat.mul_left_comm]
    omega

private theorem fromLittle_little (n k : Nat) : fromLittle (little n k) = n % 256 ^ k := by
  induction k with
  | zero => simp [little, fromLittle, Nat.mod_one]
  | succ k ih =>
    have hsplit : little n (k + 1) =
        little n k ++ [(n / 256 ^ k) % 256] := by
      simp [little, List.range_succ]
    rw [hsplit, fromLittle_append, ih]
    have hlen : (little n k).length = k := by simp [little]
    rw [hlen, Nat.pow_succ]
    exact (Nat.mod_mul).symm

private theorem get_pack_singleton (w r : Nat) (hw : r < 2 ^ w) :
    TinylambOpt.ResearchGoals.Compression.Wire.get
      (TinylambOpt.ResearchGoals.Compression.Wire.pack w [r]) w 0 = r := by
  have hword : TinylambOpt.ResearchGoals.Compression.Wire.word w [r] = r % 2 ^ w := by
    simp [TinylambOpt.ResearchGoals.Compression.Wire.word]
  have hb : w ≤ 8 * byteCount w := by simp only [byteCount]; omega
  have hpow : 256 ^ byteCount w = 2 ^ (8 * byteCount w) := by
    rw [show (256 : Nat) = 2 ^ 8 from rfl, ← Nat.pow_mul]
  have hraw : fromLittle
      (little (r % 2 ^ w) (byteCount w)) = r := by
    rw [fromLittle_little]
    have hmod : (r % 2 ^ w) % 256 ^ byteCount w = r % 2 ^ w := by
      rw [hpow]
      exact Nat.mod_eq_of_lt
        (Nat.lt_of_lt_of_le (Nat.mod_lt r (Nat.pow_pos (by decide)))
          (Nat.pow_le_pow_right (by decide) hb))
    rw [hmod, Nat.mod_eq_of_lt hw]
  have hfrom : fromLittle
      (TinylambOpt.ResearchGoals.Compression.Wire.pack w [r]) = r := by
    simpa [TinylambOpt.ResearchGoals.Compression.Wire.pack,
      TinylambOpt.ResearchGoals.Compression.Wire.word] using hraw
  simp only [TinylambOpt.ResearchGoals.Compression.Wire.get, hfrom,
    Nat.zero_mul, Nat.pow_zero, Nat.div_one, Nat.mod_eq_of_lt hw]

private theorem width_lt (n : Nat) : n < 2 ^ width n := by
  simp only [width]
  split
  · rename_i h; simp at h; subst h; decide
  · rename_i h
    have hn : 0 < n := Nat.pos_of_ne_zero (by simpa using h)
    exact Nat.lt_log2_self

private theorem codeBits_bound (m : Machine) (n q r : Nat) (hr : r < count m n q) :
    r < 2 ^ codeBits m n q := by
  simp only [codeBits]
  have hlt : count m n q - 1 < 2 ^ width (count m n q - 1) := width_lt _
  omega

theorem sound_WireLookup : IndependentHits.WireLookup.Goal := by
  intro q xs r hr
  have hbound := rank_bound model xs q r hr
  rw [get_pack_singleton _ _ (codeBits_bound model xs.length q r hbound)]
  exact decoder_all model q xs r hr

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_IndependentHits_WireLookup

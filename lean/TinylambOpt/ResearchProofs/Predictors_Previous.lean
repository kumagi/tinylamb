import TinylambOpt.ResearchGoals.Compression.Predictors

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Predictors.Previous

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Compression.Predictors

/-! Local wire-level facts ( specialised copies of the corresponding lemmas in
`ResearchProofs.Compression.Wire`) so this file verifies independently. -/

private theorem width_eq (n : Nat) (hn : n ≠ 0) : width n = n.log2 + 1 := by
  unfold width
  simp [hn]

private theorem lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  by_cases hn : n = 0
  · subst hn
    simp [width]
  · rw [width_eq n hn]
    exact Nat.lt_log2_self

private theorem width_le_iff (n W : Nat) (hn : n ≠ 0) : width n ≤ W ↔ n < 2 ^ W := by
  rw [width_eq n hn]
  constructor
  · intro h
    exact (Nat.log2_lt hn).mp (by omega)
  · intro h
    have hh := (Nat.log2_lt hn).mpr h
    omega

private theorem ubytes_le_iff (n k : Nat) (hk : 0 < k) : ubytes n ≤ k ↔ n < 128 ^ k := by
  unfold ubytes
  constructor
  · intro h
    have hw7 : (width n + 6) / 7 ≤ k := (Nat.max_le.mp h).2
    have hw : width n ≤ 7 * k := by
      have hh := (Nat.div_le_iff_le_mul_add_pred (by decide : 0 < 7)).mp hw7
      omega
    calc n < 2 ^ width n := lt_two_pow_width n
      _ ≤ 2 ^ (7 * k) := Nat.pow_le_pow_right (by decide : 0 < 2) hw
      _ = 128 ^ k := by
        have h128 : (128 : Nat) = 2 ^ 7 := by decide
        rw [h128, ← Nat.pow_mul]
  · intro h
    apply Nat.max_le.mpr
    constructor
    · omega
    · have hw : width n ≤ 7 * k := by
        by_cases hn : n = 0
        · subst hn
          simp [width]
        · have hlt : n < 2 ^ (7 * k) := by
            have h128 : (128 : Nat) = 2 ^ 7 := by decide
            rw [h128, ← Nat.pow_mul] at h
            exact h
          exact (width_le_iff n (7 * k) hn).mpr hlt
      rw [Nat.div_le_iff_le_mul_add_pred (by decide : 0 < 7)]
      omega

private theorem uleb_len_pos (n : Nat) : 0 < (uleb n).length := by
  unfold uleb
  split <;> simp

private theorem uleb_len_succ (n : Nat) (h : ¬ n < 128) :
    (uleb n).length = 1 + (uleb (n / 128)).length := by
  rw [uleb]
  simp [h]
  omega

private theorem uleb_len_le_one (n : Nat) : (uleb n).length ≤ 1 ↔ n < 128 := by
  by_cases hn : n < 128
  · have h1 : uleb n = [n] := by rw [uleb]; simp [hn]
    rw [h1]
    simp [hn]
  · have hlen : 2 ≤ (uleb n).length := by
      rw [uleb_len_succ n hn]
      have := uleb_len_pos (n / 128)
      omega
    constructor
    · intro h; omega
    · intro h; omega

private theorem uleb_len_le_succ (n k : Nat) :
    (uleb n).length ≤ k + 1 ↔ n < 128 ^ (k + 1) := by
  induction k generalizing n with
  | zero =>
      simpa using uleb_len_le_one n
  | succ k ih =>
      by_cases hn : n < 128
      · have h1 : uleb n = [n] := by rw [uleb]; simp [hn]
        have hlen1 : (uleb n).length = 1 := by rw [h1]; simp
        constructor
        · intro _
          calc n < 128 := hn
            _ ≤ 128 ^ (k + 1 + 1) := by
              have hk1 : 1 ≤ k + 1 + 1 := by omega
              simpa using Nat.pow_le_pow_right (by decide : 0 < 128) hk1
        · intro _
          rw [hlen1]
          omega
      · rw [uleb_len_succ n hn]
        have hih := ih (n / 128)
        constructor
        · intro h
          have hd : (uleb (n / 128)).length ≤ k + 1 := by omega
          have hdiv : n / 128 < 128 ^ (k + 1) := hih.mp hd
          rw [Nat.div_lt_iff_lt_mul (by decide : 0 < 128)] at hdiv
          calc n < 128 ^ (k + 1) * 128 := hdiv
            _ = 128 ^ (k + 1 + 1) := (Nat.pow_succ 128 (k + 1)).symm
        · intro h
          have hdiv : n / 128 < 128 ^ (k + 1) := by
            rw [Nat.div_lt_iff_lt_mul (by decide : 0 < 128)]
            calc n < 128 ^ (k + 1 + 1) := h
              _ = 128 ^ (k + 1) * 128 := by rw [Nat.pow_succ]
          have hd := hih.mpr hdiv
          omega

private theorem uleb_len_le_iff (n k : Nat) (hk : 0 < k) :
    (uleb n).length ≤ k ↔ n < 128 ^ k := by
  obtain ⟨j, rfl⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hk)
  exact uleb_len_le_succ n j

private theorem uleb_len (n : Nat) : (uleb n).length = ubytes n := by
  have hlenpos : 0 < (uleb n).length := uleb_len_pos n
  have hubpos : 0 < ubytes n := by unfold ubytes; omega
  apply Nat.le_antisymm
  · exact (uleb_len_le_iff n (ubytes n) hubpos).mpr
      ((ubytes_le_iff n (ubytes n) hubpos).mp (Nat.le_refl _))
  · exact (ubytes_le_iff n (uleb n).length hlenpos).mpr
      ((uleb_len_le_iff n (uleb n).length hlenpos).mp (Nat.le_refl _))

private theorem max?_some_of_ne_nil : ∀ {xs : List Nat}, xs ≠ [] → ∃ m, xs.max? = some m := by
  intro xs
  cases xs with
  | nil => simp
  | cons a as =>
      intro _
      rw [List.max?_cons]
      exact ⟨as.max?.elim a (max a), rfl⟩

private theorem le_peak {xs : List Nat} {x : Nat} (hx : x ∈ xs) : x ≤ peak xs := by
  have hxsne : xs ≠ [] := by
    intro hnil
    subst xs
    simp at hx
  obtain ⟨m, hm⟩ := max?_some_of_ne_nil (xs := xs) hxsne
  have hmprops := List.max?_eq_some_iff.mp hm
  have hxm : x ≤ m := hmprops.2 x hx
  have hp : peak xs = m := by simp [peak, hm]
  rw [hp]
  exact hxm

private theorem byteCount_mono' {a b : Nat} (hab : a ≤ b) : byteCount a ≤ byteCount b := by
  unfold byteCount
  exact Nat.div_le_div_right (by omega)

private theorem unzig_zig (x : Int) : unzig (zig x) = x := by
  cases x with
  | ofNat n => simp [zig, unzig]
  | negSucc n =>
      have h : (2 * n + 1) % 2 = 1 := by omega
      simp [zig, unzig, h]
      omega

private theorem lag_take_one (h : List Int) : lag (h.take 1) 0 = lag h 0 := by
  cases h with
  | nil => simp [lag]
  | cons x xs => simp [lag]

private theorem encode_eq_of_lag_eq (side : Nat → Int) (i : Nat) (h₁ h₂ : List Int) (xs : List Int)
    (hlag : lag h₁ 0 = lag h₂ 0) :
    encode Previous.predict side i h₁ xs = encode Previous.predict side i h₂ xs := by
  induction xs generalizing i h₁ h₂ with
  | nil => simp [encode]
  | cons x xs ih =>
      simp only [encode]
      have hp : Previous.predict i h₁ side = Previous.predict i h₂ side := by
        dsimp [Previous.predict]
        rw [hlag]
      rw [hp]
      congr 1
      apply ih
      simp [lag]

private theorem encode_length (side : Nat → Int) (i : Nat) (h xs : List Int) :
    (encode Previous.predict side i h xs).length = xs.length := by
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih => simp [encode, ih]

private theorem flat_encode (v : Int) (n : Nat) (side : Nat → Int) (i : Nat) (h : List Int)
    (hlag : lag h 0 = v) :
    encode Previous.predict side i h (List.replicate n v) = List.replicate n 0 := by
  induction n generalizing i h with
  | zero => simp [encode]
  | succ n ih =>
      simp only [List.replicate_succ, encode]
      have hp : Previous.predict i h side = v := by
        dsimp [Previous.predict]
        exact hlag
      rw [hp]
      simp [zig]
      have hlag' : lag (v :: h) 0 = v := by simp [lag]
      rw [ih (i + 1) (v :: h) hlag']

private theorem encode_residuals_lt (side : Nat → Int) (i : Nat) (h xs : List Int) (w : Nat)
    (hcert : certificate Previous.predict side i h xs w) :
    ∀ e ∈ encode Previous.predict side i h xs, e < 2 ^ w := by
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih =>
      have hhead : zig (x - Previous.predict i h side) < 2 ^ w := by
        have h0 := hcert 0 (by simp)
        simpa [Previous.predict, lag] using h0
      have htailcert : certificate Previous.predict side (i + 1) (x :: h) xs w := by
        intro j hj
        have hj' : j + 1 < (x :: xs).length := by simp; omega
        have hc := hcert (j + 1) hj'
        simpa [Previous.predict, lag, List.take_succ_cons, List.reverse_cons,
          List.append_assoc, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hc
      have htail := ih (i + 1) (x :: h) htailcert
      intro e he
      simp only [encode, List.mem_cons] at he
      rcases he with rfl | he
      · exact hhead
      · exact htail e he

theorem sound_ResidualRoundTrip : Previous.ResidualRoundTrip.Goal := by
  intro side i h xs
  induction xs generalizing i h with
  | nil => simp [encode, restore]
  | cons x xs ih =>
      simp only [encode, restore]
      rw [unzig_zig]
      have hx : Previous.predict i h side + (x - Previous.predict i h side) = x := by omega
      rw [hx]
      congr 1
      exact ih (i + 1) (x :: h)

theorem sound_ByteBudget : Previous.ByteBudget.Goal := by
  intro side i h xs w extra oldBytes hcert hlt
  let es := encode Previous.predict side i h xs
  have hes_bound : ∀ e ∈ es, e < 2 ^ w := by
    dsimp [es]
    exact encode_residuals_lt side i h xs w hcert
  have hw_le : width (peak es) ≤ w := by
    by_cases hes : es = []
    · rw [hes]
      simp [peak, width]
    · obtain ⟨m, hm⟩ := max?_some_of_ne_nil hes
      have hmprops := List.max?_eq_some_iff.mp hm
      have hm_lt : m < 2 ^ w := hes_bound m hmprops.1
      have hp : peak es = m := by simp [peak, hm]
      rw [hp]
      by_cases hm0 : m = 0
      · simp [hm0, width]
      · exact (width_le_iff m w hm0).mpr hm_lt
  have hlen_es : es.length = xs.length := by
    dsimp [es]
    exact encode_length side i h xs
  have hubpos : 0 < ubytes w := by unfold ubytes; omega
  have hwlt : w < 128 ^ ubytes w := (ubytes_le_iff w (ubytes w) hubpos).mp (Nat.le_refl _)
  have hw'lt : width (peak es) < 128 ^ ubytes w := Nat.lt_of_le_of_lt hw_le hwlt
  have hub_le : ubytes (width (peak es)) ≤ ubytes w :=
    (ubytes_le_iff (width (peak es)) (ubytes w) hubpos).mpr hw'lt
  have hbc_le : byteCount (es.length * width (peak es)) ≤ byteCount (xs.length * w) := by
    apply byteCount_mono'
    rw [hlen_es]
    exact Nat.mul_le_mul_left xs.length hw_le
  have hpack_len : (pack (width (peak es)) es).length =
      byteCount (es.length * width (peak es)) := by
    simp [pack, little]
  have hframe_len : (frame es).length =
      1 + ubytes es.length + ubytes (width (peak es)) + byteCount (es.length * width (peak es)) := by
    simp [frame, uleb_len, hpack_len]
    omega
  rw [hlen_es] at hbc_le
  have hframe_le : (frame es).length ≤ quote xs.length w := by
    rw [hframe_len]
    dsimp [quote]
    rw [hlen_es]
    omega
  change extra + (frame es).length < oldBytes
  omega

theorem sound_RestartSeed : Previous.RestartSeed.Goal := by
  intro side i h xs
  exact encode_eq_of_lag_eq side i (h.take 1) h xs (lag_take_one h)

theorem sound_FlatRun : Previous.FlatRun.Goal := by
  intro v n side i
  exact flat_encode v n side i [v] (by simp [lag])

end TinylambOpt.ResearchProofs.Compression.Predictors.Previous

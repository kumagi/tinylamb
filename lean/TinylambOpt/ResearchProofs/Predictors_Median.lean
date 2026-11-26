import TinylambOpt.ResearchGoals.Compression.Predictors

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `Median.ByteBudget` — `∀ side i h xs w extra oldBytes, certificate predict side i h
  xs w → extra + quote xs.length w < oldBytes → extra + (frame (encode predict side i
  h xs)).length < oldBytes`
- `Median.OutlierRecovery` — `∀ a b side i, predict i [a,b,a] side = a ∧ predict i
  [b,a,a] side = a`
- `Median.ResidualRoundTrip` — `∀ side i h xs, restore predict side i h (encode predict
  side i h xs) = xs`
- `Median.RestartSeed` — `∀ side i h xs, encode predict side i (h.take (3)) xs = encode
  predict side i h xs`
-/

namespace TinylambOpt.ResearchProofs.Compression.Predictors.Median

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Compression.Predictors
open TinylambOpt.ResearchGoals.Compression.Predictors.Median

set_option maxHeartbeats 0

/-! ## Local wire facts (the proof module for Wire may not be rebuilt yet) -/

private theorem unzig_zig (x : Int) : unzig (zig x) = x := by
  cases x with
  | ofNat n => simp [zig, unzig]
  | negSucc n =>
      have h : (2 * n + 1) % 2 = 1 := by omega
      simp [zig, unzig, h]
      omega

private theorem width_eq (n : Nat) (hn : n ≠ 0) : width n = n.log2 + 1 := by
  unfold width
  simp [hn]

private theorem lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  by_cases hn : n = 0
  · subst hn
    simp [width]
  · rw [width_eq n hn]
    exact Nat.lt_log2_self

private theorem width_le_iff_bound (n W : Nat) (hn : n ≠ 0) : width n ≤ W ↔ n < 2 ^ W := by
  rw [width_eq n hn]
  constructor
  · intro h
    exact (Nat.log2_lt hn).mp (by omega)
  · intro h
    have hh := (Nat.log2_lt hn).mpr h
    omega

private theorem width_le_of_bound (n W : Nat) (h : n < 2 ^ W) : width n ≤ W := by
  by_cases hn : n = 0
  · subst hn
    simp [width]
  · exact (width_le_iff_bound n W hn).mpr h

private theorem ubytes_le_iff_pow (n k : Nat) (hk : 0 < k) : ubytes n ≤ k ↔ n < 128 ^ k := by
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
          exact (width_le_iff_bound n (7 * k) hn).mpr hlt
      rw [Nat.div_le_iff_le_mul_add_pred (by decide : 0 < 7)]
      omega

private theorem lt_pow_ubytes (n : Nat) : n < 128 ^ ubytes n := by
  have hpos : 0 < ubytes n := by unfold ubytes; omega
  exact (ubytes_le_iff_pow n (ubytes n) hpos).mp (Nat.le_refl _)

private theorem ubytes_mono {a b : Nat} (h : a ≤ b) : ubytes a ≤ ubytes b := by
  have hbpos : 0 < ubytes b := by unfold ubytes; omega
  exact (ubytes_le_iff_pow a (ubytes b) hbpos).mpr
    (Nat.lt_of_le_of_lt h ((ubytes_le_iff_pow b (ubytes b) hbpos).mp (Nat.le_refl _)))

private theorem uleb_len_pos (n : Nat) : 0 < (uleb n).length := by
  unfold uleb
  split <;> simp

private theorem uleb_len_succ (n : Nat) (h : ¬ n < 128) :
    (uleb n).length = 1 + (uleb (n / 128)).length := by
  rw [uleb]
  simp [h]
  omega

private theorem uleb_len_le_fuel (n fuel : Nat) (hfuel : 0 < fuel) (hf : n < 128 ^ fuel) :
    (uleb n).length ≤ fuel := by
  obtain ⟨k, rfl⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hfuel)
  induction k generalizing n with
  | zero =>
      by_cases hlt : n < 128
      · rw [uleb]
        simp [hlt]
      · have : 128 ≤ n := by omega
        have hpow : 128 ^ 1 ≤ n := by simpa using this
        omega
  | succ k ih =>
      by_cases hlt : n < 128
      · rw [uleb]
        simp [hlt]
      · have hdiv : n / 128 < 128 ^ (k + 1) := by
          rw [Nat.div_lt_iff_lt_mul (by decide : 0 < 128)]
          simpa [Nat.pow_succ, Nat.mul_comm] using hf
        rw [uleb_len_succ n hlt]
        have hrec := ih (n / 128) (by omega) hdiv
        omega

private theorem uleb_bound (n : Nat) : n < 128 ^ (uleb n).length := by
  induction n using Nat.strongRecOn with
  | _ n ih =>
      by_cases hlt : n < 128
      · have hlen : (uleb n).length = 1 := by rw [uleb]; simp [hlt]
        rw [hlen]
        simpa using hlt
      · have hdivlt : n / 128 < n := Nat.div_lt_self (by omega) (by decide)
        have hrec := ih (n / 128) hdivlt
        have hlen : (uleb n).length = 1 + (uleb (n / 128)).length := uleb_len_succ n hlt
        rw [hlen]
        rw [show 1 + (uleb (n / 128)).length = (uleb (n / 128)).length + 1 by omega]
        rw [Nat.pow_succ]
        exact (Nat.div_lt_iff_lt_mul (by decide : 0 < 128)).mp hrec

private theorem uleb_len_eq_ubytes (n : Nat) : (uleb n).length = ubytes n := by
  apply Nat.le_antisymm
  · exact uleb_len_le_fuel n (ubytes n) (by unfold ubytes; omega) (lt_pow_ubytes n)
  · exact (ubytes_le_iff_pow n (uleb n).length (uleb_len_pos n)).mpr (uleb_bound n)

private theorem packed_charge (w : Nat) (xs : List Nat) :
    (pack w xs).length = byteCount (xs.length * w) := by
  simp [pack, little]

private theorem byteCount_mono {a b : Nat} (h : a ≤ b) : byteCount a ≤ byteCount b := by
  unfold byteCount
  exact Nat.div_le_div_right (by omega)

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

/-! ## Predictor-specific helpers -/

private theorem lag_take (n k : Nat) (h : List Int) (hk : k < n) :
    lag (h.take n) k = lag h k := by
  unfold lag
  rw [List.getElem?_take]
  simp [hk]

private theorem predict_take3_eq (h1 h2 : List Int) (side : Nat → Int) (i : Nat)
    (heq : h1.take 3 = h2.take 3) : predict i h1 side = predict i h2 side := by
  have h0 : lag h1 0 = lag h2 0 := by
    calc lag h1 0 = lag (h1.take 3) 0 := (lag_take 3 0 h1 (by omega)).symm
      _ = lag (h2.take 3) 0 := by rw [heq]
      _ = lag h2 0 := lag_take 3 0 h2 (by omega)
  have h1' : lag h1 1 = lag h2 1 := by
    calc lag h1 1 = lag (h1.take 3) 1 := (lag_take 3 1 h1 (by omega)).symm
      _ = lag (h2.take 3) 1 := by rw [heq]
      _ = lag h2 1 := lag_take 3 1 h2 (by omega)
  have h2' : lag h1 2 = lag h2 2 := by
    calc lag h1 2 = lag (h1.take 3) 2 := (lag_take 3 2 h1 (by omega)).symm
      _ = lag (h2.take 3) 2 := by rw [heq]
      _ = lag h2 2 := lag_take 3 2 h2 (by omega)
  unfold predict
  rw [h0, h1', h2']

private theorem encode_of_take3_eq (side : Nat → Int) (i : Nat) (h1 h2 : List Int)
    (xs : List Int) (heq : h1.take 3 = h2.take 3) :
    encode predict side i h1 xs = encode predict side i h2 xs := by
  induction xs generalizing i h1 h2 with
  | nil => rfl
  | cons x xs ih =>
      have hp : predict i h1 side = predict i h2 side :=
        predict_take3_eq h1 h2 side i heq
      simp only [encode]
      rw [hp]
      congr 1
      have htake2 : h1.take 2 = h2.take 2 := by
        have hc := congrArg (List.take 2) heq
        simpa [List.take_take] using hc
      apply ih (i + 1) (x :: h1) (x :: h2)
      simp [List.take_succ_cons, htake2]

private theorem encode_length (side : Nat → Int) (i : Nat) (h xs : List Int) :
    (encode predict side i h xs).length = xs.length := by
  induction xs generalizing i h with
  | nil => rfl
  | cons x xs ih => simp [encode, ih]

private theorem encode_get (side : Nat → Int) (i : Nat) (h xs : List Int)
    (j : Nat) (hj : j < xs.length) :
    (encode predict side i h xs)[j]?.getD 0 =
      zig ((xs[j]?.getD 0) - predict (i + j) ((xs.take j).reverse ++ h) side) := by
  induction xs generalizing i h j with
  | nil => simp at hj
  | cons x xs ih =>
      cases j with
      | zero => simp [encode]
      | succ j =>
          have hj' : j < xs.length := by simpa using hj
          have hih := ih (i + 1) (x :: h) j hj'
          simpa [encode, List.take_succ_cons, List.reverse_cons, List.append_assoc,
            Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hih

private theorem frame_length (es : List Nat) :
    (frame es).length = quote es.length (width (peak es)) := by
  simp [frame, quote, uleb_len_eq_ubytes, packed_charge]
  omega

private theorem quote_mono_right (n w' w : Nat) (h : w' ≤ w) : quote n w' ≤ quote n w := by
  unfold quote
  have hu : ubytes w' ≤ ubytes w := ubytes_mono h
  have hb : byteCount (n * w') ≤ byteCount (n * w) := byteCount_mono (Nat.mul_le_mul_left n h)
  omega

/-! ## Goals -/

theorem sound_ResidualRoundTrip :
    TinylambOpt.ResearchGoals.Compression.Predictors.Median.ResidualRoundTrip.Goal := by
  intro side i h xs
  induction xs generalizing i h with
  | nil => rfl
  | cons x xs ih =>
      have hx : predict i h side + unzig (zig (x - predict i h side)) = x := by
        rw [unzig_zig]
        omega
      simp [encode, restore, hx, ih]

theorem sound_RestartSeed :
    TinylambOpt.ResearchGoals.Compression.Predictors.Median.RestartSeed.Goal := by
  intro side i h xs
  apply encode_of_take3_eq
  simp [List.take_take]

theorem sound_OutlierRecovery :
    TinylambOpt.ResearchGoals.Compression.Predictors.Median.OutlierRecovery.Goal := by
  intro a b side i
  constructor <;> (unfold predict lag <;> simp <;> omega)

theorem sound_ByteBudget :
    TinylambOpt.ResearchGoals.Compression.Predictors.Median.ByteBudget.Goal := by
  intro side i h xs w extra oldBytes hcert hbudget
  let es := encode predict side i h xs
  have hcount : es.length = xs.length := by dsimp [es]; exact encode_length side i h xs
  have hes : ∀ e ∈ es, e < 2 ^ w := by
    intro e he
    rcases List.mem_iff_getElem.mp he with ⟨j, hj, hjeq⟩
    have hjxs : j < xs.length := by simpa [es, hcount] using hj
    have hval : (es[j]?.getD 0) = es[j] := by
      have hjes : es[j]? = some (es[j]) := List.getElem?_eq_getElem hj
      rw [hjes]
      rfl
    rw [← hjeq, ← hval, show (es[j]?.getD 0) =
      zig ((xs[j]?.getD 0) - predict (i + j) ((xs.take j).reverse ++ h) side) by
        dsimp [es]
        exact encode_get side i h xs j hjxs]
    exact hcert j hjxs
  have hwle : width (peak es) ≤ w := by
    by_cases he : es = []
    · rw [he]
      simp [peak, width]
    · obtain ⟨m, hm⟩ := max?_some_of_ne_nil (xs := es) he
      have hmprops := List.max?_eq_some_iff.mp hm
      have hmlt : m < 2 ^ w := hes m hmprops.1
      have hp : peak es = m := by simp [peak, hm]
      rw [hp]
      exact width_le_of_bound m w hmlt
  have hlen : (frame es).length = quote es.length (width (peak es)) := frame_length es
  change extra + (frame es).length < oldBytes
  rw [hlen, hcount]
  have hq : quote xs.length (width (peak es)) ≤ quote xs.length w :=
    quote_mono_right xs.length (width (peak es)) w hwle
  omega

end TinylambOpt.ResearchProofs.Compression.Predictors.Median

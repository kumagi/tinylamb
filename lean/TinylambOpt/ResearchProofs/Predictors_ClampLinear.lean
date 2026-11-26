import TinylambOpt.ResearchGoals.Compression.Predictors

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `ClampLinear.BoundedResidual` — `∀ h side i x lo hi, lo ≤ x → x ≤ hi → lo ≤ lag h 1 →
  lag h 1 ≤ hi → lo ≤ lag h 2 → lag h 2 ≤ hi → (x-predict i h side).natAbs ≤
  (hi-lo).toNat`
- `ClampLinear.ByteBudget` — `∀ side i h xs w extra oldBytes, certificate predict side
  i h xs w → extra + quote xs.length w < oldBytes → extra + (frame (encode predict
  side i h xs)).length < oldBytes`
- `ClampLinear.ResidualRoundTrip` — `∀ side i h xs, restore predict side i h (encode
  predict side i h xs) = xs`
- `ClampLinear.RestartSeed` — `∀ side i h xs, encode predict side i (h.take (3)) xs =
  encode predict side i h xs`
-/

namespace TinylambOpt.ResearchProofs.Compression.Predictors.ClampLinear

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Compression.Predictors
open TinylambOpt.ResearchGoals.Compression.Predictors.ClampLinear

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! ## generic encode / restore facts -/

private theorem unzig_zig (x : Int) : unzig (zig x) = x := by
  cases x with
  | ofNat n =>
      simp [zig, unzig]
  | negSucc n =>
      have h : (2 * n + 1) % 2 = 1 := by omega
      simp [zig, unzig, h]
      omega

private theorem encode_restore (p : Predict) (side : Nat → Int) :
    ∀ (i : Nat) (h xs : List Int),
      restore p side i h (encode p side i h xs) = xs := by
  intro i h xs
  induction xs generalizing i h with
  | nil => simp [encode, restore]
  | cons x xs ih =>
      simp only [encode, restore]
      rw [unzig_zig]
      have hval : p i h side + (x - p i h side) = x := by omega
      rw [hval]
      rw [ih (i + 1) (x :: h)]

private theorem encode_length (p : Predict) (side : Nat → Int) :
    ∀ (i : Nat) (h xs : List Int), (encode p side i h xs).length = xs.length := by
  intro i h xs
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih =>
      simp only [encode, List.length_cons]
      rw [ih]

private theorem encode_lt_certificate (p : Predict) (side : Nat → Int)
    (i : Nat) (h xs : List Int) (w : Nat)
    (hc : certificate p side i h xs w) :
    ∀ e ∈ encode p side i h xs, e < 2 ^ w := by
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih =>
      simp only [encode]
      intro e he
      simp only [List.mem_cons] at he
      rcases he with rfl | he
      · have h0 := hc 0 (by simp)
        simpa using h0
      · apply ih (i + 1) (x :: h)
        · intro j hj
          have hj' : j + 1 < (x :: xs).length := by
            simp only [List.length_cons]
            omega
          have hh := hc (j + 1) hj'
          simpa [List.take_succ_cons, List.reverse_cons, List.append_assoc,
            Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hh
        · exact he

/-! ## wire-size arithmetic (local copies of the wire lemmas we need) -/

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

private theorem width_mono {a b : Nat} (hab : a ≤ b) : width a ≤ width b := by
  by_cases ha : a = 0
  · subst a
    simp [width]
  · have hlt : a < 2 ^ width b := Nat.lt_of_le_of_lt hab (lt_two_pow_width b)
    exact (width_le_iff a (width b) ha).mpr hlt

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

private theorem ubytes_mono {a b : Nat} (hab : a ≤ b) : ubytes a ≤ ubytes b := by
  unfold ubytes
  apply Nat.max_le.mpr
  constructor
  · exact Nat.le_max_left 1 ((width b + 6) / 7)
  · exact Nat.le_trans (Nat.div_le_div_right (Nat.add_le_add_right (width_mono hab) 6))
      (Nat.le_max_right 1 ((width b + 6) / 7))

private theorem byteCount_mono {a b : Nat} (h : a ≤ b) : byteCount a ≤ byteCount b := by
  unfold byteCount
  exact Nat.div_le_div_right (by omega)

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
    · intro h
      omega
    · intro h
      omega

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

private theorem uleb_len_eq_ubytes (n : Nat) : (uleb n).length = ubytes n := by
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

private theorem peak_encode_lt (p : Predict) (side : Nat → Int)
    (i : Nat) (h xs : List Int) (w : Nat)
    (hc : certificate p side i h xs w) :
    peak (encode p side i h xs) < 2 ^ w := by
  let es := encode p side i h xs
  have hall : ∀ e ∈ es, e < 2 ^ w := by
    simpa [es] using encode_lt_certificate p side i h xs w hc
  change peak es < 2 ^ w
  by_cases hne : es = []
  · have hp0 : peak es = 0 := by simp [peak, hne]
    rw [hp0]
    exact Nat.pow_pos (by decide : 0 < 2)
  · obtain ⟨m, hm⟩ := max?_some_of_ne_nil hne
    have hmprops := List.max?_eq_some_iff.mp hm
    have hmlt := hall m hmprops.1
    simpa [peak, hm] using hmlt

private theorem width_encode_le (p : Predict) (side : Nat → Int)
    (i : Nat) (h xs : List Int) (w : Nat)
    (hc : certificate p side i h xs w) :
    width (peak (encode p side i h xs)) ≤ w := by
  have hpk := peak_encode_lt p side i h xs w hc
  by_cases h0 : peak (encode p side i h xs) = 0
  · simp [h0, width]
  · exact (width_le_iff (peak (encode p side i h xs)) w h0).mpr hpk

private theorem frame_length_le_quote (p : Predict) (side : Nat → Int)
    (i : Nat) (h xs : List Int) (w : Nat)
    (hc : certificate p side i h xs w) :
    (frame (encode p side i h xs)).length ≤ quote xs.length w := by
  let es := encode p side i h xs
  have hes_len : es.length = xs.length := by
    dsimp [es]
    exact encode_length p side i h xs
  have hw_le : width (peak es) ≤ w := by
    dsimp [es]
    exact width_encode_le p side i h xs w hc
  have huleb_len : (uleb es.length).length = ubytes es.length := uleb_len_eq_ubytes es.length
  have hpacklen : (pack (width (peak es)) es).length =
      byteCount (es.length * width (peak es)) := by
    simp [pack, little]
  have hubpos : 0 < ubytes w := by unfold ubytes; omega
  have hulebw : (uleb (width (peak es))).length ≤ ubytes w := by
    have hbound : width (peak es) < 128 ^ (ubytes w) :=
      Nat.lt_of_le_of_lt hw_le ((ubytes_le_iff w (ubytes w) hubpos).mp (Nat.le_refl _))
    exact (uleb_len_le_iff (width (peak es)) (ubytes w) hubpos).mpr hbound
  have hpack_bound : byteCount (xs.length * width (peak es)) ≤ byteCount (xs.length * w) := by
    apply byteCount_mono
    rw [← hes_len]
    exact Nat.mul_le_mul_left es.length hw_le
  change (frame es).length ≤ quote xs.length w
  dsimp [frame, quote]
  simp only [List.length_append]
  rw [huleb_len, hpacklen, hes_len]
  omega

/-! ## restart-seed helpers: the clamp predictor only reads lag 0, 1, 2 -/

private theorem lag_take_three (h : List Int) (j : Nat) (hj : j < 3) :
    lag (h.take 3) j = lag h j := by
  unfold lag
  have ht : (h.take 3)[j]? = h[j]? := by
    rw [List.getElem?_take]
    simp [hj]
  rw [ht]

private theorem predict_take_three (i : Nat) (h : List Int) (side : Nat → Int) :
    predict i (h.take 3) side = predict i h side := by
  unfold predict
  rw [lag_take_three h 0 (by omega)]
  rw [lag_take_three h 1 (by omega)]
  rw [lag_take_three h 2 (by omega)]

private theorem encode_restart_of_stable (p : Predict)
    (hp : ∀ (i : Nat) (h : List Int) (side : Nat → Int),
      p i (h.take 3) side = p i h side) :
    ∀ (side : Nat → Int) (i : Nat) (h xs : List Int),
      encode p side i (h.take 3) xs = encode p side i h xs := by
  intro side i h xs
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih =>
      simp only [encode]
      rw [hp i h side]
      have h1 := ih (i + 1) (x :: h.take 3)
      have h2 := ih (i + 1) (x :: h)
      have htake : (x :: h.take 3).take 3 = (x :: h).take 3 := by
        simp [List.take_take]
      rw [htake] at h1
      congr 1
      exact h1.symm.trans h2

/-! ## the four goals -/

theorem sound_ResidualRoundTrip : ResidualRoundTrip.Goal := by
  intro side i h xs
  exact encode_restore predict side i h xs

theorem sound_ByteBudget : ByteBudget.Goal := by
  intro side i h xs w extra oldBytes hcert hlt
  have hle := frame_length_le_quote predict side i h xs w hcert
  omega

theorem sound_RestartSeed : RestartSeed.Goal := by
  intro side i h xs
  exact encode_restart_of_stable predict predict_take_three side i h xs

theorem sound_BoundedResidual : BoundedResidual.Goal := by
  intro h side i x lo hi hxlo hxhi h1lo h1hi h2lo h2hi
  unfold predict
  let p : Int := max (min (lag h 1) (lag h 2))
    (min (max (lag h 1) (lag h 2)) (2 * lag h 0 - lag h 1))
  change (x - p).natAbs ≤ (hi - lo).toNat
  have hplo : lo ≤ p := by
    dsimp [p]
    have hmin : lo ≤ min (lag h 1) (lag h 2) := (Int.le_min).mpr ⟨h1lo, h2lo⟩
    exact Int.le_trans hmin (Int.le_max_left _ _)
  have hphi : p ≤ hi := by
    dsimp [p]
    have hminmax : min (lag h 1) (lag h 2) ≤ max (lag h 1) (lag h 2) :=
      Int.le_trans (Int.min_le_left _ _) (Int.le_max_left _ _)
    have hleft : min (lag h 1) (lag h 2) ≤ hi := Int.le_trans hminmax (Int.max_le.mpr ⟨h1hi, h2hi⟩)
    have hright : min (max (lag h 1) (lag h 2)) (2 * lag h 0 - lag h 1) ≤ hi :=
      Int.le_trans (Int.min_le_left _ _) (Int.max_le.mpr ⟨h1hi, h2hi⟩)
    exact Int.max_le.mpr ⟨hleft, hright⟩
  have hlohi : 0 ≤ hi - lo := by omega
  have hk : ((hi - lo).toNat : Int) = hi - lo := Int.toNat_of_nonneg hlohi
  by_cases hnonneg : 0 ≤ x - p
  · have hm : ((x - p).natAbs : Int) = x - p := Int.natAbs_of_nonneg hnonneg
    omega
  · have hnn : 0 ≤ -(x - p) := by omega
    have hm' : ((-(x - p)).natAbs : Int) = -(x - p) := Int.natAbs_of_nonneg hnn
    rw [Int.natAbs_neg] at hm'
    omega

end TinylambOpt.ResearchProofs.Compression.Predictors.ClampLinear

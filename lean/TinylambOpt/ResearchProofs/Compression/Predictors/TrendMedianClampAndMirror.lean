import TinylambOpt.ResearchGoals.Compression.Predictors

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved (16 claims, including the following)

- `ClampLinear.BoundedResidual` — `∀ h side i x lo hi, lo ≤ x → x ≤ hi → lo ≤ lag h 1 →
  lag h 1 ≤ hi → lo ≤ lag h 2 → lag h 2 ≤ hi → (x-predict i h side).natAbs ≤
  (hi-lo).toNat`
- `ClampLinear.ByteBudget` — `∀ side i h xs w extra oldBytes, certificate predict side
  i h xs w → extra + quote xs.length w < oldBytes → extra + (frame (encode predict
  side i h xs)).length < oldBytes`
- `ClampLinear.ResidualRoundTrip` — `∀ side i h xs, restore predict side i h (encode
  predict side i h xs) = xs`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Predictors.TrendMedianClampAndMirror

open TinylambOpt.ResearchGoals.Compression.Predictors
open TinylambOpt.ResearchGoals.Compression.Wire

/-! Proofs for the `TrendSeason`, `Median`, `ClampLinear` and `Mirror`
predictor goals. All sixteen goals in this file are true. The generic
helpers establish that `encode`/`restore` are inverses for any predictor,
that the wire frame never exceeds the certified quote, and that a truncated
restart seed suffices whenever the predictor reads a bounded prefix of the
history. -/

/-! ## shared codec helpers -/

private theorem unzig_zig (x : Int) : unzig (zig x) = x := by
  cases x with
  | ofNat n => simp [zig, unzig]
  | negSucc n =>
      have h : (2 * n + 1) % 2 = 1 := by omega
      simp [zig, unzig, h]
      omega

private theorem restore_encode (p : Predict) (side : Nat → Int) :
    ∀ i h xs, restore p side i h (encode p side i h xs) = xs := by
  intro i h xs
  induction xs generalizing i h with
  | nil => rfl
  | cons x xs ih =>
      change (p i h side + unzig (zig (x - p i h side))) ::
          restore p side (i + 1) ((p i h side + unzig (zig (x - p i h side))) :: h)
            (encode p side (i + 1) (x :: h) xs) = x :: xs
      rw [unzig_zig]
      have hy : p i h side + (x - p i h side) = x := by omega
      rw [hy]
      rw [ih]

private theorem encode_length (p : Predict) (side : Nat → Int) :
    ∀ i h xs, (encode p side i h xs).length = xs.length := by
  intro i h xs
  induction xs generalizing i h with
  | nil => rfl
  | cons x xs ih => simp [encode, ih]

/-! The j-th residual is exactly the term bounded by `certificate`. -/
private theorem encode_residual (p : Predict) (side : Nat → Int) :
    ∀ (i : Nat) (h xs : List Int) (j : Nat), j < xs.length →
      (encode p side i h xs)[j]?.getD 0 =
        zig (xs[j]?.getD 0 - p (i + j) ((xs.take j).reverse ++ h) side) := by
  intro i h xs
  induction xs generalizing i h with
  | nil => intro j hj; simp at hj
  | cons x xs ih =>
      intro j hj
      cases j with
      | zero => simp [encode]
      | succ j =>
          have hj' : j < xs.length := by simpa using hj
          have hrev : ((x :: xs).take (j + 1)).reverse = (xs.take j).reverse ++ [x] := by simp
          rw [encode]
          rw [List.getElem?_cons_succ]
          rw [ih (i + 1) (x :: h) j hj']
          rw [hrev]
          have hi : i + 1 + j = i + (j + 1) := by omega
          rw [hi]
          simp [List.append_assoc]

private theorem encode_lt_of_cert (p : Predict) (side : Nat → Int) (i : Nat)
    (h xs : List Int) (w : Nat) (hc : certificate p side i h xs w) :
    ∀ n ∈ encode p side i h xs, n < 2 ^ w := by
  intro n hn
  rw [List.mem_iff_getElem] at hn
  obtain ⟨j, hj, hnj⟩ := hn
  have hlen : (encode p side i h xs).length = xs.length := encode_length p side i h xs
  have hjxs : j < xs.length := by omega
  have hval := encode_residual p side i h xs j hjxs
  have hget : (encode p side i h xs)[j]?.getD 0 = (encode p side i h xs)[j] := by
    rw [List.getElem?_eq_getElem hj]
    simp
  rw [hget] at hval
  rw [hnj] at hval
  rw [hval]
  exact hc j hjxs

/-! ## byte accounting helpers -/

private theorem peak_le_of_mem_le (ns : List Nat) (k : Nat)
    (hmem : ∀ n ∈ ns, n ≤ k) : peak ns ≤ k := by
  unfold peak
  cases hm : ns.max? with
  | none => simp
  | some m =>
      have hprops := List.max?_eq_some_iff.mp hm
      have hmle : m ≤ k := hmem m hprops.1
      simpa [hm] using hmle

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

private theorem width_le_of_lt_pow (n w : Nat) (hn : n ≠ 0) (h : n < 2 ^ w) : width n ≤ w :=
  (width_le_iff n w hn).mpr h

private theorem width_mono {a b : Nat} (h : a ≤ b) : width a ≤ width b := by
  by_cases ha : a = 0
  · subst a
    simp [width]
  · have hb : b ≠ 0 := by omega
    rw [width_eq a ha, width_eq b hb]
    have hle : 2 ^ a.log2 ≤ a := (Nat.le_log2 ha).mp (Nat.le_refl _)
    have hle' : 2 ^ a.log2 ≤ b := Nat.le_trans hle h
    have hlog : a.log2 ≤ b.log2 := (Nat.le_log2 hb).mpr hle'
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
  | zero => simpa using uleb_len_le_one n
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

private theorem varint_charge (n : Nat) : (uleb n).length = ubytes n := by
  have hlenpos : 0 < (uleb n).length := uleb_len_pos n
  have hubpos : 0 < ubytes n := by unfold ubytes; omega
  apply Nat.le_antisymm
  · have hle : (uleb n).length ≤ ubytes n :=
      (uleb_len_le_iff n (ubytes n) hubpos).mpr
        ((ubytes_le_iff n (ubytes n) hubpos).mp (Nat.le_refl _))
    exact hle
  · exact (ubytes_le_iff n (uleb n).length hlenpos).mpr
      ((uleb_len_le_iff n (uleb n).length hlenpos).mp (Nat.le_refl _))

private theorem packed_charge (w : Nat) (xs : List Nat) :
    (pack w xs).length = byteCount (xs.length * w) := by
  simp [pack, little]

private theorem ubytes_mono {a b : Nat} (h : a ≤ b) : ubytes a ≤ ubytes b := by
  unfold ubytes
  apply Nat.max_le.mpr
  constructor
  · exact Nat.le_max_left 1 ((width b + 6) / 7)
  · exact Nat.le_trans (Nat.div_le_div_right (Nat.add_le_add_right (width_mono h) 6))
      (Nat.le_max_right 1 ((width b + 6) / 7))

private theorem frame_length (es : List Nat) : (frame es).length =
    1 + (uleb es.length).length + (uleb (width (peak es))).length +
      (pack (width (peak es)) es).length := by
  simp [frame]
  omega

private theorem frame_le_quote (es : List Nat) (w : Nat) (hw : width (peak es) ≤ w) :
    (frame es).length ≤ quote es.length w := by
  rw [frame_length, varint_charge, varint_charge, packed_charge]
  have hub := ubytes_mono hw
  have hbc : byteCount (es.length * width (peak es)) ≤ byteCount (es.length * w) := by
    unfold byteCount
    exact Nat.div_le_div_right (Nat.add_le_add_right (Nat.mul_le_mul_left es.length hw) 7)
  dsimp [quote]
  omega

private theorem byteBudget_of_cert (p : Predict) (side : Nat → Int) (i : Nat)
    (h xs : List Int) (w extra oldBytes : Nat)
    (hc : certificate p side i h xs w) (hb : extra + quote xs.length w < oldBytes) :
    extra + (frame (encode p side i h xs)).length < oldBytes := by
  let es := encode p side i h xs
  have hlen : es.length = xs.length := by
    dsimp [es]
    exact encode_length p side i h xs
  have hmem : ∀ n ∈ es, n < 2 ^ w := by
    dsimp [es]
    exact encode_lt_of_cert p side i h xs w hc
  have hpk : peak es ≤ 2 ^ w - 1 := by
    apply peak_le_of_mem_le
    intro n hn
    have hnlt := hmem n hn
    have hpowpos : 0 < 2 ^ w := Nat.pow_pos (by decide : 0 < 2)
    omega
  have hw' : width (peak es) ≤ w := by
    by_cases hp0 : peak es = 0
    · simp [width, hp0]
    · apply width_le_of_lt_pow
      · exact hp0
      · omega
  have hframe := frame_le_quote es w hw'
  have hqeq : quote es.length w = quote xs.length w := by
    dsimp [quote]
    rw [hlen]
  have hf : (frame es).length ≤ quote xs.length w := by
    rw [hqeq] at hframe
    exact hframe
  change extra + (frame es).length < oldBytes
  omega

/-! ## restart-seed helpers -/

/-! If two histories agree on their first `k` lags, the encoded residual
streams agree: the agreement is preserved when the newly restored value is
prepended to both histories. -/
private theorem encode_eq_of_lt_agree (p : Predict) (side : Nat → Int) (k i : Nat)
    (hpred : ∀ j h₁ h₂, (∀ n < k, lag h₁ n = lag h₂ n) → p j h₁ side = p j h₂ side)
    (h₁ h₂ : List Int) (hinit : ∀ n < k, lag h₁ n = lag h₂ n) :
    ∀ xs, encode p side i h₁ xs = encode p side i h₂ xs := by
  intro xs
  induction xs generalizing i h₁ h₂ with
  | nil => rfl
  | cons x xs ih =>
      simp only [encode]
      rw [hpred i h₁ h₂ hinit]
      have hnew : ∀ n < k, lag (x :: h₁) n = lag (x :: h₂) n := by
        intro n hn
        cases n with
        | zero => simp [lag]
        | succ n =>
            have hn' : n < k := by omega
            have hh := hinit n hn'
            simpa [lag] using hh
      rw [ih (i + 1) (x :: h₁) (x :: h₂) hnew]

private theorem restore_eq_of_lt_agree (p : Predict) (side : Nat → Int) (k i : Nat)
    (hpred : ∀ j h₁ h₂, (∀ n < k, lag h₁ n = lag h₂ n) → p j h₁ side = p j h₂ side)
    (h₁ h₂ : List Int) (hinit : ∀ n < k, lag h₁ n = lag h₂ n) :
    ∀ es, restore p side i h₁ es = restore p side i h₂ es := by
  intro es
  induction es generalizing i h₁ h₂ with
  | nil => rfl
  | cons e es ih =>
      have hp := hpred i h₁ h₂ hinit
      unfold restore
      simp only [hp]
      have hnew : ∀ n < k, lag ((p i h₂ side + unzig e) :: h₁) n =
          lag ((p i h₂ side + unzig e) :: h₂) n := by
        intro n hn
        cases n with
        | zero => simp [lag]
        | succ n =>
            have hn' : n < k := by omega
            have hh := hinit n hn'
            simpa [lag] using hh
      rw [ih (i + 1) _ _ hnew]

private theorem lag_take_eq (h : List Int) (k n : Nat) (hn : n < k) :
    lag (h.take k) n = lag h n := by
  simp [lag, hn]

private theorem lag_map_add (l : List Int) (n : Nat) (d : Int) (hn : n < l.length) :
    lag (l.map (fun x => x + d)) n = lag l n + d := by
  unfold lag
  rw [List.getElem?_map, List.getElem?_eq_getElem hn]
  simp

/-! ## TrendSeason -/

theorem TrendSeason_ResidualRoundTrip_sound :
    TinylambOpt.ResearchGoals.Compression.Predictors.TrendSeason.ResidualRoundTrip.Goal := by
  intro period side i h xs
  exact restore_encode (TrendSeason.predict period) side i h xs

theorem TrendSeason_ByteBudget_sound :
    TinylambOpt.ResearchGoals.Compression.Predictors.TrendSeason.ByteBudget.Goal := by
  intro period side i h xs w extra oldBytes hc hb
  exact byteBudget_of_cert (TrendSeason.predict period) side i h xs w extra oldBytes hc hb

theorem TrendSeason_RestartSeed_sound :
    TinylambOpt.ResearchGoals.Compression.Predictors.TrendSeason.RestartSeed.Goal := by
  intro period side i h xs
  apply encode_eq_of_lt_agree (TrendSeason.predict period) side (period + 2) i
  · intro j h₁ h₂ hag
    unfold TrendSeason.predict
    rw [hag 0 (by omega), hag period (by omega), hag (period + 1) (by omega)]
  · intro n hn
    exact lag_take_eq h (period + 2) n hn

theorem TrendSeason_TranslatedCycle_sound :
    TinylambOpt.ResearchGoals.Compression.Predictors.TrendSeason.TranslatedCycle.Goal := by
  intro period side i h xs delta hlen
  induction xs generalizing i h with
  | nil => rfl
  | cons x xs ih =>
      have hp : TrendSeason.predict period i (h.map (fun y => y + delta)) side =
          TrendSeason.predict period i h side + delta := by
        unfold TrendSeason.predict
        rw [lag_map_add h 0 delta (by omega),
            lag_map_add h period delta (by omega),
            lag_map_add h (period + 1) delta (by omega)]
        omega
      simp only [encode, List.map_cons]
      rw [hp]
      have hres : zig (x + delta - (TrendSeason.predict period i h side + delta)) =
          zig (x - TrendSeason.predict period i h side) := by
        congr 1
        omega
      rw [hres]
      have hlen' : period + 2 ≤ (x :: h).length := by simp; omega
      have hrec := ih (i + 1) (x :: h) hlen'
      simpa using hrec

/-! ## Median -/

private theorem median_repeat2 (a b : Int) : max (min a b) (min (max a b) a) = a := by
  by_cases h : a ≤ b
  · rw [Int.min_eq_left h, Int.max_eq_right h, Int.min_eq_right h, Int.max_eq_left (by omega)]
  · have hba : b ≤ a := by omega
    rw [Int.min_eq_right hba, Int.max_eq_left hba, Int.min_eq_left (by omega), Int.max_eq_right hba]

private theorem median_repeat2' (a b : Int) : max (min b a) (min (max b a) a) = a := by
  by_cases h : b ≤ a
  · rw [Int.min_eq_left h, Int.max_eq_right h, Int.min_eq_left (by omega), Int.max_eq_right h]
  · have hab : a ≤ b := by omega
    rw [Int.min_eq_right hab, Int.max_eq_left hab, Int.min_eq_right hab,
      Int.max_eq_left (by omega)]

theorem Median_ResidualRoundTrip_sound :
    TinylambOpt.ResearchGoals.Compression.Predictors.Median.ResidualRoundTrip.Goal := by
  intro side i h xs
  exact restore_encode Median.predict side i h xs

theorem Median_ByteBudget_sound :
    TinylambOpt.ResearchGoals.Compression.Predictors.Median.ByteBudget.Goal := by
  intro side i h xs w extra oldBytes hc hb
  exact byteBudget_of_cert Median.predict side i h xs w extra oldBytes hc hb

theorem Median_RestartSeed_sound :
    TinylambOpt.ResearchGoals.Compression.Predictors.Median.RestartSeed.Goal := by
  intro side i h xs
  apply encode_eq_of_lt_agree Median.predict side 3 i
  · intro j h₁ h₂ hag
    unfold Median.predict
    rw [hag 0 (by omega), hag 1 (by omega), hag 2 (by omega)]
  · intro n hn
    exact lag_take_eq h 3 n hn

theorem Median_OutlierRecovery_sound :
    TinylambOpt.ResearchGoals.Compression.Predictors.Median.OutlierRecovery.Goal := by
  intro a b side i
  constructor
  · simpa [Median.predict, lag] using median_repeat2 a b
  · simpa [Median.predict, lag] using median_repeat2' a b

/-! ## ClampLinear -/

private theorem clamp_bounds (l1 l2 e : Int) :
    min l1 l2 ≤ max (min l1 l2) (min (max l1 l2) e) ∧
    max (min l1 l2) (min (max l1 l2) e) ≤ max l1 l2 := by
  constructor
  · exact Int.le_max_left _ _
  · rw [Int.max_le]
    constructor <;> omega

theorem ClampLinear_ResidualRoundTrip_sound :
    TinylambOpt.ResearchGoals.Compression.Predictors.ClampLinear.ResidualRoundTrip.Goal := by
  intro side i h xs
  exact restore_encode ClampLinear.predict side i h xs

theorem ClampLinear_ByteBudget_sound :
    TinylambOpt.ResearchGoals.Compression.Predictors.ClampLinear.ByteBudget.Goal := by
  intro side i h xs w extra oldBytes hc hb
  exact byteBudget_of_cert ClampLinear.predict side i h xs w extra oldBytes hc hb

theorem ClampLinear_RestartSeed_sound :
    TinylambOpt.ResearchGoals.Compression.Predictors.ClampLinear.RestartSeed.Goal := by
  intro side i h xs
  apply encode_eq_of_lt_agree ClampLinear.predict side 3 i
  · intro j h₁ h₂ hag
    unfold ClampLinear.predict
    rw [hag 0 (by omega), hag 1 (by omega), hag 2 (by omega)]
  · intro n hn
    exact lag_take_eq h 3 n hn

theorem ClampLinear_BoundedResidual_sound :
    TinylambOpt.ResearchGoals.Compression.Predictors.ClampLinear.BoundedResidual.Goal := by
  intro h side i x lo hi hxlo hxhi hl1lo hl1hi hl2lo hl2hi
  let p := ClampLinear.predict i h side
  have hpmin := (clamp_bounds (lag h 1) (lag h 2) (2 * lag h 0 - lag h 1)).1
  have hpmax := (clamp_bounds (lag h 1) (lag h 2) (2 * lag h 0 - lag h 1)).2
  have hminlo : lo ≤ min (lag h 1) (lag h 2) := by
    rw [Int.le_min]
    exact ⟨hl1lo, hl2lo⟩
  have hmaxhi : max (lag h 1) (lag h 2) ≤ hi := by
    rw [Int.max_le]
    exact ⟨hl1hi, hl2hi⟩
  have hplo : lo ≤ p := by
    dsimp [p, ClampLinear.predict] at hpmin ⊢
    omega
  have hphi : p ≤ hi := by
    dsimp [p, ClampLinear.predict] at hpmax ⊢
    omega
  have hdiff_nonneg : 0 ≤ hi - lo := by omega
  change (x - p).natAbs ≤ (hi - lo).toNat
  rw [← Int.ofNat_le]
  rcases Int.natAbs_eq (x - p) with hnp | hnp
  · rw [← hnp, Int.toNat_of_nonneg hdiff_nonneg]
    omega
  · have hneg : -(x - p) = ((x - p).natAbs : Int) := by omega
    rw [← hneg, Int.toNat_of_nonneg hdiff_nonneg]
    omega

/-! ## Mirror -/

private theorem pow_alt_succ (j : Nat) (a : Int) :
    (-1 : Int) ^ (j + 1) * (-a) = (-1 : Int) ^ (j + 1 + 1) * a := by
  have hp : (-1 : Int) ^ (j + 1 + 1) = -((-1 : Int) ^ (j + 1)) := by
    rw [show j + 1 + 1 = (j + 1).succ by omega, Int.pow_succ]
    omega
  rw [hp]
  rw [Int.mul_neg, Int.neg_mul]

private theorem mirror_from_two (side : Nat → Int) (i : Nat) :
    ∀ n a, restore Mirror.predict side i ([a, -a] : List Int) (List.replicate n 0) =
      (List.range n).map (fun j => (-1 : Int) ^ (j + 1) * a) := by
  intro n
  induction n generalizing i with
  | zero => intro a; rfl
  | succ n ih =>
      intro a
      change (((Mirror.predict i ([a, -a] : List Int) side + unzig 0) : Int) ::
          restore Mirror.predict side (i + 1)
            ((Mirror.predict i ([a, -a] : List Int) side + unzig 0) :: ([a, -a] : List Int))
              (List.replicate n 0)) =
        (List.range (n + 1)).map (fun j => (-1 : Int) ^ (j + 1) * a)
      have hp : Mirror.predict i [a, -a] side = -a := by simp [Mirror.predict, lag]
      rw [hp]
      simp [unzig]
      have hinit : ∀ n' < 1, lag ([-a, a, -a] : List Int) n' = lag ([-a, a] : List Int) n' := by
        intro n' hn'
        have hn0 : n' = 0 := by omega
        subst n'
        simp [lag]
      have hsame := restore_eq_of_lt_agree Mirror.predict side 1 (i + 1)
        (fun j h₁ h₂ hag => by
          change -(lag h₁ 0) = -(lag h₂ 0)
          rw [hag 0 (by omega)])
        ([-a, a, -a] : List Int) ([-a, a] : List Int) hinit (List.replicate n 0)
      rw [hsame]
      have hrec : restore Mirror.predict side (i + 1) ([-a, a] : List Int)
          (List.replicate n 0) =
          (List.range n).map (fun j => (-1 : Int) ^ (j + 1) * (-a)) := by
        simpa using ih (i + 1) (-a)
      rw [hrec]
      rw [List.range_succ_eq_map, List.map_cons, List.map_map]
      simp
      intro j hj
      exact pow_alt_succ j a

private theorem mirror_from_head (side : Nat → Int) (i : Nat) :
    ∀ n v, restore Mirror.predict side i ([-v] : List Int) (List.replicate n 0) =
      (List.range n).map (fun j => (-1 : Int) ^ j * v) := by
  intro n
  induction n with
  | zero => intro v; rfl
  | succ n ih =>
      intro v
      change (((Mirror.predict i ([-v] : List Int) side + unzig 0) : Int) ::
          restore Mirror.predict side (i + 1)
            ((Mirror.predict i ([-v] : List Int) side + unzig 0) :: ([-v] : List Int))
              (List.replicate n 0)) =
        (List.range (n + 1)).map (fun j => (-1 : Int) ^ j * v)
      have hp : Mirror.predict i [-v] side = v := by simp [Mirror.predict, lag]
      rw [hp]
      simp [unzig]
      rw [mirror_from_two side (i + 1) n v]
      rw [List.range_succ_eq_map, List.map_cons, List.map_map]
      simp

theorem Mirror_ResidualRoundTrip_sound :
    TinylambOpt.ResearchGoals.Compression.Predictors.Mirror.ResidualRoundTrip.Goal := by
  intro side i h xs
  exact restore_encode Mirror.predict side i h xs

theorem Mirror_ByteBudget_sound :
    TinylambOpt.ResearchGoals.Compression.Predictors.Mirror.ByteBudget.Goal := by
  intro side i h xs w extra oldBytes hc hb
  exact byteBudget_of_cert Mirror.predict side i h xs w extra oldBytes hc hb

theorem Mirror_RestartSeed_sound :
    TinylambOpt.ResearchGoals.Compression.Predictors.Mirror.RestartSeed.Goal := by
  intro side i h xs
  apply encode_eq_of_lt_agree Mirror.predict side 1 i
  · intro j h₁ h₂ hag
    unfold Mirror.predict
    rw [hag 0 (by omega)]
  · intro n hn
    exact lag_take_eq h 1 n hn

theorem Mirror_AlternatingRun_sound :
    TinylambOpt.ResearchGoals.Compression.Predictors.Mirror.AlternatingRun.Goal := by
  intro v n side i
  exact mirror_from_head side i n v

end TinylambOpt.ResearchProofs.Compression.Predictors.TrendMedianClampAndMirror

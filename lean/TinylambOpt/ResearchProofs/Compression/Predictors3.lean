import TinylambOpt.ResearchGoals.Compression.Predictors

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Predictors3

open TinylambOpt.ResearchGoals.Compression.Predictors
open TinylambOpt.ResearchGoals.Compression.Wire

/-! # Predictor goals: `Ordinal`, `Bucket`, `ModuloCounter`, `SideAffine`

All sixteen goals in these four namespaces are true.  The residual codec
round-trips and the byte certificate bounds the frame for *any* predictor;
the restart seed is enough because the predictor ignores history (`Ordinal`,
`Bucket`, `SideAffine`) or reads only its head (`ModuloCounter`).  The fourth
goal of each predictor is proved directly from its definition.
-/

/-! ## small arithmetic helpers (kept local: proof modules may be stale) -/

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

private theorem uleb_len_le_ubytes (n : Nat) : (uleb n).length ≤ ubytes n := by
  have hpos : 0 < ubytes n := by unfold ubytes; omega
  exact (uleb_len_le_iff n (ubytes n) hpos).mpr
    ((ubytes_le_iff n (ubytes n) hpos).mp (Nat.le_refl _))

private theorem max?_some_of_ne_nil : ∀ {xs : List Nat}, xs ≠ [] → ∃ m, xs.max? = some m := by
  intro xs
  cases xs with
  | nil => simp
  | cons a as =>
      intro _
      rw [List.max?_cons]
      exact ⟨as.max?.elim a (max a), rfl⟩

private theorem width_peak_le (xs : List Nat) (w : Nat) (hb : ∀ e ∈ xs, e < 2 ^ w) :
    width (peak xs) ≤ w := by
  cases xs with
  | nil => simp [peak, width]
  | cons a as =>
      obtain ⟨m, hm⟩ := max?_some_of_ne_nil (by simp : (a :: as) ≠ [])
      have hmprops := List.max?_eq_some_iff.mp hm
      have hm_lt : m < 2 ^ w := hb m hmprops.1
      have hp : peak (a :: as) = m := by simp [peak, hm]
      rw [hp]
      by_cases hm0 : m = 0
      · simp [hm0, width]
      · exact (width_le_iff m w hm0).mpr hm_lt

private theorem log2_mono {a b : Nat} (h : a ≤ b) : a.log2 ≤ b.log2 := by
  by_cases ha : a = 0
  · subst a
    simp
  · have hb : b ≠ 0 := by omega
    by_cases hle : a.log2 ≤ b.log2
    · exact hle
    · exfalso
      have hlt' : b.log2 < a.log2 := by omega
      have hb_lt : b < 2 ^ a.log2 := (Nat.log2_lt hb).mp hlt'
      have ha_le : 2 ^ a.log2 ≤ a := (Nat.log2_eq_iff ha).mp rfl |>.1
      omega

private theorem width_mono {a b : Nat} (h : a ≤ b) : width a ≤ width b := by
  by_cases ha : a = 0
  · subst a
    simp [width]
  · simp [width, ha]
    by_cases hb : b = 0
    · subst b
      omega
    · simp [hb]
      have := log2_mono h
      omega

private theorem ubytes_mono {a b : Nat} (h : a ≤ b) : ubytes a ≤ ubytes b := by
  unfold ubytes
  apply Nat.max_le.mpr
  constructor
  · omega
  · have hw := width_mono h
    apply Nat.le_trans (Nat.div_le_div_right (by omega : width a + 6 ≤ width b + 6))
    exact Nat.le_max_right _ _

private theorem byteCount_mono {a b : Nat} (h : a ≤ b) : byteCount a ≤ byteCount b := by
  unfold byteCount
  exact Nat.div_le_div_right (by omega)

/-! ## codec facts -/

private theorem unzig_zig (z : Int) : unzig (zig z) = z := by
  cases z with
  | ofNat n => simp [zig, unzig]
  | negSucc n =>
      have h : (2 * n + 1) % 2 = 1 := by omega
      simp [zig, unzig, h]
      omega

private theorem restore_encode (p : Predict) : ∀ (side : Nat → Int) i h xs,
    restore p side i h (encode p side i h xs) = xs := by
  intro side i h xs
  induction xs generalizing i h with
  | nil => simp [encode, restore]
  | cons x xs ih =>
      simp only [encode, restore]
      rw [unzig_zig]
      have hv : p i h side + (x - p i h side) = x := by omega
      rw [hv]
      rw [ih]

private theorem encode_length (p : Predict) : ∀ (side : Nat → Int) i h xs,
    (encode p side i h xs).length = xs.length := by
  intro side i h xs
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih => simp [encode, ih]

private theorem encode_bounded (p : Predict) : ∀ (side : Nat → Int) i h xs w,
    certificate p side i h xs w → ∀ e ∈ encode p side i h xs, e < 2 ^ w := by
  intro side i h xs w hcert
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih =>
      have hhead : zig (x - p i h side) < 2 ^ w := by
        have h0 := hcert 0 (by simp)
        simpa using h0
      have htail : certificate p side (i + 1) (x :: h) xs w := by
        intro j hj
        have hj' : j + 1 < (x :: xs).length := by simpa using Nat.succ_lt_succ hj
        have hc := hcert (j + 1) hj'
        simpa [List.take_succ_cons, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hc
      have iht := ih (i + 1) (x :: h) htail
      intro e he
      have he' : e = zig (x - p i h side) ∨ e ∈ encode p side (i + 1) (x :: h) xs := by
        simpa [encode] using he
      rcases he' with hEq | hmem
      · rw [hEq]
        exact hhead
      · exact iht e hmem

private theorem frame_length_le (es : List Nat) :
    (frame es).length ≤ quote es.length (width (peak es)) := by
  dsimp [frame, quote]
  have h1 := uleb_len_le_ubytes es.length
  have h2 := uleb_len_le_ubytes (width (peak es))
  simp [pack, little]
  omega

private theorem quote_le (a b we w : Nat) (hlen : a = b) (hwe : we ≤ w) :
    quote a we ≤ quote b w := by
  unfold quote
  rw [hlen]
  have hub : ubytes we ≤ ubytes w := ubytes_mono hwe
  have hbc : byteCount (b * we) ≤ byteCount (b * w) :=
    byteCount_mono (Nat.mul_le_mul_left b hwe)
  omega

private theorem byte_budget (p : Predict) : ∀ (side : Nat → Int) i h xs w extra oldBytes,
    certificate p side i h xs w →
    extra + quote xs.length w < oldBytes →
    extra + (frame (encode p side i h xs)).length < oldBytes := by
  intro side i h xs w extra oldBytes hcert hlt
  let es := encode p side i h xs
  have hlen : es.length = xs.length := by dsimp [es]; exact encode_length p side i h xs
  have hb : ∀ e ∈ es, e < 2 ^ w := by dsimp [es]; exact encode_bounded p side i h xs w hcert
  have hwe : width (peak es) ≤ w := width_peak_le es w hb
  have hle : (frame es).length ≤ quote xs.length w :=
    Nat.le_trans (frame_length_le es) (quote_le es.length xs.length (width (peak es)) w hlen hwe)
  dsimp [es] at hle ⊢
  omega

private theorem encode_no_history (p : Predict)
    (hp : ∀ i h side, p i h side = p i [] side) :
    ∀ (side : Nat → Int) i h xs, encode p side i h xs = encode p side i [] xs := by
  intro side i h xs
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih => simp [encode, hp, ih]

private theorem restore_no_history (p : Predict)
    (hp : ∀ i h side, p i h side = p i [] side) :
    ∀ (side : Nat → Int) i h es, restore p side i h es = restore p side i [] es := by
  intro side i h es
  induction es generalizing i h with
  | nil => simp [restore]
  | cons e es ih =>
      simp only [restore]
      rw [hp i h side]
      congr 1
      calc
        restore p side (i + 1) ((p i [] side + unzig e) :: h) es =
            restore p side (i + 1) [] es := ih (i + 1) ((p i [] side + unzig e) :: h)
        _ = restore p side (i + 1) [p i [] side + unzig e] es :=
            (ih (i + 1) [p i [] side + unzig e]).symm

private theorem encode_lag_eq (p : Predict)
    (hp : ∀ i h₁ h₂ side, lag h₁ 0 = lag h₂ 0 → p i h₁ side = p i h₂ side) :
    ∀ (side : Nat → Int) i h₁ h₂ xs, lag h₁ 0 = lag h₂ 0 →
      encode p side i h₁ xs = encode p side i h₂ xs := by
  intro side i h₁ h₂ xs hlag
  induction xs generalizing i h₁ h₂ with
  | nil => simp [encode]
  | cons x xs ih =>
      simp only [encode]
      rw [hp i h₁ h₂ side hlag]
      congr 1
      exact ih (i + 1) (x :: h₁) (x :: h₂) (by simp [lag])

private theorem lag_take_one (h : List Int) : lag (h.take 1) 0 = lag h 0 := by
  cases h with
  | nil => simp [lag]
  | cons x xs => simp [lag]

private theorem map_range_succ {α : Type} (f : Nat → α) : ∀ n : Nat,
    (List.range (n + 1)).map f = f 0 :: (List.range n).map (fun j => f (j + 1)) := by
  intro n
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, ih, List.range_succ, List.map_append]
      simp

private theorem encode_zero_of_match (p : Predict)
    (hp : ∀ i h side, p i h side = p i [] side) :
    ∀ (side : Nat → Int) i xs,
      (∀ j < xs.length, xs[j]?.getD 0 = p (i + j) [] side) →
      encode p side i [] xs = List.replicate xs.length 0 := by
  intro side i xs hmatch
  induction xs generalizing i with
  | nil => simp [encode]
  | cons x xs ih =>
      have hx : x = p i [] side := by simpa using hmatch 0 (by simp)
      have htail : ∀ j < xs.length, xs[j]?.getD 0 = p ((i + 1) + j) [] side := by
        intro j hj
        have hc := hmatch (j + 1) (by simpa using Nat.succ_lt_succ hj)
        simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hc
      simp only [encode]
      rw [encode_no_history p hp side (i + 1) [x] xs]
      rw [hx]
      simp [zig, List.replicate_succ, ih (i + 1) htail]

/-! ## Ordinal -/

theorem Ordinal_ResidualRoundTrip_sound : Ordinal.ResidualRoundTrip.Goal := by
  intro base slope side i h xs
  exact restore_encode (Ordinal.predict base slope) side i h xs

theorem Ordinal_ByteBudget_sound : Ordinal.ByteBudget.Goal := by
  intro base slope side i h xs w extra oldBytes hcert hlt
  exact byte_budget (Ordinal.predict base slope) side i h xs w extra oldBytes hcert hlt

theorem Ordinal_RestartSeed_sound : Ordinal.RestartSeed.Goal := by
  intro base slope side i h xs
  let p := Ordinal.predict base slope
  have hp : ∀ i h side, p i h side = p i [] side := by
    intro i h side
    simp [p, Ordinal.predict]
  change encode p side i [] xs = encode p side i h xs
  exact (encode_no_history p hp side i h xs).symm

theorem Ordinal_AbsoluteSeek_sound : Ordinal.AbsoluteSeek.Goal := by
  intro base slope side i n
  let p := Ordinal.predict base slope
  have hp : ∀ i h side, p i h side = p i [] side := by
    intro i h side
    simp [p, Ordinal.predict]
  change encode p side i [] ((List.range n).map (fun (j : Nat) => base + slope * (i + j : Nat))) =
    List.replicate n 0
  have hres := encode_zero_of_match p hp side i
    ((List.range n).map (fun (j : Nat) => base + slope * (i + j : Nat))) (by
      intro j hj
      have hj' : j < n := by simpa using hj
      simp [List.getElem?_range hj', p, Ordinal.predict])
  simpa using hres

/-! ## Bucket -/

theorem Bucket_ResidualRoundTrip_sound : Bucket.ResidualRoundTrip.Goal := by
  intro base step period side i h xs
  exact restore_encode (Bucket.predict base step period) side i h xs

theorem Bucket_ByteBudget_sound : Bucket.ByteBudget.Goal := by
  intro base step period side i h xs w extra oldBytes hcert hlt
  exact byte_budget (Bucket.predict base step period) side i h xs w extra oldBytes hcert hlt

theorem Bucket_RestartSeed_sound : Bucket.RestartSeed.Goal := by
  intro base step period side i h xs
  let p := Bucket.predict base step period
  have hp : ∀ i h side, p i h side = p i [] side := by
    intro i h side
    simp [p, Bucket.predict]
  change encode p side i [] xs = encode p side i h xs
  exact (encode_no_history p hp side i h xs).symm

theorem Bucket_RunCrossing_sound : Bucket.RunCrossing.Goal := by
  intro base step period side i n
  let p := Bucket.predict base step period
  have hp : ∀ i h side, p i h side = p i [] side := by
    intro i h side
    simp [p, Bucket.predict]
  change restore p side i [] (List.replicate n 0) =
    (List.range n).map (fun (j : Nat) => base + step * ((i + j) / (period + 1) : Nat))
  induction n generalizing i with
  | zero => simp [restore]
  | succ n ih =>
      rw [map_range_succ]
      simp only [List.replicate_succ, restore]
      rw [restore_no_history p hp side (i + 1)]
      rw [ih]
      simp only [List.cons.injEq]
      constructor
      · simp [p, Bucket.predict, unzig]
      · apply List.map_congr_left
        intro j hj
        simp [Nat.add_comm, Nat.add_left_comm]

/-! ## ModuloCounter -/

theorem ModuloCounter_ResidualRoundTrip_sound : ModuloCounter.ResidualRoundTrip.Goal := by
  intro step period side i h xs
  exact restore_encode (ModuloCounter.predict step period) side i h xs

theorem ModuloCounter_ByteBudget_sound : ModuloCounter.ByteBudget.Goal := by
  intro step period side i h xs w extra oldBytes hcert hlt
  exact byte_budget (ModuloCounter.predict step period) side i h xs w extra oldBytes hcert hlt

theorem ModuloCounter_RestartSeed_sound : ModuloCounter.RestartSeed.Goal := by
  intro step period side i h xs
  let p := ModuloCounter.predict step period
  have hp : ∀ i h₁ h₂ side, lag h₁ 0 = lag h₂ 0 → p i h₁ side = p i h₂ side := by
    intro i h₁ h₂ side hlag
    simp [p, ModuloCounter.predict, hlag]
  change encode p side i (h.take 1) xs = encode p side i h xs
  exact encode_lag_eq p hp side i (h.take 1) h xs (lag_take_one h)

private theorem modulo_restore_from (step : Int) (period : Nat) (side : Nat → Int) :
    ∀ (i n : Nat) (h : List Int),
      restore (ModuloCounter.predict step period) side i h (List.replicate n 0) =
        (List.range n).map (fun (j : Nat) =>
          (lag h 0 + step * (j : Int) + step) % (period + 1 : Nat)) := by
  intro i n
  induction n generalizing i with
  | zero => simp [restore]
  | succ n ih =>
      intro h
      rw [map_range_succ]
      simp only [List.replicate_succ, restore]
      simp [unzig]
      rw [ih]
      constructor
      · simp [ModuloCounter.predict, lag]
      · apply List.map_congr_left
        intro j hj
        simp [lag, ModuloCounter.predict]
        have hassoc :
            (h[0]?.getD 0 + step) % ((period : Int) + 1) + step * (j : Int) + step =
              (h[0]?.getD 0 + step) % ((period : Int) + 1) + (step * (j : Int) + step) := by
          omega
        rw [hassoc]
        rw [Int.emod_add_emod]
        congr 1
        rw [Int.mul_add, Int.mul_one]
        omega

theorem ModuloCounter_WrappedProgression_sound : ModuloCounter.WrappedProgression.Goal := by
  intro step period a side i n
  rw [modulo_restore_from]
  apply List.map_congr_left
  intro j hj
  simp [lag]
  have hnum : a - step + step * (j : Int) + step = a + step * (j : Int) := by omega
  rw [hnum]

/-! ## SideAffine -/

theorem SideAffine_ResidualRoundTrip_sound : SideAffine.ResidualRoundTrip.Goal := by
  intro base slope side i h xs
  exact restore_encode (SideAffine.predict base slope) side i h xs

theorem SideAffine_ByteBudget_sound : SideAffine.ByteBudget.Goal := by
  intro base slope side i h xs w extra oldBytes hcert hlt
  exact byte_budget (SideAffine.predict base slope) side i h xs w extra oldBytes hcert hlt

theorem SideAffine_RestartSeed_sound : SideAffine.RestartSeed.Goal := by
  intro base slope side i h xs
  let p := SideAffine.predict base slope
  have hp : ∀ i h side, p i h side = p i [] side := by
    intro i h side
    simp [p, SideAffine.predict]
  change encode p side i [] xs = encode p side i h xs
  exact (encode_no_history p hp side i h xs).symm

theorem SideAffine_ProportionalColumns_sound : SideAffine.ProportionalColumns.Goal := by
  intro base slope side i n
  let p := SideAffine.predict base slope
  have hp : ∀ i h side, p i h side = p i [] side := by
    intro i h side
    simp [p, SideAffine.predict]
  change encode p side i [] ((List.range n).map (fun (j : Nat) => base + slope * side (i + j))) =
    List.replicate n 0
  have hres := encode_zero_of_match p hp side i
    ((List.range n).map (fun (j : Nat) => base + slope * side (i + j))) (by
      intro j hj
      have hj' : j < n := by simpa using hj
      simp [List.getElem?_range hj', p, SideAffine.predict])
  simpa using hres

end TinylambOpt.ResearchProofs.Compression.Predictors3

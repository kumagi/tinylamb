import TinylambOpt.ResearchGoals.Compression.Predictors

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Predictors.TrendSeason

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Compression.Predictors
open TinylambOpt.ResearchGoals.Compression.Predictors.TrendSeason

/-! ## shared wire helpers -/

private theorem unzig_zig (x : Int) : unzig (zig x) = x := by
  cases x with
  | ofNat n =>
      simp [zig, unzig]
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

private theorem width_le_iff (n W : Nat) (hn : n ≠ 0) : width n ≤ W ↔ n < 2 ^ W := by
  rw [width_eq n hn]
  constructor
  · intro h
    exact (Nat.log2_lt hn).mp (by omega)
  · intro h
    have hh := (Nat.log2_lt hn).mpr h
    omega

private theorem width_mono {a b : Nat} (h : a ≤ b) : width a ≤ width b := by
  by_cases ha : a = 0
  · subst a
    simp [width]
  · have hb : b ≠ 0 := by omega
    exact (width_le_iff a (width b) ha).mpr
      (Nat.lt_of_le_of_lt h (lt_two_pow_width b))

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
  have hubpos : 0 < ubytes n := by unfold ubytes; omega
  exact (uleb_len_le_iff n (ubytes n) hubpos).mpr
    ((ubytes_le_iff n (ubytes n) hubpos).mp (Nat.le_refl _))

private theorem ubytes_mono {a b : Nat} (h : a ≤ b) : ubytes a ≤ ubytes b := by
  unfold ubytes
  have hw : width a ≤ width b := width_mono h
  exact Nat.max_le.mpr
    ⟨Nat.le_max_left _ _,
      Nat.le_trans (Nat.div_le_div_right (Nat.add_le_add_right hw 6)) (Nat.le_max_right _ _)⟩

private theorem max?_some_of_ne_nil : ∀ {xs : List Nat}, xs ≠ [] → ∃ m, xs.max? = some m := by
  intro xs
  cases xs with
  | nil => simp
  | cons a as =>
      intro _
      rw [List.max?_cons]
      exact ⟨as.max?.elim a (max a), rfl⟩

private theorem peak_lt_of_all_lt {es : List Nat} {b : Nat} (hb : 0 < b)
    (h : ∀ e ∈ es, e < b) : peak es < b := by
  cases es with
  | nil => simpa [peak] using hb
  | cons e es =>
      have hne : (e :: es) ≠ [] := by simp
      obtain ⟨m, hm⟩ := max?_some_of_ne_nil (xs := e :: es) hne
      have hmprops := List.max?_eq_some_iff.mp hm
      have hmb : m < b := h m hmprops.1
      have hp : peak (e :: es) = m := by simp [peak, hm]
      rw [hp]
      exact hmb

private theorem width_le_of_peak_lt {es : List Nat} {w : Nat} (h : peak es < 2 ^ w) :
    width (peak es) ≤ w := by
  by_cases h0 : peak es = 0
  · simp [h0, width]
  · exact (width_le_iff (peak es) w h0).mpr h

private theorem pack_length (w : Nat) (es : List Nat) :
    (pack w es).length = byteCount (es.length * w) := by
  simp [pack, little]

private theorem frame_length (es : List Nat) :
    (frame es).length =
      1 + (uleb es.length).length + (uleb (width (peak es))).length +
        byteCount (es.length * width (peak es)) := by
  rw [frame]
  simp only [List.length_append, List.length_cons, List.length_nil, Nat.zero_add, pack_length]

private theorem quote_bound (n W w : Nat) (hW : W ≤ w) :
    1 + (uleb n).length + (uleb W).length + byteCount (n * W) ≤ quote n w := by
  have h1 := uleb_len_le_ubytes n
  have h2 := uleb_len_le_ubytes W
  have h3 := ubytes_mono hW
  have h4 : byteCount (n * W) ≤ byteCount (n * w) := by
    unfold byteCount
    exact Nat.div_le_div_right (Nat.add_le_add_right (Nat.mul_le_mul_left n hW) 7)
  dsimp [quote]
  omega

/-! ## encode / lag helpers -/

private theorem encode_profile (p : Predict) (side : Nat → Int) (i : Nat) (h xs : List Int) (w : Nat)
    (hcert : certificate p side i h xs w) :
    (encode p side i h xs).length = xs.length ∧
      ∀ e ∈ encode p side i h xs, e < 2 ^ w := by
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih =>
      have hhead : zig (x - p i h side) < 2 ^ w := by
        have hc := hcert 0 (by simp)
        simpa [certificate] using hc
      have htailcert : certificate p side (i + 1) (x :: h) xs w := by
        intro j hj
        have hc := hcert (j + 1) (by simpa using Nat.succ_lt_succ hj)
        simpa [certificate, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hc
      have ht := ih (i + 1) (x :: h) htailcert
      simp only [encode, List.length_cons, List.mem_cons]
      constructor
      · rw [ht.1]
      · intro e he
        rcases he with heq | he
        · rw [heq]
          exact hhead
        · exact ht.2 e he

private theorem lag_append (h t : List Int) (j : Nat) (hj : j < h.length) :
    lag (h ++ t) j = lag h j := by
  unfold lag
  rw [List.getElem?_append]
  simp [hj]

private theorem trend_restart_aux (period : Nat) (side : Nat → Int) (i : Nat)
    (h1 h2 : List Int) (ht : ∃ t, h2 = h1 ++ t) (hlen : period + 2 ≤ h1.length)
    (xs : List Int) :
    encode (predict period) side i h1 xs = encode (predict period) side i h2 xs := by
  induction xs generalizing i h1 h2 with
  | nil => rfl
  | cons x xs ih =>
      rcases ht with ⟨t, rfl⟩
      have h0 : lag (h1 ++ t) 0 = lag h1 0 := lag_append h1 t 0 (by omega)
      have hp : lag (h1 ++ t) period = lag h1 period :=
        lag_append h1 t period (by omega)
      have hq : lag (h1 ++ t) (period + 1) = lag h1 (period + 1) :=
        lag_append h1 t (period + 1) (by omega)
      have hpred : predict period i (h1 ++ t) side = predict period i h1 side := by
        unfold predict
        rw [h0, hp, hq]
      have hlen' : period + 2 ≤ (x :: h1).length := by
        simp only [List.length_cons]
        omega
      simp only [encode]
      rw [hpred]
      rw [ih (i + 1) (x :: h1) (x :: (h1 ++ t)) ⟨t, rfl⟩ hlen']

private theorem lag_map_add (h : List Int) (delta : Int) (j : Nat) (hj : j < h.length) :
    lag (h.map (fun y => y + delta)) j = lag h j + delta := by
  unfold lag
  rw [List.getElem?_map]
  simp [List.getElem?_eq_getElem hj]

/-! ## goals -/

theorem sound_ResidualRoundTrip :
    TinylambOpt.ResearchGoals.Compression.Predictors.TrendSeason.ResidualRoundTrip.Goal := by
  intro period side i h xs
  induction xs generalizing i h with
  | nil => rfl
  | cons x xs ih =>
      simp only [encode, restore]
      rw [unzig_zig]
      have hx : predict period i h side + (x - predict period i h side) = x := by omega
      rw [hx, ih]

theorem sound_ByteBudget :
    TinylambOpt.ResearchGoals.Compression.Predictors.TrendSeason.ByteBudget.Goal := by
  intro period side i h xs w extra oldBytes hcert hquote
  let es := encode (predict period) side i h xs
  have hp := encode_profile (predict period) side i h xs w hcert
  have hlen : es.length = xs.length := by simpa [es] using hp.1
  have hmem : ∀ e ∈ es, e < 2 ^ w := by simpa [es] using hp.2
  have hpeak : peak es < 2 ^ w := peak_lt_of_all_lt (Nat.two_pow_pos w) hmem
  have hW : width (peak es) ≤ w := width_le_of_peak_lt hpeak
  have hframe : (frame es).length ≤ quote xs.length w := by
    rw [frame_length]
    rw [← hlen]
    exact quote_bound es.length (width (peak es)) w hW
  change extra + (frame es).length < oldBytes
  omega

theorem sound_RestartSeed :
    TinylambOpt.ResearchGoals.Compression.Predictors.TrendSeason.RestartSeed.Goal := by
  intro period side i h xs
  by_cases hlen : period + 2 ≤ h.length
  · have htake_len : (h.take (period + 2)).length = period + 2 := by
      rw [List.length_take, Nat.min_eq_left hlen]
    have haux := trend_restart_aux period side i (h.take (period + 2)) (h.take (period + 2) ++ h.drop (period + 2))
      ⟨h.drop (period + 2), rfl⟩ (by rw [htake_len]; omega) xs
    simpa [List.take_append_drop] using haux
  · have hle : h.length ≤ period + 2 := by omega
    rw [List.take_of_length_le hle]

theorem sound_TranslatedCycle :
    TinylambOpt.ResearchGoals.Compression.Predictors.TrendSeason.TranslatedCycle.Goal := by
  intro period side i h xs delta hlen
  revert i h
  induction xs with
  | nil =>
      intro i h hlen
      rfl
  | cons x xs ih =>
      intro i h hlen
      have h0 : lag (h.map (fun y => y + delta)) 0 = lag h 0 + delta :=
        lag_map_add h delta 0 (by omega)
      have hp : lag (h.map (fun y => y + delta)) period = lag h period + delta :=
        lag_map_add h delta period (by omega)
      have hq : lag (h.map (fun y => y + delta)) (period + 1) = lag h (period + 1) + delta :=
        lag_map_add h delta (period + 1) (by omega)
      have hpred : predict period i (h.map (fun y => y + delta)) side =
          predict period i h side + delta := by
        unfold predict
        rw [h0, hp, hq]
        omega
      have hres : (x + delta) - predict period i (h.map (fun y => y + delta)) side =
          x - predict period i h side := by omega
      have hmap : (x :: h).map (fun y => y + delta) = (x + delta) :: h.map (fun y => y + delta) := by
        simp
      have hmapxs : (x :: xs).map (fun y => y + delta) =
          (x + delta) :: xs.map (fun y => y + delta) := by
        simp
      have hlen' : period + 2 ≤ (x :: h).length := by
        simp only [List.length_cons]
        omega
      rw [hmapxs]
      simp only [encode]
      rw [hres, ← hmap]
      rw [ih (i + 1) (x :: h) hlen']

end TinylambOpt.ResearchProofs.Compression.Predictors.TrendSeason

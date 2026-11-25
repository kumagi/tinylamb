import TinylambOpt.ResearchGoals.Compression.Predictors

namespace TinylambOpt.ResearchProofs.Compression.Predictors.Quadratic

open TinylambOpt.ResearchGoals.Compression.Predictors
open TinylambOpt.ResearchGoals.Compression.Predictors.Quadratic
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0

/-! ## Shared wire / codec helpers (self-contained copies) -/

private theorem unzig_zig (x : Int) : unzig (zig x) = x := by
  cases x with
  | ofNat n =>
      simp [zig, unzig]
  | negSucc n =>
      have h : (2 * n + 1) % 2 = 1 := by omega
      simp [zig, unzig, h]
      omega

private theorem encode_length (p : Predict) (side : Nat → Int) (i : Nat) (h : List Int)
    (xs : List Int) : (encode p side i h xs).length = xs.length := by
  induction xs generalizing i h with
  | nil => rfl
  | cons x xs ih => simp [encode, ih]

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

private theorem width_mono {n m : Nat} (h : n ≤ m) : width n ≤ width m := by
  by_cases hn : n = 0
  · subst n
    simp [width]
  · have hm : m ≠ 0 := by omega
    rw [width_eq n hn, width_eq m hm]
    have hlt : n < 2 ^ (m.log2 + 1) :=
      Nat.lt_of_le_of_lt h Nat.lt_log2_self
    have hlog : n.log2 < m.log2 + 1 := by exact (Nat.log2_lt hn).mpr hlt
    omega

private theorem ubytes_mono {n m : Nat} (h : n ≤ m) : ubytes n ≤ ubytes m := by
  have hw : width n ≤ width m := width_mono h
  unfold ubytes
  apply Nat.max_le.mpr
  constructor
  · exact Nat.le_max_left _ _
  · exact Nat.le_trans (Nat.div_le_div_right (by omega)) (Nat.le_max_right _ _)

private theorem byteCount_mono {a b : Nat} (h : a ≤ b) : byteCount a ≤ byteCount b := by
  unfold byteCount
  exact Nat.div_le_div_right (by omega)

private theorem pack_length (w : Nat) (xs : List Nat) :
    (pack w xs).length = byteCount (xs.length * w) := by
  simp [pack, little]

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

private theorem uleb_len_bound (n : Nat) : n < 128 ^ (uleb n).length := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      by_cases hn : n < 128
      · simp [uleb, hn]
      · rw [uleb]
        simp only [hn, ite_false, List.length_cons]
        have hlt : n / 128 < n := Nat.div_lt_self (by omega) (by decide)
        have ih' := ih (n / 128) hlt
        have hstep : n / 128 + 1 ≤ 128 ^ (uleb (n / 128)).length := Nat.succ_le_of_lt ih'
        have hmul := Nat.mul_le_mul_left 128 hstep
        rw [Nat.mul_add] at hmul
        simp only [Nat.mul_one] at hmul
        have hn' : n < 128 * (n / 128 + 1) := by
          have hm := Nat.mod_add_div n 128
          omega
        rw [Nat.pow_succ]
        omega

private theorem uleb_length (n : Nat) : (uleb n).length = ubytes n := by
  have hub : 0 < ubytes n := by unfold ubytes; omega
  apply Nat.le_antisymm
  · exact (uleb_len_le_iff n (ubytes n) hub).mpr
      ((ubytes_le_iff n (ubytes n) hub).mp (Nat.le_refl _))
  · have hpos : 0 < (uleb n).length := uleb_len_pos n
    exact (ubytes_le_iff n (uleb n).length hpos).mpr (uleb_len_bound n)

/-! ## ResidualRoundTrip -/

theorem sound_ResidualRoundTrip : ResidualRoundTrip.Goal := by
  intro side i h xs
  induction xs generalizing i h with
  | nil => rfl
  | cons x xs ih =>
      simp only [encode, restore]
      have hx : predict i h side + unzig (zig (x - predict i h side)) = x := by
        rw [unzig_zig]
        omega
      rw [hx]
      exact congrArg (List.cons x) (ih (i + 1) (x :: h))

/-! ## ByteBudget -/

private theorem encode_cert_mem (side : Nat → Int) (i : Nat) (h : List Int) (xs : List Int)
    (w : Nat) (hc : certificate predict side i h xs w) :
    ∀ e ∈ encode predict side i h xs, e < 2 ^ w := by
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih =>
      intro e he
      simp only [encode, List.mem_cons] at he
      rcases he with rfl | he
      · have h0 := hc 0 (by simp)
        simpa using h0
      · apply ih (i + 1) (x :: h)
        · intro j hj
          have hc' := hc (j + 1) (by simp; omega)
          simpa [List.take_succ_cons, List.append_assoc, Nat.add_assoc, Nat.add_comm,
            Nat.add_left_comm] using hc'
        · exact he

private theorem width_encode_le (side : Nat → Int) (i : Nat) (h : List Int) (xs : List Int)
    (w : Nat) (hc : certificate predict side i h xs w) :
    width (peak (encode predict side i h xs)) ≤ w := by
  let es := encode predict side i h xs
  have hall : ∀ e ∈ es, e < 2 ^ w := by
    dsimp [es]
    exact encode_cert_mem side i h xs w hc
  by_cases hzero : peak es = 0
  · rw [hzero]
    simp [width]
  · rw [width_eq (peak es) hzero]
    have hlt : peak es < 2 ^ w := by
      cases hmax : es.max? with
      | none =>
          simp [peak, hmax] at hzero
      | some m =>
          have hmem : m ∈ es := List.max?_mem hmax
          have hpeak : peak es = m := by simp [peak, hmax]
          rw [hpeak]
          exact hall m hmem
    have hlog : (peak es).log2 < w := by exact (Nat.log2_lt hzero).mpr hlt
    omega

private theorem frame_length_le_quote (side : Nat → Int) (i : Nat) (h : List Int) (xs : List Int)
    (w : Nat) (hc : certificate predict side i h xs w) :
    (frame (encode predict side i h xs)).length ≤ quote xs.length w := by
  let es := encode predict side i h xs
  let w' := width (peak es)
  have hw' : w' ≤ w := by
    dsimp [w', es]
    exact width_encode_le side i h xs w hc
  have hes : es.length = xs.length := by
    dsimp [es]
    exact encode_length predict side i h xs
  have hub : ubytes w' ≤ ubytes w := ubytes_mono hw'
  have hbc : byteCount (es.length * w') ≤ byteCount (xs.length * w) := by
    apply byteCount_mono
    rw [hes]
    exact Nat.mul_le_mul_left xs.length hw'
  change (frame es).length ≤ quote xs.length w
  dsimp [frame, quote]
  simp only [List.length_append]
  rw [uleb_length es.length, uleb_length w', pack_length]
  dsimp [w'] at hbc hub hw' ⊢
  rw [hes] at hbc ⊢
  omega

theorem sound_ByteBudget : ByteBudget.Goal := by
  intro side i h xs w extra oldBytes hc hlt
  have hframe : (frame (encode predict side i h xs)).length ≤ quote xs.length w :=
    frame_length_le_quote side i h xs w hc
  omega

/-! ## RestartSeed -/

private theorem lag_take_three (h : List Int) (k : Nat) (hk : k < 3) :
    lag (h.take 3) k = lag h k := by
  have hk' : k = 0 ∨ k = 1 ∨ k = 2 := by omega
  rcases hk' with rfl | rfl | rfl <;>
    cases h with
    | nil => simp [lag]
    | cons a as =>
        cases as with
        | nil => simp [lag]
        | cons b bs =>
            cases bs with
            | nil => simp [lag]
            | cons c cs => simp [lag]

private theorem predict_eq_of_lag3 {h₁ h₂ : List Int} (i : Nat) (side : Nat → Int)
    (h0 : lag h₁ 0 = lag h₂ 0) (h1 : lag h₁ 1 = lag h₂ 1) (h2 : lag h₁ 2 = lag h₂ 2) :
    predict i h₁ side = predict i h₂ side := by
  simp [predict, h0, h1, h2]

private theorem encode_eq_of_lag3 (side : Nat → Int) :
    ∀ (i : Nat) (h₁ h₂ : List Int) (xs : List Int),
      lag h₁ 0 = lag h₂ 0 → lag h₁ 1 = lag h₂ 1 → lag h₁ 2 = lag h₂ 2 →
      encode predict side i h₁ xs = encode predict side i h₂ xs := by
  intro i h₁ h₂ xs
  induction xs generalizing i h₁ h₂ with
  | nil => simp [encode]
  | cons x xs ih =>
      intro hl0 hl1 hl2
      simp only [encode]
      have hp : predict i h₁ side = predict i h₂ side :=
        predict_eq_of_lag3 i side hl0 hl1 hl2
      rw [hp]
      apply congrArg (List.cons (zig (x - predict i h₂ side)))
      apply ih (i + 1) (x :: h₁) (x :: h₂)
      · simp [lag]
      · simpa [lag] using hl0
      · simpa [lag] using hl1

theorem sound_RestartSeed : RestartSeed.Goal := by
  intro side i h xs
  apply encode_eq_of_lag3 side i (h.take 3) h xs
  · exact lag_take_three h 0 (by omega)
  · exact lag_take_three h 1 (by omega)
  · exact lag_take_three h 2 (by omega)

private theorem restore_eq_of_lag3 (side : Nat → Int) :
    ∀ (i : Nat) (h₁ h₂ : List Int) (es : List Nat),
      lag h₁ 0 = lag h₂ 0 → lag h₁ 1 = lag h₂ 1 → lag h₁ 2 = lag h₂ 2 →
      restore predict side i h₁ es = restore predict side i h₂ es := by
  intro i h₁ h₂ es
  induction es generalizing i h₁ h₂ with
  | nil => simp [restore]
  | cons e es ih =>
      intro hl0 hl1 hl2
      simp only [restore]
      have hp : predict i h₁ side = predict i h₂ side :=
        predict_eq_of_lag3 i side hl0 hl1 hl2
      rw [hp]
      apply congrArg (List.cons (predict i h₂ side + unzig e))
      apply ih (i + 1) ((predict i h₂ side + unzig e) :: h₁)
        ((predict i h₂ side + unzig e) :: h₂)
      · simp [lag]
      · simpa [lag] using hl0
      · simpa [lag] using hl1

/-! ## PolynomialRun -/

private theorem range_succ_cons (n : Nat) :
    List.range (n + 1) = 0 :: (List.range n).map (fun k => k + 1) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, ih]
      simp only [List.cons_append]
      congr 1
      rw [← ih]
      rw [List.range_succ]
      simp only [List.map_append, List.map_cons, List.map_nil]

private theorem poly_run_aux (a b c : Int) (side : Nat → Int) :
    ∀ (n step idx : Nat),
      restore predict side idx
          [a * ((step : Int) - 1)^2 + b * ((step : Int) - 1) + c,
           a * ((step : Int) - 2)^2 + b * ((step : Int) - 2) + c,
           a * ((step : Int) - 3)^2 + b * ((step : Int) - 3) + c]
          (List.replicate n 0) =
        (List.range n).map
          (fun (k : Nat) => a * ((step : Int) + (k : Int))^2 + b * ((step : Int) + (k : Int)) + c) := by
  intro n
  induction n with
  | zero =>
      intro step idx
      simp [restore]
  | succ n ih =>
      intro step idx
      simp only [List.replicate_succ, restore]
      have hpred :
          predict idx
            [a * ((step : Int) - 1)^2 + b * ((step : Int) - 1) + c,
             a * ((step : Int) - 2)^2 + b * ((step : Int) - 2) + c,
             a * ((step : Int) - 3)^2 + b * ((step : Int) - 3) + c] side =
          a * (step : Int)^2 + b * (step : Int) + c := by
        simp [predict, lag]
        grind
      rw [hpred]
      have hz : unzig 0 = 0 := by simp [unzig]
      rw [hz]
      simp only [Int.add_zero]
      have hstep1 : ((step + 1 : Nat) : Int) = (step : Int) + 1 := by omega
      have hnext :
          [a * (step : Int)^2 + b * (step : Int) + c,
           a * ((step : Int) - 1)^2 + b * ((step : Int) - 1) + c,
           a * ((step : Int) - 2)^2 + b * ((step : Int) - 2) + c] =
          [a * (((step + 1 : Nat) : Int) - 1)^2 + b * (((step + 1 : Nat) : Int) - 1) + c,
           a * (((step + 1 : Nat) : Int) - 2)^2 + b * (((step + 1 : Nat) : Int) - 2) + c,
           a * (((step + 1 : Nat) : Int) - 3)^2 + b * (((step + 1 : Nat) : Int) - 3) + c] := by
        rw [hstep1]
        have e1 : (step : Int) + 1 - 1 = (step : Int) := by omega
        have e2 : (step : Int) + 1 - 2 = (step : Int) - 1 := by omega
        have e3 : (step : Int) + 1 - 3 = (step : Int) - 2 := by omega
        rw [e1, e2, e3]
      -- restore pushes the new value on the history, so drop the stale fourth
      -- entry (the predictor only reads the newest three).
      have hdrop :
          restore predict side (idx + 1)
            [a * (step : Int)^2 + b * (step : Int) + c,
             a * ((step : Int) - 1)^2 + b * ((step : Int) - 1) + c,
             a * ((step : Int) - 2)^2 + b * ((step : Int) - 2) + c,
             a * ((step : Int) - 3)^2 + b * ((step : Int) - 3) + c]
            (List.replicate n 0) =
          restore predict side (idx + 1)
            [a * (((step + 1 : Nat) : Int) - 1)^2 + b * (((step + 1 : Nat) : Int) - 1) + c,
             a * (((step + 1 : Nat) : Int) - 2)^2 + b * (((step + 1 : Nat) : Int) - 2) + c,
             a * (((step + 1 : Nat) : Int) - 3)^2 + b * (((step + 1 : Nat) : Int) - 3) + c]
            (List.replicate n 0) := by
        rw [← hnext]
        apply restore_eq_of_lag3
        · simp [lag]
        · simp [lag]
        · simp [lag]
      rw [hdrop]
      rw [ih (step + 1) (idx + 1)]
      rw [range_succ_cons]
      simp only [List.map_cons, List.map_map]
      apply congrArg (List.cons (a * (step : Int)^2 + b * (step : Int) + c))
      apply List.map_congr_left
      intro k hk
      have hk1 : ((k + 1 : Nat) : Int) = (k : Int) + 1 := by omega
      change a * (((step + 1 : Nat) : Int) + (k : Int))^2 + b * (((step + 1 : Nat) : Int) + (k : Int)) + c =
        a * ((step : Int) + ((k + 1 : Nat) : Int))^2 + b * ((step : Int) + ((k + 1 : Nat) : Int)) + c
      rw [hstep1, hk1]
      have hidx : (step : Int) + 1 + (k : Int) = (step : Int) + ((k : Int) + 1) := by omega
      rw [hidx]

theorem sound_PolynomialRun : PolynomialRun.Goal := by
  intro a b c n side i
  dsimp
  have h := poly_run_aux a b c side n 0 i
  simpa using h

end TinylambOpt.ResearchProofs.Compression.Predictors.Quadratic

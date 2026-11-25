import TinylambOpt.ResearchGoals.Compression.Predictors

namespace TinylambOpt.ResearchProofs.Compression.Predictors.Linear

open TinylambOpt.ResearchGoals.Compression.Predictors
open TinylambOpt.ResearchGoals.Compression.Predictors.Linear
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0

/-! ## Wire helpers (reproduced locally; no dependency on another proof module) -/

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
    calc
      n < 2 ^ width n := lt_two_pow_width n
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
          calc
            n < 128 := hn
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
          calc
            n < 128 ^ (k + 1) * 128 := hdiv
            _ = 128 ^ (k + 1 + 1) := (Nat.pow_succ 128 (k + 1)).symm
        · intro h
          have hdiv : n / 128 < 128 ^ (k + 1) := by
            rw [Nat.div_lt_iff_lt_mul (by decide : 0 < 128)]
            calc
              n < 128 ^ (k + 1 + 1) := h
              _ = 128 ^ (k + 1) * 128 := by rw [Nat.pow_succ]
          have hd := hih.mpr hdiv
          omega

private theorem uleb_len_le_iff (n k : Nat) (hk : 0 < k) :
    (uleb n).length ≤ k ↔ n < 128 ^ k := by
  obtain ⟨j, rfl⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hk)
  exact uleb_len_le_succ n j

private theorem uleb_length (n : Nat) : (uleb n).length = ubytes n := by
  have hlenpos : 0 < (uleb n).length := uleb_len_pos n
  have hubpos : 0 < ubytes n := by unfold ubytes; omega
  apply Nat.le_antisymm
  · exact (uleb_len_le_iff n (ubytes n) hubpos).mpr
      ((ubytes_le_iff n (ubytes n) hubpos).mp (Nat.le_refl _))
  · exact (ubytes_le_iff n (uleb n).length hlenpos).mpr
      ((uleb_len_le_iff n (uleb n).length hlenpos).mp (Nat.le_refl _))

private theorem unzig_zig (x : Int) : unzig (zig x) = x := by
  cases x with
  | ofNat n => simp [zig, unzig]
  | negSucc n =>
      have h : (2 * n + 1) % 2 = 1 := by omega
      simp [zig, unzig, h]
      omega

private theorem max?_some_of_ne_nil : ∀ {xs : List Nat}, xs ≠ [] → ∃ m, xs.max? = some m := by
  intro xs
  cases xs with
  | nil => simp
  | cons a as =>
      intro _
      rw [List.max?_cons]
      exact ⟨as.max?.elim a (max a), rfl⟩

private theorem width_peak_le_of_all_lt (xs : List Nat) (w : Nat)
    (h : ∀ x ∈ xs, x < 2 ^ w) : width (peak xs) ≤ w := by
  cases xs with
  | nil => simp [peak, width]
  | cons a as =>
      obtain ⟨m, hm⟩ := max?_some_of_ne_nil (by simp : (a :: as) ≠ [])
      have hmprops := List.max?_eq_some_iff.mp hm
      have hm_lt : m < 2 ^ w := h m hmprops.1
      have hp : peak (a :: as) = m := by simp [peak, hm]
      rw [hp]
      by_cases hm0 : m = 0
      · simp [hm0, width]
      · exact (width_le_iff m w hm0).mpr hm_lt

private theorem byteCount_mono {a b : Nat} (h : a ≤ b) : byteCount a ≤ byteCount b := by
  unfold byteCount
  exact Nat.div_le_div_right (by omega)

private theorem width_mono {n m : Nat} (h : n ≤ m) : width n ≤ width m := by
  have hlt : n < 2 ^ width m := Nat.lt_of_le_of_lt h (lt_two_pow_width m)
  have hw := width_peak_le_of_all_lt [n] (width m) (by
    intro x hx
    simp only [List.mem_singleton] at hx
    subst x
    exact hlt)
  simpa [peak] using hw

private theorem ubytes_mono {n m : Nat} (h : n ≤ m) : ubytes n ≤ ubytes m := by
  have hw : width n ≤ width m := width_mono h
  unfold ubytes
  apply Nat.max_le.mpr
  constructor
  · exact Nat.le_max_left _ _
  · exact Nat.le_trans (Nat.div_le_div_right (by omega)) (Nat.le_max_right _ _)

/-! ## Generic encode/restore facts -/

private theorem encode_length (p : Predict) (side : Nat → Int) (i : Nat) (h : List Int)
    (xs : List Int) : (encode p side i h xs).length = xs.length := by
  induction xs generalizing i h with
  | nil => rfl
  | cons x xs ih => simp [encode, ih]

private theorem unzig_zero : unzig 0 = 0 := by simp [unzig]

/-! ## Certificate bounds the encoded frame -/

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
  apply width_peak_le_of_all_lt
  exact encode_cert_mem side i h xs w hc

private theorem frame_length_le_quote (side : Nat → Int) (i : Nat) (h : List Int) (xs : List Int)
    (w : Nat) (hc : certificate predict side i h xs w) :
    (frame (encode predict side i h xs)).length ≤ quote xs.length w := by
  have hw' : width (peak (encode predict side i h xs)) ≤ w :=
    width_encode_le side i h xs w hc
  have hes : (encode predict side i h xs).length = xs.length :=
    encode_length predict side i h xs
  have hub : ubytes (width (peak (encode predict side i h xs))) ≤ ubytes w :=
    ubytes_mono hw'
  have hbc : byteCount ((encode predict side i h xs).length *
        width (peak (encode predict side i h xs))) ≤ byteCount (xs.length * w) := by
    apply byteCount_mono
    rw [hes]
    exact Nat.mul_le_mul_left xs.length hw'
  dsimp [frame, quote]
  simp only [List.length_append]
  rw [uleb_length, uleb_length]
  simp [pack, little]
  rw [hes] at hbc
  rw [hes]
  omega

/-! ## ResidualRoundTrip -/

theorem sound_ResidualRoundTrip : Linear.ResidualRoundTrip.Goal := by
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

theorem sound_ByteBudget : Linear.ByteBudget.Goal := by
  intro side i h xs w extra oldBytes hc hlt
  have hframe : (frame (encode predict side i h xs)).length ≤ quote xs.length w :=
    frame_length_le_quote side i h xs w hc
  omega

/-! ## RestartSeed -/

private theorem lag_take_two (h : List Int) (k : Nat) (hk : k < 2) :
    lag (h.take 2) k = lag h k := by
  have hk01 : k = 0 ∨ k = 1 := by omega
  rcases hk01 with rfl | rfl <;>
    cases h with
    | nil => simp [lag]
    | cons a as =>
        cases as with
        | nil => simp [lag]
        | cons b bs => simp [lag]

private theorem predict_eq_of_lag {h₁ h₂ : List Int} (i : Nat) (side : Nat → Int)
    (h0 : lag h₁ 0 = lag h₂ 0) (h1 : lag h₁ 1 = lag h₂ 1) :
    predict i h₁ side = predict i h₂ side := by
  simp [predict, h0, h1]

private theorem encode_eq_of_lag (side : Nat → Int) :
    ∀ (i : Nat) (h₁ h₂ : List Int) (xs : List Int),
      lag h₁ 0 = lag h₂ 0 → lag h₁ 1 = lag h₂ 1 →
      encode predict side i h₁ xs = encode predict side i h₂ xs := by
  intro i h₁ h₂ xs
  induction xs generalizing i h₁ h₂ with
  | nil => simp [encode]
  | cons x xs ih =>
      intro hl0 hl1
      simp only [encode]
      have hp : predict i h₁ side = predict i h₂ side :=
        predict_eq_of_lag i side hl0 hl1
      rw [hp]
      apply congrArg (List.cons (zig (x - predict i h₂ side)))
      apply ih (i + 1) (x :: h₁) (x :: h₂)
      · simp [lag]
      · simpa [lag] using hl0

theorem sound_RestartSeed : Linear.RestartSeed.Goal := by
  intro side i h xs
  apply encode_eq_of_lag side i (h.take 2) h xs
  · exact lag_take_two h 0 (by omega)
  · exact lag_take_two h 1 (by omega)

/-! ## ConstantStep -/

private theorem range_succ_prepend (n : Nat) :
    List.range (n + 1) = 0 :: (List.range n).map (fun k => k + 1) := by
  induction n with
  | zero => simp
  | succ n ih =>
      have ih' : List.range n ++ [n] = 0 :: (List.range n).map (fun k => k + 1) := by
        simpa [List.range_succ] using ih
      calc
        List.range (Nat.succ n + 1) = (List.range n ++ [n]) ++ [n + 1] := by
          rw [Nat.succ_eq_add_one, List.range_succ, List.range_succ]
        _ = (0 :: (List.range n).map (fun k => k + 1)) ++ [n + 1] := by rw [ih']
        _ = 0 :: ((List.range n).map (fun k => k + 1) ++ [n + 1]) := rfl
        _ = 0 :: (List.range (n + 1)).map (fun k => k + 1) := by
          rw [List.range_succ, List.map_append]
          simp

private theorem const_step_aux (a d : Int) (side : Nat → Int) :
    ∀ (n step idx : Nat) (tail : List Int),
      restore predict side idx
          ([a + d * ((step : Int) - 1), a + d * ((step : Int) - 2)] ++ tail)
          (List.replicate n 0) =
        (List.range n).map (fun (k : Nat) => a + d * ((step : Int) + (k : Int))) := by
  intro n
  induction n with
  | zero =>
      intro step idx tail
      simp [restore]
  | succ n ih =>
      intro step idx tail
      simp only [List.replicate_succ, restore]
      have hpred :
          predict idx ([a + d * ((step : Int) - 1), a + d * ((step : Int) - 2)] ++ tail) side =
            a + d * (step : Int) := by
        simp [predict, lag]
        grind
      rw [hpred, unzig_zero]
      simp only [Int.add_zero]
      have hstep1 : ((step + 1 : Nat) : Int) = (step : Int) + 1 := by omega
      have hnext :
          (a + d * (step : Int)) ::
            ([a + d * ((step : Int) - 1), a + d * ((step : Int) - 2)] ++ tail) =
          ([a + d * (((step + 1 : Nat) : Int) - 1),
            a + d * (((step + 1 : Nat) : Int) - 2)] ++
            ((a + d * ((step : Int) - 2)) :: tail)) := by
        rw [hstep1]
        grind
      rw [hnext]
      rw [ih (step + 1) (idx + 1) ((a + d * ((step : Int) - 2)) :: tail)]
      rw [range_succ_prepend n]
      simp only [List.map_cons, List.map_map]
      simp
      intro k hk
      grind

theorem sound_ConstantStep : Linear.ConstantStep.Goal := by
  intro a d n side i
  have h := const_step_aux a d side n 0 i []
  have hstart :
      [a + d * (((0 : Nat) : Int) - 1), a + d * (((0 : Nat) : Int) - 2)] ++ ([] : List Int) =
        [a - d, a - 2 * d] := by
    grind
  have hfun :
      (fun (k : Nat) => a + d * (((0 : Nat) : Int) + (k : Int))) =
        (fun (j : Nat) => a + d * (j : Int)) := by
    funext k
    grind
  rw [hstart, hfun] at h
  exact h

end TinylambOpt.ResearchProofs.Compression.Predictors.Linear

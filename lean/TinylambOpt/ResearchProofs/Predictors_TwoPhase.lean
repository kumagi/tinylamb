import TinylambOpt.ResearchGoals.Compression.Predictors

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `TwoPhase.ByteBudget` — `∀ evenStep oddStep side i h xs w extra oldBytes, certificate
  (predict evenStep oddStep) side i h xs w → extra + quote xs.length w < oldBytes →
  extra + (frame (encode (predict evenStep oddStep) side i h xs)).length < oldBytes`
- `TwoPhase.ResidualRoundTrip` — `∀ evenStep oddStep side i h xs, restore (predict
  evenStep oddStep) side i h (encode (predict evenStep oddStep) side i h xs) = xs`
- `TwoPhase.RestartSeed` — `∀ evenStep oddStep side i h xs, encode (predict evenStep
  oddStep) side i (h.take (2)) xs = encode (predict evenStep oddStep) side i h xs`
- `TwoPhase.TwoLanes` — `∀ a b d e side n, restore (predict d e) side 0 [b-e,a-d]
  (List.replicate n 0) = (List.range n).map (fun j => if j%2 == 0 then a+d*(j/2:Nat)
  else b+e*(j/2:Nat))`
-/

namespace TinylambOpt.ResearchProofs.Compression.Predictors.TwoPhase

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Compression.Predictors
open TinylambOpt.ResearchGoals.Compression.Predictors.TwoPhase

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! ## Generic codec helpers -/

private theorem unzig_zig (x : Int) : unzig (zig x) = x := by
  cases x with
  | ofNat n => simp [zig, unzig]
  | negSucc n =>
      have h : (2 * n + 1) % 2 = 1 := by omega
      simp [zig, unzig, h]
      omega

private theorem restore_encode (p : Predict) (side : Nat → Int) :
    ∀ (i : Nat) (h xs : List Int), restore p side i h (encode p side i h xs) = xs := by
  intro i h xs
  induction xs generalizing i h with
  | nil => simp [encode, restore]
  | cons x xs ih =>
      simp only [encode, restore]
      rw [unzig_zig]
      rw [show p i h side + (x - p i h side) = x by omega]
      congr 1
      exact ih (i + 1) (x :: h)

private theorem encode_length (p : Predict) (side : Nat → Int) (i : Nat) (h : List Int) :
    ∀ xs, (encode p side i h xs).length = xs.length := by
  intro xs
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih => simp [encode, ih]

private theorem certificate_tail (p : Predict) (side : Nat → Int) (i : Nat) (h : List Int)
    (x : Int) (xs : List Int) (w : Nat) (hc : certificate p side i h (x :: xs) w) :
    certificate p side (i + 1) (x :: h) xs w := by
  intro j hj
  have hsource := hc (j + 1) (by simpa using hj)
  dsimp [certificate] at hsource ⊢
  simpa [List.take_succ_cons, List.reverse_cons, List.append_assoc,
    Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hsource

private theorem encode_cert (p : Predict) (side : Nat → Int) (i : Nat) (h : List Int)
    (xs : List Int) (w : Nat) (hc : certificate p side i h xs w) :
    ∀ x ∈ encode p side i h xs, x < 2 ^ w := by
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih =>
      simp only [encode, List.mem_cons]
      intro z hz
      rcases hz with rfl | hz
      · simpa [certificate] using hc 0 (by simp)
      · exact ih (i + 1) (x :: h) (certificate_tail p side i h x xs w hc) z hz

/-! ## Wire charge helpers -/

private theorem width_spec (n : Nat) (hn : n ≠ 0) :
    2 ^ (width n - 1) ≤ n ∧ n < 2 ^ width n := by
  unfold width
  simp [hn]
  have h := (Nat.log2_eq_iff hn).mp rfl
  constructor <;> omega

private theorem width_le_of_lt (n w : Nat) (h : n < 2 ^ w) : width n ≤ w := by
  by_cases hn : n = 0
  · simp [width, hn]
  · have hlog : Nat.log2 n < w := (Nat.log2_lt hn).mpr h
    unfold width
    simp [hn]
    omega

private theorem max?_some_of_ne_nil : ∀ {xs : List Nat}, xs ≠ [] → ∃ m, xs.max? = some m := by
  intro xs
  cases xs with
  | nil => simp
  | cons a as =>
      intro _
      rw [List.max?_cons]
      exact ⟨as.max?.elim a (max a), rfl⟩

private theorem peak_mem {xs : List Nat} (hne : xs ≠ []) : peak xs ∈ xs := by
  obtain ⟨m, hm⟩ := max?_some_of_ne_nil hne
  have hprops := List.max?_eq_some_iff.mp hm
  simpa [peak, hm] using hprops.1

private theorem lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  by_cases hn : n = 0
  · subst n
    simp [width]
  · exact (width_spec n hn).2

private theorem ubytes_pos (n : Nat) : 0 < ubytes n := by
  unfold ubytes
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
        · subst n
          simp [width]
        · have hlt : n < 2 ^ (7 * k) := by
            have h128 : (128 : Nat) = 2 ^ 7 := by decide
            rw [h128, ← Nat.pow_mul] at h
            exact h
          have hlog : Nat.log2 n < 7 * k := (Nat.log2_lt hn).mpr hlt
          unfold width
          simp [hn]
          omega
      rw [Nat.div_le_iff_le_mul_add_pred (by decide : 0 < 7)]
      omega

private theorem uleb_len_pos (n : Nat) : 0 < (uleb n).length := by
  unfold uleb
  split <;> simp

private theorem uleb_len_le_pow (n k : Nat) (hk : 0 < k) (h : n < 128 ^ k) :
    (uleb n).length ≤ k := by
  induction k generalizing n with
  | zero => omega
  | succ k ih =>
      by_cases hn : n < 128
      · rw [uleb]
        simp only [hn, ite_true, List.length_singleton]
        omega
      · by_cases hk0 : k = 0
        · subst k
          simp at h
          omega
        · rw [uleb]
          simp only [hn, ite_false, List.length_cons]
          have hkpos : 0 < k := Nat.pos_of_ne_zero hk0
          have hdiv : n / 128 < 128 ^ k := by
            apply (Nat.div_lt_iff_lt_mul (by omega : 0 < 128)).mpr
            rw [Nat.pow_succ] at h
            simpa [Nat.mul_comm] using h
          have ih' := ih (n / 128) hkpos hdiv
          omega

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
  apply Nat.le_antisymm
  · exact uleb_len_le_pow n (ubytes n) (ubytes_pos n)
      ((ubytes_le_iff n (ubytes n) (ubytes_pos n)).mp (Nat.le_refl _))
  · exact (ubytes_le_iff n (uleb n).length (uleb_len_pos n)).mpr (uleb_len_bound n)

private theorem byteCount_mono {a b : Nat} (h : a ≤ b) : byteCount a ≤ byteCount b := by
  unfold byteCount
  exact Nat.div_le_div_right (by omega)

private theorem pack_length (w : Nat) (xs : List Nat) :
    (pack w xs).length = byteCount (xs.length * w) := by
  simp [pack, little]

/-! ## ResidualRoundTrip -/

theorem sound_ResidualRoundTrip : ResidualRoundTrip.Goal := by
  intro evenStep oddStep side i h xs
  exact restore_encode (predict evenStep oddStep) side i h xs

/-! ## ByteBudget -/

theorem sound_ByteBudget : ByteBudget.Goal := by
  intro evenStep oddStep side i h xs w extra oldBytes hcert hlt
  let es := encode (predict evenStep oddStep) side i h xs
  have hbnd : ∀ x ∈ es, x < 2 ^ w := by
    dsimp [es]
    exact encode_cert (predict evenStep oddStep) side i h xs w hcert
  have hwle : width (peak es) ≤ w := by
    by_cases hes : es = []
    · rw [hes]
      simp [peak, width]
    · exact width_le_of_lt (peak es) w (hbnd (peak es) (peak_mem hes))
  have hframe : (frame es).length =
      (uleb es.length).length +
        ((uleb (width (peak es))).length +
          byteCount (es.length * width (peak es))) + 1 := by
    simp [frame, pack_length]
  rw [hframe, uleb_length es.length, uleb_length (width (peak es))]
  have hubw : ubytes (width (peak es)) ≤ ubytes w := by
    apply (ubytes_le_iff (width (peak es)) (ubytes w) (ubytes_pos w)).mpr
    exact Nat.lt_of_le_of_lt hwle
      ((ubytes_le_iff w (ubytes w) (ubytes_pos w)).mp (Nat.le_refl _))
  have hbc : byteCount (es.length * width (peak es)) ≤ byteCount (es.length * w) :=
    byteCount_mono (Nat.mul_le_mul_left es.length hwle)
  have helen : es.length = xs.length := by
    dsimp [es]
    exact encode_length (predict evenStep oddStep) side i h xs
  unfold quote at hlt
  rw [helen] at hbc ⊢
  omega

/-! ## RestartSeed -/

private theorem lag_cons_zero (x : Int) (h : List Int) : lag (x :: h) 0 = x := by
  simp [lag]

private theorem lag_cons_one (x : Int) (h : List Int) : lag (x :: h) 1 = lag h 0 := by
  cases h <;> simp [lag]

private theorem lag_take_two (h : List Int) :
    lag (h.take 2) 0 = lag h 0 ∧ lag (h.take 2) 1 = lag h 1 := by
  cases h with
  | nil => simp [lag]
  | cons a hs =>
      cases hs with
      | nil => simp [lag]
      | cons b hs => simp [lag]

private theorem encode_lag_eq (d e : Int) (side : Nat → Int) (i : Nat) (h₁ h₂ : List Int)
    (h0 : lag h₁ 0 = lag h₂ 0) (h1 : lag h₁ 1 = lag h₂ 1) :
    ∀ xs, encode (predict d e) side i h₁ xs = encode (predict d e) side i h₂ xs := by
  intro xs
  induction xs generalizing i h₁ h₂ with
  | nil => simp [encode]
  | cons x xs ih =>
      simp only [encode]
      have hhead : (predict d e) i h₁ side = (predict d e) i h₂ side := by
        dsimp [predict]
        rw [h1]
      rw [hhead]
      congr 1
      apply ih (i + 1) (x :: h₁) (x :: h₂)
      · simp [lag]
      · simpa [lag_cons_one] using h0

theorem sound_RestartSeed : RestartSeed.Goal := by
  intro evenStep oddStep side i h xs
  exact encode_lag_eq evenStep oddStep side i (h.take 2) h
    (lag_take_two h).1 (lag_take_two h).2 xs

/-! ## TwoLanes -/

private def laneVal (a b d e : Int) (j : Nat) : Int :=
  if j % 2 == 0 then a + d * (j / 2 : Nat) else b + e * (j / 2 : Nat)

private theorem laneVal_add_two (a b d e : Int) (k : Nat) :
    laneVal a b d e (k + 2) = laneVal a b d e k + (if (k + 2) % 2 == 0 then d else e) := by
  have hmod : (k + 2) % 2 = k % 2 := by omega
  have hdiv : (k + 2) / 2 = k / 2 + 1 := by omega
  unfold laneVal
  by_cases hk : k % 2 == 0
  · simp [hk, hdiv]
    grind
  · have hkf : (k % 2 == 0) = false := Bool.eq_false_of_ne_true hk
    simp [hkf, hdiv]
    grind

private theorem reverse_map_range_nth (f : Nat → Int) (m k : Nat) (hk : k < m) :
    ((List.range m).map f).reverse[k]? = some (f (m - 1 - k)) := by
  rw [List.getElem?_reverse (by simpa using hk)]
  simp only [List.length_map, List.length_range]
  rw [List.getElem?_map]
  rw [List.getElem?_range (by omega : m - 1 - k < m)]
  simp

private theorem lag_reverse_range_nth (f : Nat → Int) (k : Nat) (seed : List Int) :
    lag (((List.range (k + 2)).map f).reverse ++ seed) 1 = f k := by
  dsimp [lag]
  rw [List.getElem?_append]
  have hidx : 1 < k + 2 := by omega
  simp [hidx]

private theorem twoPredict_at (a b d e : Int) (side : Nat → Int) (m : Nat) :
    (predict d e) m (((List.range m).map (laneVal a b d e)).reverse ++ [b - e, a - d]) side =
      laneVal a b d e m := by
  cases m with
  | zero =>
      simp [predict, lag, laneVal]
  | succ m =>
      cases m with
      | zero =>
          simp [predict, lag, laneVal]
      | succ k =>
          have hlag : lag (((List.range (k + 2)).map (laneVal a b d e)).reverse ++ [b - e, a - d]) 1 =
              laneVal a b d e k := by
            exact lag_reverse_range_nth (laneVal a b d e) k [b - e, a - d]
          unfold predict
          rw [hlag]
          rw [← laneVal_add_two a b d e k]

private theorem shift_range (f : Nat → Int) (m n : Nat) :
    f m :: (List.range n).map (fun k => f ((m + 1) + k)) =
      (List.range (n + 1)).map (fun k => f (m + k)) := by
  apply List.ext_getElem?
  intro j
  cases j with
  | zero =>
      by_cases hn : n = 0
      · subst n
        simp
      · have hnpos : 0 < n := Nat.pos_of_ne_zero hn
        simp [List.range_succ, hnpos]
  | succ j =>
      rw [List.getElem?_cons_succ]
      by_cases hj : j < n
      · have hj1 : j + 1 < n + 1 := by omega
        rw [List.getElem?_map]
        rw [List.getElem?_range hj]
        rw [List.getElem?_map]
        rw [List.getElem?_range hj1]
        simp
        congr 1
        omega
      · have hj1 : ¬ j + 1 < n + 1 := by omega
        simp [hj, hj1]

private theorem restore_two_phase (a b d e : Int) (side : Nat → Int) :
    ∀ (n m : Nat) (h : List Int),
      h = ((List.range m).map (laneVal a b d e)).reverse ++ [b - e, a - d] →
      restore (predict d e) side m h (List.replicate n 0) =
        (List.range n).map (fun k => laneVal a b d e (m + k)) := by
  intro n
  induction n with
  | zero =>
      intro m h hh
      simp [restore]
  | succ n ih =>
      intro m h hh
      have hp : (predict d e) m h side = laneVal a b d e m := by
        rw [hh]
        exact twoPredict_at a b d e side m
      simp only [restore, List.replicate_succ]
      rw [show unzig 0 = 0 by simp [unzig]]
      rw [hp]
      simp
      have hhist : laneVal a b d e m :: h =
          ((List.range (m + 1)).map (laneVal a b d e)).reverse ++ [b - e, a - d] := by
        rw [hh, List.range_succ, List.map_append, List.reverse_append]
        simp
      rw [ih (m + 1) (laneVal a b d e m :: h) hhist]
      exact shift_range (laneVal a b d e) m n

theorem sound_TwoLanes : TwoLanes.Goal := by
  intro a b d e side n
  simpa [laneVal] using restore_two_phase a b d e side n 0 [b - e, a - d] (by simp)

end TinylambOpt.ResearchProofs.Compression.Predictors.TwoPhase

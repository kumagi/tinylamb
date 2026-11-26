import TinylambOpt.ResearchGoals.Compression.Predictors

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `ModuloCounter.ByteBudget` — `∀ step period side i h xs w extra oldBytes, certificate
  (predict step period) side i h xs w → extra + quote xs.length w < oldBytes → extra
  + (frame (encode (predict step period) side i h xs)).length < oldBytes`
- `ModuloCounter.ResidualRoundTrip` — `∀ step period side i h xs, restore (predict step
  period) side i h (encode (predict step period) side i h xs) = xs`
- `ModuloCounter.RestartSeed` — `∀ step period side i h xs, encode (predict step
  period) side i (h.take (1)) xs = encode (predict step period) side i h xs`
- `ModuloCounter.WrappedProgression` — `∀ step period a side i n, restore (predict step
  period) side i [a-step] (List.replicate n 0) = (List.range n).map (fun (j : Nat) =>
  (a+step*j)%(period+1:Nat))`
-/

namespace TinylambOpt.ResearchProofs.Compression.Predictors.ModuloCounter

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Compression.Predictors
open TinylambOpt.ResearchGoals.Compression.Predictors.ModuloCounter

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! ## shared helpers -/

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

/-! ## certificate bounds the encoded residuals -/

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

/-! ## wire-size arithmetic (local copies of the Wire lemmas we need) -/

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

private theorem bits_le_byteCount (bits : Nat) : bits ≤ 8 * byteCount bits := by
  unfold byteCount
  have hm := Nat.mod_lt (bits + 7) (by decide : 0 < 8)
  have hd := Nat.mod_add_div (bits + 7) 8
  omega

private theorem byteCount_le_of_le (bits b : Nat) (h : bits ≤ 8 * b) : byteCount bits ≤ b := by
  unfold byteCount
  have hm := Nat.mod_lt (bits + 7) (by decide : 0 < 8)
  have hd := Nat.mod_add_div (bits + 7) 8
  omega

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
  have hall : ∀ e ∈ encode p side i h xs, e < 2 ^ w :=
    encode_lt_certificate p side i h xs w hc
  cases henc : encode p side i h xs with
  | nil =>
      simpa [peak, henc] using (Nat.pow_pos (by decide : (0 : Nat) < 2) : 0 < 2 ^ w)
  | cons a as =>
      obtain ⟨m, hm⟩ := max?_some_of_ne_nil (by simp : (a :: as) ≠ [])
      have hmprops := List.max?_eq_some_iff.mp hm
      have hmlt := hall m (by simpa [henc] using hmprops.1)
      simpa [peak, henc, hm] using hmlt

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
  let w' := width (peak es)
  have hes_len : es.length = xs.length := by
    dsimp [es]
    exact encode_length p side i h xs
  have hw_le : w' ≤ w := by
    dsimp [w', es]
    exact width_encode_le p side i h xs w hc
  have huleb_len : (uleb es.length).length = ubytes es.length := uleb_len_eq_ubytes es.length
  have hpacklen : (pack w' es).length = byteCount (es.length * w') := by
    simp [pack, little]
  have hubpos : 0 < ubytes w := by unfold ubytes; omega
  have hulebw : (uleb w').length ≤ ubytes w := by
    have hbound : w' < 128 ^ (ubytes w) :=
      Nat.lt_of_le_of_lt hw_le ((ubytes_le_iff w (ubytes w) hubpos).mp (Nat.le_refl _))
    exact (uleb_len_le_iff w' (ubytes w) hubpos).mpr hbound
  have hpack_bound : byteCount (es.length * w') ≤ byteCount (xs.length * w) := by
    apply byteCount_mono
    rw [hes_len]
    exact Nat.mul_le_mul_left xs.length hw_le
  have hframe_eq : (frame es).length =
      1 + (uleb es.length).length + (uleb w').length + (pack w' es).length := by
    change ([0] ++ uleb es.length ++ uleb w' ++ pack w' es).length = _
    simp only [List.length_append, List.length_cons, List.length_nil]
  have hquote_eq : quote xs.length w =
      1 + ubytes xs.length + ubytes w + byteCount (xs.length * w) := rfl
  rw [hframe_eq, hquote_eq, huleb_len, hpacklen, hes_len]
  have hpack_bound2 : byteCount (xs.length * w') ≤ byteCount (xs.length * w) := by
    simpa [hes_len] using hpack_bound
  exact Nat.add_le_add (Nat.add_le_add_left hulebw (1 + ubytes xs.length)) hpack_bound2

/-! ## goals -/

theorem sound_ResidualRoundTrip : ResidualRoundTrip.Goal := by
  intro step period side i h xs
  exact encode_restore (predict step period) side i h xs

theorem sound_ByteBudget : ByteBudget.Goal := by
  intro step period side i h xs w extra oldBytes hcert hlt
  have hle := frame_length_le_quote (predict step period) side i h xs w hcert
  omega

private theorem lag_take_one (h : List Int) : lag (h.take 1) 0 = lag h 0 := by
  cases h with
  | nil => simp [lag]
  | cons x xs => simp [lag]

private theorem encode_restart_of_stable (p : Predict)
    (hp : ∀ i h side, p i (h.take 1) side = p i h side) :
    ∀ (side : Nat → Int) (i : Nat) (h xs : List Int),
      encode p side i (h.take 1) xs = encode p side i h xs := by
  intro side i h xs
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih =>
      simp only [encode]
      rw [hp i h side]
      have h1 := ih (i + 1) (x :: h.take 1)
      have h2 := ih (i + 1) (x :: h)
      simpa using h1.symm.trans h2

theorem sound_RestartSeed : RestartSeed.Goal := by
  intro step period side i h xs
  apply encode_restart_of_stable (predict step period)
  intro j hj side
  simp [predict, lag_take_one]

private theorem map_range_succ (f : Nat → Int) (n : Nat) :
    (List.range (n + 1)).map f = f 0 :: (List.range n).map (fun j => f (j + 1)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, ih]
      simp only [List.cons_append, List.cons.injEq]
      congr 1
      rw [List.range_succ, List.map_append]
      simp

private theorem restore_zero_aux (step : Int) (period : Nat) (side : Nat → Int) :
    ∀ (i : Nat) (n : Nat) (a : Int) (h : List Int),
      lag h 0 % (period + 1 : Nat) = (a - step) % (period + 1 : Nat) →
      restore (predict step period) side i h (List.replicate n 0) =
        (List.range n).map (fun j : Nat => (a + step * (j : Nat)) % (period + 1 : Nat)) := by
  intro i n
  induction n generalizing i with
  | zero =>
      intro a h hlag
      simp [restore]
  | succ n ih =>
      intro a h hlag
      simp only [restore, List.replicate_succ]
      have hu : unzig 0 = 0 := by simp [unzig]
      rw [hu, Int.add_zero]
      have hhead : predict step period i h side = a % (period + 1 : Nat) := by
        have hp : predict step period i h side =
            (lag h 0 + step) % (period + 1 : Nat) := by simp [predict]
        rw [hp]
        rw [← Int.emod_add_emod (lag h 0) (period + 1 : Nat) step]
        rw [hlag]
        rw [Int.emod_add_emod (a - step) (period + 1 : Nat) step]
        congr 1
        omega
      rw [hhead]
      have hlagnew :
          lag (a % (period + 1 : Nat) :: h) 0 % (period + 1 : Nat) =
            ((a + step) - step) % (period + 1 : Nat) := by
        simp [lag]
      rw [ih (i + 1) (a + step) (a % (period + 1 : Nat) :: h) hlagnew]
      rw [map_range_succ]
      simp only [List.cons.injEq]
      constructor
      · simp
      · apply List.map_congr_left
        intro j hj
        congr 1
        have hjcast : ((j + 1 : Nat) : Int) = (j : Int) + 1 := by omega
        rw [hjcast, Int.mul_add, Int.mul_one]
        omega

theorem sound_WrappedProgression : WrappedProgression.Goal := by
  intro step period a side i n
  apply restore_zero_aux step period side i n a [a - step]
  simp [lag]

end TinylambOpt.ResearchProofs.Compression.Predictors.ModuloCounter

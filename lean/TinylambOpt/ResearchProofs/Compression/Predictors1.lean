import TinylambOpt.ResearchGoals.Compression.Predictors

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Predictors1

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Compression.Predictors

/-! Proofs for the 16 goals in the `Previous`, `Linear`, `Quadratic` and
`Seasonal` predictor namespaces. All 16 goals turn out to be true. -/

/-! ## Shared helpers -/

private theorem unzig_zig (x : Int) : unzig (zig x) = x := by
  cases x with
  | ofNat n => simp [zig, unzig]
  | negSucc n =>
      have h : (2 * n + 1) % 2 = 1 := by omega
      simp [zig, unzig, h]
      omega

private theorem restore_encode (p : Predict) (side : Nat → Int) :
    ∀ (i : Nat) (h xs : List Int),
      restore p side i h (encode p side i h xs) = xs := by
  intro i h xs
  induction xs generalizing i h with
  | nil => simp [encode, restore]
  | cons x xs ih =>
      simp only [encode, restore]
      rw [unzig_zig]
      have hx : p i h side + (x - p i h side) = x := by omega
      rw [hx]
      congr 1
      exact ih (i + 1) (x :: h)

private theorem encode_length (p : Predict) (side : Nat → Int) :
    ∀ (i : Nat) (h xs : List Int), (encode p side i h xs).length = xs.length := by
  intro i h xs
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih => simp [encode, ih]

private theorem certificate_tail (p : Predict) (side : Nat → Int)
    (i : Nat) (h : List Int) (x : Int) (xs : List Int) (w : Nat)
    (hc : certificate p side i h (x :: xs) w) :
    certificate p side (i + 1) (x :: h) xs w := by
  intro j hj
  have hj' : j + 1 < (x :: xs).length := by simp; omega
  have hc' := hc (j + 1) hj'
  simpa [List.take_succ_cons, List.reverse_cons, List.append_assoc,
    Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hc'

private theorem encode_residuals_lt (p : Predict) (side : Nat → Int)
    (i : Nat) (h xs : List Int) (w : Nat)
    (hc : certificate p side i h xs w) :
    ∀ e ∈ encode p side i h xs, e < 2 ^ w := by
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih =>
      have hhead : zig (x - p i h side) < 2 ^ w := by
        have h0 := hc 0 (by simp)
        simpa using h0
      have htail : ∀ e ∈ encode p side (i + 1) (x :: h) xs, e < 2 ^ w :=
        ih (i + 1) (x :: h) (certificate_tail p side i h x xs w hc)
      intro e he
      simp only [encode, List.mem_cons] at he
      rcases he with rfl | he
      · exact hhead
      · exact htail e he

/-! Width / varint arithmetic (self-contained; the compiled Wire proof
module is stale in this environment). -/

private theorem max?_some_of_ne_nil : ∀ {xs : List Nat}, xs ≠ [] → ∃ m, xs.max? = some m := by
  intro xs hne
  cases xs with
  | nil => contradiction
  | cons a as =>
      rw [List.max?_cons]
      exact ⟨as.max?.elim a (max a), rfl⟩

private theorem peak_lt_of_all_lt (xs : List Nat) (w : Nat)
    (h : ∀ e ∈ xs, e < 2 ^ w) : peak xs < 2 ^ w := by
  cases xs with
  | nil => simp [peak, Nat.pow_pos]
  | cons x xs =>
      have hne : (x :: xs) ≠ [] := by simp
      obtain ⟨m, hm⟩ := max?_some_of_ne_nil hne
      have hmprops := List.max?_eq_some_iff.mp hm
      have hp : peak (x :: xs) = m := by simp [peak, hm]
      rw [hp]
      exact h m hmprops.1

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

private theorem width_le_of_lt (n w : Nat) (h : n < 2 ^ w) : width n ≤ w := by
  by_cases hn : n = 0
  · subst hn
    simp [width]
  · exact (width_le_iff n w hn).mpr h

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

private theorem ubytes_mono {a b : Nat} (h : a ≤ b) : ubytes a ≤ ubytes b := by
  have hbpos : 0 < ubytes b := by unfold ubytes; omega
  have hb_lt : b < 128 ^ (ubytes b) := (ubytes_le_iff b (ubytes b) hbpos).mp (Nat.le_refl _)
  have ha_lt : a < 128 ^ (ubytes b) := Nat.lt_of_le_of_lt h hb_lt
  exact (ubytes_le_iff a (ubytes b) hbpos).mpr ha_lt

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

private theorem uleb_len_le_ubytes (n : Nat) : (uleb n).length ≤ ubytes n := by
  have hpos : 0 < ubytes n := by unfold ubytes; omega
  exact (uleb_len_le_iff n (ubytes n) hpos).mpr
    ((ubytes_le_iff n (ubytes n) hpos).mp (Nat.le_refl _))

private theorem byteCount_mono' {a b : Nat} (h : a ≤ b) : byteCount a ≤ byteCount b := by
  unfold byteCount
  exact Nat.div_le_div_right (by omega)

private theorem byteBudget_of_certificate (p : Predict) (side : Nat → Int) :
    ∀ (i : Nat) (h xs : List Int) (w extra oldBytes : Nat),
      certificate p side i h xs w →
      extra + quote xs.length w < oldBytes →
      extra + (frame (encode p side i h xs)).length < oldBytes := by
  intro i h xs w extra oldBytes hcert hlt
  let es := encode p side i h xs
  have hlt_es : ∀ e ∈ es, e < 2 ^ w := by
    dsimp [es]
    exact encode_residuals_lt p side i h xs w hcert
  have hpeak : peak es < 2 ^ w := peak_lt_of_all_lt es w hlt_es
  have hwp : width (peak es) ≤ w := width_le_of_lt _ _ hpeak
  have hpacklen : (pack (width (peak es)) es).length =
      byteCount (es.length * width (peak es)) := by
    simp [pack, little]
  have hes_len : es.length = xs.length := by
    dsimp [es]
    exact encode_length p side i h xs
  have hub : ubytes (width (peak es)) ≤ ubytes w := ubytes_mono hwp
  have huleb_n : (uleb es.length).length ≤ ubytes xs.length := by
    rw [hes_len]
    exact uleb_len_le_ubytes xs.length
  have huleb_w : (uleb (width (peak es))).length ≤ ubytes w :=
    Nat.le_trans (uleb_len_le_ubytes (width (peak es))) hub
  have hbc : byteCount (es.length * width (peak es)) ≤ byteCount (xs.length * w) := by
    apply byteCount_mono'
    rw [hes_len]
    exact Nat.mul_le_mul_left xs.length hwp
  change extra + (frame es).length < oldBytes
  dsimp [quote] at hlt
  dsimp [frame, quote]
  simp only [List.length_append]
  rw [hpacklen]
  omega

private theorem lag_take (n k : Nat) (h : List Int) (hk : k < n) :
    lag (h.take n) k = lag h k := by
  unfold lag
  rw [List.getElem?_take]
  simp [hk]

private theorem take_take_self (n : Nat) (l : List Int) : (l.take n).take n = l.take n := by
  rw [List.take_take]
  simp

private theorem encode_of_take_eq (p : Predict) (side : Nat → Int) (m : Nat)
    (hpred : ∀ (k : Nat) (h : List Int), p k h side = p k (h.take m) side) :
    ∀ (i : Nat) (h1 h2 : List Int) (xs : List Int),
      h1.take m = h2.take m →
      encode p side i h1 xs = encode p side i h2 xs := by
  intro i h1 h2 xs heq
  induction xs generalizing i h1 h2 with
  | nil => simp [encode]
  | cons x xs ih =>
      simp only [encode]
      have hp : p i h1 side = p i h2 side := by
        rw [hpred i h1, hpred i h2, heq]
      rw [hp]
      have heq' : (x :: h1).take m = (x :: h2).take m := by
        cases m with
        | zero => simp
        | succ m' =>
            simp only [List.take_succ_cons]
            congr 1
            have htake_take : ∀ (l : List Int), (l.take (m' + 1)).take m' = l.take m' := by
              intro l
              rw [List.take_take]
              simp
            rw [← htake_take h1, ← htake_take h2, heq]
      rw [ih (i + 1) (x :: h1) (x :: h2) heq']

private theorem encode_append (p : Predict) (side : Nat → Int) :
    ∀ (i : Nat) (h xs ys : List Int),
      encode p side i h (xs ++ ys) =
        encode p side i h xs ++ encode p side (i + xs.length) (xs.reverse ++ h) ys := by
  intro i h xs
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih =>
      intro ys
      simp only [List.cons_append, encode]
      rw [ih (i + 1) (x :: h) ys]
      have hidx : (i + 1) + xs.length = i + (x :: xs).length := by simp; omega
      have hhist : xs.reverse ++ (x :: h) = (x :: xs).reverse ++ h := by
        simp [List.append_assoc]
      rw [hidx, hhist]

private theorem int_mul_add_one (d x : Int) : d * (x + 1) = d * x + d := by
  rw [Int.mul_add]
  simp

private theorem mul_const (a m k : Int) : a * (m * k) = k * (a * m) := by
  rw [← Int.mul_assoc, Int.mul_comm (a * m) k]

private theorem replicate_succ_append {α : Type} (m : Nat) (a : α) :
    List.replicate m a ++ [a] = List.replicate (m + 1) a := by
  rw [← List.replicate_one, List.replicate_append_replicate]

private theorem take_reverse_self (l : List Int) : l.reverse.take l.length = l.reverse := by
  apply List.ext_getElem?
  intro i
  rw [List.getElem?_take]
  by_cases hi : i < l.length
  · simp [hi]
  · have hlrev : l.reverse.length ≤ i := by
      rw [List.length_reverse]
      omega
    rw [List.getElem?_eq_none hlrev]
    simp [hi]

/-! ## Previous -/

theorem Previous_ResidualRoundTrip_sound : Previous.ResidualRoundTrip.Goal := by
  intro side i h xs
  exact restore_encode Previous.predict side i h xs

theorem Previous_ByteBudget_sound : Previous.ByteBudget.Goal := by
  intro side i h xs w extra oldBytes hcert hlt
  exact byteBudget_of_certificate Previous.predict side i h xs w extra oldBytes hcert hlt

theorem Previous_RestartSeed_sound : Previous.RestartSeed.Goal := by
  intro side i h xs
  have hpred : ∀ (k : Nat) (h' : List Int),
      Previous.predict k h' side = Previous.predict k (h'.take 1) side := by
    intro k h'
    dsimp [Previous.predict]
    rw [← lag_take 1 0 h' (by omega)]
  exact encode_of_take_eq Previous.predict side 1 hpred i (h.take 1) h xs
    (by rw [take_take_self])

private theorem previous_flat_encode (v : Int) :
    ∀ (n : Nat) (side : Nat → Int) (i : Nat) (h : List Int), lag h 0 = v →
      encode Previous.predict side i h (List.replicate n v) = List.replicate n 0 := by
  intro n
  induction n with
  | zero =>
      intro side i h _
      simp [encode]
  | succ n ih =>
      intro side i h hlag
      simp only [List.replicate_succ, encode]
      have hp : Previous.predict i h side = v := by
        dsimp [Previous.predict]
        exact hlag
      rw [hp]
      simp [zig]
      have hlag' : lag (v :: h) 0 = v := by simp [lag]
      rw [ih side (i + 1) (v :: h) hlag']

theorem Previous_FlatRun_sound : Previous.FlatRun.Goal := by
  intro v n side i
  exact previous_flat_encode v n side i [v] (by simp [lag])

/-! ## Linear -/

theorem Linear_ResidualRoundTrip_sound : Linear.ResidualRoundTrip.Goal := by
  intro side i h xs
  exact restore_encode Linear.predict side i h xs

theorem Linear_ByteBudget_sound : Linear.ByteBudget.Goal := by
  intro side i h xs w extra oldBytes hcert hlt
  exact byteBudget_of_certificate Linear.predict side i h xs w extra oldBytes hcert hlt

theorem Linear_RestartSeed_sound : Linear.RestartSeed.Goal := by
  intro side i h xs
  have hpred : ∀ (k : Nat) (h' : List Int),
      Linear.predict k h' side = Linear.predict k (h'.take 2) side := by
    intro k h'
    dsimp [Linear.predict]
    rw [← lag_take 2 0 h' (by omega), ← lag_take 2 1 h' (by omega)]
  exact encode_of_take_eq Linear.predict side 2 hpred i (h.take 2) h xs
    (by rw [take_take_self])

private theorem linear_zero_run :
    ∀ (a d : Int) (n : Nat) (side : Nat → Int) (i : Nat) (h : List Int),
      lag h 0 = a → lag h 1 = a - d →
      restore Linear.predict side i h (List.replicate n 0) =
        (List.range n).map (fun j : Nat => a + d * ((j : Int) + 1)) := by
  intro a d n
  induction n generalizing a d with
  | zero =>
      intro side i h h0 h1
      simp [restore]
  | succ n ih =>
      intro side i h h0 h1
      simp only [List.replicate_succ, restore]
      have hp : Linear.predict i h side = a + d := by
        dsimp [Linear.predict]
        rw [h0, h1]
        omega
      rw [hp]
      have hu : unzig 0 = (0 : Int) := by simp [unzig]
      rw [hu]
      simp
      have hl0 : lag ((a + d) :: h) 0 = a + d := by simp [lag]
      have hl1 : lag ((a + d) :: h) 1 = (a + d) - d := by
        simpa [lag] using h0
      rw [ih (a + d) d side (i + 1) ((a + d) :: h) hl0 hl1]
      rw [List.range_succ_eq_map]
      simp only [List.map_cons, List.map_map]
      simp
      congr 1
      intro j hj
      rw [int_mul_add_one d (j : Int), int_mul_add_one d ((j : Int) + 1),
        int_mul_add_one d (j : Int)]
      omega

theorem Linear_ConstantStep_sound : Linear.ConstantStep.Goal := by
  intro a d n side i
  have hl0 : lag [a - d, a - 2 * d] 0 = a - d := by simp [lag]
  have hl1 : lag [a - d, a - 2 * d] 1 = (a - d) - d := by
    simp [lag]
    omega
  have hres := linear_zero_run (a - d) d n side i [a - d, a - 2 * d] hl0 hl1
  rw [hres]
  apply List.map_congr_left
  intro j hj
  rw [int_mul_add_one d (j : Int)]
  omega

/-! ## Quadratic -/

theorem Quadratic_ResidualRoundTrip_sound : Quadratic.ResidualRoundTrip.Goal := by
  intro side i h xs
  exact restore_encode Quadratic.predict side i h xs

theorem Quadratic_ByteBudget_sound : Quadratic.ByteBudget.Goal := by
  intro side i h xs w extra oldBytes hcert hlt
  exact byteBudget_of_certificate Quadratic.predict side i h xs w extra oldBytes hcert hlt

theorem Quadratic_RestartSeed_sound : Quadratic.RestartSeed.Goal := by
  intro side i h xs
  have hpred : ∀ (k : Nat) (h' : List Int),
      Quadratic.predict k h' side = Quadratic.predict k (h'.take 3) side := by
    intro k h'
    dsimp [Quadratic.predict]
    rw [← lag_take 3 0 h' (by omega), ← lag_take 3 1 h' (by omega),
      ← lag_take 3 2 h' (by omega)]
  exact encode_of_take_eq Quadratic.predict side 3 hpred i (h.take 3) h xs
    (by rw [take_take_self])

private def poly (a b c : Int) (x : Int) : Int := a * x ^ 2 + b * x + c

private theorem poly_step (a b c m : Int) :
    3 * poly a b c m - 3 * poly a b c (m - 1) + poly a b c (m - 2) =
      poly a b c (m + 1) := by
  dsimp [poly]
  simp only [Int.pow_succ, Int.pow_zero, Int.mul_add, Int.add_mul, Int.sub_mul, Int.mul_sub,
    Int.mul_one, Int.one_mul, Int.mul_comm, Int.mul_assoc]
  rw [mul_const a m 3, mul_const a m 2]
  omega

private theorem quadratic_zero_run (a b c : Int) :
    ∀ (n : Nat) (side : Nat → Int) (i : Nat) (h : List Int) (m : Int),
      lag h 0 = poly a b c m →
      lag h 1 = poly a b c (m - 1) →
      lag h 2 = poly a b c (m - 2) →
      restore Quadratic.predict side i h (List.replicate n 0) =
        (List.range n).map (fun j : Nat => poly a b c (m + 1 + (j : Int))) := by
  intro n
  induction n with
  | zero =>
      intro side i h m h0 h1 h2
      simp [restore]
  | succ n ih =>
      intro side i h m h0 h1 h2
      simp only [List.replicate_succ, restore]
      have hp : Quadratic.predict i h side = poly a b c (m + 1) := by
        dsimp [Quadratic.predict]
        rw [h0, h1, h2]
        exact poly_step a b c m
      rw [hp]
      have hu : unzig 0 = (0 : Int) := by simp [unzig]
      rw [hu]
      simp
      have hl0 : lag (poly a b c (m + 1) :: h) 0 = poly a b c (m + 1) := by simp [lag]
      have hl1 : lag (poly a b c (m + 1) :: h) 1 = poly a b c ((m + 1) - 1) := by
        rw [show lag (poly a b c (m + 1) :: h) 1 = lag h 0 by simp [lag], h0]
        congr 1
        omega
      have hl2 : lag (poly a b c (m + 1) :: h) 2 = poly a b c ((m + 1) - 2) := by
        rw [show lag (poly a b c (m + 1) :: h) 2 = lag h 1 by simp [lag], h1]
        congr 1
        omega
      rw [ih side (i + 1) (poly a b c (m + 1) :: h) (m + 1) hl0 hl1 hl2]
      rw [List.range_succ_eq_map]
      simp only [List.map_cons, List.map_map]
      simp
      congr 1
      intro j hj
      congr 1
      omega

theorem Quadratic_PolynomialRun_sound : Quadratic.PolynomialRun.Goal := by
  intro a b c n side i
  let q : Int → Int := fun j => a * j ^ 2 + b * j + c
  change restore Quadratic.predict side i [q (-1), q (-2), q (-3)]
      (List.replicate n 0) = (List.range n).map (fun j : Nat => q j)
  have hq (x : Int) : q x = poly a b c x := by
    rfl
  have hl0 : lag [q (-1), q (-2), q (-3)] 0 = poly a b c (-1) := by
    simp [lag, hq]
  have hl1 : lag [q (-1), q (-2), q (-3)] 1 = poly a b c ((-1 : Int) - 1) := by
    simp [lag, hq]
  have hl2 : lag [q (-1), q (-2), q (-3)] 2 = poly a b c ((-1 : Int) - 2) := by
    simp [lag, hq]
  have hres := quadratic_zero_run a b c n side i [q (-1), q (-2), q (-3)] (-1) hl0 hl1 hl2
  rw [hres]
  apply List.map_congr_left
  intro j hj
  rw [hq]
  have harg : (-1 : Int) + 1 + (j : Int) = (j : Int) := by omega
  rw [harg]

/-! ## Seasonal -/

theorem Seasonal_ResidualRoundTrip_sound : Seasonal.ResidualRoundTrip.Goal := by
  intro period side i h xs
  exact restore_encode (Seasonal.predict period) side i h xs

theorem Seasonal_ByteBudget_sound : Seasonal.ByteBudget.Goal := by
  intro period side i h xs w extra oldBytes hcert hlt
  exact byteBudget_of_certificate (Seasonal.predict period) side i h xs w extra oldBytes hcert hlt

theorem Seasonal_RestartSeed_sound : Seasonal.RestartSeed.Goal := by
  intro period side i h xs
  have hpred : ∀ (k : Nat) (h' : List Int),
      Seasonal.predict period k h' side = Seasonal.predict period k (h'.take (period + 1)) side := by
    intro k h'
    dsimp [Seasonal.predict]
    rw [← lag_take (period + 1) period h' (by omega)]
  exact encode_of_take_eq (Seasonal.predict period) side (period + 1) hpred
    i (h.take (period + 1)) h xs (by rw [take_take_self])

private theorem seasonal_lag_at (block : List Int) (hb : block ≠ [])
    (m : Nat) (hm : m < block.length) (h0 : List Int)
    (h0take : h0.take block.length = block.reverse) :
    lag ((block.take m).reverse ++ h0) (block.length - 1) = block[m] := by
  have hml : m ≤ block.length := Nat.le_of_lt hm
  have hlenpre : (block.take m).length = m := by
    rw [List.length_take, Nat.min_eq_left hml]
  unfold lag
  rw [List.getElem?_append]
  have hnot : ¬ block.length - 1 < (block.take m).reverse.length := by
    rw [List.length_reverse, hlenpre]
    omega
  simp only [hnot, ↓reduceIte]
  have hsub : block.length - 1 - (block.take m).reverse.length = block.length - 1 - m := by
    rw [List.length_reverse, hlenpre]
  rw [hsub]
  have hidxlt : block.length - 1 - m < block.length := by
    have : 0 < block.length := Nat.pos_of_ne_zero (by
      intro hz
      apply hb
      exact List.eq_nil_of_length_eq_zero hz)
    omega
  have hh0 : h0[block.length - 1 - m]?.getD 0 =
      (h0.take block.length)[block.length - 1 - m]?.getD 0 := by
    rw [List.getElem?_take]
    simp [hidxlt]
  rw [hh0, h0take]
  rw [List.getElem?_reverse hidxlt]
  have hbidx : block.length - 1 - (block.length - 1 - m) = m := by omega
  rw [hbidx]
  simp [List.getElem?_eq_getElem hm]

private theorem seasonal_block (block : List Int) (hb : block ≠ []) :
    ∀ (m : Nat), m ≤ block.length →
    ∀ (side : Nat → Int) (i : Nat) (h0 : List Int),
      h0.take block.length = block.reverse →
      encode (Seasonal.predict (block.length - 1)) side i h0 (block.take m) =
        List.replicate m 0 := by
  intro m
  induction m with
  | zero =>
      intro hm side i h0 ht
      simp [encode]
  | succ m ih =>
      intro hm side i h0 ht
      have hmlt : m < block.length := by omega
      have htake : block.take (m + 1) = block.take m ++ [block[m]] := by
        rw [List.take_add_one]
        rw [List.getElem?_eq_getElem hmlt]
        rfl
      rw [htake, encode_append]
      rw [ih (by omega) side i h0 ht]
      have hlen : (block.take m).length = m := by
        rw [List.length_take, Nat.min_eq_left (Nat.le_of_lt hmlt)]
      rw [hlen]
      simp only [encode]
      have hp : Seasonal.predict (block.length - 1) (i + m)
          ((block.take m).reverse ++ h0) side = block[m] := by
        dsimp [Seasonal.predict, lag]
        exact seasonal_lag_at block hb m hmlt h0 ht
      rw [hp]
      simp [zig]
      simpa using (replicate_succ_append m (0 : Nat))

private theorem seasonal_repeat (block : List Int) (hb : block ≠ []) :
    ∀ (n : Nat) (side : Nat → Int) (i : Nat) (h0 : List Int),
      h0.take block.length = block.reverse →
      encode (Seasonal.predict (block.length - 1)) side i h0
        (List.flatten (List.replicate n block)) = List.replicate (n * block.length) 0 := by
  intro n
  induction n with
  | zero =>
      intro side i h0 ht
      simp [encode]
  | succ n ih =>
      intro side i h0 ht
      simp only [List.replicate_succ, List.flatten_cons]
      rw [encode_append]
      have hblk : encode (Seasonal.predict (block.length - 1)) side i h0 block =
          List.replicate block.length 0 := by
        simpa using seasonal_block block hb block.length (Nat.le_refl _) side i h0 ht
      rw [hblk]
      have htnew : (block.reverse ++ h0).take block.length = block.reverse := by
        rw [List.take_append_of_le_length (by simp : block.length ≤ block.reverse.length)]
        exact take_reverse_self block
      rw [ih side (i + block.length) (block.reverse ++ h0) htnew]
      rw [List.replicate_append_replicate]
      congr 1
      rw [Nat.add_mul]
      simp
      omega

theorem Seasonal_RepeatCycle_sound : Seasonal.RepeatCycle.Goal := by
  intro block n side i hb
  exact seasonal_repeat block hb n side i block.reverse (take_reverse_self block)

end TinylambOpt.ResearchProofs.Compression.Predictors1

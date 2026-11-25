import TinylambOpt.ResearchGoals.Compression.Predictors

namespace TinylambOpt.ResearchProofs.Compression.Predictors.SideQuotient

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Compression.Predictors
open TinylambOpt.ResearchGoals.Compression.Predictors.SideQuotient

set_option maxHeartbeats 0
set_option maxRecDepth 10000

abbrev P := TinylambOpt.ResearchGoals.Compression.Predictors.SideQuotient.predict

/-! ## generic encode/restore helpers -/

private theorem unzig_zig (x : Int) : unzig (zig x) = x := by
  cases x with
  | ofNat n => simp [zig, unzig]
  | negSucc n =>
      have h : (2 * n + 1) % 2 = 1 := by omega
      have hdiv : (2 * n + 1) / 2 = n := by omega
      simp [zig, unzig, h, hdiv]
      omega

private theorem encode_restore (p : Predict) (side : Nat → Int) (i : Nat) (h : List Int) :
    ∀ xs, restore p side i h (encode p side i h xs) = xs := by
  intro xs
  induction xs generalizing i h with
  | nil => simp [encode, restore]
  | cons x xs ih =>
      simp only [encode, restore]
      have hfirst : p i h side + unzig (zig (x - p i h side)) = x := by
        rw [unzig_zig]
        omega
      rw [hfirst]
      rw [ih]

private theorem encode_length (p : Predict) (side : Nat → Int) (i : Nat) (h : List Int) :
    ∀ xs, (encode p side i h xs).length = xs.length := by
  intro xs
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih => simp [encode, ih]

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
  have hmprops := List.max?_eq_some_iff.mp hm
  have hp : peak xs = m := by simp [peak, hm]
  rw [hp]
  exact hmprops.1

private theorem encode_lt_of_certificate (p : Predict) (side : Nat → Int) (i : Nat)
    (h : List Int) (xs : List Int) (w : Nat) (hc : certificate p side i h xs w) :
    ∀ e ∈ encode p side i h xs, e < 2 ^ w := by
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih =>
      dsimp [certificate] at hc
      have hhead : zig (x - p i h side) < 2 ^ w := by
        have hh := hc 0 (by simp)
        simpa using hh
      have hctail : certificate p side (i + 1) (x :: h) xs w := by
        intro j hj
        have hc1 := hc (j + 1) (by simpa using Nat.succ_lt_succ hj)
        rw [show i + (j + 1) = i + (1 + j) by omega] at hc1
        simpa [Nat.add_assoc, List.take_succ_cons, List.reverse_cons, List.append_assoc] using hc1
      have htail := ih (i + 1) (x :: h) hctail
      simp only [encode, List.mem_cons]
      intro e he
      rcases he with rfl | he
      · exact hhead
      · exact htail e he

private theorem history_irrelevant (p : Predict) (side : Nat → Int)
    (hind : ∀ i h, p i h side = p i [] side) :
    ∀ (i : Nat) (h h' : List Int) (xs : List Int),
      encode p side i h xs = encode p side i h' xs ∧
      ∀ es, restore p side i h es = restore p side i h' es := by
  intro i h h' xs
  constructor
  · induction xs generalizing i h h' with
    | nil => simp [encode]
    | cons x xs ih =>
        simp only [encode]
        rw [hind i h, hind i h']
        rw [ih (i + 1) (x :: h) (x :: h')]
  · intro es
    induction es generalizing i h h' with
    | nil => simp [restore]
    | cons e es ih =>
        simp only [restore]
        rw [hind i h, hind i h']
        rw [ih (i + 1) ((p i [] side + unzig e) :: h) ((p i [] side + unzig e) :: h')]

/-! ## wire arithmetic helpers (local, so this file is self-contained) -/

private theorem width_eq (n : Nat) (hn : n ≠ 0) : width n = n.log2 + 1 := by
  unfold width
  simp [hn]

private theorem lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  by_cases hn : n = 0
  · subst n
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

private theorem width_le_of_lt_pow {n w : Nat} (h : n < 2 ^ w) : width n ≤ w := by
  by_cases hn : n = 0
  · subst n
    simp [width]
  · exact (width_le_iff n w hn).mpr h

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
          have hpow : 128 ^ (k + 1 + 1) = 128 ^ (k + 1) * 128 := by
            rw [show k + 1 + 1 = (k + 1) + 1 by omega, Nat.pow_succ]
          rwa [hpow]
        · intro h
          have hpow : 128 ^ (k + 1 + 1) = 128 ^ (k + 1) * 128 := by
            rw [show k + 1 + 1 = (k + 1) + 1 by omega, Nat.pow_succ]
          have hdiv : n / 128 < 128 ^ (k + 1) := by
            rw [Nat.div_lt_iff_lt_mul (by decide : 0 < 128)]
            rwa [hpow] at h
          have hd := hih.mpr hdiv
          omega

private theorem uleb_len_le (n k : Nat) (hk : 0 < k) : (uleb n).length ≤ k ↔ n < 128 ^ k := by
  obtain ⟨j, rfl⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hk)
  exact uleb_len_le_succ n j

private theorem pack_length (w : Nat) (xs : List Nat) :
    (pack w xs).length = byteCount (xs.length * w) := by
  simp [pack, little]

private theorem byteCount_mono {a b : Nat} (h : a ≤ b) : byteCount a ≤ byteCount b := by
  unfold byteCount
  exact Nat.div_le_div_right (Nat.add_le_add_right h 7)

private theorem range_map_shift (f : Nat → Int) (i n : Nat) :
    (List.range (n + 1)).map (fun j => f (i + j)) =
      f i :: (List.range n).map (fun j => f ((i + 1) + j)) := by
  apply List.ext_getElem
  · simp
  · intro k hk1 hk2
    cases k with
    | zero => simp
    | succ k =>
        simp only [List.getElem_cons_succ, List.getElem_map, List.getElem_range]
        congr 1
        omega

/-! ## the four SideQuotient goals -/

theorem sound_ResidualRoundTrip :
    TinylambOpt.ResearchGoals.Compression.Predictors.SideQuotient.ResidualRoundTrip.Goal := by
  intro period side i h xs
  exact encode_restore (P period) side i h xs

theorem sound_ByteBudget :
    TinylambOpt.ResearchGoals.Compression.Predictors.SideQuotient.ByteBudget.Goal := by
  intro period side i h xs w extra oldBytes hc hlt
  let p := P period
  let es := encode p side i h xs
  have hall : ∀ e ∈ es, e < 2 ^ w := by
    dsimp [es]
    exact encode_lt_of_certificate p side i h xs w (by simpa [p] using hc)
  have hwle : width (peak es) ≤ w := by
    by_cases he : es = []
    · rw [he]
      simp [peak, width]
    · have hp : peak es < 2 ^ w := hall (peak es) (peak_mem he)
      exact width_le_of_lt_pow hp
  have hlen : es.length = xs.length := by
    dsimp [es]
    exact encode_length p side i h xs
  have hframe : (frame es).length ≤ quote xs.length w := by
    simp only [frame, quote, List.length_append, List.length_cons, List.length_nil, Nat.zero_add]
    have hposw : 0 < ubytes w := ubytes_pos w
    have hwlt : w < 128 ^ ubytes w :=
      (ubytes_le_iff w (ubytes w) hposw).mp (by omega)
    have hulebN : (uleb es.length).length ≤ ubytes es.length := by
      apply (uleb_len_le es.length (ubytes es.length) (ubytes_pos es.length)).mpr
      exact (ubytes_le_iff es.length (ubytes es.length) (ubytes_pos es.length)).mp (by omega)
    have hulebW : (uleb (width (peak es))).length ≤ ubytes w := by
      apply (uleb_len_le (width (peak es)) (ubytes w) hposw).mpr
      exact Nat.lt_of_le_of_lt hwle hwlt
    rw [pack_length]
    rw [hlen] at hulebN ⊢
    have hmul : xs.length * width (peak es) ≤ xs.length * w := Nat.mul_le_mul_left _ hwle
    have hbc := byteCount_mono hmul
    omega
  have hres : extra + (frame es).length < oldBytes := by omega
  simpa [es] using hres

theorem sound_RestartSeed :
    TinylambOpt.ResearchGoals.Compression.Predictors.SideQuotient.RestartSeed.Goal := by
  intro period side i h xs
  have hind : ∀ j (hh : List Int), P period j hh side = P period j [] side := by
    intro j hh
    rfl
  exact (history_irrelevant (P period) side hind i (h.take 0) h xs).1

theorem sound_DateProjection :
    TinylambOpt.ResearchGoals.Compression.Predictors.SideQuotient.DateProjection.Goal := by
  intro period side i n
  have aux : ∀ (j : Nat) (hh : List Int) (m : Nat),
      restore (P period) side j hh (List.replicate m 0) =
        (List.range m).map (fun t => side (j + t) / ((period : Int) + 1)) := by
    intro j hh m
    induction m generalizing j hh with
    | zero => simp [restore]
    | succ m ih =>
        simp only [restore, List.replicate_succ]
        simp [TinylambOpt.ResearchGoals.Compression.Predictors.SideQuotient.predict, unzig]
        rw [ih (j + 1) ((side j / ((period : Int) + 1)) :: hh)]
        rw [range_map_shift (fun k => side k / ((period : Int) + 1)) j m]
  simpa using aux i [] n

end TinylambOpt.ResearchProofs.Compression.Predictors.SideQuotient

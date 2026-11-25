import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.Round2.Task51

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.PagedTombstones

-- =====================================================================
-- Generic list helpers
-- =====================================================================

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem filterMap_congr' {α β : Type} {l : List α} {f g : α → Option β}
    (h : ∀ x ∈ l, f x = g x) : l.filterMap f = l.filterMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filterMap_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem range_filterMap_all {α : Type} (n : Nat) (f : Nat → Option α) (g : Nat → α)
    (h : ∀ i, i < n → f i = some (g i)) :
    (List.range n).filterMap f = (List.range n).map g := by
  rw [← filterMap_some_eq_map (List.range n) g]
  apply filterMap_congr'
  intro i hi
  exact h i (List.mem_range.mp hi)

private theorem decide_eq_of_iff {a b : Prop} [Decidable a] [Decidable b] (h : a ↔ b) :
    decide a = decide b := by
  by_cases ha : a
  · have hb : b := h.mp ha
    simp [ha, hb]
  · have hb : ¬ b := fun hb => ha (h.mpr hb)
    simp [ha, hb]

private theorem filter_congr_bool {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, p x = q x) : l.filter p = l.filter q := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filter_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem map_pair_filter {α β : Type} (xs : List α) (f : α → Nat) (g : α → β)
    (p : Nat → Bool) :
    (xs.map (fun x => (f x, g x))).filter (fun r => p r.1) =
      (xs.filter (fun x => p (f x))).map (fun x => (f x, g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => by_cases h : p (f x) <;> simp [h, ih]

private theorem sum_map_add_int {α : Type} (l : List α) (f g : α → Int) :
    ((l.map fun x => f x + g x).sum) = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_map_sub {α : Type} (l : List α) (f g : α → Int) :
    ((l.map fun x => f x - g x).sum) = (l.map f).sum - (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem countP_eq_sum {α : Type} (l : List α) (p : α → Bool) :
    l.countP p = (l.map (fun x => if p x then 1 else 0)).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih] <;> omega

private theorem sum_map_const_int {α : Type} (l : List α) (v : Int) :
    (l.map (fun _ => v)).sum = (l.length : Int) * v := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.natCast_add, Int.add_mul]; omega

private theorem sum_range_succ (f : Nat → Int) (n : Nat) :
    ((List.range (n + 1)).map f).sum = ((List.range n).map f).sum + f n := by
  rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
  simp

private theorem sum_natCast_eq {α : Type} (l : List α) (g : α → Nat) :
    ((l.map (fun x => (g x : Int))).sum) = (((l.map g).sum : Nat) : Int) := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.natCast_add]

private theorem choose2_succ (k : Nat) :
    TinylambOpt.ResearchGoals.PairAggregates.choose2 (k + 1) =
      TinylambOpt.ResearchGoals.PairAggregates.choose2 k + k := by
  unfold TinylambOpt.ResearchGoals.PairAggregates.choose2
  by_cases hk : k = 0
  · simp [hk]
  · have hkm1 : k + 1 - 1 = k := by omega
    rw [hkm1]
    have hmul1 : (k + 1) * k = k * k + k := by
      rw [Nat.add_mul, Nat.one_mul]
    have hmul2 : k * (k - 1) = k * k - k := by
      rw [Nat.mul_sub_left_distrib, Nat.mul_one]
    have hle : k ≤ k * k := by
      have := Nat.mul_le_mul_left k (show 1 ≤ k by omega)
      simpa using this
    have hEq : (k + 1) * k = k * (k - 1) + 2 * k := by
      rw [hmul1, hmul2]
      omega
    rw [hEq]
    exact Nat.add_mul_div_left (k * (k - 1)) k (by decide : 0 < 2)

private theorem tri_succ (k : Nat) : tri (k + 1) = tri k + (k : Int) := by
  dsimp [tri]
  rw [choose2_succ]
  simp [Int.natCast_add]

private theorem squares_succ (k : Nat) :
    squares (k + 1) = squares k + (k : Nat) * k := by
  unfold squares
  have hkm1 : k + 1 - 1 = k := by omega
  rw [hkm1]
  have h2 : 2 * (k + 1) - 1 = 2 * k + 1 := by omega
  rw [h2]
  have hp : (k + 1) * k * (2 * k + 1) = k * (k - 1) * (2 * k - 1) + 6 * (k * k) := by
    by_cases hk : k = 0
    · simp [hk]
    · obtain ⟨m, rfl⟩ := Nat.exists_eq_succ_of_ne_zero hk
      have hm : m + 1 - 1 = m := by omega
      have h2m : 2 * (m + 1) - 1 = 2 * m + 1 := by omega
      rw [hm, h2m]
      change (m + 1 + 1) * (m + 1) * (2 * (m + 1) + 1) =
        (m + 1) * m * (2 * m + 1) + 6 * ((m + 1) * (m + 1))
      grind
  rw [hp, Nat.add_mul_div_left _ _ (by decide : 0 < 6)]
  simp [Int.natCast_add, Int.natCast_mul]

private theorem partialSum_succ (a : Lane) (k : Nat) :
    partialSum a (k + 1) = partialSum a k + (a.base + (k : Int) * a.step) := by
  rw [partialSum, partialSum, tri_succ]
  simp [Int.natCast_add, Int.add_mul]
  omega

private theorem partialSum_eq_sum (a : Lane) (k : Nat) :
    partialSum a k = ((List.range k).map (fun i : Nat => a.base + (i : Int) * a.step)).sum := by
  induction k with
  | zero => simp [partialSum, tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ k ih =>
      rw [partialSum_succ, ih]
      rw [List.range_succ, List.map_append, List.sum_append]
      simp

-- =====================================================================
-- Arithmetic progressions: membership and intersection
-- (adapted from ResearchProofs/Atlas/EncodedPages4.lean)
-- =====================================================================

private def progVal (a : Progression) (k : Nat) : Int := a.lo + (k : Int) * a.step

private def progMem (a : Progression) (v : Int) : Prop :=
  ∃ k, k < a.n ∧ v = progVal a k

private theorem contains_iff_mem (a : Progression) (v : Int) :
    contains a v = true ↔ progMem a v := by
  unfold contains lastVal progMem progVal
  constructor
  · intro h
    have h' : 0 < a.n ∧ a.lo ≤ v ∧ v ≤ a.lo + ((a.n - 1 : Nat) : Int) * a.step ∧
        (a.step == 0 || (v - a.lo) % (a.step : Int) == 0) = true := by
      simpa only [Bool.and_eq_true, decide_eq_true_eq, and_assoc] using h
    rcases h' with ⟨hn, hlo, hhi, hmod⟩
    by_cases hs : a.step == 0
    · have hs' : a.step = 0 := Nat.eq_of_beq_eq_true hs
      refine ⟨0, hn, ?_⟩
      simp only [hs', Int.mul_zero, Int.add_zero] at hhi hlo ⊢
      omega
    · have hs' : a.step ≠ 0 := by simpa using hs
      have hpos : (0 : Int) < a.step := by exact_mod_cast Nat.pos_of_ne_zero hs'
      have hmod' : (v - a.lo) % (a.step : Int) = 0 := by
        simpa [hs] using hmod
      let q := (v - a.lo) / a.step
      have hqmul : a.step * q = v - a.lo := by
        simpa only [Int.mul_comm] using Int.ediv_mul_cancel_of_emod_eq_zero hmod'
      have hqmul' : q * a.step = v - a.lo := by simpa only [Int.mul_comm] using hqmul
      have hqnon : 0 ≤ q := Int.nonneg_of_mul_nonneg_right (by omega) hpos
      have hqle : q ≤ (a.n - 1 : Nat) := by
        apply Int.le_of_mul_le_mul_left (b := q) (c := (a.n - 1 : Nat)) _ hpos
        change a.step * ((v - a.lo) / a.step) ≤ a.step * (a.n - 1 : Nat)
        have hh : a.step * ((v - a.lo) / a.step) = v - a.lo := by
          simpa only [Int.mul_comm] using Int.ediv_mul_cancel_of_emod_eq_zero hmod'
        rw [hh]
        have hh2 := Int.sub_le_sub_right hhi a.lo
        have hcancel : a.lo + ((a.n - 1 : Nat) : Int) * a.step - a.lo =
            ((a.n - 1 : Nat) : Int) * a.step := by omega
        rw [hcancel] at hh2
        simpa only [Int.mul_comm] using hh2
      have hqnat : q.toNat < a.n := by
        have hle : q.toNat ≤ a.n - 1 := (Int.toNat_le (m := q) (n := a.n - 1)).mpr hqle
        omega
      refine ⟨q.toNat, hqnat, ?_⟩
      rw [Int.toNat_of_nonneg hqnon]
      symm
      calc
        a.lo + q * a.step = a.lo + (v - a.lo) := congrArg (fun x : Int => a.lo + x) hqmul'
        _ = v := by omega
  · rintro ⟨k, hk, rfl⟩
    change (decide (0 < a.n) && decide (a.lo ≤ a.lo + (k : Int) * a.step) &&
      decide (a.lo + (k : Int) * a.step ≤ a.lo + ((a.n - 1 : Nat) : Int) * a.step) &&
      (a.step == 0 || (a.lo + (k : Int) * a.step - a.lo) % (a.step : Int) == 0)) = true
    have hsub : a.lo + (k : Int) * a.step - a.lo = (k : Int) * a.step := by omega
    rw [hsub]
    by_cases hs : a.step = 0
    · simp [hs]
      omega
    · have hpos : (0 : Int) < a.step := by exact_mod_cast Nat.pos_of_ne_zero hs
      have hkint : (k : Int) ≤ (a.n - 1 : Nat) := by exact_mod_cast (by omega)
      have hmul : a.step * (k : Int) ≤ a.step * (a.n - 1 : Nat) := by
        simpa only [Int.mul_comm] using Int.mul_le_mul_of_nonneg_left hkint (Int.le_of_lt hpos)
      have hmul' : (k : Int) * a.step ≤ ((a.n - 1 : Nat) : Int) * a.step := by
        simpa only [Int.mul_comm] using hmul
      simp only [Bool.and_eq_true, decide_eq_true_eq, and_assoc]
      refine ⟨by omega, ?_, ?_, ?_⟩
      · exact Int.le_add_of_nonneg_right (Int.mul_nonneg (by omega) (Int.le_of_lt hpos))
      · exact Int.add_le_add_left hmul' a.lo
      · rw [Bool.or_eq_true]
        right
        change (((k : Int) * a.step) % a.step == 0) = true
        rw [beq_iff_eq]
        apply Int.emod_eq_zero_of_dvd
        exact ⟨(k : Int), Int.mul_comm _ _⟩

private theorem bezout_spec (a b : Nat) :
    (a.gcd b : Int) = (a : Int) * (bezout a b).1 + (b : Int) * (bezout a b).2 := by
  induction b using Nat.strongRecOn generalizing a with
  | ind b ih =>
      by_cases hb : b = 0
      · subst b
        simp [bezout]
      · have hmod : a % b < b := Nat.mod_lt a (Nat.pos_of_ne_zero hb)
        have hrec := ih (a % b) hmod b
        have hfst : (bezout a b).1 = (bezout b (a % b)).2 := by
          rw [bezout.eq_1]
          simp [hb]
        have hsnd : (bezout a b).2 =
            (bezout b (a % b)).1 - ((a / b : Nat) : Int) * (bezout b (a % b)).2 := by
          rw [bezout.eq_1]
          simp [hb]
        rw [hfst, hsnd]
        rcases hbez : bezout b (a % b) with ⟨x, y⟩
        simp only [hbez]
        have hgcd : a.gcd b = b.gcd (a % b) := by
          rw [Nat.gcd_comm a b, Nat.gcd_rec b a, Nat.gcd_comm]
        rw [hgcd, hrec]
        have hd : (a : Int) = (b : Int) * ((a / b : Nat) : Int) + ((a % b : Nat) : Int) := by
          exact_mod_cast (Nat.div_add_mod a b).symm
        rw [hd]
        grind

private theorem reduced_coprime (a b : Nat) (ha : 0 < a) (hb : 0 < b) :
    (a / a.gcd b).gcd (b / a.gcd b) = 1 := by
  exact Nat.gcd_div_gcd_div_gcd_of_pos_left ha

private theorem bezout_reduced_inverse (a b : Nat) (ha : 0 < a) (hb : 0 < b) :
    ((((a / a.gcd b : Nat) : Int) * (bezout (a / a.gcd b) (b / a.gcd b)).1) - 1) %
        (b / a.gcd b : Nat) = 0 := by
  let g := a.gcd b
  let A := a / g
  let B := b / g
  have hg : 0 < g := Nat.gcd_pos_of_pos_left b ha
  have hA : 0 < A := Nat.div_gcd_pos_of_pos_left b ha
  have hB : 0 < B := Nat.div_gcd_pos_of_pos_right a hb
  have hcop : A.gcd B = 1 := by
    simpa [A, B, g] using reduced_coprime a b ha hb
  have hspec := bezout_spec A B
  rw [hcop] at hspec
  simp only [Int.ofNat_one] at hspec
  apply Int.emod_eq_zero_of_dvd
  refine ⟨-(bezout A B).2, ?_⟩
  change (A : Int) * (bezout A B).1 - 1 = (B : Int) * -(bezout A B).2
  rw [hspec]
  grind

private theorem mem_of_bounds_dvd (a : Progression) (v : Int) (hn : 0 < a.n) (hs : 0 < a.step)
    (hlo : a.lo ≤ v) (hhi : v ≤ lastVal a) (hdvd : (a.step : Int) ∣ v - a.lo) :
    contains a v = true := by
  obtain ⟨q, hq⟩ := hdvd
  have hpos : (0 : Int) < a.step := by exact_mod_cast hs
  have hqnon : 0 ≤ q := Int.nonneg_of_mul_nonneg_right (by omega) hpos
  have hqle : q ≤ (a.n - 1 : Nat) := by
    apply Int.le_of_mul_le_mul_left (b := q) (c := (a.n - 1 : Nat)) _ hpos
    rw [← hq]
    unfold lastVal at hhi
    rw [Int.mul_comm ((a.n - 1 : Nat) : Int) a.step] at hhi
    omega
  apply (contains_iff_mem a v).mpr
  refine ⟨q.toNat, ?_, ?_⟩
  · have hle : q.toNat ≤ a.n - 1 := (Int.toNat_le (m := q) (n := a.n - 1)).mpr hqle
    omega
  · unfold progVal
    rw [Int.toNat_of_nonneg hqnon]
    change v = a.lo + q * a.step
    rw [Int.mul_comm a.step q] at hq
    omega

private theorem span_contains_iff (first step high x : Int) (hs : 0 < step) :
    let n := if first ≤ high then ((high - first) / step).toNat + 1 else 0
    contains ⟨first, step.toNat, n⟩ x = true ↔
      first ≤ x ∧ x ≤ high ∧ (step : Int) ∣ x - first := by
  dsimp only
  constructor
  · intro hx
    have hm := (contains_iff_mem ⟨first, step.toNat, if first ≤ high then ((high - first) / step).toNat + 1 else 0⟩ x).mp hx
    obtain ⟨k, hk, hval⟩ := hm
    unfold progVal at hval
    have hstepcast : (step.toNat : Int) = step := Int.toNat_of_nonneg (by omega)
    simp only [hstepcast] at hval
    by_cases hfh : first ≤ high
    · change k < (if first ≤ high then ((high - first) / step).toNat + 1 else 0) at hk
      rw [if_pos hfh] at hk
      have ht : 0 ≤ (high - first) / step :=
        Int.ediv_nonneg (by omega) (Int.le_of_lt hs)
      have htcast : (((high - first) / step).toNat : Int) = (high - first) / step :=
        Int.toNat_of_nonneg ht
      have hkq : (k : Int) ≤ (high - first) / step :=
        (Int.le_ediv_iff_mul_le hs).mpr (by
          have hkNat : k ≤ ((high - first) / step).toNat := by omega
          have hkInt : (k : Int) ≤ (((high - first) / step).toNat : Int) := by exact_mod_cast hkNat
          calc
            (k : Int) * step ≤ (((high - first) / step).toNat : Int) * step :=
              Int.mul_le_mul_of_nonneg_right hkInt (Int.le_of_lt hs)
            _ = ((high - first) / step) * step := by rw [htcast]
            _ ≤ high - first := Int.ediv_mul_le _ (by omega))
      have hkle : (k : Int) * step ≤ high - first :=
        (Int.le_ediv_iff_mul_le hs).mp hkq
      refine ⟨?_, ?_, ?_⟩
      · rw [hval]
        exact Int.le_add_of_nonneg_right (Int.mul_nonneg (by omega) (Int.le_of_lt hs))
      · calc
          x = first + (k : Int) * step := hval
          _ ≤ first + (high - first) := Int.add_le_add_left hkle first
          _ = high := by omega
      · refine ⟨(k : Int), ?_⟩
        calc
          x - first = (k : Int) * step := by omega
          _ = step * (k : Int) := Int.mul_comm _ _
    · simp [hfh] at hk
  · rintro ⟨hlo, hhi, hdvd⟩
    have hfh : first ≤ high := by omega
    rw [if_pos hfh]
    obtain ⟨q, hq⟩ := hdvd
    have hqnon : 0 ≤ q := Int.nonneg_of_mul_nonneg_right (by omega) hs
    have htcast : (((high - first) / step).toNat : Int) = (high - first) / step :=
      Int.toNat_of_nonneg (Int.ediv_nonneg (by omega) (Int.le_of_lt hs))
    have hqle : q ≤ (high - first) / step := by
      apply (Int.le_ediv_iff_mul_le hs).mpr
      calc
        q * step = x - first := by rw [Int.mul_comm q step]; exact hq.symm
        _ ≤ high - first := Int.sub_le_sub_right hhi first
    apply (contains_iff_mem ⟨first, step.toNat, ((high - first) / step).toNat + 1⟩ x).mpr
    refine ⟨q.toNat, ?_, ?_⟩
    · have hle : q.toNat ≤ ((high - first) / step).toNat := by
        apply (Int.toNat_le (m := q) (n := ((high - first) / step).toNat)).mpr
        rw [htcast]
        exact hqle
      exact Nat.lt_succ_of_le hle
    · unfold progVal
      rw [Int.toNat_of_nonneg hqnon]
      change x = first + q * (step.toNat : Int)
      rw [Int.toNat_of_nonneg (by omega : 0 ≤ step)]
      rw [Int.mul_comm step q] at hq
      omega

private theorem root_dvd (as bs : Nat) (ha : 0 < as) (hb : 0 < bs) (alo blo gap : Int)
    (hgapdef : gap = blo - alo) (hgap : (as.gcd bs : Int) ∣ gap) :
    let g := as.gcd bs
    let A := as / g
    let B := bs / g
    let root := alo + (as : Int) * ((gap / (g : Int) * (bezout A B).1) % (B : Int))
    (as : Int) ∣ root - alo ∧ (bs : Int) ∣ root - blo := by
  dsimp only
  let g := as.gcd bs
  let A := as / g
  let B := bs / g
  let t := (gap / (g : Int) * (bezout A B).1) % (B : Int)
  have hg : 0 < g := Nat.gcd_pos_of_pos_left bs ha
  have hA : 0 < A := Nat.div_gcd_pos_of_pos_left bs ha
  have hB : 0 < B := Nat.div_gcd_pos_of_pos_right as hb
  have hasg : (as : Int) = (g : Int) * (A : Int) := by
    exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_left as bs)).symm
  have hbsg : (bs : Int) = (g : Int) * (B : Int) := by
    exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_right as bs)).symm
  obtain ⟨z, hz⟩ := hgap
  have hzdiv : gap / (g : Int) = z := Int.ediv_eq_of_eq_mul_right (by omega) hz
  have hinv := bezout_reduced_inverse as bs ha hb
  have hinv' : (B : Int) ∣ (A : Int) * (bezout A B).1 - 1 :=
    Int.dvd_of_emod_eq_zero (by simpa [A, B, g] using hinv)
  have hmod := Int.mul_ediv_add_emod (z * (bezout A B).1) (B : Int)
  have hrem : z * (bezout A B).1 - t =
      (B : Int) * ((z * (bezout A B).1) / (B : Int)) := by
    dsimp [t]
    rw [hzdiv]
    omega
  have hdiff : (B : Int) ∣ z * ((A : Int) * (bezout A B).1 - 1) :=
    Int.dvd_mul_of_dvd_right hinv'
  have hmult : (B : Int) ∣ (A : Int) * (z * (bezout A B).1 - t) :=
    Int.dvd_mul_of_dvd_right (Int.dvd_of_emod_eq_zero (by
      rw [hrem]
      exact Int.emod_eq_zero_of_dvd ⟨_, rfl⟩))
  have hcomb : (B : Int) ∣ (A : Int) * t - z := by
    have heq : z * ((A : Int) * (bezout A B).1 - 1) -
        (A : Int) * (z * (bezout A B).1 - t) = (A : Int) * t - z := by grind
    rw [← heq]
    exact Int.dvd_sub hdiff hmult
  have hbs : (bs : Int) ∣ (as : Int) * t - gap := by
    rw [hbsg, hasg, hz]
    have hh := Int.mul_dvd_mul_left (g : Int) hcomb
    simpa only [Int.mul_sub, Int.mul_assoc] using hh
  constructor
  · have hroot : alo + (as : Int) * ((gap / (g : Int) * (bezout A B).1) % (B : Int)) - alo =
        (as : Int) * t := by dsimp [t]; omega
    rw [hroot]
    exact ⟨t, rfl⟩
  · have hroot : alo + (as : Int) * ((gap / (g : Int) * (bezout A B).1) % (B : Int)) - blo =
        (as : Int) * t - gap := by dsimp [t]; omega
    rw [hroot]
    exact hbs

private theorem first_root_dvd (low root first period : Int) (hp : 0 < period)
    (hfirst : first = low + (root - low) % period) : period ∣ first - root := by
  have hm := Int.mul_ediv_add_emod (root - low) period
  refine ⟨-((root - low) / period), ?_⟩
  grind

private theorem first_le_of_congruence (low root first x period : Int) (hp : 0 < period)
    (hfirst : first = low + (root - low) % period) (hx : low ≤ x)
    (hxdvd : period ∣ x - root) : first ≤ x := by
  have hremnon : 0 ≤ (root - low) % period := Int.emod_nonneg _ (by omega)
  have hremlt : (root - low) % period < period := Int.emod_lt_of_pos _ hp
  have hflow : low ≤ first := by omega
  have hfhigh : first < low + period := by omega
  have hfroot := first_root_dvd low root first period hp hfirst
  by_cases hle : first ≤ x
  · exact hle
  · exfalso
    have hlt : x < first := by omega
    have hpos : 0 < first - x := by omega
    have hltp : first - x < period := by omega
    have hdiff : period ∣ first - x := by
      have heq : (first - root) - (x - root) = first - x := by omega
      rw [← heq]
      exact Int.dvd_sub hfroot hxdvd
    obtain ⟨k, hk⟩ := hdiff
    have hkpos : 0 < k := Int.pos_of_mul_pos_right (by omega) hp
    have hkge : 1 ≤ k := by omega
    have hmul := Int.mul_le_mul_of_nonneg_left hkge (Int.le_of_lt hp)
    omega

private theorem contains_const_iff (lo : Int) (n : Nat) (v : Int) :
    contains ⟨lo, 0, n⟩ v = true ↔ 0 < n ∧ v = lo := by
  rw [contains_iff_mem]
  constructor
  · rintro ⟨k, hk, hv⟩
    unfold progVal at hv
    simp only [Int.ofNat_zero, Int.mul_zero, Int.add_zero] at hv
    change k < n at hk
    have hnpos : 0 < n := by omega
    exact ⟨hnpos, hv⟩
  · rintro ⟨hn, hv⟩
    subst v
    exact ⟨0, hn, by simp [progVal]⟩

private theorem contains_bounds (a : Progression) (v : Int) (h : contains a v = true) :
    a.lo ≤ v ∧ v ≤ lastVal a := by
  have hm := (contains_iff_mem a v).mp h
  obtain ⟨k, hk, hval⟩ := hm
  unfold progVal at hval
  have hkint : (k : Int) ≤ (a.n - 1 : Nat) := by exact_mod_cast (by omega)
  have hmul := Int.mul_le_mul_of_nonneg_right hkint (by omega : 0 ≤ (a.step : Int))
  have hmul' : (k : Int) * a.step ≤ ((a.n - 1 : Nat) : Int) * a.step := hmul
  unfold lastVal
  constructor
  · rw [hval]
    exact Int.le_add_of_nonneg_right (Int.mul_nonneg (by omega) (by omega))
  · rw [hval]
    exact Int.add_le_add_left hmul' a.lo

private theorem contains_dvd (a : Progression) (v : Int) (h : contains a v = true) :
    (a.step : Int) ∣ v - a.lo := by
  obtain ⟨k, _, hval⟩ := (contains_iff_mem a v).mp h
  unfold progVal at hval
  refine ⟨(k : Int), ?_⟩
  rw [hval]
  grind

private theorem common_gap_dvd (a b : Progression) (v : Int)
    (ha : contains a v = true) (hb : contains b v = true) :
    (a.step.gcd b.step : Int) ∣ b.lo - a.lo := by
  obtain ⟨i, _, hi⟩ := (contains_iff_mem a v).mp ha
  obtain ⟨j, _, hj⟩ := (contains_iff_mem b v).mp hb
  unfold progVal at hi hj
  have hg_dvd_b : (a.step.gcd b.step : Int) ∣ (b.step : Int) := by
    exact_mod_cast Nat.gcd_dvd_right a.step b.step
  have hg_dvd_a : (a.step.gcd b.step : Int) ∣ (a.step : Int) := by
    exact_mod_cast Nat.gcd_dvd_left a.step b.step
  have h1 : (a.step.gcd b.step : Int) ∣ (b.step : Int) * (j : Int) :=
    Int.dvd_mul_of_dvd_left (c := (j : Int)) hg_dvd_b
  have h2 : (a.step.gcd b.step : Int) ∣ (a.step : Int) * (i : Int) :=
    Int.dvd_mul_of_dvd_left (c := (i : Int)) hg_dvd_a
  have heq : (a.step : Int) * (i : Int) - (b.step : Int) * (j : Int) = b.lo - a.lo := by
    rw [Int.mul_comm (a.step : Int) (i : Int), Int.mul_comm (b.step : Int) (j : Int)]
    have hvi : v - a.lo = (i : Int) * a.step := by rw [hi]; grind
    have hvj : v - b.lo = (j : Int) * b.step := by rw [hj]; grind
    omega
  rw [← heq]
  exact Int.dvd_sub h2 h1

private theorem contains_prog_one (lo : Int) (k : Nat) (hk : k ≤ 1) (v : Int) :
    contains ⟨lo, 1, k⟩ v = true ↔ 0 < k ∧ v = lo := by
  rw [contains_iff_mem]
  constructor
  · rintro ⟨i, hi, hv⟩
    unfold progVal at hv
    change i < k at hi
    have hi0 : i = 0 := by omega
    have hkpos : 0 < k := by omega
    subst i
    simp only [Int.ofNat_zero, Int.zero_mul, Int.add_zero] at hv
    exact ⟨hkpos, hv⟩
  · rintro ⟨hk, hv⟩
    subst v
    exact ⟨0, hk, by simp [progVal]⟩

private theorem period_dvd_delta (s t : Nat) (hs : 0 < s) (ht : 0 < t) (delta : Int)
    (hds : (s : Int) ∣ delta) (hdt : (t : Int) ∣ delta) :
    (((s / Nat.gcd s t) * t : Nat) : Int) ∣ delta := by
  let g := Nat.gcd s t
  let sa := s / g
  let tb := t / g
  have hg : 0 < g := Nat.gcd_pos_of_pos_left t hs
  have hsEq : (s : Int) = (g : Int) * (sa : Int) := by
    exact_mod_cast (by simpa [sa, g, Nat.mul_comm] using (Nat.div_mul_cancel (Nat.gcd_dvd_left s t)).symm)
  have htEq : (t : Int) = (g : Int) * (tb : Int) := by
    exact_mod_cast (by simpa [tb, g, Nat.mul_comm] using (Nat.div_mul_cancel (Nat.gcd_dvd_right s t)).symm)
  have hcop : Nat.gcd sa tb = 1 := Nat.coprime_div_gcd_div_gcd hg
  obtain ⟨k1, hk1⟩ := hds
  obtain ⟨k2, hk2⟩ := hdt
  rcases hpair : bezout sa tb with ⟨u, v⟩
  have hbez := bezout_spec sa tb
  rw [hpair] at hbez
  change (Nat.gcd sa tb : Int) = (sa : Int) * u + (tb : Int) * v at hbez
  rw [hcop] at hbez
  refine ⟨u * k2 + v * k1, ?_⟩
  change delta = ((sa * t : Nat) : Int) * (u * k2 + v * k1)
  grind

private theorem intersection_contains (a b : Progression) (v : Int) :
    contains (intersection a b) v = true ↔ contains a v = true ∧ contains b v = true := by
  rcases a with ⟨alo, ast, an⟩
  rcases b with ⟨blo, bst, bn⟩
  by_cases han : an = 0
  · subst an
    simp [intersection, contains]
  · by_cases hbn : bn = 0
    · subst bn
      simp [intersection, contains]
    · have hanp : 0 < an := by omega
      have hbnp : 0 < bn := by omega
      by_cases has : ast = 0
      · subst ast
        have hinter : intersection ⟨alo, 0, an⟩ ⟨blo, bst, bn⟩ =
            ⟨alo, 1, if contains ⟨blo, bst, bn⟩ alo then 1 else 0⟩ := by
          simp [intersection, han, hbn]
        rw [hinter]
        by_cases hb : contains ⟨blo, bst, bn⟩ alo = true
        · simp only [hb, if_true]
          rw [contains_prog_one _ _ (by decide)]
          constructor
          · intro hv
            exact ⟨(contains_const_iff alo an v).mpr ⟨by omega, hv.2⟩, hv.2 ▸ hb⟩
          · rintro ⟨ha, _⟩
            have hv : v = alo := (contains_const_iff alo an v).mp ha |>.2
            exact ⟨by omega, hv⟩
        · have hbf : contains ⟨blo, bst, bn⟩ alo = false := by
            cases h : contains ⟨blo, bst, bn⟩ alo <;> simp_all
          simp only [hb, if_false]
          rw [contains_prog_one _ _ (by decide)]
          constructor
          · rintro ⟨hk, _⟩
            change 0 < 0 at hk
            omega
          · rintro ⟨ha, hbv⟩
            exfalso
            have hv : v = alo := (contains_const_iff alo an v).mp ha |>.2
            have hbfv : contains ⟨blo, bst, bn⟩ v = false := by simpa [hv] using hbf
            rw [hbfv] at hbv
            cases hbv
      · by_cases hbs : bst = 0
        · subst bst
          have hinter : intersection ⟨alo, ast, an⟩ ⟨blo, 0, bn⟩ =
              ⟨blo, 1, if contains ⟨alo, ast, an⟩ blo then 1 else 0⟩ := by
            simp [intersection, han, hbn, has]
          rw [hinter]
          by_cases ha : contains ⟨alo, ast, an⟩ blo = true
          · simp only [ha, if_true]
            rw [contains_prog_one _ _ (by decide)]
            constructor
            · intro hv
              exact ⟨hv.2 ▸ ha, (contains_const_iff blo bn v).mpr ⟨by omega, hv.2⟩⟩
            · rintro ⟨_, hbv⟩
              have hv : v = blo := (contains_const_iff blo bn v).mp hbv |>.2
              exact ⟨by omega, hv⟩
          · have haf : contains ⟨alo, ast, an⟩ blo = false := by
              cases h : contains ⟨alo, ast, an⟩ blo <;> simp_all
            simp only [ha, if_false]
            rw [contains_prog_one _ _ (by decide)]
            constructor
            · rintro ⟨hk, _⟩
              change 0 < 0 at hk
              omega
            · rintro ⟨hav, hbv⟩
              exfalso
              have hv : v = blo := (contains_const_iff blo bn v).mp hbv |>.2
              have hafv : contains ⟨alo, ast, an⟩ v = false := by simpa [hv] using haf
              rw [hafv] at hav
              cases hav
        · have hasp : 0 < ast := Nat.pos_of_ne_zero has
          have hbsp : 0 < bst := Nat.pos_of_ne_zero hbs
          let g := ast.gcd bst
          let gap := blo - alo
          let period := (ast / g) * bst
          let unit := (bezout (ast / g) (bst / g)).1
          let root := alo + (ast : Int) * ((gap / (g : Int) * unit) % ((bst / g : Nat) : Int))
          let low := max alo blo
          let first := low + (root - low) % (period : Int)
          let high := min (lastVal ⟨alo, ast, an⟩) (lastVal ⟨blo, bst, bn⟩)
          have hpNat : 0 < period := by
            dsimp [period]
            exact Nat.mul_pos (Nat.div_gcd_pos_of_pos_left bst hasp) hbsp
          have hp : (0 : Int) < period := by exact_mod_cast hpNat
          have hgapdef : gap = blo - alo := rfl
          by_cases hgapmod : gap % (g : Int) = 0
          · have hgap : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgapmod
            have hroot := root_dvd ast bst hasp hbsp alo blo gap hgapdef hgap
            have hfirst : first = low + (root - low) % (period : Int) := rfl
            have hfroot := first_root_dvd low root first period hp hfirst
            have hremnon : 0 ≤ (root - low) % (period : Int) := Int.emod_nonneg _ (by omega)
            have hflow : low ≤ first := by dsimp [first]; omega
            have hpa : (ast : Int) ∣ (period : Int) := by
              refine ⟨((bst / g : Nat) : Int), ?_⟩
              have hnat : period = ast * (bst / g) := by
                dsimp [period, g]
                have h1 : ast / ast.gcd bst * ast.gcd bst = ast :=
                  Nat.div_mul_cancel (Nat.gcd_dvd_left ast bst)
                have h2 : ast.gcd bst * (bst / ast.gcd bst) = bst :=
                  Nat.mul_div_cancel' (Nat.gcd_dvd_right ast bst)
                grind
              exact_mod_cast hnat
            have hpb : (bst : Int) ∣ (period : Int) := by
              refine ⟨((ast / g : Nat) : Int), ?_⟩
              have hnat : period = bst * (ast / g) := by
                dsimp [period, g]
                rw [Nat.mul_comm]
              exact_mod_cast hnat
            have hinter : intersection ⟨alo, ast, an⟩ ⟨blo, bst, bn⟩ =
                ⟨first, period, if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ := by
              unfold intersection
              simp [han, hbn, has, hbs, hgapmod, g, gap, period, unit, root, low, first, high]
            rw [hinter]
            have hspan := span_contains_iff first (period : Int) high v hp
            have hpernonneg : (0 : Int) ≤ (period : Int) := by exact_mod_cast (Nat.zero_le period)
            have htonat : (period : Int).toNat = period := by exact_mod_cast (Int.toNat_of_nonneg hpernonneg)
            simp only [htonat] at hspan
            rw [hspan]
            have hrootA : (ast : Int) ∣ root - alo := by simpa only [root, g, unit] using hroot.1
            have hrootB : (bst : Int) ∣ root - blo := by simpa only [root, g, unit] using hroot.2
            constructor
            · rintro ⟨hfirstx, hxhigh, hxfirst⟩
              have hxfirstA : (ast : Int) ∣ v - first := Int.dvd_trans hpa hxfirst
              have hrootAst : (ast : Int) ∣ first - root := Int.dvd_trans hpa hfroot
              have hxrootA : (ast : Int) ∣ v - root := by
                have htmp := Int.dvd_add (a := (ast : Int)) (b := v - first) (c := first - root) hxfirstA hrootAst
                have heq : v - first + (first - root) = v - root := by omega
                rwa [heq] at htmp
              have hxalo : (ast : Int) ∣ v - alo := by
                have htmp := Int.dvd_add (a := (ast : Int)) (b := v - root) (c := root - alo) hxrootA hrootA
                have heq : v - root + (root - alo) = v - alo := by omega
                rwa [heq] at htmp
              have hxfirstB : (bst : Int) ∣ v - first := Int.dvd_trans hpb hxfirst
              have hrootBst : (bst : Int) ∣ first - root := Int.dvd_trans hpb hfroot
              have hxrootB : (bst : Int) ∣ v - root := by
                have htmp := Int.dvd_add (a := (bst : Int)) (b := v - first) (c := first - root) hxfirstB hrootBst
                have heq : v - first + (first - root) = v - root := by omega
                rwa [heq] at htmp
              have hxblo : (bst : Int) ∣ v - blo := by
                have htmp := Int.dvd_add (a := (bst : Int)) (b := v - root) (c := root - blo) hxrootB hrootB
                have heq : v - root + (root - blo) = v - blo := by omega
                rwa [heq] at htmp
              have halow : alo ≤ low := by dsimp [low]; omega
              have hblow : blo ≤ low := by dsimp [low]; omega
              have hhigha : high ≤ lastVal ⟨alo, ast, an⟩ := by dsimp [high]; omega
              have hhighb : high ≤ lastVal ⟨blo, bst, bn⟩ := by dsimp [high]; omega
              constructor
              · exact mem_of_bounds_dvd ⟨alo, ast, an⟩ v hanp hasp
                  (Int.le_trans halow (Int.le_trans hflow hfirstx)) (Int.le_trans hxhigh hhigha) hxalo
              · exact mem_of_bounds_dvd ⟨blo, bst, bn⟩ v hbnp hbsp
                  (Int.le_trans hblow (Int.le_trans hflow hfirstx)) (Int.le_trans hxhigh hhighb) hxblo
            · rintro ⟨ha, hb⟩
              have hab := contains_bounds ⟨alo, ast, an⟩ v ha
              have hbb := contains_bounds ⟨blo, bst, bn⟩ v hb
              have hxalo : (ast : Int) ∣ v - alo := contains_dvd ⟨alo, ast, an⟩ v ha
              have hxblo : (bst : Int) ∣ v - blo := contains_dvd ⟨blo, bst, bn⟩ v hb
              have hxrootA : (ast : Int) ∣ v - root := by
                have htmp := Int.dvd_sub (a := (ast : Int)) (b := v - alo) (c := root - alo) hxalo hrootA
                have heq : v - alo - (root - alo) = v - root := by omega
                rwa [heq] at htmp
              have hxrootB : (bst : Int) ∣ v - root := by
                have htmp := Int.dvd_sub (a := (bst : Int)) (b := v - blo) (c := root - blo) hxblo hrootB
                have heq : v - blo - (root - blo) = v - root := by omega
                rwa [heq] at htmp
              have hrootPer : (period : Int) ∣ v - root := by
                have hpv := period_dvd_delta ast bst hasp hbsp (v - root) hxrootA hxrootB
                simpa [period, g] using hpv
              have hxfirst : (period : Int) ∣ v - first := by
                have heq : v - first = (v - root) - (first - root) := by omega
                rw [heq]
                exact Int.dvd_sub (a := (period : Int)) (b := v - root) (c := first - root) hrootPer hfroot
              have halov : alo ≤ v := (contains_bounds ⟨alo, ast, an⟩ v ha).1
              have hblov : blo ≤ v := (contains_bounds ⟨blo, bst, bn⟩ v hb).1
              have hlowv : low ≤ v := by dsimp [low]; omega
              have hhigh' : v ≤ high := by dsimp [high]; omega
              exact ⟨first_le_of_congruence low root first v period hp hfirst hlowv hrootPer, hhigh', hxfirst⟩
          · have hbne : (gap % (g : Int) == 0) = false := beq_eq_false_iff_ne.mpr hgapmod
            have hinter : intersection ⟨alo, ast, an⟩ ⟨blo, bst, bn⟩ = ⟨0, 1, 0⟩ := by
              unfold intersection
              simp [han, hbn, has, hbs, hbne, gap, g, hgapmod]
            rw [hinter]
            have hleft : (contains ⟨0, 1, 0⟩ v = true) = False := by simp [contains]
            rw [hleft]
            constructor
            · intro h
              cases h
            · rintro ⟨ha, hb⟩
              have hg := common_gap_dvd ⟨alo, ast, an⟩ ⟨blo, bst, bn⟩ v ha hb
              exact (hgapmod (Int.emod_eq_zero_of_dvd hg)).elim

private def progVals (p : Progression) : List Int :=
  (List.range p.n).map (fun k : Nat => progVal p k)

private theorem mem_progVals (p : Progression) (v : Int) :
    v ∈ progVals p ↔ contains p v = true := by
  unfold progVals
  rw [List.mem_map]
  constructor
  · rintro ⟨k, hk, hv⟩
    exact (contains_iff_mem p v).mpr ⟨k, List.mem_range.mp hk, hv.symm⟩
  · intro h
    obtain ⟨k, hk, hv⟩ := (contains_iff_mem p v).mp h
    exact ⟨k, List.mem_range.mpr hk, hv.symm⟩

private theorem nodup_progVals (p : Progression) (hp : 0 < p.step) : (progVals p).Nodup := by
  unfold progVals List.Nodup
  refine List.Pairwise.map (fun k : Nat => progVal p k) ?_ List.nodup_range
  intro a b hne hab
  unfold progVal at hab
  have hmul : (a : Int) * (p.step : Int) = (b : Int) * (p.step : Int) := by omega
  have hpne : (p.step : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hp)
  have habInt : (a : Int) = (b : Int) := Int.eq_of_mul_eq_mul_right hpne hmul
  exact hne (by exact_mod_cast habInt)

private theorem nodup_filter_bool {α : Type} (l : List α) (h : l.Nodup) (p : α → Bool) :
    (l.filter p).Nodup := by
  unfold List.Nodup at h ⊢
  exact List.Pairwise.filter p h

private theorem intersection_step_pos (A B : Progression) :
    0 < (intersection A B).step := by
  unfold intersection
  by_cases h : A.n == 0 || B.n == 0
  · simp [h]
  · rw [if_neg h]
    by_cases hA : A.step == 0
    · simp [hA]
    · rw [if_neg hA]
      by_cases hB : B.step == 0
      · simp [hB]
      · rw [if_neg hB]
        by_cases hg : (B.lo - A.lo) % (A.step.gcd B.step : Int) != 0
        · simp [hg]
        · rw [if_neg hg]
          dsimp
          have hAs : 0 < A.step := Nat.pos_of_ne_zero (by simpa using hA)
          have hBs : 0 < B.step := Nat.pos_of_ne_zero (by simpa using hB)
          exact Nat.mul_pos (Nat.div_gcd_pos_of_pos_left B.step hAs) hBs

private theorem inter_count (a b : Progression) (ha : 0 < a.step) :
    (intersection a b).n = ((progVals a).filter (fun v => contains b v = true)).length := by
  let i := intersection a b
  have hi_step : 0 < i.step := intersection_step_pos a b
  have hni : (progVals i).Nodup := nodup_progVals i hi_step
  have hna : (progVals a).Nodup := nodup_progVals a ha
  have hsub1 : progVals i ⊆ (progVals a).filter (fun v => contains b v = true) := by
    intro v hv
    rw [mem_progVals] at hv
    rw [List.mem_filter]
    have hboth := (intersection_contains a b v).mp hv
    exact ⟨(mem_progVals a v).mpr hboth.1, hboth.2⟩
  have hsub2 : (progVals a).filter (fun v => contains b v = true) ⊆ progVals i := by
    intro v hv
    rw [List.mem_filter] at hv
    rw [mem_progVals]
    exact (intersection_contains a b v).mpr ⟨(mem_progVals a v).mp hv.1, hv.2⟩
  have hle1 : (progVals i).length ≤ ((progVals a).filter (fun v => contains b v = true)).length :=
    hni.length_le_of_subset hsub1
  have hle2 : ((progVals a).filter (fun v => contains b v = true)).length ≤ (progVals i).length :=
    (nodup_filter_bool (progVals a) hna (fun v => contains b v = true)).length_le_of_subset hsub2
  have heq : (progVals i).length = ((progVals a).filter (fun v => contains b v = true)).length :=
    Nat.le_antisymm hle1 hle2
  simpa [progVals, i] using heq




private theorem length_filterMap_isSome {α β : Type} (l : List α) (f : α → Option β) :
    (l.filterMap f).length = (l.filter (fun x => (f x).isSome)).length := by
  induction l with
  | nil => simp
  | cons x xs ih => cases hfx : f x <;> simp [ih, hfx]

private theorem succ_mod_step (n p : Nat) (hp : 0 < p) :
    (n + 1) % p = if n % p + 1 < p then n % p + 1 else 0 := by
  by_cases hp1 : p = 1
  · subst p
    have hm : n % 1 = 0 := by
      have := Nat.mod_lt n (by omega : 0 < 1)
      omega
    simp [hm]
  · have hp' : 1 < p := by omega
    have h1 : (n + 1) % p = (n % p + 1 % p) % p := Nat.add_mod n 1 p
    have h1p : 1 % p = 1 := Nat.mod_eq_of_lt hp'
    rw [h1, h1p]
    by_cases h : n % p + 1 < p
    · rw [Nat.mod_eq_of_lt h]
      simp [h]
    · have heq : n % p + 1 = p := by
        have hs := Nat.mod_lt n hp
        omega
      rw [heq, Nat.mod_self]
      simp [h]

private theorem succ_div_step (n p : Nat) (hp : 0 < p) :
    (n + 1) / p = n / p + if n % p + 1 < p then 0 else 1 := by
  have hd := Nat.div_add_mod n p
  have hs := Nat.mod_lt n hp
  by_cases h : n % p + 1 < p
  · have hEq : n + 1 = p * (n / p) + (n % p + 1) := by grind
    rw [hEq, Nat.mul_add_div hp (n / p) (n % p + 1), Nat.div_eq_of_lt h]
    simp [h]
  · have heq : n % p + 1 = p := by omega
    have hEq : n + 1 = p * (n / p + 1) := by grind
    rw [hEq, Nat.mul_comm p (n / p + 1), Nat.mul_div_left _ hp]
    simp [h]

private theorem tile_even_iff (w i : Nat) (hw : 0 < w) :
    ((i / w) % 2 == 0) = (i % (2 * w) < w) := by
  have h2w : 0 < 2 * w := by omega
  let q := i / (2 * w)
  let r := i % (2 * w)
  have hdecomp : i = 2 * w * q + r := by
    dsimp [q, r]
    exact (Nat.div_add_mod i (2 * w)).symm
  have hdivbase : (2 * w * q + r) / w = 2 * q + r / w := by
    rw [Nat.add_comm]
    have hmul : r + 2 * w * q = r + w * (2 * q) := by grind
    rw [hmul, Nat.add_mul_div_left _ _ hw]
    omega
  have hdiv : i / w = 2 * q + r / w := by rw [hdecomp, hdivbase]
  have hmoddiv : (i / w) % 2 = (r / w) % 2 := by
    rw [hdiv, Nat.add_comm, Nat.add_mul_mod_self_left]
  have hrlt : r < 2 * w := by dsimp [r]; exact Nat.mod_lt i h2w
  have hqle : r / w ≤ 1 := by
    apply (Nat.div_le_iff_le_mul hw).mpr
    omega
  have hqmod : (r / w) % 2 = r / w := Nat.mod_eq_of_lt (by omega)
  rw [hmoddiv, hqmod]
  by_cases hlt : r < w
  · have hz : r / w = 0 := Nat.div_eq_of_lt hlt
    simp [hz]
    simpa [r] using hlt
  · have hge : w ≤ r := by omega
    have hlt2 : r / w < 2 := (Nat.div_lt_iff_lt_mul hw).mpr (by omega)
    have hge1 : 1 ≤ r / w := (Nat.le_div_iff_mul_le hw).mpr (by simpa using hge)
    have hone : r / w = 1 := by omega
    simp [hone]
    simpa [r] using hge

private theorem min_succ_right (w r : Nat) (h : r + 1 < 2 * w) :
    min w (r + 1) = min w r + if r < w then 1 else 0 := by
  by_cases hr : r < w
  · by_cases hrw : r + 1 < w
    · have h1 : min w r = r := Nat.min_eq_right (by omega)
      have h2 : min w (r+1) = r+1 := Nat.min_eq_right (by omega)
      simp [hr, h1, h2]
    · have heq : r + 1 = w := by omega
      have h1 : min w r = r := Nat.min_eq_right (by omega)
      have h2 : min w (r+1) = w := Nat.min_eq_left (by omega)
      simp [hr, h1, h2, heq]
  · have h1 : min w r = w := Nat.min_eq_left (by omega)
    have h2 : min w (r+1) = w := Nat.min_eq_left (by omega)
    simp [hr, h1, h2]

private theorem count_tiles (n w : Nat) (hw : 0 < w) :
    ((List.range n).filter (fun i => i % (2 * w) < w)).length =
      (n / (2 * w)) * w + min w (n % (2 * w)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append, List.length_append, ih]
      rw [succ_div_step n (2*w) (by omega), succ_mod_step n (2*w) (by omega)]
      have hrem : n % (2*w) < 2*w := Nat.mod_lt n (by omega)
      by_cases hlt : n % (2*w) + 1 < 2*w
      · simp [hlt]
        rw [min_succ_right w (n % (2*w)) hlt]
        by_cases hr : n % (2*w) < w
        · have hnew : n % (2*w) + 1 < w ∨ n % (2*w) + 1 = w := by omega
          rcases hnew with hnew | heq
          · simp [hr, hnew]
            grind
          · simp [hr, heq]
            grind
        · have hwle : w ≤ n % (2*w) := by omega
          have hnot : ¬ n % (2*w) + 1 < w := by omega
          simp [hr, hnot]
      · have hmin0 : min w (n % (2*w)) = w := by
          apply Nat.min_eq_left
          omega
        have hnot : ¬ n % (2*w) < w := by omega
        simp [hlt, hnot, hmin0]
        have hmul : (n / (2*w) + 1) * w = n / (2*w) * w + w := by
          rw [Nat.add_mul, Nat.one_mul]
        rw [hmul]

theorem sound_SkipEpochs : SkipEpochs.Goal := by
  intro c
  let w := width c
  have hw : 0 < w := by dsimp [w, width]; omega
  have hrows : (rows layout c).length = ((List.range c.n).filter (fun i => i % (2 * w) < w)).length := by
    unfold rows
    rw [length_filterMap_isSome]
    apply congrArg List.length
    apply filter_congr_bool
    intro i hi
    simp only [layout, Option.map_some, Option.map_none, Option.isSome_some, Option.isSome_none]
    have ht : ((i / w) % 2 == 0) = (i % (2 * w) < w) := tile_even_iff w i hw
    by_cases h : (i / w) % 2 == 0
    · have hh : i % (2 * w) < w := by rw [← ht]; exact h
      have hh' : i / width c % 2 == 0 := by simpa [w] using h
      simpa [hh'] using hh
    · have hh : ¬ i % (2 * w) < w := by
        intro hhh
        exact h (by rw [ht]; exact hhh)
      have hh' : ¬ i / width c % 2 == 0 := by
        intro hhh
        exact h (by simpa [w] using hhh)
      have hwle : w ≤ i % (2*w) := by omega
      simpa [hh'] using hwle
  rw [hrows]
  exact count_tiles c.n w hw


private def hit (b : Lane) (v : Int) : Nat :=
  (List.range b.n).countP (fun i => v == b.base + b.step * Int.ofNat i)

private theorem countP_eq_sum_nat {α : Type} (l : List α) (p : α → Bool) :
    l.countP p = (l.map (fun x => if p x then 1 else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x <;> simp [h, ih] <;> omega

private theorem sum_range_const_nat (m v : Nat) :
    ((List.range m).map (fun _ => v)).sum = m * v := by
  induction m with
  | zero => simp
  | succ m ih =>
      rw [show m + 1 = Nat.succ m by omega, List.range_succ, List.map_append]
      rw [List.sum_append]
      simp [ih, Nat.succ_mul]

private theorem hit_step_zero (b : Lane) (v : Int) (hs : b.step = 0) :
    hit b v = if b.base == v then b.n else 0 := by
  dsimp [hit]
  rw [countP_eq_sum_nat]
  simp [hs]
  by_cases h : v = b.base
  · have hb : b.base = v := h.symm
    rw [show (fun _ : Nat => if v = b.base then 1 else 0) = (fun _ : Nat => 1) by
      funext x; simp [h]]
    rw [show (if b.base = v then b.n else 0) = b.n by simp [hb]]
    simpa using sum_range_const_nat b.n 1
  · have hb : ¬ b.base = v := by intro hh; exact h hh.symm
    rw [show (fun _ : Nat => if v = b.base then 1 else 0) = (fun _ : Nat => 0) by
      funext x; simp [h]]
    rw [show (if b.base = v then b.n else 0) = 0 by simp [hb]]
    induction b.n with
    | zero => simp
    | succ n ih => simp [List.range_succ, ih]

private theorem countP_range_unique (n : Nat) (p : Nat → Bool)
    (huniq : ∀ i j, i < n → j < n → p i = true → p j = true → i = j) :
    (List.range n).countP p = if (∃ i, i < n ∧ p i = true) then 1 else 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.countP_append]
      have htailuniq : ∀ i j, i < n → j < n → p i = true → p j = true → i = j := by
        intro i j hi hj; exact huniq i j (by omega) (by omega)
      rw [ih htailuniq]
      by_cases hpn : p n = true
      · have hnone : ¬ ∃ i, i < n ∧ p i = true := by
          rintro ⟨i, hi, hpi⟩
          have heq := huniq i n (by omega) (by omega) hpi hpn
          omega
        have hS : ∃ i, i < n+1 ∧ p i = true := ⟨n, by omega, hpn⟩
        simp [hpn, hnone, hS]
      · have hpnf : p n = false := by cases h : p n <;> simp_all
        have hiff : (∃ i, i < n+1 ∧ p i = true) ↔ (∃ i, i < n ∧ p i = true) := by
          constructor
          · rintro ⟨i, hi, hpi⟩
            have hine : i ≠ n := by
              intro heq
              subst i
              rw [hpnf] at hpi
              cases hpi
            refine ⟨i, by omega, hpi⟩
          · rintro ⟨i, hi, hpi⟩
            exact ⟨i, by omega, hpi⟩
        by_cases hnone : ∃ i, i < n ∧ p i = true
        · have hS : (∃ i, i < n+1 ∧ p i = true) := hiff.mpr hnone
          simp [hpnf, hnone, hS]
        · have hS : ¬ (∃ i, i < n+1 ∧ p i = true) := by
            intro hh
            exact hnone (hiff.mp hh)
          simp [hpnf, hnone, hS]

private theorem hit_step_ne (b : Lane) (v : Int) (hs : b.step ≠ 0) :
    hit b v = if (∃ i, i < b.n ∧ v = b.base + b.step * Int.ofNat i) then 1 else 0 := by
  dsimp [hit]
  rw [countP_range_unique b.n (fun i => v == b.base + b.step * (i : Int))]
  · have hiff : (∃ i, i < b.n ∧ (v == b.base + b.step * (i : Int)) = true) ↔
        (∃ i, i < b.n ∧ v = b.base + b.step * (i : Int)) := by
      constructor
      · rintro ⟨i, hi, hpi⟩
        exact ⟨i, hi, beq_iff_eq.mp hpi⟩
      · rintro ⟨i, hi, hval⟩
        exact ⟨i, hi, by simp [hval]⟩
    by_cases hex : ∃ i, i < b.n ∧ (v == b.base + b.step * (i : Int)) = true
    · have hex' : ∃ i, i < b.n ∧ v = b.base + b.step * (i : Int) := hiff.mp hex
      simp [hex, hex']
    · have hex' : ¬ (∃ i, i < b.n ∧ v = b.base + b.step * (i : Int)) := by
        intro hh
        exact hex (hiff.mpr hh)
      simp [hex, hex']
  · intro i j hi hj hpi hpj
    have hv1 : v = b.base + b.step * (i : Int) := beq_iff_eq.mp hpi
    have hv2 : v = b.base + b.step * (j : Int) := beq_iff_eq.mp hpj
    have hm : b.step * (i : Int) = b.step * (j : Int) := by omega
    have hsint : (b.step : Int) ≠ 0 := by exact_mod_cast hs
    have hij : (i : Int) = (j : Int) := Int.eq_of_mul_eq_mul_left hsint hm
    exact Int.ofNat_inj.mp hij



private theorem div_eq_one_of_bounds (t p : Nat) (hp : 0 < p)
    (hge : p ≤ t) (hlt : t < 2*p) : t / p = 1 := by
  have hlt2 : t / p < 2 := (Nat.div_lt_iff_lt_mul hp).mpr hlt
  have hnle : ¬ t / p ≤ 0 := by
    intro h
    have hh := (Nat.div_le_iff_le_mul hp).mp h
    omega
  omega

private theorem mod_step_count (n p r : Nat) (hp : 0 < p) (hr : r < p) :
    (n - r + p - 1) / p = n / p + if r < n % p then 1 else 0 := by
  have hd := Nat.div_add_mod n p
  have hs := Nat.mod_lt n hp
  by_cases hnr : n < r
  · have hdiv : n / p = 0 := Nat.div_eq_of_lt (by omega)
    have hm : n - r + p - 1 = p - 1 := by grind
    have hnot : ¬ r < n % p := by omega
    have hpdiv : (p - 1) / p = 0 := Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))
    simp [hm, hdiv, hnot, hpdiv]
  · have hrn : r ≤ n := by omega
    have hsub : n - r + p - 1 = n / p * p + (n % p + p - 1 - r) := by
      grind
    rw [hsub, Nat.mul_comm (n / p) p]
    rw [Nat.mul_add_div hp (n / p) (n % p + p - 1 - r)]
    by_cases hlt : r < n % p
    · have hge : p ≤ n % p + p - 1 - r := by omega
      have hlt2 : n % p + p - 1 - r < 2 * p := by omega
      have hdiv2 : (n % p + p - 1 - r) / p = 1 := div_eq_one_of_bounds _ _ hp hge hlt2
      simp [hlt, hdiv2]
    · have hle : n % p ≤ r := by omega
      have hlt2 : n % p + p - 1 - r < p := by omega
      have hdiv2 : (n % p + p - 1 - r) / p = 0 := Nat.div_eq_of_lt hlt2
      simp [hlt, hdiv2]

private theorem range_filter_mod_eq (n p r : Nat) (hp : 0 < p) (hr : r < p) :
    (List.range n).filter (fun i => i % p == r) =
      (List.range ((n - r + p - 1) / p)).map (fun q => r + q * p) := by
  induction n with
  | zero =>
      have hm : (0 - r + p - 1) / p = 0 := by
        have : 0 - r + p - 1 = p - 1 := by omega
        rw [this, Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))]
      have hpdiv : (p - 1) / p = 0 := Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))
      simp [hm, hpdiv]
  | succ n ih =>
      rw [List.range_succ, List.filter_append, ih]
      rw [mod_step_count n p r hp hr, mod_step_count (n+1) p r hp hr]
      rw [succ_mod_step n p hp, succ_div_step n p hp]
      by_cases hn : n % p == r
      · have hnp : n % p = r := Nat.eq_of_beq_eq_true hn
        have hltold : ¬ r < n % p := by omega
        have htail : List.map (fun q => r + q * p) (List.range (n / p)) ++ [n] =
            List.map (fun q => r + q * p) (List.range (n / p + 1)) := by
          rw [show n / p + 1 = Nat.succ (n / p) by omega, List.range_succ, List.map_append]
          simp
          have hd := Nat.div_add_mod n p
          rw [Nat.mul_comm p (n / p)] at hd
          have hval : r + n / p * p = n := by omega
          rw [hval]
        by_cases hs : r + 1 < p
        · have hltnew : r < r + 1 := by omega
          simp [hn, hltold, hnp, hs, hltnew]
          exact htail
        · have hltnew : ¬ r < 0 := by omega
          simp [hn, hltold, hnp, hs, hltnew]
          exact htail
      · have hne0 : n % p ≠ r := by intro he; exact hn (by rw [he]; simp)
        have hltold : r < n % p ∨ n % p < r := by
          by_cases hrlt : r < n % p
          · exact Or.inl hrlt
          · right; omega
        by_cases h : n % p + 1 < p
        · rcases hltold with hlt | hgt
          · have hltnew : r < n % p + 1 := by omega
            simp [hn, h, hlt, hltnew]
          · have hltnew : ¬ r < n % p + 1 := by omega
            have hnlt : ¬ r < n % p := by omega
            simp [hn, h, hnlt, hltnew]
        · have hne : n % p ≠ r := by intro he; exact hn (by rw [he]; simp)
          have heq : n % p + 1 = p := by
            have hs' := Nat.mod_lt n hp
            omega
          have hlt : r < n % p := by omega
          have hltnew : ¬ r < 0 := by omega
          simp [hn, h, hlt, hltnew]


private theorem sum_map_zero_nat (l : List Nat) : (l.map (fun _ => (0 : Nat))).sum = 0 := by
  induction l <;> simp [*]

private theorem sum_update_nat : ∀ (p r0 : Nat), r0 < p → ∀ (s : Nat → Nat) (v : Nat),
    ((List.range p).map (fun r => if r == r0 then v + s r else s r)).sum =
      v + ((List.range p).map s).sum := by
  intro p
  induction p with
  | zero => intro r0 h; omega
  | succ p ih =>
      intro r0 hr0 s v
      rw [show p + 1 = Nat.succ p by omega, List.range_succ, List.map_append, List.sum_append,
        List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      by_cases heq : p = r0
      · have hb : p == r0 := by rw [heq]; simp
        rw [if_pos hb]
        have hpref : (List.range p).map (fun r => if r == r0 then v + s r else s r) =
            (List.range p).map s := by
          apply List.map_congr_left
          intro r hr
          have hrlt : r < p := List.mem_range.mp hr
          have hne : ¬ r == r0 := by rw [beq_iff_eq]; omega
          simp [hne]
        rw [hpref]
        omega
      · have hb : ¬ (p == r0) := by rw [beq_iff_eq]; exact heq
        rw [if_neg hb]
        have hlt : r0 < p := by omega
        rw [ih r0 hlt s v]
        omega

private theorem striped_sum_partition_nat (p : Nat) (hp : 0 < p) (F : Nat → Nat) :
    ∀ xs : List Nat, (xs.map F).sum =
      ((List.range p).map (fun r => ((xs.filter (fun i => i % p == r)).map F).sum)).sum := by
  intro xs
  induction xs with
  | nil =>
      rw [List.map_nil, List.sum_nil]
      exact (sum_map_zero_nat (List.range p)).symm
  | cons x xs ih =>
      rw [List.map_cons, List.sum_cons, ih]
      rw [← sum_update_nat p (x % p) (Nat.mod_lt x hp)]
      apply congrArg List.sum
      apply List.map_congr_left
      intro r hr
      by_cases h : x % p == r
      · have h' : x % p = r := Nat.eq_of_beq_eq_true h
        simp [h, h']
      · have h' : x % p ≠ r := by intro e; exact h (by rw [e]; simp)
        simp [h']
        intro hrx
        exact (h' hrx.symm).elim

private theorem sum_map_const_nat {α : Type} (l : List α) (v : Nat) :
    (l.map (fun _ => v)).sum = l.length * v := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.succ_mul]; omega


private theorem contains_values_iff (b : Lane) (v : Int) :
    contains (values b) v = true ↔ ∃ i, i < b.n ∧ v = b.base + b.step * (i : Int) := by
  rw [contains_iff_mem]
  unfold progMem progVal
  by_cases hneg : b.step < 0
  · have habsint : (b.step.natAbs : Int) = -b.step := by
      rw [← Int.natAbs_neg b.step]
      exact Int.natAbs_of_nonneg (by omega)
    simp only [values, if_pos hneg]
    constructor
    · rintro ⟨k, hk, hval⟩
      let i := b.n - 1 - k
      have hi : i < b.n := by dsimp [i]; omega
      refine ⟨i, hi, ?_⟩
      rw [habsint] at hval
      dsimp [i]
      have hcast : (((b.n - 1 - k : Nat) : Int)) = ((b.n - 1 : Nat) : Int) - (k : Int) := by
        have hkle : k ≤ b.n - 1 := by omega
        omega
      rw [hcast]
      grind
    · rintro ⟨i, hi, hval⟩
      let k := b.n - 1 - i
      have hk : k < b.n := by dsimp [k]; omega
      refine ⟨k, hk, ?_⟩
      rw [habsint]
      dsimp [k]
      have hcast : (((b.n - 1 - i : Nat) : Int)) = ((b.n - 1 : Nat) : Int) - (i : Int) := by
        have hile : i ≤ b.n - 1 := by omega
        omega
      rw [hcast]
      grind
  · have hnonneg : 0 ≤ b.step := by omega
    have habsint : (b.step.natAbs : Int) = b.step := by
      rw [Int.natAbs_of_nonneg hnonneg]
    simp only [values, if_neg hneg]
    constructor
    · rintro ⟨k, hk, hval⟩
      refine ⟨k, hk, ?_⟩
      rw [habsint] at hval
      simpa [Int.mul_comm] using hval
    · rintro ⟨i, hi, hval⟩
      refine ⟨i, hi, ?_⟩
      rw [habsint]
      simpa [Int.mul_comm] using hval

private theorem hit_eq_joinCount (a b : Lane) (ha : a.step = 0) :
    hit b a.base * a.n = joinCount a b := by
  by_cases han : a.n = 0
  · have hva : values a = ⟨a.base, 0, a.n⟩ := by simp [values, ha]
    simp [han, joinCount, hva, intersection]
  · have hva : values a = ⟨a.base, 0, a.n⟩ := by simp [values, ha]
    have hvn : (values b).n = b.n := rfl
    by_cases hbn : b.n = 0
    · simp [hit, joinCount, hbn, hva, hvn, intersection]
    · have hint : intersection (values a) (values b) =
          ⟨a.base, 1, if contains (values b) a.base then 1 else 0⟩ := by
        simp [intersection, han, hva, hvn, hbn]
      unfold joinCount
      rw [hint]
      simp only [ha]
      by_cases hc : contains (values b) a.base = true
      · have hex := (contains_values_iff b a.base).mp hc
        by_cases hbz : b.step = 0
        · rw [hit_step_zero b a.base hbz]
          have hbase : b.base = a.base := by
            obtain ⟨i, hi, hval⟩ := hex
            simpa [hbz] using hval.symm
          simp [hbz, hbase, hc, hva, hvn, intersection, han]
          exact Nat.mul_comm _ _
        · rw [hit_step_ne b a.base hbz]
          simp [hbz, hex, hc]
      · have hno : ¬ (∃ i, i < b.n ∧ a.base = b.base + b.step * (i : Int)) := by
          intro hh
          exact hc ((contains_values_iff b a.base).mpr hh)
        by_cases hbz : b.step = 0
        · rw [hit_step_zero b a.base hbz]
          by_cases hbn' : b.n = 0
          · simp [hbn', hc]
          · have hbase : b.base ≠ a.base := by
              intro heq
              apply hno
              refine ⟨0, by omega, ?_⟩
              simp [hbz, heq]
            simp [hbz, hbase, hc, hva, hvn, intersection, han]
        · rw [hit_step_ne b a.base hbz]
          simp [hbz, hno, hc]





private def laneVal (a : Lane) (i : Nat) : Int := a.base + (i : Int) * a.step

private theorem laneVal_list_nodup (a : Lane) (ha : a.step ≠ 0) :
    ((List.range a.n).map (fun i => laneVal a i)).Nodup := by
  unfold List.Nodup
  refine List.Pairwise.map (fun i : Nat => laneVal a i) ?_ List.nodup_range
  intro i j hij heq
  unfold laneVal at heq
  have hm : (a.step : Int) * (i : Int) = (a.step : Int) * (j : Int) := by grind
  have hsint : (a.step : Int) ≠ 0 := by exact_mod_cast ha
  have hcast : (i : Int) = (j : Int) := Int.eq_of_mul_eq_mul_left hsint hm
  exact hij (by exact_mod_cast hcast)



private theorem laneVal_mem_values (a : Lane) (v : Int) :
    v ∈ ((List.range a.n).map (fun i => laneVal a i)) ↔ contains (values a) v = true := by
  rw [List.mem_map, contains_iff_mem]
  unfold progMem progVal
  constructor
  · rintro ⟨i, hiMem, hv⟩
    have hi : i < a.n := List.mem_range.mp hiMem
    by_cases hneg : a.step < 0
    · have habsint : (a.step.natAbs : Int) = -a.step := by
        rw [← Int.natAbs_neg a.step]
        exact Int.natAbs_of_nonneg (by omega)
      simp only [values, if_pos hneg]
      let k := a.n - 1 - i
      have hk : k < a.n := by dsimp [k]; omega
      refine ⟨k, hk, ?_⟩
      rw [habsint]
      dsimp [k]
      have hcast : (((a.n - 1 - i : Nat) : Int)) = ((a.n - 1 : Nat) : Int) - (i : Int) := by
        have hi' : i ≤ a.n - 1 := by omega
        omega
      rw [hcast]
      unfold laneVal at hv
      grind
    · have hnonneg : 0 ≤ a.step := by omega
      have habsint : (a.step.natAbs : Int) = a.step := by
        rw [Int.natAbs_of_nonneg hnonneg]
      simp only [values, if_neg hneg]
      refine ⟨i, hi, ?_⟩
      rw [habsint]
      unfold laneVal at hv
      simpa [Int.mul_comm] using hv.symm
  · rintro ⟨k, hk, hval⟩
    by_cases hneg : a.step < 0
    · have habsint : (a.step.natAbs : Int) = -a.step := by
        rw [← Int.natAbs_neg a.step]
        exact Int.natAbs_of_nonneg (by omega)
      simp only [values, if_pos hneg] at hval
      rw [habsint] at hval
      change k < a.n at hk
      let i := a.n - 1 - k
      have hi : i < a.n := by dsimp [i]; omega
      refine ⟨i, List.mem_range.mpr hi, ?_⟩
      unfold laneVal
      dsimp [i]
      have hcast : (((a.n - 1 - k : Nat) : Int)) = ((a.n - 1 : Nat) : Int) - (k : Int) := by
        have hk' : k ≤ a.n - 1 := by omega
        omega
      rw [hcast]
      grind
    · have hnonneg : 0 ≤ a.step := by omega
      have habsint : (a.step.natAbs : Int) = a.step := by
        rw [Int.natAbs_of_nonneg hnonneg]
      simp only [values, if_neg hneg] at hval
      rw [habsint] at hval
      refine ⟨k, List.mem_range.mpr hk, ?_⟩
      unfold laneVal
      simpa [Int.mul_comm] using hval.symm

private theorem sum_filter_eq_countP {α : Type} (l : List α) (p : α → Bool) (v : Nat) :
    (l.map (fun x => if p x then v else 0)).sum = (l.countP p) * v := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x
      · simp [h, ih]
        have hmul : (List.countP p xs + 1) * v = List.countP p xs * v + v := by
          rw [Nat.add_mul, Nat.one_mul]
        rw [hmul]
        grind
      · simp [h, ih]

private theorem count_laneVal_prop (a : Lane) (ha : a.step ≠ 0) (P : Int → Bool) :
    ((List.range a.n).filter (fun i => P (laneVal a i))).length =
      ((progVals (values a)).filter P).length := by
  let l1 := (List.range a.n).map (fun i => laneVal a i)
  let l2 := progVals (values a)
  have hnod1 : l1.Nodup := by
    dsimp [l1]
    exact laneVal_list_nodup a ha
  have hva : 0 < (values a).step := by
    dsimp [values]
    exact Nat.pos_of_ne_zero (Int.natAbs_ne_zero.mpr ha)
  have hnod2 : l2.Nodup := by
    dsimp [l2]
    exact nodup_progVals (values a) hva
  have hmem1 : ∀ v, v ∈ l1 ↔ contains (values a) v = true := by
    intro v
    dsimp [l1]
    exact laneVal_mem_values a v
  have hmem2 : ∀ v, v ∈ l2 ↔ contains (values a) v = true := by
    intro v
    dsimp [l2]
    exact mem_progVals (values a) v
  have hsub1 : (l1.filter P) ⊆ (l2.filter P) := by
    intro v hv
    rw [List.mem_filter] at hv ⊢
    exact ⟨(hmem2 v).mpr ((hmem1 v).mp hv.1), hv.2⟩
  have hsub2 : (l2.filter P) ⊆ (l1.filter P) := by
    intro v hv
    rw [List.mem_filter] at hv ⊢
    exact ⟨(hmem1 v).mpr ((hmem2 v).mp hv.1), hv.2⟩
  have hle1 := (nodup_filter_bool l1 hnod1 P).length_le_of_subset hsub1
  have hle2 := (nodup_filter_bool l2 hnod2 P).length_le_of_subset hsub2
  have heq := Nat.le_antisymm hle1 hle2
  have hfiltermap : (l1.filter P).length = ((List.range a.n).filter (fun i => P (laneVal a i))).length := by
    dsimp [l1]
    rw [List.filter_map, List.length_map]
    rfl
  rw [← hfiltermap]
  exact heq

private theorem hit_eq_contains_ne (b : Lane) (hb : b.step ≠ 0) (v : Int) :
    hit b v = if contains (values b) v = true then 1 else 0 := by
  rw [hit_step_ne b v hb]
  have hc := contains_values_iff b v
  by_cases h : contains (values b) v = true
  · have hex : ∃ i, i < b.n ∧ v = b.base + b.step*(i:Int) := hc.mp h
    simp [h, hex]
  · have hex : ¬ ∃ i, i < b.n ∧ v = b.base + b.step*(i:Int) := by
      intro hh
      exact h (hc.mpr hh)
    simp [h, hex]

private theorem hit_eq_joinCount_general (a b : Lane) :
    ((List.range a.n).map (fun i => hit b (laneVal a i))).sum = joinCount a b := by
  by_cases ha : a.step = 0
  · have hsum : ((List.range a.n).map (fun i => hit b (laneVal a i))).sum =
        ((List.range a.n).map (fun _ => hit b a.base)).sum := by
      apply congrArg List.sum
      apply List.map_congr_left
      intro i hi
      simp [laneVal, ha]
    rw [hsum]
    rw [sum_map_const_nat]
    simp only [List.length_range]
    rw [Nat.mul_comm]
    exact hit_eq_joinCount a b ha
  · by_cases hbn : b.n = 0
    · have hhit0 : ∀ v, hit b v = 0 := by
        intro v
        dsimp [hit]
        simp [hbn]
      rw [show ((List.range a.n).map (fun i => hit b (laneVal a i))).sum = 0 by
        rw [show (List.range a.n).map (fun i => hit b (laneVal a i)) =
            (List.range a.n).map (fun _ => 0) by
          apply List.map_congr_left
          intro i hi
          exact hhit0 (laneVal a i)]
        exact sum_map_zero_nat (List.range a.n)]
      have hvn : (values b).n = b.n := rfl
      have hint : intersection (values a) (values b) = ⟨0, 1, 0⟩ := by
        unfold intersection
        have hor : ((values a).n == 0 || (values b).n == 0) = true := by
          simp [hvn, hbn]
        rw [if_pos hor]
        rfl
      unfold joinCount
      rw [hint]
      simp [hbn]
    · by_cases hb : b.step = 0
      · have hhit : ∀ v, hit b v = if b.base == v then b.n else 0 := by
          intro v
          exact hit_step_zero b v hb
        have hsum : ((List.range a.n).map (fun i => hit b (laneVal a i))).sum =
            ((List.range a.n).map (fun i => if b.base == laneVal a i then b.n else 0)).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro i hi
          exact hhit (laneVal a i)
        rw [hsum]
        rw [sum_filter_eq_countP]
        have hcnt : (List.range a.n).countP (fun i => b.base == laneVal a i) =
            (intersection (values a) (values b)).n := by
          have hP : ∀ i, (b.base == laneVal a i) = true ↔
              contains (values b) (laneVal a i) = true := by
            intro i
            have hvb : values b = ⟨b.base, 0, b.n⟩ := by
              dsimp [values]
              rw [if_neg]
              · simp [hb]
              · omega
            rw [hvb, contains_const_iff]
            constructor
            · intro hh
              exact ⟨by omega, (beq_iff_eq.mp hh).symm⟩
            · rintro ⟨_, hv⟩
              exact beq_iff_eq.mpr hv.symm
          have hP' : (List.range a.n).countP (fun i => b.base == laneVal a i) =
              (List.range a.n).countP (fun i => contains (values b) (laneVal a i)) := by
            apply List.countP_congr
            intro i hi
            exact hP i
          rw [hP', List.countP_eq_length_filter]
          rw [count_laneVal_prop a ha (fun v => contains (values b) v)]
          have hva : 0 < (values a).step := by
            dsimp [values]
            exact Nat.pos_of_ne_zero (Int.natAbs_ne_zero.mpr ha)
          exact (inter_count (values a) (values b) hva).symm
        rw [hcnt]
        unfold joinCount
        simp [ha, hb, Nat.mul_assoc]
      · have hsum : ((List.range a.n).map (fun i => hit b (laneVal a i))).sum =
            ((List.range a.n).map (fun i => if contains (values b) (laneVal a i) = true then 1 else 0)).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro i hi
          exact hit_eq_contains_ne b hb (laneVal a i)
        rw [hsum]
        rw [← countP_eq_sum_nat]
        rw [List.countP_eq_length_filter]
        rw [count_laneVal_prop a ha (fun v => contains (values b) v)]
        have hfiltereq : (progVals (values a)).filter (fun v => contains (values b) v) =
            (progVals (values a)).filter (fun v => decide (contains (values b) v = true)) := by
          apply filter_congr_bool
          intro v hv
          cases h : contains (values b) v <;> simp [h]
        rw [hfiltereq]
        have hva : 0 < (values a).step := by
          dsimp [values]
          exact Nat.pos_of_ne_zero (Int.natAbs_ne_zero.mpr ha)
        rw [← inter_count (values a) (values b) hva]
        unfold joinCount
        simp [ha, hb]


private theorem sum_filterMap_ite {α β : Type} (l : List α) (p : α → Bool) (f : α → β)
    (g : β → Nat) :
    ((l.filterMap (fun x => if p x then some (f x) else none)).map g).sum =
      ((l.filter p).map (fun x => g (f x))).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem sum_map_ite_zero {α : Type} (l : List α) (p : α → Bool) (f : α → Nat) :
    (l.map (fun x => if p x then f x else 0)).sum = ((l.filter p).map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem rows_sum_hit (c : Config) (b : Lane) :
    (((rows layout c).map (fun r => (List.range b.n).countP
      (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum) =
    ((List.range c.n).map (fun i : Nat =>
      if (i / width c) % 2 == 0 then
        (List.range b.n).countP (fun j : Nat => c.base + c.step * (i : Int) == b.base + b.step * (j : Int))
      else 0)).sum := by
  unfold rows
  dsimp [layout]
  induction (List.range c.n) with
  | nil => simp
  | cons i is ih =>
      by_cases h : i / width c % 2 = 0
      · simp [h] at ih
        simp [h, ih, hit]
      · simp [h] at ih
        simp [h, ih]

theorem sound_ResidueJoin : ResidueJoin.Goal := by
  intro c b
  let w := width c
  let p := 2 * w
  have hw : 0 < w := by dsimp [w, width]; omega
  have hp : 0 < p := by dsimp [p]; omega
  rw [rows_sum_hit]
  have hcond : ∀ i : Nat,
      (if (i / width c) % 2 == 0 then
        (List.range b.n).countP (fun j : Nat => c.base + c.step * (i : Int) == b.base + b.step * (j : Int))
      else 0) =
      (if i % p < w then hit b (c.base + c.step * (i : Int)) else 0) := by
    intro i
    have ht := tile_even_iff w i hw
    have ht' : ((i / width c) % 2 == 0) ↔ i % p < w := by simpa [w, p] using ht
    by_cases h : (i / width c) % 2 == 0
    · have hiw : i % p < w := ht'.mp h
      simp [h, hiw, hit]
    · have hiw : ¬ i % p < w := by intro hh; exact h (ht'.mpr hh)
      simp [h, hiw]
  rw [show (List.range c.n).map (fun i : Nat =>
      if (i / width c) % 2 == 0 then
        (List.range b.n).countP (fun j : Nat => c.base + c.step * (i : Int) == b.base + b.step * (j : Int))
      else 0) =
      (List.range c.n).map (fun i : Nat =>
      if i % p < w then hit b (c.base + c.step * (i : Int)) else 0) from
    List.map_congr_left (fun i hi => hcond i)]
  let F : Nat → Nat := fun i => if i % p < w then hit b (c.base + c.step * (i : Int)) else 0
  change ((List.range c.n).map F).sum = ((layout.lanes c).map (joinCount · b)).sum
  rw [striped_sum_partition_nat p hp F (List.range c.n)]
  have hcol : ∀ r, r < p →
      ((List.range ((c.n - r + p - 1) / p)).map (fun q : Nat => F (r + q*p))).sum =
      if r < w then ((List.range ((c.n - r + p - 1) / p)).map
        (fun q : Nat => hit b ((c.base + c.step * (r : Int)) + (q : Int) * (c.step * p)))).sum else 0 := by
    intro r hr
    have hmod : ∀ q : Nat, (r + q*p) % p = r := by
      intro q
      rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hr]
    by_cases hrw : r < w
    · simp only [hrw, if_true]
      apply congrArg List.sum
      apply List.map_congr_left
      intro q hq
      dsimp [F]
      rw [hmod q]
      simp [hrw]
      have hval : c.base + c.step * ((r : Int) + (q : Int) * (p : Int)) =
          (c.base + c.step * (r : Int)) + (q : Int) * (c.step * p) := by
        dsimp [p]
        grind
      rw [hval]
    · have hrge : w ≤ r := by omega
      have hzero : ∀ q : Nat, F (r + q*p) = 0 := by
        intro q
        dsimp [F]
        rw [hmod q]
        have hnot : ¬ r < w := by omega
        simp [hnot]
      rw [show (List.range ((c.n - r + p - 1) / p)).map (fun q : Nat => F (r + q*p)) =
          (List.range ((c.n - r + p - 1) / p)).map (fun _ : Nat => 0) by
        apply List.map_congr_left
        intro q hq
        exact hzero q]
      rw [sum_map_zero_nat]
      rw [if_neg hrw]
  have hsumcol : ((List.range p).map (fun r =>
      ((List.range ((c.n - r + p - 1) / p)).map (fun q : Nat => F (r + q*p))).sum)).sum =
      ((List.range w).map (fun r =>
      ((List.range ((c.n - r + p - 1) / p)).map
        (fun q : Nat => hit b ((c.base + c.step * (r : Int)) + (q : Int) * (c.step * p)))).sum)).sum := by
    have hper : ∀ r ∈ List.range p,
        ((List.range ((c.n - r + p - 1) / p)).map (fun q : Nat => F (r + q*p))).sum =
        if r < w then ((List.range ((c.n - r + p - 1) / p)).map
          (fun q : Nat => hit b ((c.base + c.step * (r : Int)) + (q : Int) * (c.step * p)))).sum else 0 := by
      intro r hr
      exact hcol r (List.mem_range.mp hr)
    rw [show (List.range p).map (fun r =>
      ((List.range ((c.n - r + p - 1) / p)).map (fun q : Nat => F (r + q*p))).sum) =
      (List.range p).map (fun r =>
        if r < w then ((List.range ((c.n - r + p - 1) / p)).map
          (fun q : Nat => hit b ((c.base + c.step * (r : Int)) + (q : Int) * (c.step * p)))).sum else 0) from
      List.map_congr_left hper]
    have htrunc : ((List.range p).map (fun r =>
        if r < w then ((List.range ((c.n - r + p - 1) / p)).map
          (fun q : Nat => hit b ((c.base + c.step * (r : Int)) + (q : Int) * (c.step * p)))).sum else 0)).sum =
        ((List.range w).map (fun r =>
          ((List.range ((c.n - r + p - 1) / p)).map
            (fun q : Nat => hit b ((c.base + c.step * (r : Int)) + (q : Int) * (c.step * p)))).sum)).sum := by
      -- generic truncation: sum over range p of (if r<w then g r else 0) = sum over range w of g, since w <= p
      have hgen : ∀ p w : Nat, w ≤ p → ∀ g : Nat → Nat,
          ((List.range p).map (fun r => if r < w then g r else 0)).sum =
          ((List.range w).map g).sum := by
        intro p w hwp g
        induction p with
        | zero =>
            have hw0 : w = 0 := by omega
            subst w
            simp
        | succ p ihp =>
            by_cases hwp' : w ≤ p
            · rw [List.range_succ, List.map_append, List.sum_append, ihp hwp']
              have hnw : ¬ p < w := by omega
              simp [hnw]
            · have heq : w = p + 1 := by omega
              subst w
              rw [List.range_succ, List.map_append, List.sum_append]
              have hall : ∀ r ∈ List.range p, r < p + 1 := by
                intro r hr; exact Nat.lt_trans (List.mem_range.mp hr) (Nat.lt_succ_self p)
              rw [show (List.range p).map (fun r => if r < p+1 then g r else 0) =
                  (List.range p).map g from
                List.map_congr_left (fun r hr => by simp [hall r hr])]
              simp
      exact hgen p w (by dsimp [p]; omega) _
    exact htrunc
  have hpart : (List.range p).map (fun r =>
      (((List.range c.n).filter (fun i => i % p == r)).map F).sum) =
      (List.range p).map (fun r =>
      (((List.range ((c.n - r + p - 1) / p)).map (fun q : Nat => F (r + q*p))).sum)) := by
    apply List.map_congr_left
    intro r hr
    have hrp : r < p := List.mem_range.mp hr
    rw [range_filter_mod_eq c.n p r hp hrp]
    rw [List.map_map]
    rfl
  rw [hpart]
  rw [hsumcol]
  have htrunc2 : ((List.range w).map (fun r =>
      ((List.range ((c.n - r + p - 1) / p)).map
        (fun q : Nat => hit b ((c.base + c.step * (r : Int)) + (q : Int) * (c.step * p)))).sum)).sum =
      ((List.range (min c.n w)).map (fun r =>
      ((List.range ((c.n - r + p - 1) / p)).map
        (fun q : Nat => hit b ((c.base + c.step * (r : Int)) + (q : Int) * (c.step * p)))).sum)).sum := by
    by_cases hnw : c.n ≤ w
    · have hmin : min c.n w = c.n := Nat.min_eq_left hnw
      rw [hmin]
      -- for r >= c.n the inner count is 0
      have hgen : ∀ w k : Nat, k ≤ w → ∀ g : Nat → Nat,
          ((List.range w).map (fun r => if r < k then g r else 0)).sum =
          ((List.range k).map g).sum := by
        intro w k hkw g
        induction w with
        | zero =>
            have hk0 : k = 0 := by omega
            subst k
            simp
        | succ w ihw =>
            by_cases hkw' : k ≤ w
            · rw [List.range_succ, List.map_append, List.sum_append, ihw hkw']
              have hn : ¬ w < k := by omega
              simp [hn]
            · have heq : k = w + 1 := by omega
              subst k
              rw [List.range_succ, List.map_append, List.sum_append]
              have hall : ∀ r ∈ List.range w, r < w + 1 := by
                intro r hr; exact Nat.lt_trans (List.mem_range.mp hr) (Nat.lt_succ_self w)
              rw [show (List.range w).map (fun r => if r < w+1 then g r else 0) =
                  (List.range w).map g from
                List.map_congr_left (fun r hr => by simp [hall r hr])]
              simp
      have hrep : (List.range w).map (fun r =>
          ((List.range ((c.n - r + p - 1) / p)).map
            (fun q : Nat => hit b ((c.base + c.step * (r : Int)) + (q : Int) * (c.step * p)))).sum) =
          (List.range w).map (fun r => if r < c.n then
          ((List.range ((c.n - r + p - 1) / p)).map
            (fun q : Nat => hit b ((c.base + c.step * (r : Int)) + (q : Int) * (c.step * p)))).sum else 0) := by
        apply List.map_congr_left
        intro r hr
        have hrw : r < w := List.mem_range.mp hr
        by_cases hrc : r < c.n
        · simp [hrc]
        · have hcnt : (c.n - r + p - 1) / p = 0 := by
            rw [show c.n - r = 0 by omega, Nat.zero_add]
            exact Nat.div_eq_of_lt (by omega)
          rw [hcnt]
          simp [hrc]
      rw [hrep]
      exact hgen w c.n hnw _
    · have hmin : min c.n w = w := Nat.min_eq_right (by omega)
      rw [hmin]
  rw [htrunc2]
  have hcoljoin : ∀ r, r < min c.n w →
      ((List.range ((c.n - r + p - 1) / p)).map
        (fun q : Nat => hit b ((c.base + c.step * (r : Int)) + (q : Int) * (c.step * p)))).sum =
      joinCount (lane c.n r p (c.base + c.step * (r : Int)) (c.step * p)) b := by
    intro r hr
    have hrmin : r < min c.n w := hr
    have hrn : r < c.n := Nat.lt_of_lt_of_le hrmin (Nat.min_le_left _ _)
    have hrw : r < w := Nat.lt_of_lt_of_le hrmin (Nat.min_le_right _ _)
    have hlane : lane c.n r p (c.base + c.step * (r : Int)) (c.step * p) =
        ⟨r, p, (c.n - r + p - 1) / p, c.base + c.step * (r : Int), c.step * p⟩ := by
      unfold lane
      rfl
    rw [hlane]
    simpa [laneVal, Int.mul_comm, Nat.mul_comm] using
      (hit_eq_joinCount_general ⟨r, p, (c.n - r + p - 1) / p, c.base + c.step * (r : Int), c.step * p⟩ b)
  rw [show (List.range (min c.n w)).map (fun r =>
      ((List.range ((c.n - r + p - 1) / p)).map
        (fun q : Nat => hit b ((c.base + c.step * (r : Int)) + (q : Int) * (c.step * p)))).sum) =
      (List.range (min c.n w)).map (fun r =>
        joinCount (lane c.n r p (c.base + c.step * (r : Int)) (c.step * p)) b) from
    List.map_congr_left (fun r hr => hcoljoin r (List.mem_range.mp hr))]
  dsimp [layout, p, w]
  rw [List.map_map]
  rfl
-- =====================================================================
-- Round 2: generic list / sum helpers
-- =====================================================================

private theorem list_sum_perm_int {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem not_mem_erase_self_of_nodup (a : Int) {l : List Int} (hnd : l.Nodup) :
    ¬ a ∈ l.erase a := by
  induction l with
  | nil => simp
  | cons b bs ih =>
      have hbne : ¬ b ∈ bs := (List.nodup_cons.mp hnd).1
      have hbsnd : bs.Nodup := (List.nodup_cons.mp hnd).2
      by_cases hba : b == a
      · have hbeq : b = a := beq_iff_eq.mp hba
        subst b
        rw [List.erase_cons_head]
        exact hbne
      · rw [List.erase_cons_tail hba]
        intro ha_mem
        rw [List.mem_cons] at ha_mem
        rcases ha_mem with hab | habs
        · have hbnea : b ≠ a := by intro h; apply hba; rw [h]; simp
          exact hbnea hab.symm
        · exact (ih hbsnd) habs

private theorem perm_of_mem_iff_nodup {l₁ l₂ : List Int}
    (h₁ : l₁.Nodup) (h₂ : l₂.Nodup) (hm : ∀ v, v ∈ l₁ ↔ v ∈ l₂) : l₁.Perm l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
      cases l₂ with
      | nil => exact List.Perm.nil
      | cons b bs =>
          have : b ∈ ([] : List Int) := (hm b).mpr (by simp)
          simp at this
  | cons a as ih =>
      have ha2 : a ∈ l₂ := (hm a).mp (by simp)
      have hasnd : as.Nodup := h₁.tail
      have herasend : (l₂.erase a).Nodup := h₂.erase a
      have hm' : ∀ v, v ∈ as ↔ v ∈ l₂.erase a := by
        intro v
        constructor
        · intro hv
          have hv1 : v ∈ a :: as := by simp [hv]
          have hv2 : v ∈ l₂ := (hm v).mp hv1
          have hne : v ≠ a := by
            intro heq
            subst v
            exact (List.nodup_cons.mp h₁).1 hv
          exact (List.mem_erase_of_ne hne).mpr hv2
        · intro hv
          have hv2 : v ∈ l₂ := List.mem_of_mem_erase hv
          have hv1 : v ∈ a :: as := (hm v).mpr hv2
          rw [List.mem_cons] at hv1
          rcases hv1 with hva | hvas
          · have : v = a := by simpa using hva
            subst v
            exact False.elim (not_mem_erase_self_of_nodup a h₂ hv)
          · exact hvas
      have ihperm := ih hasnd herasend hm'
      exact (List.Perm.cons a ihperm).trans (List.perm_cons_erase ha2).symm

private theorem sum_eq_of_nodup_mem {l₁ l₂ : List Int}
    (h₁ : l₁.Nodup) (h₂ : l₂.Nodup) (hm : ∀ v, v ∈ l₁ ↔ v ∈ l₂) (F : Int → Int) :
    (l₁.map F).sum = (l₂.map F).sum := by
  exact list_sum_perm_int ((perm_of_mem_iff_nodup h₁ h₂ hm).map F)

private theorem sum_arith (lo step : Int) (n : Nat) :
    ((List.range n).map (fun k : Nat => lo + (k : Int) * step)).sum =
      (n : Int) * lo + tri n * step := by
  induction n with
  | zero => simp [tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ n ih =>
      rw [sum_range_succ, ih, tri_succ]
      simp [Int.natCast_add]
      grind

private theorem sum_product_arith (x y dx dy : Int) (n : Nat) :
    ((List.range n).map (fun k : Nat => (x + (k : Int) * dx) * (y + (k : Int) * dy))).sum =
      (n : Int) * x * y + tri n * (x * dy + y * dx) + (squares n : Int) * dx * dy := by
  induction n with
  | zero => simp [tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ n ih =>
      rw [sum_range_succ, ih, tri_succ, squares_succ]
      simp [Int.natCast_add]
      grind

private theorem dvd_ediv_cast {a b : Nat} (hb : 0 < b) (hdiv : (b : Int) ∣ (a : Int)) :
    (a : Int) = (b : Int) * ((a / b : Nat) : Int) := by
  obtain ⟨t, ht⟩ := hdiv
  have ht_nonneg : 0 ≤ t := by
    have hbpos : (0 : Int) < (b : Int) := by exact_mod_cast hb
    have hmulnon : 0 ≤ (b : Int) * t := by rw [← ht]; exact_mod_cast (Nat.zero_le a)
    exact Int.nonneg_of_mul_nonneg_right hmulnon hbpos
  have htNat : a = b * t.toNat := by
    apply Int.ofNat_inj.mp
    rw [Int.natCast_mul]
    rw [Int.toNat_of_nonneg ht_nonneg]
    exact ht
  have hdivNat : a / b = t.toNat := by rw [htNat, Nat.mul_div_right t.toNat hb]
  rw [hdivNat, Int.toNat_of_nonneg ht_nonneg]
  exact ht

private theorem nat_div_cast_eq_ediv {a b : Nat} (hb : 0 < b) (hdiv : (b : Int) ∣ (a : Int)) :
    ((a / b : Nat) : Int) = (a : Int) / (b : Int) := by
  obtain ⟨t, ht⟩ := hdiv
  have ht_nonneg : 0 ≤ t := by
    have hbpos : (0 : Int) < (b : Int) := by exact_mod_cast hb
    have hmulnon : 0 ≤ (b : Int) * t := by rw [← ht]; exact_mod_cast (Nat.zero_le a)
    exact Int.nonneg_of_mul_nonneg_right hmulnon hbpos
  have htNat : a = b * t.toNat := by
    apply Int.ofNat_inj.mp
    rw [Int.natCast_mul]
    rw [Int.toNat_of_nonneg ht_nonneg]
    exact ht
  rw [htNat]
  rw [Nat.mul_comm b t.toNat]
  rw [Nat.mul_div_left t.toNat hb]
  rw [Int.natCast_mul]
  rw [show (t.toNat : Int) * (b : Int) = (b : Int) * (t.toNat : Int) by exact Int.mul_comm _ _]
  rw [Int.ediv_eq_of_eq_mul_right (by omega : (b : Int) ≠ 0) rfl]

private theorem sum_update_int : ∀ (p r0 : Nat), r0 < p → ∀ (s : Nat → Int) (v : Int),
    ((List.range p).map (fun r => if r == r0 then v + s r else s r)).sum =
      v + ((List.range p).map s).sum := by
  intro p
  induction p with
  | zero => intro r0 h; omega
  | succ p ih =>
      intro r0 hr0 s v
      rw [show p + 1 = Nat.succ p by omega, List.range_succ, List.map_append, List.sum_append,
        List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      by_cases heq : p = r0
      · have hb : p == r0 := by rw [heq]; simp
        rw [if_pos hb]
        have hpref : (List.range p).map (fun r => if r == r0 then v + s r else s r) =
            (List.range p).map s := by
          apply List.map_congr_left
          intro r hr
          have hrlt : r < p := List.mem_range.mp hr
          have hne : ¬ r == r0 := by rw [beq_iff_eq]; omega
          simp [hne]
        rw [hpref]
        omega
      · have hb : ¬ (p == r0) := by rw [beq_iff_eq]; exact heq
        rw [if_neg hb]
        have hlt : r0 < p := by omega
        rw [ih r0 hlt s v]
        omega

private theorem sum_map_zero_int (l : List Nat) : (l.map (fun _ : Nat => (0 : Int))).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      rw [show x :: xs = [x] ++ xs by rfl, List.map_append, List.sum_append]
      simp [ih]

private theorem striped_sum_partition_int (p : Nat) (hp : 0 < p) (F : Nat → Int) :
    ∀ xs : List Nat, (xs.map F).sum =
      ((List.range p).map (fun r => ((xs.filter (fun i => i % p == r)).map F).sum)).sum := by
  intro xs
  induction xs with
  | nil =>
      rw [List.map_nil, List.sum_nil]
      simpa using (sum_map_zero_int (List.range p)).symm
  | cons x xs ih =>
      rw [List.map_cons, List.sum_cons, ih]
      rw [← sum_update_int p (x % p) (Nat.mod_lt x hp) (fun r =>
        ((xs.filter (fun i => i % p == r)).map F).sum) (F x)]
      apply congrArg List.sum
      apply List.map_congr_left
      intro r hr
      by_cases h : x % p == r
      · have h' : x % p = r := Nat.eq_of_beq_eq_true h
        simp [h, h']
      · have h' : x % p ≠ r := by intro e; exact h (by rw [e]; simp)
        simp [h']
        intro hrx
        exact (h' hrx.symm).elim

private theorem range_filter_eq_single_sum (n t : Nat) (f : Nat → Int) :
    (((List.range n).filter (fun i => i == t)).map f).sum = if t < n then f t else 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append]
      by_cases h : n == t
      · have hnt : n = t := Nat.eq_of_beq_eq_true h
        subst t
        simp [ih]
      · have hne : n ≠ t := by intro he; exact h (by rw [he]; simp)
        have htlt : t < n ∨ n < t := by omega
        rcases htlt with ht | ht
        · have hif : t < n + 1 := by omega
          simp [h, ih, ht, hif]
        · have hif : ¬ t < n + 1 := by omega
          simp [h, ih, show ¬ t < n from by omega, hif]

private theorem sum_ite_eq_filter {α : Type} (p : α → Bool) (l : List α) (f : α → Int) :
    ((l.map (fun i => if p i then f i else 0)).sum) = (((l.filter p).map f).sum) := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem sum_filter_compl {α : Type} (f : α → Bool) (l : List α) (g : α → Int) :
    ((l.filter f).map g).sum + ((l.filter (fun x => ! f x)).map g).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : f x <;> simp [h, ih] <;> omega

-- =====================================================================
-- Round 2: lane value helpers (generic stride)
-- =====================================================================

private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun i : Nat =>
    (a.start + i * a.stride, a.base + (i : Int) * a.step))

private def laneValueAt (a : Lane) (v : Int) : Int :=
  a.base + ((v - (a.start : Int)) / (a.stride : Int)) * a.step

private def bValueAt (b : Lane) (v : Int) : Int :=
  b.base + ((v - (b.start : Int)) / (b.stride : Int)) * b.step

private theorem lanePoints_positions (a : Lane) :
    (lanePoints a).map (fun r => (r.1 : Int)) = progVals (positions a) := by
  unfold lanePoints progVals positions progVal
  rw [List.map_map]
  apply List.map_congr_left
  intro i hi
  simp [Int.mul_comm]

private theorem lanePoints_values (a : Lane) :
    (lanePoints a).map Prod.snd = (List.range a.n).map (fun i : Nat => a.base + (i : Int) * a.step) := by
  unfold lanePoints
  rw [List.map_map]
  rfl

private theorem filter_lanePoints_laneValue (a : Lane) (hs : 0 < a.stride) (p : Nat × Int → Bool) :
    (((lanePoints a).filter p).map (fun r => laneValueAt a r.1)) =
      ((lanePoints a).filter p).map Prod.snd := by
  apply List.map_congr_left
  intro r hr
  have hrmem : r ∈ lanePoints a := (List.mem_filter.mp hr).1
  rcases List.mem_map.mp hrmem with ⟨i, hi, rfl⟩
  dsimp [laneValueAt]
  have hsne : (a.stride : Int) ≠ 0 := by omega
  have hsub : (a.start : Int) + (i : Int) * (a.stride : Int) - (a.start : Int) =
      (i : Int) * (a.stride : Int) := by omega
  rw [hsub]
  rw [Int.ediv_eq_of_eq_mul_left hsne rfl]

private theorem filter_intPositions_g (a : Lane) (hs : 0 < a.stride) (p : Int → Bool) (g : Int → Int) :
    (((lanePoints a).map (fun r => (r.1 : Int))).filter p).map g =
      ((lanePoints a).filter (fun r => p (r.1 : Int))).map (fun r => g (r.1 : Int)) := by
  have hfilt : ((lanePoints a).map (fun r => (r.1 : Int))).filter p =
      (((lanePoints a).filter (fun r => p (r.1 : Int))).map (fun r => (r.1 : Int))) := by
    induction (lanePoints a) with
    | nil => simp
    | cons r rs ih => by_cases h : p (r.1 : Int) <;> simp [h, ih]
  rw [hfilt, List.map_map]
  rfl

private theorem lane_filter_sum_as_range (a : Lane) (P : Int → Bool) :
    ((lanePoints a).filter (fun r => P r.1)).map Prod.snd =
      ((List.range a.n).filter (fun i => P ((a.start + i * a.stride : Nat) : Int))).map
        (fun i : Nat => a.base + (i : Int) * a.step) := by
  unfold lanePoints
  rw [List.filter_map]
  rw [List.map_map]
  rfl

private theorem intersection_sum (A B : Progression) (hA : 0 < A.step) (hB : 0 < B.step)
    (F : Int → Int) :
    ((progVals (intersection A B)).map F).sum =
      (((progVals A).filter (fun v => contains B v = true)).map F).sum := by
  have hnd1 : (progVals (intersection A B)).Nodup := nodup_progVals _ (intersection_step_pos A B)
  have hnd2 : ((progVals A).filter (fun v => contains B v = true)).Nodup :=
    nodup_filter_bool (progVals A) (nodup_progVals A hA) _
  apply list_sum_perm_int
  apply List.Perm.map
  apply perm_of_mem_iff_nodup hnd1 hnd2
  intro v
  rw [List.mem_filter]
  constructor
  · intro hv
    have hc := (mem_progVals (intersection A B) v).mp hv
    have hboth := (intersection_contains A B v).mp hc
    exact ⟨(mem_progVals A v).mpr hboth.1, hboth.2⟩
  · rintro ⟨hvA, hvB⟩
    exact (mem_progVals (intersection A B) v).mpr
      ((intersection_contains A B v).mpr ⟨(mem_progVals A v).mp hvA, hvB⟩)

-- =====================================================================
-- Round 2: closed forms for deletedSum and aligned
-- =====================================================================

private theorem sum_laneValues_progression (a : Lane) (hs : 0 < a.stride) (r : Progression)
    (hrsub : ∀ v ∈ progVals r, contains (positions a) v = true) :
    ((progVals r).map (fun v => laneValueAt a v)).sum =
      (r.n : Int) * firstAt a r + tri r.n * slopeAt a r := by
  by_cases hrn0 : r.n = 0
  · simp [progVals, hrn0, tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  · have hrpos : 0 < r.n := Nat.pos_of_ne_zero hrn0
    by_cases hrle1 : r.n ≤ 1
    · have hrn1 : r.n = 1 := by omega
      have hpv : progVals r = [r.lo] := by
        unfold progVals
        rw [hrn1]
        simp [progVal]
      rw [hpv, hrn1]
      simp [tri, TinylambOpt.ResearchGoals.PairAggregates.choose2, firstAt, laneValueAt]
    · have hrge2 : 2 ≤ r.n := by omega
      have hc0 : contains r r.lo = true :=
        (contains_iff_mem r r.lo).mpr ⟨0, hrpos, by simp [progVal]⟩
      have hc1 : contains r (r.lo + r.step) = true :=
        (contains_iff_mem r _).mpr ⟨1, by omega, by simp [progVal]⟩
      have hmem0 : r.lo ∈ progVals r := (mem_progVals r r.lo).mpr hc0
      have hmem1 : r.lo + r.step ∈ progVals r := (mem_progVals r _).mpr hc1
      obtain ⟨i0, hi0, hval0⟩ := (contains_iff_mem (positions a) r.lo).mp (hrsub r.lo hmem0)
      obtain ⟨i1, hi1, hval1⟩ :=
        (contains_iff_mem (positions a) (r.lo + r.step)).mp (hrsub _ hmem1)
      change r.lo = (a.start : Int) + (i0 : Int) * (a.stride : Int) at hval0
      change r.lo + r.step = (a.start : Int) + (i1 : Int) * (a.stride : Int) at hval1
      have hdivstep : (a.stride : Int) ∣ (r.step : Int) := by
        refine ⟨(i1 : Int) - (i0 : Int), ?_⟩
        grind
      have hq0 : (r.lo - (a.start : Int)) / (a.stride : Int) = (i0 : Int) := by
        have hnum : r.lo - (a.start : Int) = (a.stride : Int) * (i0 : Int) := by grind
        rw [hnum]
        exact Int.ediv_eq_of_eq_mul_right (by omega : (a.stride : Int) ≠ 0) rfl
      have hdivk : ∀ k : Nat,
          (r.lo + (k : Int) * (r.step : Int) - (a.start : Int)) / (a.stride : Int) =
            (i0 : Int) + (k : Int) * ((r.step / a.stride : Nat) : Int) := by
        intro k
        have hstepInt : (r.step : Int) = (a.stride : Int) * ((r.step / a.stride : Nat) : Int) :=
          dvd_ediv_cast hs hdivstep
        have hnum : r.lo + (k : Int) * (r.step : Int) - (a.start : Int) =
            (a.stride : Int) * ((i0 : Int) + (k : Int) * ((r.step / a.stride : Nat) : Int)) := by
          rw [hstepInt]
          grind
        rw [hnum]
        exact Int.ediv_eq_of_eq_mul_right (by omega : (a.stride : Int) ≠ 0) rfl
      have hfun : ∀ k : Nat,
          laneValueAt a (r.lo + (k : Int) * (r.step : Int)) =
            firstAt a r + (k : Int) * slopeAt a r := by
        intro k
        dsimp [laneValueAt, firstAt, slopeAt]
        rw [hdivk, hq0]
        grind
      rw [show ((progVals r).map (fun v => laneValueAt a v)) =
          ((List.range r.n).map (fun k : Nat =>
            laneValueAt a (r.lo + (k : Int) * (r.step : Int)))) by
        unfold progVals progVal
        rw [List.map_map]
        rfl]
      rw [show ((List.range r.n).map (fun k : Nat =>
          laneValueAt a (r.lo + (k : Int) * (r.step : Int)))) =
          ((List.range r.n).map (fun k : Nat => firstAt a r + (k : Int) * slopeAt a r)) by
        apply List.map_congr_left
        intro k hk
        exact hfun k]
      rw [sum_arith]

private theorem lane_delete_sum (a : Lane) (hs : 0 < a.stride) (mask : Progression) :
    (((lanePoints a).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
      partialSum a a.n - deletedSum a mask := by
  by_cases hm0 : mask.n = 0
  · have hfalse : ∀ v : Int, contains mask v = false := by
      intro v
      simp [contains, hm0]
    have hfilt : (lanePoints a).filter (fun r => !contains mask r.1) = lanePoints a := by
      apply List.filter_eq_self.mpr
      intro r hr
      rw [hfalse]
      rfl
    rw [hfilt]
    have htotal : ((lanePoints a).map Prod.snd).sum = partialSum a a.n := by
      rw [lanePoints_values, partialSum_eq_sum]
    rw [← htotal]
    have hinter : intersection (positions a) mask = ⟨0, 1, 0⟩ := by
      simp [intersection, hm0]
    simp [deletedSum, hinter, tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  · by_cases hmstep : mask.step = 0
    · have hnpos : 0 < mask.n := Nat.pos_of_ne_zero hm0
      have hcm : ∀ v : Int, contains mask v ↔ v = mask.lo := by
        intro v
        simp [contains, hmstep, hnpos, lastVal]
        constructor <;> intro h <;> omega
      have hcmBool : ∀ v : Int, contains mask v = (v == mask.lo) := by
        intro v
        by_cases hv : v = mask.lo
        · subst v
          simp [(hcm mask.lo).mpr rfl]
        · have hbf : contains mask v = false := Bool.eq_false_of_ne_true (fun ht => hv ((hcm v).mp ht))
          rw [hbf]
          simp [hv]
      have hinter : intersection (positions a) mask =
          (if (positions a).n = 0 then ⟨0, 1, 0⟩ else
            ⟨mask.lo, 1, if contains (positions a) mask.lo then 1 else 0⟩) := by
        by_cases ha0 : (positions a).n = 0
        · simp [intersection, ha0]
        · have hsne : (positions a).step ≠ 0 := by simpa [positions] using (Nat.ne_of_gt hs)
          have ha0b : ((positions a).n == 0) = false := by
            cases h : ((positions a).n == 0) <;> simp_all [beq_iff_eq]
          have hm0b : (mask.n == 0) = false := by
            cases h : (mask.n == 0) <;> simp_all [beq_iff_eq]
          simp [intersection, ha0, hm0, hmstep, hnpos, hsne, ha0b, hm0b]
      have hdel : deletedSum a mask =
          (if contains (positions a) mask.lo then laneValueAt a mask.lo else 0) := by
        unfold deletedSum
        rw [hinter]
        by_cases ha0 : (positions a).n = 0
        · have ha0' : a.n = 0 := by simpa [positions] using ha0
          have hcf : contains (positions a) mask.lo = false := by simp [contains, positions, ha0']
          rw [if_pos ha0, hcf]
          simp [tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
        · rw [if_neg ha0]
          by_cases hc : contains (positions a) mask.lo
          · simp [hc, tri, TinylambOpt.ResearchGoals.PairAggregates.choose2, firstAt, laneValueAt]
          · simp [hc, tri, TinylambOpt.ResearchGoals.PairAggregates.choose2, laneValueAt]
      have hkept : (((lanePoints a).filter (fun r => contains mask r.1)).map Prod.snd).sum =
          (if contains (positions a) mask.lo then laneValueAt a mask.lo else 0) := by
        by_cases hc : contains (positions a) mask.lo
        · obtain ⟨i, hi, hval⟩ := (contains_iff_mem (positions a) mask.lo).mp hc
          have hval' : mask.lo = (a.start : Int) + (i : Int) * (a.stride : Int) := by
            simpa [positions, progVal] using hval
          let t : Nat := i
          have htlt : t < a.n := by simpa [positions, t] using hi
          rw [if_pos hc]
          rw [show (fun r : Nat × Int => contains mask r.1) = (fun r => (r.1 : Int) == mask.lo) by
            funext r
            exact hcmBool r.1]
          unfold lanePoints
          rw [List.filter_map]
          rw [List.map_map]
          change (((List.range a.n).filter (fun j : Nat =>
            (a.start + j * a.stride == mask.lo))).map
              (fun j : Nat => a.base + (j : Int) * a.step)).sum = laneValueAt a mask.lo
          have hpred : ∀ j : Nat, j ∈ List.range a.n →
              (a.start + j * a.stride == mask.lo) = (j == t) := by
            intro j hj
            have hjlt : j < a.n := List.mem_range.mp hj
            by_cases heq : a.start + j * a.stride = mask.lo
            · have hb1 : (a.start + j * a.stride == mask.lo) = true := by rw [heq]; simp
              have heqInt : (a.start : Int) + (j : Int) * (a.stride : Int) = mask.lo := by
                exact_mod_cast heq
              have hm : (j : Int) * (a.stride : Int) = (i : Int) * (a.stride : Int) := by omega
              have hsne : (a.stride : Int) ≠ 0 := by omega
              have hjiInt : (j : Int) = (i : Int) := Int.eq_of_mul_eq_mul_right hsne hm
              have hji : j = i := by exact_mod_cast hjiInt
              have hjt : j = t := by simpa [t] using hji
              rw [hb1]
              simp [hjt]
            · have hb1 : (a.start + j * a.stride == mask.lo) = false := by
                rw [beq_eq_false_iff_ne]
                exact heq
              have hjt : ¬ j = t := by
                intro hjt
                subst j
                apply heq
                exact_mod_cast hval'.symm
              rw [hb1]
              simp [hjt]
          rw [show (List.range a.n).filter (fun j : Nat => (a.start + j * a.stride == mask.lo)) =
              (List.range a.n).filter (fun j : Nat => (j == t)) by
            apply List.filter_congr
            intro j hj
            exact hpred j hj]
          rw [range_filter_eq_single_sum]
          rw [if_pos htlt]
          dsimp [t]
          dsimp [laneValueAt]
          rw [hval']
          have hsub : (a.start : Int) + (i : Int) * (a.stride : Int) - (a.start : Int) =
              (a.stride : Int) * (i : Int) := by grind
          rw [hsub]
          rw [Int.ediv_eq_of_eq_mul_right (by omega : (a.stride : Int) ≠ 0) rfl]
        · rw [if_neg hc]
          have hnil : (lanePoints a).filter (fun r => contains mask r.1) = [] := by
            apply List.eq_nil_iff_forall_not_mem.mpr
            intro r hr
            have hrmem : r ∈ lanePoints a := (List.mem_filter.mp hr).1
            have hrm := (hcm r.1).mp (List.mem_filter.mp hr).2
            have hcpos : contains (positions a) r.1 = true := by
              rcases List.mem_map.mp hrmem with ⟨j, hj, rfl⟩
              apply (contains_iff_mem (positions a) ((a.start + j * a.stride : Nat) : Int)).mpr
              refine ⟨j, by simpa [positions] using hj, ?_⟩
              simp [positions, progVal]
            rw [hrm] at hcpos
            exact hc hcpos
          rw [hnil]
          simp
      have hpart := sum_filter_compl (fun r : Nat × Int => contains mask r.1) (lanePoints a)
        (fun r => laneValueAt a r.1)
      rw [filter_lanePoints_laneValue a hs (fun r : Nat × Int => contains mask r.1)] at hpart
      rw [filter_lanePoints_laneValue a hs (fun r : Nat × Int => !contains mask r.1)] at hpart
      have htotmap : ((lanePoints a).map (fun r => laneValueAt a r.1)).sum =
          ((lanePoints a).map Prod.snd).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro r hr
        rcases List.mem_map.mp hr with ⟨j, hj, rfl⟩
        dsimp [laneValueAt]
        have hsne : (a.stride : Int) ≠ 0 := by omega
        have hsub : (a.start : Int) + (j : Int) * (a.stride : Int) - (a.start : Int) =
            (j : Int) * (a.stride : Int) := by omega
        rw [hsub]
        rw [Int.ediv_eq_of_eq_mul_left hsne rfl]
      rw [htotmap] at hpart
      have htotal : ((lanePoints a).map Prod.snd).sum = partialSum a a.n := by
        rw [lanePoints_values, partialSum_eq_sum]
      rw [hkept] at hpart
      rw [← htotal]
      rw [hdel]
      omega
    · have hmpos : 0 < mask.step := Nat.pos_of_ne_zero hmstep
      have hposA : 0 < (positions a).step := by simpa [positions] using hs
      have hdel : deletedSum a mask =
          ((progVals (intersection (positions a) mask)).map (fun v => laneValueAt a v)).sum := by
        unfold deletedSum
        exact (sum_laneValues_progression a hs (intersection (positions a) mask) (by
          intro v hv
          have hc := (mem_progVals (intersection (positions a) mask) v).mp hv
          exact (intersection_contains (positions a) mask v).mp hc |>.1)).symm
      rw [hdel]
      rw [intersection_sum (positions a) mask hposA hmpos]
      rw [show (List.filter (fun v => contains mask v = true) (progVals (positions a))) =
          (List.filter (fun v => contains mask v) (progVals (positions a))) by
        apply filter_congr_bool
        intro v hv
        cases h : contains mask v <;> simp [h]]
      rw [← lanePoints_positions a]
      rw [filter_intPositions_g a hs (fun v => contains mask v) (fun v => laneValueAt a v)]
      rw [filter_lanePoints_laneValue a hs (fun r : Nat × Int => contains mask (r.1 : Int))]
      have hpart := sum_filter_compl (fun r : Nat × Int => contains mask r.1) (lanePoints a)
        (fun r => laneValueAt a r.1)
      rw [filter_lanePoints_laneValue a hs (fun r : Nat × Int => contains mask r.1)] at hpart
      rw [filter_lanePoints_laneValue a hs (fun r : Nat × Int => !contains mask r.1)] at hpart
      have htotmap : ((lanePoints a).map (fun r => laneValueAt a r.1)).sum =
          ((lanePoints a).map Prod.snd).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro r hr
        rcases List.mem_map.mp hr with ⟨j, hj, rfl⟩
        dsimp [laneValueAt]
        have hsne : (a.stride : Int) ≠ 0 := by omega
        have hsub : (a.start : Int) + (j : Int) * (a.stride : Int) - (a.start : Int) =
            (j : Int) * (a.stride : Int) := by omega
        rw [hsub]
        rw [Int.ediv_eq_of_eq_mul_left hsne rfl]
      rw [htotmap] at hpart
      have htotal : ((lanePoints a).map Prod.snd).sum = partialSum a a.n := by
        rw [lanePoints_values, partialSum_eq_sum]
      rw [← htotal]
      omega

private theorem lane_delete_sum_range (a : Lane) (hs : 0 < a.stride) (mask : Progression) :
    ((List.range a.n).map (fun i : Nat =>
      if !contains mask ((a.start + i * a.stride : Nat) : Int)
      then a.base + (i : Int) * a.step else 0)).sum =
      partialSum a a.n - deletedSum a mask := by
  have hrange : ((List.range a.n).map (fun i : Nat =>
      if !contains mask ((a.start + i * a.stride : Nat) : Int)
      then a.base + (i : Int) * a.step else 0)).sum =
      (((lanePoints a).filter (fun r => !contains mask r.1)).map Prod.snd).sum := by
    rw [sum_ite_eq_filter]
    unfold lanePoints
    rw [List.filter_map]
    rw [List.map_map]
    rfl
  rw [hrange]
  exact lane_delete_sum a hs mask

-- =====================================================================
-- Round 2: aligned product closed form
-- =====================================================================

private theorem aligned_pvals_sum (a : Lane) (hs : 0 < a.stride) (b : Lane) (hb : 0 < b.stride) :
    aligned a b =
      ((progVals (intersection (positions a) (positions b))).map
        (fun v => laneValueAt a v * bValueAt b v)).sum := by
  let r := intersection (positions a) (positions b)
  by_cases hrn0 : r.n = 0
  · simp [aligned, r, hrn0, progVals, tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  · have hrpos : 0 < r.n := Nat.pos_of_ne_zero hrn0
    by_cases hrle1 : r.n ≤ 1
    · have hrn1 : r.n = 1 := by omega
      have hpv : progVals r = [r.lo] := by
        unfold progVals
        rw [hrn1]
        simp [progVal]
      rw [hpv]
      simp [hrn1, aligned, r, tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2,
        firstAt, laneValueAt, bValueAt]
    · have hrge2 : 2 ≤ r.n := by omega
      have hc0 : contains r r.lo = true :=
        (contains_iff_mem r r.lo).mpr ⟨0, hrpos, by simp [progVal]⟩
      have hc1 : contains r (r.lo + r.step) = true :=
        (contains_iff_mem r _).mpr ⟨1, by omega, by simp [progVal]⟩
      have hmem0 : r.lo ∈ progVals r := (mem_progVals r r.lo).mpr hc0
      have hmem1 : r.lo + r.step ∈ progVals r := (mem_progVals r _).mpr hc1
      have hboth0 := (intersection_contains (positions a) (positions b) r.lo).mp
        ((mem_progVals r r.lo).mp hmem0)
      have hboth1 := (intersection_contains (positions a) (positions b) (r.lo + r.step)).mp
        ((mem_progVals r _).mp hmem1)
      obtain ⟨ia, hia, hva⟩ := (contains_iff_mem (positions a) r.lo).mp hboth0.1
      obtain ⟨ib, hib, hvb⟩ := (contains_iff_mem (positions b) r.lo).mp hboth0.2
      obtain ⟨ia1, hia1, hva1⟩ := (contains_iff_mem (positions a) (r.lo + r.step)).mp hboth1.1
      obtain ⟨ib1, hib1, hvb1⟩ := (contains_iff_mem (positions b) (r.lo + r.step)).mp hboth1.2
      change r.lo = (a.start : Int) + (ia : Int) * (a.stride : Int) at hva
      change r.lo = (b.start : Int) + (ib : Int) * (b.stride : Int) at hvb
      change r.lo + r.step = (a.start : Int) + (ia1 : Int) * (a.stride : Int) at hva1
      change r.lo + r.step = (b.start : Int) + (ib1 : Int) * (b.stride : Int) at hvb1
      have hdivA : (a.stride : Int) ∣ (r.step : Int) := by
        refine ⟨(ia1 : Int) - (ia : Int), ?_⟩
        grind
      have hdivB : (b.stride : Int) ∣ (r.step : Int) := by
        refine ⟨(ib1 : Int) - (ib : Int), ?_⟩
        grind
      have hqa0 : (r.lo - (a.start : Int)) / (a.stride : Int) = (ia : Int) := by
        have hnum : r.lo - (a.start : Int) = (a.stride : Int) * (ia : Int) := by grind
        rw [hnum]
        exact Int.ediv_eq_of_eq_mul_right (by omega : (a.stride : Int) ≠ 0) rfl
      have hqb0 : (r.lo - (b.start : Int)) / (b.stride : Int) = (ib : Int) := by
        have hnum : r.lo - (b.start : Int) = (b.stride : Int) * (ib : Int) := by grind
        rw [hnum]
        exact Int.ediv_eq_of_eq_mul_right (by omega : (b.stride : Int) ≠ 0) rfl
      have hdivkA : ∀ k : Nat,
          (r.lo + (k : Int) * (r.step : Int) - (a.start : Int)) / (a.stride : Int) =
            (ia : Int) + (k : Int) * ((r.step / a.stride : Nat) : Int) := by
        intro k
        have hstepInt : (r.step : Int) =
            (a.stride : Int) * ((r.step / a.stride : Nat) : Int) := dvd_ediv_cast hs hdivA
        have hnum : r.lo + (k : Int) * (r.step : Int) - (a.start : Int) =
            (a.stride : Int) * ((ia : Int) + (k : Int) * ((r.step / a.stride : Nat) : Int)) := by
          rw [hstepInt]
          grind
        rw [hnum]
        exact Int.ediv_eq_of_eq_mul_right (by omega : (a.stride : Int) ≠ 0) rfl
      have hdivkB : ∀ k : Nat,
          (r.lo + (k : Int) * (r.step : Int) - (b.start : Int)) / (b.stride : Int) =
            (ib : Int) + (k : Int) * ((r.step / b.stride : Nat) : Int) := by
        intro k
        have hstepInt : (r.step : Int) =
            (b.stride : Int) * ((r.step / b.stride : Nat) : Int) := dvd_ediv_cast hb hdivB
        have hnum : r.lo + (k : Int) * (r.step : Int) - (b.start : Int) =
            (b.stride : Int) * ((ib : Int) + (k : Int) * ((r.step / b.stride : Nat) : Int)) := by
          rw [hstepInt]
          grind
        rw [hnum]
        exact Int.ediv_eq_of_eq_mul_right (by omega : (b.stride : Int) ≠ 0) rfl
      have hterm : ∀ k : Nat,
          laneValueAt a (r.lo + (k : Int) * (r.step : Int)) *
              bValueAt b (r.lo + (k : Int) * (r.step : Int)) =
            (firstAt a r + (k : Int) * slopeAt a r) *
              (firstAt b r + (k : Int) * slopeAt b r) := by
        intro k
        dsimp [laneValueAt, bValueAt, firstAt, slopeAt]
        rw [hdivkA, hdivkB, hqa0, hqb0]
        grind
      rw [show ((progVals r).map (fun v => laneValueAt a v * bValueAt b v)) =
          ((List.range r.n).map (fun k : Nat =>
            laneValueAt a (r.lo + (k : Int) * (r.step : Int)) *
              bValueAt b (r.lo + (k : Int) * (r.step : Int)))) by
        unfold progVals progVal
        rw [List.map_map]
        rfl]
      rw [show ((List.range r.n).map (fun k : Nat =>
          laneValueAt a (r.lo + (k : Int) * (r.step : Int)) *
              bValueAt b (r.lo + (k : Int) * (r.step : Int)))) =
          ((List.range r.n).map (fun k : Nat =>
            (firstAt a r + (k : Int) * slopeAt a r) *
              (firstAt b r + (k : Int) * slopeAt b r))) by
        apply List.map_congr_left
        intro k hk
        exact hterm k]
      rw [sum_product_arith]
      unfold aligned
      dsimp [r]

private theorem lane_aligned_sum (a : Lane) (hs : 0 < a.stride) (b : Lane) (hb : 0 < b.stride) :
    ((lanePoints a).map (fun r => ((List.range b.n).map (fun i : Nat =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)).sum =
    aligned a b := by
  have hposb : 0 < (positions b).step := by simpa [positions] using hb
  have hposa : 0 < (positions a).step := by simpa [positions] using hs
  rw [aligned_pvals_sum a hs b hb]
  rw [intersection_sum (positions a) (positions b) hposa hposb]
  rw [show (List.filter (fun v => contains (positions b) v = true) (progVals (positions a))) =
      (List.filter (fun v => contains (positions b) v) (progVals (positions a))) by
    apply filter_congr_bool
    intro v hv
    cases h : contains (positions b) v <;> simp [h]]
  rw [← lanePoints_positions a]
  rw [filter_intPositions_g a hs (fun v => contains (positions b) v)
    (fun v => laneValueAt a v * bValueAt b v)]
  have hinner : ∀ r : Nat × Int, r ∈ lanePoints a →
      ((List.range b.n).map (fun i : Nat =>
        if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum =
      (if contains (positions b) (r.1 : Int) then
        laneValueAt a (r.1 : Int) * bValueAt b (r.1 : Int) else 0) := by
    intro r hr
    by_cases hc : contains (positions b) (r.1 : Int)
    · rw [if_pos hc]
      obtain ⟨j, hj, hvj⟩ := (contains_iff_mem (positions b) (r.1 : Int)).mp hc
      change (r.1 : Int) = (b.start : Int) + (j : Int) * (b.stride : Int) at hvj
      have hbneI : (b.stride : Int) ≠ 0 := by omega
      have hdivj : ((r.1 : Int) - (b.start : Int)) / (b.stride : Int) = (j : Int) := by
        rw [hvj]
        have hsub : (b.start : Int) + (j : Int) * (b.stride : Int) - (b.start : Int) =
            (b.stride : Int) * (j : Int) := by grind
        rw [hsub]
        exact Int.ediv_eq_of_eq_mul_right hbneI rfl
      have hp : ∀ i : Nat, i ∈ List.range b.n →
          (r.1 == b.start + i * b.stride) = (i == j) := by
        intro i hi
        have hil : i < b.n := List.mem_range.mp hi
        by_cases heq : i = j
        · subst i
          have hnat : r.1 = b.start + j * b.stride := by exact_mod_cast hvj
          simp [hnat]
        · have hb1 : (r.1 == b.start + i * b.stride) = false := by
            rw [beq_eq_false_iff_ne]
            intro h
            have hiVal : (r.1 : Int) = (b.start : Int) + (i : Int) * (b.stride : Int) := by
              exact_mod_cast h
            have : (b.start : Int) + (j : Int) * (b.stride : Int) =
                (b.start : Int) + (i : Int) * (b.stride : Int) := by rw [← hvj, hiVal]
            have hmul : (j : Int) * (b.stride : Int) = (i : Int) * (b.stride : Int) := by omega
            have hji : (j : Int) = (i : Int) := Int.eq_of_mul_eq_mul_right hbneI hmul
            exact heq (by exact_mod_cast hji.symm)
          rw [hb1]
          simp [heq]
      rw [sum_ite_eq_filter]
      rw [show (List.range b.n).filter (fun i : Nat => (r.1 == b.start + i * b.stride)) =
          (List.range b.n).filter (fun i : Nat => (i == j)) by
        apply List.filter_congr
        intro i hi
        exact hp i hi]
      rw [range_filter_eq_single_sum]
      have hjb : j < b.n := by simpa [positions] using hj
      rw [if_pos hjb]
      dsimp [bValueAt]
      rw [hdivj]
      have hrval : laneValueAt a (r.1 : Int) = r.2 := by
        rcases List.mem_map.mp hr with ⟨i, hi, rfl⟩
        dsimp [laneValueAt]
        have hsne : (a.stride : Int) ≠ 0 := by omega
        have hsub : (a.start : Int) + (i : Int) * (a.stride : Int) - (a.start : Int) =
            (i : Int) * (a.stride : Int) := by grind
        rw [hsub]
        rw [Int.ediv_eq_of_eq_mul_left hsne rfl]
      rw [hrval]
      grind
    · rw [if_neg hc]
      rw [sum_ite_eq_filter]
      have hnil : (List.range b.n).filter (fun i : Nat => (r.1 == b.start + i * b.stride)) = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro t ht
        have htmem := (List.mem_filter.mp ht).1
        have htlt : t < b.n := List.mem_range.mp htmem
        have heq := (List.mem_filter.mp ht).2
        have hv : (r.1 : Int) = (b.start : Int) + (t : Int) * (b.stride : Int) := by
          exact_mod_cast (Nat.eq_of_beq_eq_true heq)
        apply hc
        apply (contains_iff_mem (positions b) (r.1 : Int)).mpr
        refine ⟨t, htlt, ?_⟩
        simpa [positions, progVal] using hv
      rw [hnil]
      simp
  rw [show ((lanePoints a).map (fun r => ((List.range b.n).map (fun i : Nat =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)) =
      ((lanePoints a).map (fun r => if contains (positions b) (r.1 : Int) then
        laneValueAt a (r.1 : Int) * bValueAt b (r.1 : Int) else 0)) by
    apply List.map_congr_left
    intro r hr
    exact hinner r hr]
  rw [show ((lanePoints a).map (fun r => if contains (positions b) (r.1 : Int) then
        laneValueAt a (r.1 : Int) * bValueAt b (r.1 : Int) else 0)).sum =
      (((lanePoints a).filter (fun r => contains (positions b) (r.1 : Int))).map
        (fun r => laneValueAt a (r.1 : Int) * bValueAt b (r.1 : Int))).sum by
    exact sum_ite_eq_filter (fun r : Nat × Int => contains (positions b) (r.1 : Int))
      (lanePoints a) (fun r => laneValueAt a (r.1 : Int) * bValueAt b (r.1 : Int))]

private theorem lane_aligned_sum_range (a : Lane) (hs : 0 < a.stride) (b : Lane) (hb : 0 < b.stride) :
    ((List.range a.n).map (fun i : Nat =>
      ((List.range b.n).map (fun j : Nat =>
        if (a.start + i * a.stride : Nat) == b.start + j * b.stride
        then (a.base + (i : Int) * a.step) * (b.base + b.step * (j : Int)) else 0)).sum)).sum =
    aligned a b := by
  have hmap : (lanePoints a).map (fun r => ((List.range b.n).map (fun i : Nat =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum) =
      (List.range a.n).map (fun i : Nat => ((List.range b.n).map (fun j : Nat =>
        if (a.start + i * a.stride : Nat) == b.start + j * b.stride
        then (a.base + (i : Int) * a.step) * (b.base + b.step * (j : Int)) else 0)).sum) := by
    unfold lanePoints
    rw [List.map_map]
    rfl
  rw [← hmap]
  exact lane_aligned_sum a hs b hb

-- =====================================================================
-- Round 2: final goals
-- =====================================================================

private theorem sum_range_trunc {p w : Nat} (hwp : w ≤ p) (g : Nat → Int) :
    ((List.range p).map (fun r => if r < w then g r else 0)).sum = ((List.range w).map g).sum := by
  induction p with
  | zero =>
      have hw0 : w = 0 := by omega
      subst w
      simp
  | succ p ih =>
      by_cases hwp' : w ≤ p
      · rw [List.range_succ, List.map_append, List.sum_append, ih hwp']
        have hnw : ¬ p < w := by omega
        simp [hnw]
      · have heq : w = p + 1 := by omega
        subst w
        rw [List.range_succ, List.map_append, List.sum_append]
        have hall : ∀ r ∈ List.range p, r < p + 1 := by
          intro r hr
          exact Nat.lt_trans (List.mem_range.mp hr) (Nat.lt_succ_self p)
        rw [show (List.range p).map (fun r => if r < p + 1 then g r else 0) =
            (List.range p).map g from
          List.map_congr_left (fun r hr => by simp [hall r hr])]
        simp

theorem sound_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.PagedTombstones.DeleteProgression.Goal := by
  intro c mask
  let w := width c
  let p := 2 * w
  have hw : 0 < w := by dsimp [w, width]; omega
  have hp : 0 < p := by dsimp [p]; omega
  have hrows : (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
      ((List.range c.n).map (fun i : Nat =>
        if i % p < w && !contains mask (i : Int) then c.base + c.step * (i : Int) else 0)).sum := by
    unfold rows
    dsimp [layout]
    induction (List.range c.n) with
    | nil => simp
    | cons i is ih =>
        rw [List.filterMap_cons]
        simp at ih
        have ht := tile_even_iff w i hw
        have ht' : ((i / width c) % 2 == 0) ↔ i % p < w := by simpa [w, p] using ht
        by_cases h : (i / width c) % 2 == 0
        · have hiw : i % p < w := ht'.mp h
          simp [h]
          rw [List.filter_cons]
          by_cases hm : contains mask (i : Int)
          · have hf : (!contains mask (i : Int)) = false := by simp [hm]
            simp [hf]
            rw [ih]
            simp [hiw, hm]
          · have hf : (!contains mask (i : Int)) = true := by simp [hm]
            simp [hf]
            rw [ih]
            simp [hiw, hm]
        · have hnot : ¬ i % p < w := by intro hiw; exact h (ht'.mpr hiw)
          simp [h] at ih ⊢
          rw [ih]
          simp [hnot]
  rw [hrows]
  let F : Nat → Int := fun i =>
    if i % p < w && !contains mask (i : Int) then c.base + c.step * (i : Int) else 0
  change ((List.range c.n).map F).sum =
    ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum
  rw [striped_sum_partition_int p hp F (List.range c.n)]
  have hpart : (List.range p).map (fun r =>
      (((List.range c.n).filter (fun i => i % p == r)).map F).sum) =
      (List.range p).map (fun r =>
        ((List.range ((c.n - r + p - 1) / p)).map (fun q : Nat => F (r + q * p))).sum) := by
    apply List.map_congr_left
    intro r hr
    have hrp : r < p := List.mem_range.mp hr
    rw [range_filter_mod_eq c.n p r hp hrp]
    rw [List.map_map]
    rfl
  rw [hpart]
  have hcol : ∀ r, r < p →
      ((List.range ((c.n - r + p - 1) / p)).map (fun q : Nat => F (r + q * p))).sum =
      if r < w then ((List.range ((c.n - r + p - 1) / p)).map (fun q : Nat =>
        if !contains mask ((r + q * p : Nat) : Int)
        then (c.base + c.step * (r : Int)) + (q : Int) * (c.step * p) else 0)).sum else 0 := by
    intro r hr
    have hmod : ∀ q : Nat, (r + q * p) % p = r := by
      intro q
      rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hr]
    by_cases hrw : r < w
    · simp only [hrw, if_true]
      apply congrArg List.sum
      apply List.map_congr_left
      intro q hq
      dsimp [F]
      rw [hmod q]
      simp [hrw]
      have hval : c.base + c.step * ((r : Int) + (q : Int) * (p : Int)) =
          (c.base + c.step * (r : Int)) + (q : Int) * (c.step * p) := by
        dsimp [p]
        grind
      rw [hval]
    · have hzero : ∀ q : Nat, F (r + q * p) = 0 := by
        intro q
        dsimp [F]
        rw [hmod q]
        simp [hrw]
      rw [show (List.range ((c.n - r + p - 1) / p)).map (fun q : Nat => F (r + q * p)) =
          (List.range ((c.n - r + p - 1) / p)).map (fun _ : Nat => (0 : Int)) by
        apply List.map_congr_left
        intro q hq
        exact hzero q]
      rw [sum_map_zero_int]
      rw [if_neg hrw]
  rw [show (List.range p).map (fun r =>
      ((List.range ((c.n - r + p - 1) / p)).map (fun q : Nat => F (r + q * p))).sum) =
      (List.range p).map (fun r =>
        if r < w then ((List.range ((c.n - r + p - 1) / p)).map (fun q : Nat =>
          if !contains mask ((r + q * p : Nat) : Int)
          then (c.base + c.step * (r : Int)) + (q : Int) * (c.step * p) else 0)).sum else 0) by
    apply List.map_congr_left
    intro r hr
    exact hcol r (List.mem_range.mp hr)]
  rw [sum_range_trunc (by dsimp [p]; omega)]
  let g : Nat → Int := fun r =>
    ((List.range ((c.n - r + p - 1) / p)).map (fun q : Nat =>
      if !contains mask ((r + q * p : Nat) : Int)
      then (c.base + c.step * (r : Int)) + (q : Int) * (c.step * p) else 0)).sum
  change ((List.range w).map g).sum = _
  rw [show ((List.range w).map g).sum =
      ((List.range (min c.n w)).map g).sum by
    by_cases hnw : c.n ≤ w
    · have hmin : min c.n w = c.n := Nat.min_eq_left hnw
      rw [hmin]
      have hrep : (List.range w).map g =
          (List.range w).map (fun r => if r < c.n then g r else 0) := by
        apply List.map_congr_left
        intro r hr
        have hrw : r < w := List.mem_range.mp hr
        by_cases hrc : r < c.n
        · simp [hrc]
        · dsimp [g]
          have hcnt : (c.n - r + p - 1) / p = 0 := by
            rw [show c.n - r = 0 by omega, Nat.zero_add]
            exact Nat.div_eq_of_lt (by omega)
          rw [hcnt]
          simp [hrc]
      rw [hrep]
      exact sum_range_trunc hnw g
    · have hmin : min c.n w = w := Nat.min_eq_right (by omega)
      rw [hmin]]
  have hlane : ∀ r, r < min c.n w →
      g r = partialSum (lane c.n r p (c.base + c.step * r) (c.step * p))
              (lane c.n r p (c.base + c.step * r) (c.step * p)).n -
            deletedSum (lane c.n r p (c.base + c.step * r) (c.step * p)) mask := by
    intro r hr
    have hrn : r < c.n := Nat.lt_of_lt_of_le hr (Nat.min_le_left _ _)
    have hrw : r < w := Nat.lt_of_lt_of_le hr (Nat.min_le_right _ _)
    have hlane_eq : lane c.n r p (c.base + c.step * r) (c.step * p) =
        ⟨r, p, (c.n - r + p - 1) / p, c.base + c.step * r, c.step * p⟩ := by
      unfold lane
      rfl
    rw [hlane_eq]
    dsimp [g]
    simpa [Int.mul_comm, Int.add_assoc] using
      (lane_delete_sum_range ⟨r, p, (c.n - r + p - 1) / p, c.base + c.step * r, c.step * p⟩
        (by dsimp [p]; omega) mask)
  rw [show (List.range (min c.n w)).map g =
      (List.range (min c.n w)).map (fun r =>
        partialSum (lane c.n r p (c.base + c.step * r) (c.step * p))
              (lane c.n r p (c.base + c.step * r) (c.step * p)).n -
            deletedSum (lane c.n r p (c.base + c.step * r) (c.step * p)) mask) by
    apply List.map_congr_left
    intro r hr
    exact hlane r (List.mem_range.mp hr)]
  dsimp [layout, p, w]
  rw [List.map_map]
  rfl

theorem sound_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.PagedTombstones.AlignedProduct.Goal := by
  intro c b hb
  let w := width c
  let p := 2 * w
  have hw : 0 < w := by dsimp [w, width]; omega
  have hp : 0 < p := by dsimp [p]; omega
  have hrows :
      ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
        if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)).sum =
      ((List.range c.n).map (fun i : Nat =>
        if i % p < w then ((List.range b.n).map (fun j : Nat =>
          if i == b.start + j * b.stride
          then (c.base + c.step * (i : Int)) * (b.base + b.step * (j : Int)) else 0)).sum
        else 0)).sum := by
    unfold rows
    dsimp [layout]
    induction (List.range c.n) with
    | nil => simp
    | cons i is ih =>
        rw [List.filterMap_cons]
        simp at ih
        have ht := tile_even_iff w i hw
        have ht' : ((i / width c) % 2 == 0) ↔ i % p < w := by simpa [w, p] using ht
        by_cases h : (i / width c) % 2 == 0
        · have hiw : i % p < w := ht'.mp h
          simp [h] at ih ⊢
          rw [ih]
          simp [hiw]
        · have hnot : ¬ i % p < w := by intro hiw; exact h (ht'.mpr hiw)
          simp [h] at ih ⊢
          rw [ih]
          simp [hnot]
  rw [hrows]
  let F : Nat → Int := fun i =>
    if i % p < w then ((List.range b.n).map (fun j : Nat =>
      if i == b.start + j * b.stride
      then (c.base + c.step * (i : Int)) * (b.base + b.step * (j : Int)) else 0)).sum
    else 0
  change ((List.range c.n).map F).sum = ((layout.lanes c).map (aligned · b)).sum
  rw [striped_sum_partition_int p hp F (List.range c.n)]
  have hpart : (List.range p).map (fun r =>
      (((List.range c.n).filter (fun i => i % p == r)).map F).sum) =
      (List.range p).map (fun r =>
        ((List.range ((c.n - r + p - 1) / p)).map (fun q : Nat => F (r + q * p))).sum) := by
    apply List.map_congr_left
    intro r hr
    have hrp : r < p := List.mem_range.mp hr
    rw [range_filter_mod_eq c.n p r hp hrp]
    rw [List.map_map]
    rfl
  rw [hpart]
  have hcol : ∀ r, r < p →
      ((List.range ((c.n - r + p - 1) / p)).map (fun q : Nat => F (r + q * p))).sum =
      if r < w then ((List.range ((c.n - r + p - 1) / p)).map (fun q : Nat =>
        ((List.range b.n).map (fun j : Nat =>
          if (r + q * p : Nat) == b.start + j * b.stride
          then ((c.base + c.step * (r : Int)) + (q : Int) * (c.step * p)) *
               (b.base + b.step * (j : Int)) else 0)).sum)).sum else 0 := by
    intro r hr
    have hmod : ∀ q : Nat, (r + q * p) % p = r := by
      intro q
      rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hr]
    by_cases hrw : r < w
    · simp only [hrw, if_true]
      apply congrArg List.sum
      apply List.map_congr_left
      intro q hq
      dsimp [F]
      rw [hmod q]
      simp [hrw]
      have hval : c.base + c.step * ((r : Int) + (q : Int) * (p : Int)) =
          (c.base + c.step * (r : Int)) + (q : Int) * (c.step * p) := by
        dsimp [p]
        grind
      rw [hval]
    · have hzero : ∀ q : Nat, F (r + q * p) = 0 := by
        intro q
        dsimp [F]
        rw [hmod q]
        simp [hrw]
      rw [show (List.range ((c.n - r + p - 1) / p)).map (fun q : Nat => F (r + q * p)) =
          (List.range ((c.n - r + p - 1) / p)).map (fun _ : Nat => (0 : Int)) by
        apply List.map_congr_left
        intro q hq
        exact hzero q]
      rw [sum_map_zero_int]
      rw [if_neg hrw]
  rw [show (List.range p).map (fun r =>
      ((List.range ((c.n - r + p - 1) / p)).map (fun q : Nat => F (r + q * p))).sum) =
      (List.range p).map (fun r =>
        if r < w then ((List.range ((c.n - r + p - 1) / p)).map (fun q : Nat =>
          ((List.range b.n).map (fun j : Nat =>
            if (r + q * p : Nat) == b.start + j * b.stride
            then ((c.base + c.step * (r : Int)) + (q : Int) * (c.step * p)) *
                 (b.base + b.step * (j : Int)) else 0)).sum)).sum else 0) by
    apply List.map_congr_left
    intro r hr
    exact hcol r (List.mem_range.mp hr)]
  rw [sum_range_trunc (by dsimp [p]; omega)]
  let g : Nat → Int := fun r =>
    ((List.range ((c.n - r + p - 1) / p)).map (fun q : Nat =>
      ((List.range b.n).map (fun j : Nat =>
        if (r + q * p : Nat) == b.start + j * b.stride
        then ((c.base + c.step * (r : Int)) + (q : Int) * (c.step * p)) *
             (b.base + b.step * (j : Int)) else 0)).sum)).sum
  change ((List.range w).map g).sum = _
  rw [show ((List.range w).map g).sum =
      ((List.range (min c.n w)).map g).sum by
    by_cases hnw : c.n ≤ w
    · have hmin : min c.n w = c.n := Nat.min_eq_left hnw
      rw [hmin]
      have hrep : (List.range w).map g =
          (List.range w).map (fun r => if r < c.n then g r else 0) := by
        apply List.map_congr_left
        intro r hr
        have hrw : r < w := List.mem_range.mp hr
        by_cases hrc : r < c.n
        · simp [hrc]
        · dsimp [g]
          have hcnt : (c.n - r + p - 1) / p = 0 := by
            rw [show c.n - r = 0 by omega, Nat.zero_add]
            exact Nat.div_eq_of_lt (by omega)
          rw [hcnt]
          simp [hrc]
      rw [hrep]
      exact sum_range_trunc hnw g
    · have hmin : min c.n w = w := Nat.min_eq_right (by omega)
      rw [hmin]]
  have hlane : ∀ r, r < min c.n w →
      g r = aligned (lane c.n r p (c.base + c.step * r) (c.step * p)) b := by
    intro r hr
    have hrn : r < c.n := Nat.lt_of_lt_of_le hr (Nat.min_le_left _ _)
    have hrw : r < w := Nat.lt_of_lt_of_le hr (Nat.min_le_right _ _)
    have hlane_eq : lane c.n r p (c.base + c.step * r) (c.step * p) =
        ⟨r, p, (c.n - r + p - 1) / p, c.base + c.step * r, c.step * p⟩ := by
      unfold lane
      rfl
    rw [hlane_eq]
    dsimp [g]
    simpa [Int.mul_comm, Int.add_assoc] using
      (lane_aligned_sum_range ⟨r, p, (c.n - r + p - 1) / p, c.base + c.step * r, c.step * p⟩
        (by dsimp [p]; omega) b hb)
  rw [show (List.range (min c.n w)).map g =
      (List.range (min c.n w)).map (fun r =>
        aligned (lane c.n r p (c.base + c.step * r) (c.step * p)) b) by
    apply List.map_congr_left
    intro r hr
    exact hlane r (List.mem_range.mp hr)]
  dsimp [layout, p, w]
  rw [List.map_map]
  rfl

end TinylambOpt.ResearchProofs.Round2.Task51

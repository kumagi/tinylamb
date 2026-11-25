import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.LinearOrdinal.Remaining

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal

/--! ## Generic list helpers -/

private theorem list_sum_perm {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem perm_length {α : Type} {l₁ l₂ : List α} (p : l₁.Perm l₂) :
    l₁.length = l₂.length := p.length_eq

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

private theorem sum_map_mul_int (l : List Int) (c : Int) :
    (l.map (fun x => c * x)).sum = c * l.sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.mul_add]

private theorem sum_map_const_int {α : Type} (l : List α) (v : Int) :
    (l.map (fun _ => v)).sum = (l.length : Int) * v := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.natCast_add, Int.add_mul]; omega

private theorem sum_range_succ (f : Nat → Int) (n : Nat) :
    ((List.range (n + 1)).map f).sum = ((List.range n).map f).sum + f n := by
  rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
  simp

private theorem sum_map_ite_zero {α : Type} (l : List α) (p : α → Bool) (f : α → Int) :
    ((l.filter p).map f).sum = (l.map (fun x => if p x then f x else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem sum_filter_add_compl {α : Type} (l : List α) (p : α → Bool) (f : α → Int) :
    ((l.filter p).map f).sum + ((l.filter (fun x => !p x)).map f).sum = (l.map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih] <;> omega

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

private theorem rows_range_map (lay : Layout) (c : Config) (f : Nat → Int)
    (h : ∀ i, i < c.n → lay.read c i = some (f i)) :
    rows lay c = (List.range c.n).map (fun i => (i, f i)) := by
  unfold rows
  exact range_filterMap_all c.n (fun i => (lay.read c i).map (fun v => (i, v)))
    (fun i => (i, f i)) (by intro i hi; simp [h i hi])

/--! ## Arithmetic on lanes -/

private theorem choose2_succ (k : Nat) :
    TinylambOpt.ResearchGoals.PairAggregates.choose2 (k + 1) =
      TinylambOpt.ResearchGoals.PairAggregates.choose2 k + k := by
  unfold TinylambOpt.ResearchGoals.PairAggregates.choose2
  by_cases hk : k = 0
  · simp [hk]
  · have hkm1 : k + 1 - 1 = k := by omega
    rw [hkm1]
    have hmul1 : (k + 1) * k = k * k + k := by rw [Nat.add_mul, Nat.one_mul]
    have hmul2 : k * (k - 1) = k * k - k := by rw [Nat.mul_sub_left_distrib, Nat.mul_one]
    have hle : k ≤ k * k := by
      have := Nat.mul_le_mul_left k (show 1 ≤ k by omega)
      simpa using this
    have hcan : k * k - k + k = k * k := Nat.sub_add_cancel hle
    have hEq : (k + 1) * k = k * (k - 1) + 2 * k := by rw [hmul1, hmul2]; omega
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

private theorem sum_squares_eq (k : Nat) :
    ((List.range k).map (fun i : Nat => (i : Int) * (i : Int))).sum = (squares k : Int) := by
  induction k with
  | zero => simp [squares]
  | succ k ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih, squares_succ]
      simp [Int.natCast_add, Int.natCast_mul]

/--! ## Finite progressions and CRT intersections (mirrors EncodedPages4) -/

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
  simp [contains, lastVal]
  omega

private theorem contains_step_one (lo : Int) (n : Nat) (v : Int) :
    contains ⟨lo, 1, n⟩ v = true ↔ 0 < n ∧ lo ≤ v ∧ v - lo < (n : Int) := by
  unfold contains lastVal
  simp
  by_cases hn : 0 < n
  · have hncast : ((n - 1 : Nat) : Int) = (n : Int) - 1 := by omega
    rw [hncast]
    omega
  · simp [hn]

private theorem contains_bounds (a : Progression) (v : Int) (h : contains a v = true) :
    a.lo ≤ v ∧ v ≤ lastVal a := by
  have hm := (contains_iff_mem a v).mp h
  obtain ⟨k, hk, hval⟩ := hm
  unfold progVal at hval
  have hsnon : (0 : Int) ≤ (a.step : Int) := by exact_mod_cast (Nat.zero_le a.step)
  have hkint : (k : Int) ≤ ((a.n - 1 : Nat) : Int) := by exact_mod_cast (by omega)
  have hmul' : (k : Int) * (a.step : Int) ≤ ((a.n - 1 : Nat) : Int) * (a.step : Int) :=
    Int.mul_le_mul_of_nonneg_right hkint hsnon
  unfold lastVal
  constructor
  · rw [hval]
    exact Int.le_add_of_nonneg_right (Int.mul_nonneg (by exact_mod_cast Nat.zero_le k) hsnon)
  · rw [hval]
    exact Int.add_le_add_left hmul' a.lo

private theorem contains_dvd (a : Progression) (v : Int) (h : contains a v = true) :
    (a.step : Int) ∣ v - a.lo := by
  obtain ⟨k, _, hval⟩ := (contains_iff_mem a v).mp h
  unfold progVal at hval
  refine ⟨(k : Int), ?_⟩
  grind

private theorem common_gap_dvd (a b : Progression) (v : Int)
    (ha : contains a v = true) (hb : contains b v = true) :
    (a.step.gcd b.step : Int) ∣ b.lo - a.lo := by
  obtain ⟨i, _, hi⟩ := (contains_iff_mem a v).mp ha
  obtain ⟨j, _, hj⟩ := (contains_iff_mem b v).mp hb
  unfold progVal at hi hj
  have hg1 : ((a.step.gcd b.step : Nat) : Int) ∣ (b.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_right a.step b.step)
  have hg2 : ((a.step.gcd b.step : Nat) : Int) ∣ (a.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_left a.step b.step)
  have h1' : ((a.step.gcd b.step : Nat) : Int) ∣ (j : Int) * (b.step : Int) :=
    Int.dvd_mul_of_dvd_right hg1
  have h1 : ((a.step.gcd b.step : Nat) : Int) ∣ (b.step : Int) * (j : Int) := by
    simpa [Int.mul_comm] using h1'
  have h2' : ((a.step.gcd b.step : Nat) : Int) ∣ (i : Int) * (a.step : Int) :=
    Int.dvd_mul_of_dvd_right hg2
  have h2 : ((a.step.gcd b.step : Nat) : Int) ∣ (a.step : Int) * (i : Int) := by
    simpa [Int.mul_comm] using h2'
  have heq : (a.step : Int) * (i : Int) - (b.step : Int) * (j : Int) = b.lo - a.lo := by
    have hi' : (a.step : Int) * (i : Int) = v - a.lo := by
      rw [Int.mul_comm]
      omega
    have hj' : (b.step : Int) * (j : Int) = v - b.lo := by
      rw [Int.mul_comm]
      omega
    omega
  rw [← heq]
  exact Int.dvd_sub h2 h1

private theorem mul_div_gcd_eq_lcm (a b : Nat) (ha : 0 < a) :
    a * (b / a.gcd b) = Nat.lcm a b := by
  have hg : 0 < a.gcd b := Nat.gcd_pos_of_pos_left b ha
  apply Nat.eq_of_mul_eq_mul_left (n := a.gcd b) hg
  rw [Nat.gcd_mul_lcm]
  calc
    a.gcd b * (a * (b / a.gcd b)) = a * (a.gcd b * (b / a.gcd b)) := by grind
    _ = a * b := by rw [Nat.mul_comm (a.gcd b), Nat.div_mul_cancel (Nat.gcd_dvd_right a b)]

private theorem lcm_dvd_of_dvd_of_dvd (a b : Int) (X : Int) (ha : a ∣ X) (hb : b ∣ X) :
    (Int.lcm a b : Int) ∣ X := by
  have hxa : a ∣ (X.natAbs : Int) := Int.dvd_natAbs.mpr ha
  have hxb : b ∣ (X.natAbs : Int) := Int.dvd_natAbs.mpr hb
  have h : Int.lcm a b ∣ X.natAbs := Int.lcm_dvd hxa hxb
  exact Int.dvd_natAbs.mp (by exact_mod_cast h)

private theorem intersection_contains (a b : Progression) (v : Int) :
    contains (intersection a b) v = true ↔ contains a v = true ∧ contains b v = true := by
  by_cases han : a.n = 0
  · simp [han, intersection, contains]
  · by_cases hbn : b.n = 0
    · simp [hbn, intersection, contains]
    · have hanp : 0 < a.n := by omega
      have hbnp : 0 < b.n := by omega
      by_cases has : a.step = 0
      · unfold intersection
        rw [if_neg (by simp [han, hbn]), if_pos (by simp [has])]
        change contains ⟨a.lo, 1, if contains b a.lo then 1 else 0⟩ v = true ↔
          contains a v = true ∧ contains b v = true
        have haeq : a = ⟨a.lo, 0, a.n⟩ := by cases a; simp_all
        have haconst : contains a v = true ↔ 0 < a.n ∧ v = a.lo := by
          rw [haeq]
          exact contains_const_iff a.lo a.n v
        by_cases hba : contains b a.lo
        · rw [if_pos hba, contains_step_one]
          constructor
          · rintro ⟨_, hlo, hlt⟩
            have hvn : v = a.lo := by omega
            exact ⟨haconst.mpr ⟨hanp, hvn⟩, by simpa [hvn] using hba⟩
          · rintro ⟨ha, hb⟩
            have hvn : v = a.lo := (haconst.mp ha).2
            rw [hvn]
            omega
        · rw [if_neg hba, contains_step_one]
          constructor
          · intro ha
            omega
          · rintro ⟨ha, hb⟩
            have hvn : v = a.lo := (haconst.mp ha).2
            exact False.elim (hba (by simpa [hvn] using hb))
      · by_cases hbs : b.step = 0
        · unfold intersection
          rw [if_neg (by simp [han, hbn]), if_neg (by simp [has]), if_pos (by simp [hbs])]
          change contains ⟨b.lo, 1, if contains a b.lo then 1 else 0⟩ v = true ↔
            contains a v = true ∧ contains b v = true
          have hbeq : b = ⟨b.lo, 0, b.n⟩ := by cases b; simp_all
          have hbconst : contains b v = true ↔ 0 < b.n ∧ v = b.lo := by
            rw [hbeq]
            exact contains_const_iff b.lo b.n v
          by_cases hab : contains a b.lo
          · rw [if_pos hab, contains_step_one]
            constructor
            · rintro ⟨_, hlo, hlt⟩
              have hvn : v = b.lo := by omega
              exact ⟨by simpa [hvn] using hab, hbconst.mpr ⟨hbnp, hvn⟩⟩
            · rintro ⟨ha, hb⟩
              have hvn : v = b.lo := (hbconst.mp hb).2
              rw [hvn]
              omega
          · rw [if_neg hab, contains_step_one]
            constructor
            · intro ha
              omega
            · rintro ⟨ha, hb⟩
              have hvn : v = b.lo := (hbconst.mp hb).2
              exact False.elim (hab (by simpa [hvn] using ha))
        · have hasp : 0 < a.step := Nat.pos_of_ne_zero has
          have hbsp : 0 < b.step := Nat.pos_of_ne_zero hbs
          let g := a.step.gcd b.step
          let gap := b.lo - a.lo
          let period := (a.step / g) * b.step
          let unit := (bezout (a.step / g) (b.step / g)).1
          let root := a.lo + (a.step : Int) * ((gap / (g : Int) * unit) % ((b.step / g : Nat) : Int))
          let low := max a.lo b.lo
          let first := low + (root - low) % (period : Int)
          let high := min (lastVal a) (lastVal b)
          have hgpos : 0 < g := Nat.gcd_pos_of_pos_left b.step hasp
          have hdivpos : 0 < a.step / g :=
            Nat.div_pos (Nat.le_of_dvd hasp (Nat.gcd_dvd_left a.step b.step)) hgpos
          have hpNat : 0 < period := by dsimp [period]; exact Nat.mul_pos hdivpos hbsp
          have hp : (0 : Int) < period := by exact_mod_cast hpNat
          have hgapdef : gap = b.lo - a.lo := rfl
          by_cases hgapmod : gap % (g : Int) = 0
          · have hgap : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgapmod
            have hroot := root_dvd a.step b.step hasp hbsp a.lo b.lo gap hgapdef hgap
            have hfirst : first = low + (root - low) % (period : Int) := rfl
            have hfroot := first_root_dvd low root first period hp hfirst
            have hremnon : 0 ≤ (root - low) % (period : Int) := Int.emod_nonneg _ (by omega)
            have hflow : low ≤ first := by dsimp [first]; omega
            have hbdiv : b.step = g * (b.step / g) :=
              (Nat.mul_div_cancel' (Nat.gcd_dvd_right a.step b.step)).symm
            have hadiv : a.step = g * (a.step / g) :=
              (Nat.mul_div_cancel' (Nat.gcd_dvd_left a.step b.step)).symm
            have hadiv' : a.step / g * g = a.step :=
              Nat.div_mul_cancel (Nat.gcd_dvd_left a.step b.step)
            have hbdiv' : b.step / g * g = b.step :=
              Nat.div_mul_cancel (Nat.gcd_dvd_right a.step b.step)
            have hperA : period = a.step * (b.step / g) := by
              dsimp [period]
              calc
                a.step / g * b.step = a.step / g * (b.step / g * g) := by rw [hbdiv']
                _ = (a.step / g * g) * (b.step / g) := by grind
                _ = a.step * (b.step / g) := by rw [hadiv']
            have hperB : period = b.step * (a.step / g) := by
              calc
                period = a.step * (b.step / g) := hperA
                _ = (a.step / g * g) * (b.step / g) := by rw [hadiv']
                _ = (b.step / g * g) * (a.step / g) := by grind
                _ = b.step * (a.step / g) := by rw [hbdiv']
            have hpa : (a.step : Int) ∣ (period : Int) := by
              refine ⟨((b.step / g : Nat) : Int), ?_⟩
              exact_mod_cast hperA
            have hpb : (b.step : Int) ∣ (period : Int) := by
              refine ⟨((a.step / g : Nat) : Int), ?_⟩
              exact_mod_cast hperB
            unfold intersection
            rw [if_neg (by simp [han, hbn]), if_neg (by simp [has]),
              if_neg (by simp [hbs]), if_neg (by
                change ¬ ((gap % (g : Int) != 0) = true)
                intro hc
                have hne : (gap % (g : Int) != 0) = false := by
                  simp [beq_iff_eq, hgapmod]
                rw [hne] at hc
                contradiction)]
            change contains ⟨first, (period : Int).toNat,
              if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ v = true ↔
              contains a v = true ∧ contains b v = true
            rw [show contains ⟨first, (period : Int).toNat,
                if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ v = true ↔
                first ≤ v ∧ v ≤ high ∧ (period : Int) ∣ v - first from
                span_contains_iff first (period : Int) high v hp]
            constructor
            · rintro ⟨hfirstx, hxhigh, hxfirst⟩
              have hxroot : (period : Int) ∣ v - root := by
                have heq : v - root = (v - first) + (first - root) := by omega
                rw [heq]
                exact Int.dvd_add hxfirst hfroot
              have hvrootA : (a.step : Int) ∣ v - root := Int.dvd_trans hpa hxroot
              have hvrootB : (b.step : Int) ∣ v - root := Int.dvd_trans hpb hxroot
              have hxalo : (a.step : Int) ∣ v - a.lo := by
                have heq : v - a.lo = (v - root) + (root - a.lo) := by omega
                rw [heq]
                exact Int.dvd_add hvrootA hroot.1
              have hxblo : (b.step : Int) ∣ v - b.lo := by
                have heq : v - b.lo = (v - root) + (root - b.lo) := by omega
                rw [heq]
                exact Int.dvd_add hvrootB hroot.2
              have hxlow : low ≤ v := by omega
              have hab := contains_bounds a v
              have hbb := contains_bounds b v
              constructor
              · exact mem_of_bounds_dvd a v hanp hasp (by omega) (by omega) hxalo
              · exact mem_of_bounds_dvd b v hbnp hbsp (by omega) (by omega) hxblo
            · rintro ⟨ha, hb⟩
              have hab := contains_bounds a v ha
              have hbb := contains_bounds b v hb
              have hxalo : (a.step : Int) ∣ v - a.lo := contains_dvd a v ha
              have hxblo : (b.step : Int) ∣ v - b.lo := contains_dvd b v hb
              have hvar : (a.step : Int) ∣ v - root := by
                have heq : v - root = (v - a.lo) - (root - a.lo) := by omega
                rw [heq]
                exact Int.dvd_sub hxalo hroot.1
              have hvbr : (b.step : Int) ∣ v - root := by
                have heq : v - root = (v - b.lo) - (root - b.lo) := by omega
                rw [heq]
                exact Int.dvd_sub hxblo hroot.2
              have hperLcmNat : period = Nat.lcm a.step b.step := by
                rw [hperA]
                exact mul_div_gcd_eq_lcm a.step b.step hasp
              have hxroot : (period : Int) ∣ v - root := by
                have hl := lcm_dvd_of_dvd_of_dvd (a.step : Int) (b.step : Int) (v - root)
                  hvar hvbr
                rw [hperLcmNat]
                simpa using hl
              have hxfirst : (period : Int) ∣ v - first := by
                have heq : v - first = (v - root) - (first - root) := by omega
                rw [heq]
                exact Int.dvd_sub hxroot hfroot
              have hxlow : low ≤ v := by
                have hab := contains_bounds a v ha
                have hbb := contains_bounds b v hb
                dsimp [low]
                omega
              have hfirstx : first ≤ v :=
                first_le_of_congruence low root first v (period : Int) hp hfirst hxlow hxroot
              have hxhigh : v ≤ high := by
                have hab := contains_bounds a v ha
                have hbb := contains_bounds b v hb
                dsimp [high]
                omega
              exact ⟨hfirstx, hxhigh, hxfirst⟩
          · have hgapne : gap % (g : Int) ≠ 0 := by simpa using hgapmod
            unfold intersection
            rw [if_neg (by simp [han, hbn]), if_neg (by simp [has]),
              if_neg (by simp [hbs]), if_pos (by
                have hne : gap % (g : Int) ≠ 0 := hgapmod
                have hne' : (b.lo - a.lo) % (a.step.gcd b.step : Int) ≠ 0 := by
                  simpa [gap, g] using hne
                simpa [hne'])]
            change contains ⟨0, 1, 0⟩ v = true ↔ contains a v = true ∧ contains b v = true
            have hl : contains ⟨0, 1, 0⟩ v = false := by simp [contains]
            rw [hl]
            constructor
            · intro h
              cases h
            · rintro ⟨ha, hb⟩
              have hg := common_gap_dvd a b v ha hb
              exact False.elim (hgapne (by
                have hz := Int.emod_eq_zero_of_dvd hg
                simpa [gap, g] using hz))

/--! ## Common list / progression helpers for the three goals -/

private theorem map_pair_filter {α β : Type} (xs : List α) (f : α → Nat) (g : α → β)
    (p : Nat → Bool) :
    (xs.map (fun x => (f x, g x))).filter (fun r => p r.1) =
      (xs.filter (fun x => p (f x))).map (fun x => (f x, g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => by_cases h : p (f x) <;> simp [h, ih]


private def valsList (p : Progression) : List Int :=
  (List.range p.n).map (fun (k : Nat) => p.lo + (k : Int) * (p.step : Int))

private theorem mem_valsList (p : Progression) (v : Int) :
    v ∈ valsList p ↔ contains p v = true := by
  unfold valsList
  rw [List.mem_map]
  constructor
  · rintro ⟨k, hk, hv⟩
    apply (contains_iff_mem p v).mpr
    refine ⟨k, List.mem_range.mp hk, ?_⟩
    simpa [progVal] using hv.symm
  · intro hc
    rcases (contains_iff_mem p v).mp hc with ⟨k, hk, hv⟩
    refine ⟨k, List.mem_range.mpr hk, ?_⟩
    simpa [progVal] using hv.symm

private theorem valsList_nodup_of_step_pos (p : Progression) (hp : 0 < p.step) :
    (valsList p).Nodup := by
  unfold valsList
  exact List.Pairwise.map (fun k : Nat => p.lo + (k : Int) * (p.step : Int))
    (fun a b hne heq => hne (by
      have hmul : (a : Int) * (p.step : Int) = (b : Int) * (p.step : Int) := by omega
      have hnz : (p.step : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hp)
      have hab : (a : Int) = (b : Int) := Int.eq_of_mul_eq_mul_right hnz hmul
      exact_mod_cast hab)) (List.nodup_range : (List.range p.n).Nodup)

private theorem map_nat_nodup (l : List Nat) (hn : l.Nodup) :
    (l.map (fun (i : Nat) => (i : Int))).Nodup := by
  exact List.Pairwise.map (fun (i : Nat) => (i : Int))
    (fun a b h hEq => h (Int.ofNat_inj.mp hEq)) hn

private theorem valsList_perm {p q : Progression} (hp : 0 < p.step) (hq : 0 < q.step)
    (h : ∀ v, contains p v = true ↔ contains q v = true) :
    List.Perm (valsList p) (valsList q) := by
  apply (List.perm_ext_iff_of_nodup (valsList_nodup_of_step_pos p hp)
    (valsList_nodup_of_step_pos q hq)).mpr
  intro v
  rw [mem_valsList, mem_valsList, h v]

private theorem inter_step_one (n : Nat) (m : Progression) :
    0 < (intersection (⟨0, 1, n⟩ : Progression) m).step := by
  by_cases hn0 : n = 0
  · simp [intersection, hn0]
  by_cases hmn0 : m.n = 0
  · simp [intersection, hn0, hmn0]
  by_cases hms0 : m.step = 0
  · simp [intersection, hn0, hmn0, hms0]
  have hg : Nat.gcd 1 m.step = 1 := Nat.gcd_one_left m.step
  have hgap : (m.lo - 0) % (Nat.gcd 1 m.step : Int) = 0 := by simp [hg]
  unfold intersection
  rw [if_neg (by simp [hn0, hmn0]), if_neg (by simp), if_neg (by simp [hms0]),
    if_neg (by simp [hgap])]
  change 0 < (1 / Nat.gcd 1 m.step) * m.step
  rw [hg]
  simpa using (Nat.pos_of_ne_zero hms0 : 0 < m.step)

private theorem inter_step_pos_both (a b : Progression) (ha : 0 < a.step) (hb : 0 < b.step)
    (han : a.n ≠ 0) (hbn : b.n ≠ 0) : 0 < (intersection a b).step := by
  by_cases hg : (b.lo - a.lo) % (a.step.gcd b.step : Int) = 0
  · unfold intersection
    rw [if_neg (by simp [han, hbn]), if_neg (by simp [Nat.ne_of_gt ha]),
      if_neg (by simp [Nat.ne_of_gt hb]), if_neg (by simp [hg])]
    change 0 < (a.step / a.step.gcd b.step) * b.step
    exact Nat.mul_pos (Nat.div_gcd_pos_of_pos_left b.step ha) hb
  · unfold intersection
    rw [if_neg (by simp [han, hbn]), if_neg (by simp [Nat.ne_of_gt ha]),
      if_neg (by simp [Nat.ne_of_gt hb]), if_pos (by simp [hg])]
    change 0 < (1 : Nat)
    omega

private theorem aligned_expand (n : Nat) (x y dx dy : Int) :
    ((List.range n).map (fun (k : Nat) => (x + (k : Int) * dx) * (y + (k : Int) * dy))).sum =
      (n : Int) * x * y + tri n * (x * dy + y * dx) + (squares n : Int) * dx * dy := by
  induction n with
  | zero => simp [tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih, tri_succ, squares_succ]
      simp [Int.natCast_add, Int.natCast_mul]
      grind

private theorem inter_posA_posB_step (c : Config) (b : Lane) (hb : 0 < b.stride)
    (hn : c.n ≠ 0) (hbn : b.n ≠ 0) :
    (intersection (⟨0, 1, c.n⟩ : Progression) ⟨b.start, b.stride, b.n⟩).step = b.stride := by
  have hg : Nat.gcd 1 b.stride = 1 := Nat.gcd_one_left b.stride
  have hgap : (b.start - 0) % (Nat.gcd 1 b.stride : Int) = 0 := by simp [hg]
  unfold intersection
  rw [if_neg (by simp [hn, hbn]), if_neg (by simp), if_neg (by simp [Nat.ne_of_gt hb]),
    if_neg (by simp [hgap])]
  change (1 / Nat.gcd 1 b.stride) * b.stride = b.stride
  rw [hg]
  simp

private theorem range_filter_eq_const (n a : Nat) (ha : a < n) :
    (List.range n).filter (fun i => i == a) = [a] := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [List.range_succ, List.filter_append]
      by_cases hlt : a < n
      · have ht : (List.filter (fun i => i == a) [n]) = [] := by
          have hne : (n == a) = false := beq_eq_false_iff_ne.mpr (by omega)
          simp [hne]
        rw [ih hlt, ht]
        simp
      · have hEq : a = n := by omega
        subst a
        have hpre : (List.range n).filter (fun i => i == n) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro i hi
          have hi' : i < n := List.mem_range.mp hi
          intro htrue
          have hb : (i == n) = false := beq_eq_false_iff_ne.mpr (Nat.ne_of_lt hi')
          rw [hb] at htrue
          cases htrue
        rw [hpre]
        simp

private theorem inner_stride_sum (c : Config) (b : Lane) (hb : 0 < b.stride) (j : Nat) :
    ((List.range c.n).map (fun (i : Nat) =>
      if i == b.start + j * b.stride then
        (c.base + c.step * (i : Int)) * (b.base + b.step * (j : Int))
      else 0)).sum =
    if b.start + j * b.stride < c.n then
      (c.base + c.step * ((b.start + j * b.stride : Nat) : Int)) * (b.base + b.step * (j : Int))
    else 0 := by
  rw [← sum_map_ite_zero]
  by_cases hj : b.start + j * b.stride < c.n
  · rw [if_pos hj]
    rw [range_filter_eq_const c.n (b.start + j * b.stride) hj]
    simp
  · rw [if_neg hj]
    have hf : (List.range c.n).filter (fun i => i == b.start + j * b.stride) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro i hi
      have hi' : i < c.n := List.mem_range.mp hi
      have hle : c.n ≤ b.start + j * b.stride := Nat.le_of_not_gt hj
      intro htrue
      have hb : (i == b.start + j * b.stride) = false :=
        beq_eq_false_iff_ne.mpr (Nat.ne_of_lt (Nat.lt_of_lt_of_le hi' hle))
      rw [hb] at htrue
      cases htrue
    rw [hf]
    simp

private theorem sum_map_ite_zero_prop {α : Type} (l : List α) (p : α → Prop) [DecidablePred p]
    (f : α → Int) :
    ((l.filter p).map f).sum = (l.map (fun x => if p x then f x else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem sum_map_ite_zero_nat {α : Type} (l : List α) (p : α → Bool) (f : α → Nat) :
    ((l.filter p).map f).sum = (l.map (fun x => if p x then f x else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem sum_map_ite_zero_prop_nat {α : Type} (l : List α) (p : α → Prop) [DecidablePred p]
    (f : α → Nat) :
    ((l.filter p).map f).sum = (l.map (fun x => if p x then f x else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem nat_sum_cast {α : Type} (l : List α) (f : α → Nat) :
    ((l.map f).sum : Int) = (l.map (fun x => (f x : Int))).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem sum_map_one_nat {α : Type} (l : List α) :
    (l.map (fun _ : α => (1 : Nat))).sum = l.length := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.add_comm]

private theorem sum_map_zero_nat (l : List Nat) : (l.map (fun _ : Nat => (0 : Nat))).sum = 0 := by
  induction l <;> simp [*]

private theorem sum_map_const_nat_list {α : Type} (l : List α) (v : Nat) :
    (l.map (fun _ : α => v)).sum = l.length * v := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.succ_mul]; omega

private theorem sum_map_const_nat (m v : Nat) :
    ((List.range m).map (fun _ : Nat => v)).sum = m * v := by
  induction m with
  | zero => simp
  | succ m ih => rw [List.range_succ, List.map_append, List.sum_append, ih]; simp [Nat.succ_mul]

private theorem sum_map_zero_int {α : Type} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l <;> simp [*]

private theorem rows_linear (c : Config) :
    rows layout c = (List.range c.n).map (fun (i : Nat) => (i, c.base + c.step * (i : Int))) := by
  apply rows_range_map
  intro i _
  rfl

private theorem laneA_eq (c : Config) :
    lane c.n 0 1 c.base c.step = ⟨0, 1, c.n, c.base, c.step⟩ := by
  unfold lane
  simp

private theorem sum_map_swap {α β : Type} (l₁ : List α) (l₂ : List β) (f : α → β → Int) :
    (l₁.map (fun a => (l₂.map (fun b => f a b)).sum)).sum =
      (l₂.map (fun b => (l₁.map (fun a => f a b)).sum)).sum := by
  induction l₁ with
  | nil => simp [sum_map_zero_int]
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih, sum_map_add_int]

/--! ## Goal: AlignedProduct -/

theorem sound_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal.AlignedProduct.Goal := by
  intro c b hb
  let posA : Progression := ⟨0, 1, c.n⟩
  let posB : Progression := ⟨b.start, b.stride, b.n⟩
  let r := intersection posA posB
  let G (p : Int) : Int := (c.base + c.step * p) *
    (b.base + b.step * ((((p - (b.start : Int)) / (b.stride : Int)).toNat : Nat) : Int))
  have hrows := rows_linear c
  rw [hrows]
  simp only [layout, List.map_cons, List.map_nil]
  rw [laneA_eq]
  -- swap the two sums
  rw [sum_map_swap]
  have hinner : ∀ (j : Nat),
      ((List.range c.n).map (fun (i : Nat) =>
        if i == b.start + j * b.stride then
          (c.base + c.step * (i : Int)) * (b.base + b.step * (j : Int))
        else 0)).sum =
      if b.start + j * b.stride < c.n then
        (c.base + c.step * ((b.start + j * b.stride : Nat) : Int)) * (b.base + b.step * (j : Int))
      else 0 := by
    intro j
    exact inner_stride_sum c b hb j
  simp only [List.map_map]
  change ((List.range b.n).map (fun (j : Nat) =>
      ((List.range c.n).map (fun (i : Nat) =>
        if i == b.start + j * b.stride then
          (c.base + c.step * (i : Int)) * (b.base + b.step * (j : Int))
        else 0)).sum)).sum = _
  rw [show (List.range b.n).map (fun (j : Nat) =>
      ((List.range c.n).map (fun (i : Nat) =>
        if i == b.start + j * b.stride then
          (c.base + c.step * (i : Int)) * (b.base + b.step * (j : Int))
        else 0)).sum) =
      (List.range b.n).map (fun (j : Nat) =>
        if b.start + j * b.stride < c.n then
          (c.base + c.step * ((b.start + j * b.stride : Nat) : Int)) * (b.base + b.step * (j : Int))
        else 0) by
    apply List.map_congr_left
    intro j hj
    exact hinner j]
  rw [← sum_map_ite_zero_prop]
  simp only [List.sum_cons, List.sum_nil, Int.add_zero]
  let jf := (List.range b.n).filter (fun j : Nat => b.start + j * b.stride < c.n)
  let jList : List Int := jf.map (fun j : Nat => (b.start + j * b.stride : Int))
  change (jf.map (fun (j : Nat) =>
    (c.base + c.step * ((b.start + j * b.stride : Nat) : Int)) *
      (b.base + b.step * (j : Int)))).sum = aligned ⟨0, 1, c.n, c.base, c.step⟩ b
  have hjf_nodup : jList.Nodup := by
    dsimp [jList, jf]
    exact List.Pairwise.map (fun (j : Nat) => (b.start + j * b.stride : Int))
      (fun a bb hne heq => hne (by
        have hs : (b.stride : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hb)
        have hmul : (bb : Int) * (b.stride : Int) = (a : Int) * (b.stride : Int) := by omega
        have hab : bb = a := by exact_mod_cast (Int.eq_of_mul_eq_mul_right hs hmul)
        exact hab.symm))
      (List.Pairwise.filter (fun j : Nat => b.start + j * b.stride < c.n)
        (List.nodup_range : (List.range b.n).Nodup))
  have hGj : ∀ (j : Nat), j ∈ jf →
      G (b.start + j * b.stride : Int) =
        (c.base + c.step * ((b.start + j * b.stride : Nat) : Int)) *
          (b.base + b.step * (j : Int)) := by
    intro j hj
    have hdivN : (b.start + j * b.stride - b.start) / b.stride = j := by
      rw [Nat.add_sub_cancel_left]
      rw [Nat.mul_comm]
      exact Nat.mul_div_right j hb
    dsimp [G]
    have hdivInt : ((b.start : Int) + (j : Int) * (b.stride : Int) - (b.start : Int)) /
        (b.stride : Int) = (j : Int) := by
      have hsub : (b.start : Int) + (j : Int) * (b.stride : Int) - (b.start : Int) =
          (j : Int) * (b.stride : Int) := by grind
      rw [hsub, Int.mul_comm]
      exact Int.mul_ediv_cancel_left (j : Int) (by exact_mod_cast (Nat.ne_of_gt hb))
    rw [hdivInt]
    simp
  have hL : (jf.map (fun (j : Nat) =>
      (c.base + c.step * ((b.start + j * b.stride : Nat) : Int)) *
        (b.base + b.step * (j : Int)))).sum =
      (jList.map G).sum := by
    dsimp [jList]
    rw [List.map_map]
    apply congrArg List.sum
    apply List.map_congr_left
    intro j hj
    exact (hGj j hj).symm
  rw [hL]
  -- now compute aligned as a sum over valsList r
  by_cases hr0 : r.n = 0
  · have hjfe : jf = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro j hjmem
      have hjlt : j < b.n := List.mem_range.mp hjmem
      intro hltBool
      have hlt : b.start + j * b.stride < c.n := of_decide_eq_true hltBool
      have hposmem : contains posB ((b.start + j * b.stride : Nat) : Int) = true := by
        apply (contains_iff_mem posB ((b.start + j * b.stride : Nat) : Int)).mpr
        refine ⟨j, hjlt, ?_⟩
        simp [posB, progVal]
      have hAmem : contains posA ((b.start + j * b.stride : Nat) : Int) = true := by
        apply (contains_iff_mem posA ((b.start + j * b.stride : Nat) : Int)).mpr
        refine ⟨b.start + j * b.stride, hlt, ?_⟩
        simp [posA, progVal]
      have hrmem := (intersection_contains posA posB ((b.start + j * b.stride : Nat) : Int)).mpr ⟨hAmem, hposmem⟩
      have hrlist := (contains_iff_mem r ((b.start + j * b.stride : Nat) : Int)).mp hrmem
      rcases hrlist with ⟨k, hk, _⟩
      omega
    have hr0' : (intersection (positions (⟨0, 1, c.n, c.base, c.step⟩ : Lane)) (positions b)).n = 0 := by
      simpa [r, posA, posB, positions] using hr0
    have hjListE : jList = [] := by simp [jList, hjfe]
    rw [hjListE]
    simp [aligned, hr0', tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  · have hcn0 : c.n ≠ 0 := by
      intro h
      have : r.n = 0 := by simp [r, posA, posB, h, intersection]
      exact hr0 this
    have hbn0 : b.n ≠ 0 := by
      intro h
      have : r.n = 0 := by simp [r, posA, posB, h, intersection]
      exact hr0 this
    have hrstep : r.step = b.stride := by
      dsimp [r, posA, posB]
      exact inter_posA_posB_step c b hb hcn0 hbn0
    have hrnodup : (valsList r).Nodup := valsList_nodup_of_step_pos r
      (inter_step_pos_both posA posB (by simp [posA]) hb hcn0 hbn0)
    have hjrperm : List.Perm (valsList r) jList := by
      apply (List.perm_ext_iff_of_nodup hrnodup hjf_nodup).mpr
      intro p
      constructor
      · intro hp
        have hpmem : contains r p = true := (mem_valsList r p).mp hp
        have hboth := (intersection_contains posA posB p).mp hpmem
        rcases (contains_iff_mem posB p).mp hboth.2 with ⟨j, hj, hjval⟩
        apply List.mem_map.mpr
        refine ⟨j, ?_, ?_⟩
        · dsimp [jf]
          apply List.mem_filter.mpr
          constructor
          · exact List.mem_range.mpr hj
          · dsimp [posA] at hboth
            have hA := (contains_iff_mem (⟨0, 1, c.n⟩ : Progression) p).mp hboth.1
            rcases hA with ⟨i, hi, hival⟩
            have hpNatEq : p.toNat = i := by
              rw [hival]
              simp [posA, progVal]
            have hplt : p.toNat < c.n := by
              rw [hpNatEq]
              exact hi
            have hpEqI : p = (b.start + j * b.stride : Int) := by
              simpa [posB, progVal] using hjval
            have hpnon : 0 ≤ p := by rw [hival]; simp [progVal, posA]
            have hpEq : p.toNat = b.start + j * b.stride := by
              apply Int.ofNat_inj.mp
              rw [Int.toNat_of_nonneg hpnon]
              exact hpEqI
            simpa [hpEq] using hplt
        · simpa [progVal, posB] using hjval.symm
      · intro hp
        rcases List.mem_map.mp hp with ⟨j, hjf, hjEq⟩
        have hltBool := (List.mem_filter.mp hjf).2
        have hlt : b.start + j * b.stride < c.n := of_decide_eq_true hltBool
        apply (mem_valsList r p).mpr
        apply (intersection_contains posA posB p).mpr
        constructor
        · apply (contains_iff_mem posA p).mpr
          refine ⟨b.start + j * b.stride, hlt, ?_⟩
          simp [posA, progVal, ← hjEq]
        · apply (contains_iff_mem posB p).mpr
          refine ⟨j, List.mem_range.mp (List.mem_filter.mp hjf).1, ?_⟩
          simpa [posB, progVal, ← hjEq]
    -- aligned expansion
    let x := firstAt ⟨0, 1, c.n, c.base, c.step⟩ r
    let y := firstAt b r
    let dx := slopeAt ⟨0, 1, c.n, c.base, c.step⟩ r
    let dy := slopeAt b r
    have hx : x = c.base + c.step * r.lo := by
      dsimp [x, firstAt]
      simp only [Int.sub_zero, Int.ediv_one]
      grind
    have hdx : dx = (r.step : Int) * c.step := by
      dsimp [dx, slopeAt]
      simp
    have hy : y = b.base + ((r.lo - (b.start : Int)) / (b.stride : Int)) * b.step := by
      dsimp [y, firstAt]
    have hdy : dy = b.step := by
      have hdivI : ((r.step : Int) / (b.stride : Int)) = 1 := by
        rw [hrstep]
        exact Int.ediv_self (by exact_mod_cast (Nat.ne_of_gt hb))
      dsimp [dy, slopeAt]
      rw [hdivI]
      simp
    have haligned : aligned ⟨0, 1, c.n, c.base, c.step⟩ b =
        ((List.range r.n).map (fun (k : Nat) => (x + (k : Int) * dx) * (y + (k : Int) * dy))).sum := by
      dsimp [aligned, x, y, dx, dy]
      exact (aligned_expand r.n
        (firstAt ⟨0, 1, c.n, c.base, c.step⟩ r) (firstAt b r)
        (slopeAt ⟨0, 1, c.n, c.base, c.step⟩ r) (slopeAt b r)).symm
    have hterm : ∀ (k : Nat), k < r.n →
        (x + (k : Int) * dx) * (y + (k : Int) * dy) =
          G (r.lo + (k : Int) * (r.step : Int)) := by
      intro k hk
      have hpMem : contains r (r.lo + (k : Int) * (r.step : Int)) = true := by
        apply (contains_iff_mem r _).mpr
        refine ⟨k, hk, ?_⟩
        simp [progVal]
      have hposMem := (intersection_contains posA posB (r.lo + (k : Int) * (r.step : Int))).mp hpMem |>.2
      rcases (contains_iff_mem posB (r.lo + (k : Int) * (r.step : Int))).mp hposMem with ⟨j, hj, hjval⟩
      have hpEq : r.lo + (k : Int) * (r.step : Int) = b.start + (j : Int) * (b.stride : Int) := by
        simpa [posB, progVal] using hjval
      have hdivN : (b.start + j * b.stride - b.start) / b.stride = j := by
        rw [Nat.add_sub_cancel_left]
        rw [Nat.mul_comm]
        exact Nat.mul_div_right j hb
      have hdivInt : ((b.start : Int) + (j : Int) * (b.stride : Int) - (b.start : Int)) /
          (b.stride : Int) = (j : Int) := by
        have hs : (b.stride : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hb)
        have hsub : (b.start : Int) + (j : Int) * (b.stride : Int) - (b.start : Int) =
            (j : Int) * (b.stride : Int) := by grind
        rw [hsub, Int.mul_comm]
        exact Int.mul_ediv_cancel_left (j : Int) hs
      have hGval : G (r.lo + (k : Int) * (r.step : Int)) =
          (c.base + c.step * (b.start + (j : Int) * (b.stride : Int))) *
            (b.base + b.step * (j : Int)) := by
        dsimp [G]
        rw [hpEq]
        push_cast
        rw [hdivInt]
        simp
      have hpEq2 : r.lo + (k : Int) * (b.stride : Int) =
          (b.start : Int) + (j : Int) * (b.stride : Int) := by
        simpa [hrstep] using hpEq
      have hloDiv : (r.lo - (b.start : Int)) / (b.stride : Int) = (j : Int) - (k : Int) := by
        have hsub : r.lo - (b.start : Int) = ((j : Int) - (k : Int)) * (b.stride : Int) := by
          grind
        rw [hsub, Int.mul_comm]
        exact Int.mul_ediv_cancel_left ((j : Int) - (k : Int))
          (by exact_mod_cast (Nat.ne_of_gt hb))
      rw [hGval, hx, hdx, hy, hdy]
      rw [hloDiv]
      grind
    rw [haligned]
    have hsumRange : ((List.range r.n).map (fun (k : Nat) =>
        (x + (k : Int) * dx) * (y + (k : Int) * dy))).sum =
        ((valsList r).map G).sum := by
      rw [show ((List.range r.n).map (fun (k : Nat) =>
        (x + (k : Int) * dx) * (y + (k : Int) * dy))) =
        ((List.range r.n).map (fun (k : Nat) => G (r.lo + (k : Int) * (r.step : Int)))) by
        apply List.map_congr_left
        intro k hk
        exact hterm k (List.mem_range.mp hk)]
      unfold valsList
      rw [List.map_map]
      rfl
    rw [hsumRange]
    exact list_sum_perm (hjrperm.map G).symm
  
/--! ## Goal: ResidueJoin -/

private theorem filter_congr_bool {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, p x = q x) : l.filter p = l.filter q := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filter_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem countP_eq_of_inj (n : Nat) (f : Nat → Int) (hf : Function.Injective f) (v : Int) :
    (List.range n).countP (fun j => f j == v) = if h : ∃ j, j < n ∧ f j = v then 1 else 0 := by
  rw [List.countP_eq_length_filter]
  by_cases h : ∃ j, j < n ∧ f j = v
  · rw [dif_pos h]
    rcases h with ⟨j, hj, hfv⟩
    have heqiff : ∀ k, (f k == v) = (k == j) := by
      intro k
      by_cases hk : k = j
      · subst k
        simp [hfv]
      · have hne : f k ≠ v := by
          intro hkv
          exact hk (hf (by grind))
        simp [beq_eq_false_iff_ne.mpr hne, beq_eq_false_iff_ne.mpr hk]
    rw [show (List.range n).filter (fun k => f k == v) = (List.range n).filter (fun k => k == j) by
      apply filter_congr_bool
      intro k hk
      exact heqiff k]
    rw [range_filter_eq_const n j hj]
    simp
  · rw [dif_neg h]
    have hf : (List.range n).filter (fun k => f k == v) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro k hk hkv
      exact h ⟨k, List.mem_range.mp hk, by simpa using hkv⟩
    rw [hf]
    simp

private theorem mem_values_iff (b : Lane) (v : Int) :
    contains (values b) v = true ↔ ∃ j, j < b.n ∧ b.base + b.step * (j : Int) = v := by
  rw [contains_iff_mem]
  unfold progMem progVal
  dsimp [values]
  constructor
  · rintro ⟨k, hk, hv⟩
    by_cases hs : b.step < 0
    · have hstepabs : (b.step.natAbs : Int) = -b.step := by
        rw [← Int.natAbs_neg]
        exact Int.natAbs_of_nonneg (by omega)
      rw [hstepabs] at hv
      simp [hs] at hv
      refine ⟨b.n - 1 - k, by omega, ?_⟩
      have hk_le : k ≤ b.n - 1 := by omega
      rw [Int.natCast_sub (by omega : k ≤ b.n - 1)]
      grind
    · have hstepabs : (b.step.natAbs : Int) = b.step := by
        exact Int.natAbs_of_nonneg (by omega)
      refine ⟨k, hk, ?_⟩
      rw [hstepabs] at hv
      simp [hs] at hv
      simpa [Int.mul_comm] using hv.symm
  · rintro ⟨j, hj, hv⟩
    by_cases hs : b.step < 0
    · have hstepabs : (b.step.natAbs : Int) = -b.step := by
        rw [← Int.natAbs_neg]
        exact Int.natAbs_of_nonneg (by omega)
      refine ⟨b.n - 1 - j, by omega, ?_⟩
      rw [hstepabs]
      simp [hs]
      have hj_le : j ≤ b.n - 1 := by omega
      rw [Int.natCast_sub (by omega : j ≤ b.n - 1)]
      grind
    · have hstepabs : (b.step.natAbs : Int) = b.step := by
        exact Int.natAbs_of_nonneg (by omega)
      refine ⟨j, hj, ?_⟩
      rw [hstepabs]
      simp [hs]
      simpa [Int.mul_comm] using hv.symm

private theorem values_step_pos (b : Lane) (hb : b.step ≠ 0) : 0 < (values b).step := by
  unfold values
  exact Nat.pos_of_ne_zero (Int.natAbs_ne_zero.mpr hb)

private theorem beq_comm_int (x y : Int) : (x == y) = (y == x) := by
  apply Bool.eq_iff_iff.mpr
  rw [beq_iff_eq, beq_iff_eq, eq_comm]

private theorem general_residue_join (a b : Lane) :
    ((List.range a.n).map (fun (i : Nat) => (List.range b.n).countP
      (fun (j : Nat) => (a.base + a.step * (i : Int)) == (b.base + b.step * (j : Int))))).sum =
    joinCount a b := by
  by_cases ha0g : a.n = 0
  · have hz : (intersection (values a) (values b)).n = 0 := by
      unfold intersection
      rw [if_pos (by simp [ha0g, values])]
      rfl
    simp [ha0g, hz, joinCount, sum_map_zero_nat]
  by_cases hbg0 : b.n = 0
  · have hz : (intersection (values a) (values b)).n = 0 := by
      unfold intersection
      rw [if_pos (by simp [hbg0, values])]
      rfl
    simp [hbg0, hz, joinCount, sum_map_zero_nat]
  by_cases has : a.step = 0
  · by_cases hbs : b.step = 0
    · -- both constant
      by_cases ha0 : a.n = 0
      · have hz : (intersection (values a) (values b)).n = 0 := by
          unfold intersection
          rw [if_pos (by simp [ha0, values])]
          rfl
        simp [ha0, hz, joinCount, sum_map_zero_nat]
      by_cases hb0 : b.n = 0
      · have hz : (intersection (values a) (values b)).n = 0 := by
          unfold intersection
          rw [if_pos (by simp [hb0, values])]
          rfl
        simp [hb0, hz, joinCount, sum_map_zero_nat]
      have hinter : (intersection (values a) (values b)).n =
          if a.base == b.base then 1 else 0 := by
        unfold intersection
        rw [if_neg (by simp [ha0, hb0, values]), if_pos (by simp [values, has])]
        change (if contains (values b) (values a).lo then 1 else 0) = _
        have hblo : (values b).lo = b.base := by simp [values, hbs]
        have hvalo : (values a).lo = a.base := by simp [values, has]
        have hbpos : 0 < b.n := by omega
        simp [hblo, hvalo, values, has, hbs, hbpos, contains_const_iff, beq_iff_eq]
      unfold joinCount
      rw [if_pos (beq_iff_eq.mpr has), if_pos (beq_iff_eq.mpr hbs), hinter]
      by_cases heq : a.base == b.base
      · simp [heq, has, hbs, sum_map_const_nat]
      · have hp : (a.base == b.base) = false := beq_eq_false_iff_ne.mpr (by intro h; exact heq (beq_iff_eq.mpr (by simpa [h])))
        have hzmap : (List.range a.n).map (fun (i : Nat) =>
            (List.range b.n).countP (fun (j : Nat) =>
              a.base + a.step * (i : Int) == b.base + b.step * (j : Int))) =
            (List.range a.n).map (fun _ : Nat => 0) := by
          apply List.map_congr_left
          intro i hi
          have hf : (fun (j : Nat) => a.base + a.step * (i : Int) == b.base + b.step * (j : Int)) =
              (fun _ : Nat => false) := by
            funext j
            simp [has, hbs, hp]
          rw [hf]
          simp
        rw [hzmap]
        simp [sum_map_zero_nat]
        have hne : a.base ≠ b.base := by intro h; exact heq (beq_iff_eq.mpr h)
        simp [hne]
    · -- a constant, b progression
      have hbv : 0 < (values b).step := values_step_pos b hbs
      have hinjB : Function.Injective (fun j : Nat => b.base + b.step * (j : Int)) := by
        intro j k hjk
        have hm : b.step * (j : Int) = b.step * (k : Int) := Int.add_left_cancel hjk
        have hs : (b.step : Int) ≠ 0 := by exact_mod_cast hbs
        have hjki : (j : Int) = (k : Int) := Int.eq_of_mul_eq_mul_left hs hm
        exact_mod_cast hjki
      have hvalA : ∀ i : Nat, a.base + a.step * (i : Int) = a.base := by intro i; simp [has]
      simp only [hvalA]
      let K := (List.range b.n).countP (fun (j : Nat) => a.base == b.base + b.step * (j : Int))
      change ((List.range a.n).map (fun _ : Nat => K)).sum = joinCount a b
      have hcst : ((List.range a.n).map (fun _ : Nat => K)).sum = a.n * K := by
        simpa using (sum_map_const_nat a.n K)
      rw [hcst]
      have hcntN : (List.range b.n).countP (fun (j : Nat) => b.base + b.step * (j : Int) == a.base) =
          if contains (values b) a.base then 1 else 0 := by
        have hc := countP_eq_of_inj b.n (fun j : Nat => b.base + b.step * (j : Int)) hinjB a.base
        rw [hc]
        by_cases hmem : contains (values b) a.base
        · rw [if_pos hmem, dif_pos]
          rcases (mem_values_iff b a.base).mp hmem with ⟨j, hj, hv⟩
          exact ⟨j, hj, hv⟩
        · rw [if_neg hmem, dif_neg]
          intro h
          rcases h with ⟨j, hj, hv⟩
          exact hmem ((mem_values_iff b a.base).mpr ⟨j, hj, hv⟩)
      have hcnt : K = if contains (values b) a.base then 1 else 0 := by
        dsimp [K]
        rw [show (List.range b.n).countP (fun (j : Nat) => a.base == b.base + b.step * (j : Int)) =
            (List.range b.n).countP (fun (j : Nat) => b.base + b.step * (j : Int) == a.base) by
          rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
          congr 1
          apply filter_congr_bool
          intro j hj
          exact beq_comm_int _ _]
        exact hcntN
      rw [hcnt]
      unfold joinCount
      simp [has, hbs]
      dsimp [values] at hbv
      have hinter : (intersection (values a) (values b)).n =
          if contains (values b) (values a).lo then 1 else 0 := by
        unfold intersection
        rw [if_neg (by simp [values, ha0g, hbg0]), if_pos (by simp [values, has])]
        change (if contains (values b) (values a).lo then 1 else 0) = _
        split <;> rfl
      rw [hinter]
      have hlo : (values a).lo = a.base := by simp [values, has]
      rw [hlo]
      split <;> simp
  · by_cases hbs : b.step = 0
    · -- a progression, b constant
      have hav : 0 < (values a).step := values_step_pos a has
      have hinjA : Function.Injective (fun i : Nat => a.base + a.step * (i : Int)) := by
        intro i k hik
        have hm : a.step * (i : Int) = a.step * (k : Int) := Int.add_left_cancel hik
        have hs : (a.step : Int) ≠ 0 := by exact_mod_cast has
        have hiki : (i : Int) = (k : Int) := Int.eq_of_mul_eq_mul_left hs hm
        exact_mod_cast hiki
      have hvalB : ∀ j : Nat, b.base + b.step * (j : Int) = b.base := by intro j; simp [hbs]
      simp only [hvalB]
      have hcntF : ((List.range a.n).filter (fun (i : Nat) => a.base + a.step * (i : Int) == b.base)).length =
          if contains (values a) b.base then 1 else 0 := by
        rw [← List.countP_eq_length_filter]
        have hc := countP_eq_of_inj a.n (fun i : Nat => a.base + a.step * (i : Int)) hinjA b.base
        rw [hc]
        by_cases hmem : contains (values a) b.base
        · rw [if_pos hmem, dif_pos]
          rcases (mem_values_iff a b.base).mp hmem with ⟨i, hi, hv⟩
          exact ⟨i, hi, hv⟩
        · rw [if_neg hmem, dif_neg]
          intro h
          rcases h with ⟨i, hi, hv⟩
          exact hmem ((mem_values_iff a b.base).mpr ⟨i, hi, hv⟩)
      have houterNat : ((List.range a.n).map (fun (i : Nat) => (List.range b.n).countP
          (fun (j : Nat) => (a.base + a.step * (i : Int)) == b.base))).sum =
          b.n * ((List.range a.n).filter (fun (i : Nat) => a.base + a.step * (i : Int) == b.base)).length := by
        rw [show (List.range a.n).map (fun (i : Nat) => (List.range b.n).countP
            (fun (j : Nat) => (a.base + a.step * (i : Int)) == b.base)) =
            (List.range a.n).map (fun (i : Nat) =>
              if (a.base + a.step * (i : Int)) == b.base then b.n else 0) by
          apply List.map_congr_left
          intro i hi
          by_cases h : (a.base + a.step * (i : Int)) == b.base
          · simp [h]
          · simp [h]]
        rw [← sum_map_ite_zero_nat]
        rw [sum_map_const_nat_list]
        grind
      have hinter : (intersection (values a) (values b)).n =
          if contains (values a) (values b).lo then 1 else 0 := by
        unfold intersection
        rw [if_neg (by simp [values, ha0g, hbg0]), if_neg (by simp [values, has]),
          if_pos (by simp [values, hbs])]
        change (if contains (values a) (values b).lo then 1 else 0) = _
        split <;> rfl
      rw [houterNat, hcntF]
      unfold joinCount
      simp [has, hbs]
      rw [hinter]
      have hblo : (values b).lo = b.base := by simp [values, hbs]
      rw [hblo]
      grind
    · -- both progressions
      have hav : 0 < (values a).step := values_step_pos a has
      have hbv : 0 < (values b).step := values_step_pos b hbs
      have hinjB : Function.Injective (fun j : Nat => b.base + b.step * (j : Int)) := by
        intro j k hjk
        have hm : b.step * (j : Int) = b.step * (k : Int) := Int.add_left_cancel hjk
        have hs : (b.step : Int) ≠ 0 := by exact_mod_cast hbs
        have hjki : (j : Int) = (k : Int) := Int.eq_of_mul_eq_mul_left hs hm
        exact_mod_cast hjki
      have hinjA : Function.Injective (fun i : Nat => a.base + a.step * (i : Int)) := by
        intro i k hik
        have hm : a.step * (i : Int) = a.step * (k : Int) := Int.add_left_cancel hik
        have hs : (a.step : Int) ≠ 0 := by exact_mod_cast has
        have hiki : (i : Int) = (k : Int) := Int.eq_of_mul_eq_mul_left hs hm
        exact_mod_cast hiki
      have hinner : ∀ i : Nat, (List.range b.n).countP
          (fun (j : Nat) => (a.base + a.step * (i : Int)) == (b.base + b.step * (j : Int))) =
          if contains (values b) (a.base + a.step * (i : Int)) then 1 else 0 := by
        intro i
        have hc := countP_eq_of_inj b.n (fun j : Nat => b.base + b.step * (j : Int)) hinjB
          (a.base + a.step * (i : Int))
        rw [show (List.range b.n).countP
          (fun (j : Nat) => (a.base + a.step * (i : Int)) == (b.base + b.step * (j : Int))) =
          (List.range b.n).countP
          (fun (j : Nat) => b.base + b.step * (j : Int) == a.base + a.step * (i : Int)) by
          rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
          congr 1
          apply filter_congr_bool
          intro j hj
          exact beq_comm_int _ _]
        rw [hc]
        by_cases hmem : contains (values b) (a.base + a.step * (i : Int))
        · rw [if_pos hmem, dif_pos]
          rcases (mem_values_iff b (a.base + a.step * (i : Int))).mp hmem with ⟨j, hj, hv⟩
          exact ⟨j, hj, hv⟩
        · rw [if_neg hmem, dif_neg]
          intro h
          rcases h with ⟨j, hj, hv⟩
          exact hmem ((mem_values_iff b (a.base + a.step * (i : Int))).mpr ⟨j, hj, hv⟩)
      -- the filtered list of positions corresponds to valsList of intersection
      let fl : List Int := ((List.range a.n).filter
        (fun (i : Nat) => contains (values b) (a.base + a.step * (i : Int)))).map
          (fun (i : Nat) => a.base + a.step * (i : Int))
      have hfl_nodup : fl.Nodup := by
        dsimp [fl]
        exact List.Pairwise.map (fun (i : Nat) => a.base + a.step * (i : Int))
          (fun i k hne heq => hne (hinjA heq))
          (List.Pairwise.filter (fun i : Nat => contains (values b) (a.base + a.step * (i : Int)))
            (List.nodup_range : (List.range a.n).Nodup))
      let ov := intersection (values a) (values b)
      have hov_nodup : (valsList ov).Nodup :=
        valsList_nodup_of_step_pos ov (inter_step_pos_both (values a) (values b) hav hbv
          (by simp [values, ha0g, hbg0]) (by simp [values, ha0g, hbg0]))
      have hperm : List.Perm (valsList ov) fl := by
        apply (List.perm_ext_iff_of_nodup hov_nodup hfl_nodup).mpr
        intro v
        rw [mem_valsList]
        constructor
        · intro hv
          have hb := (intersection_contains (values a) (values b) v).mp hv
          rcases (mem_values_iff a v).mp hb.1 with ⟨i, hi, hval⟩
          apply List.mem_map.mpr
          refine ⟨i, ?_, ?_⟩
          · apply List.mem_filter.mpr
            exact ⟨List.mem_range.mpr hi, by simpa [hval] using hb.2⟩
          · exact hval
        · intro hv
          rcases List.mem_map.mp hv with ⟨i, hi, hival⟩
          apply (intersection_contains (values a) (values b) v).mpr
          constructor
          · apply (mem_values_iff a v).mpr
            refine ⟨i, List.mem_range.mp (List.mem_filter.mp hi).1, ?_⟩
            simpa [hival]
          · simpa [hival] using (List.mem_filter.mp hi).2
      have houterNat : ((List.range a.n).map (fun (i : Nat) => (List.range b.n).countP
          (fun (j : Nat) => (a.base + a.step * (i : Int)) == (b.base + b.step * (j : Int))))).sum =
          ((List.range a.n).filter (fun (i : Nat) => contains (values b) (a.base + a.step * (i : Int)))).length := by
        rw [show (List.range a.n).map (fun (i : Nat) => (List.range b.n).countP
          (fun (j : Nat) => (a.base + a.step * (i : Int)) == (b.base + b.step * (j : Int)))) =
          (List.range a.n).map (fun (i : Nat) => if contains (values b) (a.base + a.step * (i : Int)) then 1 else 0) by
          apply List.map_congr_left
          intro i hi
          simpa using hinner i]
        rw [← sum_map_ite_zero_prop_nat]
        simp [sum_map_one_nat]
      have hflen : ((List.range a.n).filter
          (fun (i : Nat) => contains (values b) (a.base + a.step * (i : Int)))).length = ov.n := by
        have hpl := perm_length hperm
        dsimp [fl] at hpl
        rw [List.length_map] at hpl
        have hvl : (valsList ov).length = ov.n := by simp [valsList]
        omega
      rw [houterNat, hflen]
      unfold joinCount
      simp [has, hbs, ov]

theorem sound_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal.ResidueJoin.Goal := by
  intro c b
  have hrows := rows_linear c
  rw [hrows]
  simp only [layout, List.map_cons, List.map_nil]
  rw [List.map_map]
  rw [laneA_eq]
  simp only [List.sum_cons, List.sum_nil, Int.add_zero]
  change ((List.range c.n).map (fun (i : Nat) => (List.range b.n).countP
    (fun (j : Nat) => (c.base + c.step * (i : Int)) == (b.base + b.step * (j : Int))))).sum =
    joinCount ⟨0, 1, c.n, c.base, c.step⟩ b
  exact general_residue_join ⟨0, 1, c.n, c.base, c.step⟩ b

/--! ## Goal: DeleteProgression -/

private theorem deletedSum_as_sum (c : Config) (mask : Progression) :
    deletedSum (lane c.n 0 1 c.base c.step) mask =
      ((valsList (intersection (⟨0, 1, c.n⟩ : Progression) mask)).map
        (fun v => c.base + v * c.step)).sum := by
  rw [laneA_eq]
  unfold deletedSum positions firstAt slopeAt valsList
  dsimp
  simp only [Int.sub_zero, Int.ediv_one, Nat.div_one]
  simp only [List.map_map]
  let ov := intersection (⟨0, 1, c.n⟩ : Progression) mask
  change (ov.n : Int) * (c.base + ov.lo * c.step) + tri ov.n * ((ov.step : Int) * c.step) =
    ((List.range ov.n).map (fun (k : Nat) => c.base + (ov.lo + (k : Int) * (ov.step : Int)) * c.step)).sum
  rw [show ((List.range ov.n).map (fun (k : Nat) => c.base + (ov.lo + (k : Int) * (ov.step : Int)) * c.step)) =
      ((List.range ov.n).map (fun (k : Nat) => (c.base + ov.lo * c.step) + (k : Int) * ((ov.step : Int) * c.step))) by
    apply List.map_congr_left
    intro k hk
    grind]
  rw [← partialSum_eq_sum ⟨0, 1, ov.n, c.base + ov.lo * c.step, (ov.step : Int) * c.step⟩ ov.n]
  simp [partialSum]

theorem sound_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal.DeleteProgression.Goal := by
  intro c mask
  let a := lane c.n 0 1 c.base c.step
  let ov := intersection (⟨0, 1, c.n⟩ : Progression) mask
  have hrows := rows_linear c
  have htotal : partialSum (⟨0, 1, c.n, c.base, c.step⟩ : Lane) c.n =
      ((List.range c.n).map (fun i : Nat => c.base + (i : Int) * c.step)).sum := by
    exact partialSum_eq_sum ⟨0, 1, c.n, c.base, c.step⟩ c.n
  have hdel := deletedSum_as_sum c mask
  let fl : List Int := ((List.range c.n).filter (fun (i : Nat) => contains mask (i : Int))).map
    (fun (i : Nat) => (i : Int))
  have hfl_nodup : fl.Nodup := by
    dsimp [fl]
    apply map_nat_nodup
    exact List.Pairwise.filter (fun x : Nat => contains mask (x : Int))
      (List.nodup_range : (List.range c.n).Nodup)
  have hperm : List.Perm (valsList ov) fl := by
    apply (List.perm_ext_iff_of_nodup (valsList_nodup_of_step_pos ov (inter_step_one c.n mask))
      hfl_nodup).mpr
    intro v
    rw [mem_valsList]
    constructor
    · intro hv
      have hb := (intersection_contains (⟨0, 1, c.n⟩ : Progression) mask v).mp hv
      rcases (contains_iff_mem (⟨0, 1, c.n⟩ : Progression) v).mp hb.1 with ⟨k, hk, hkv⟩
      have hvn : (v.toNat : Int) = v := by
        have hvnon : 0 ≤ v := by dsimp [progVal] at hkv; omega
        exact Int.toNat_of_nonneg hvnon
      rw [← hvn]
      apply List.mem_map.mpr
      refine ⟨v.toNat, ?_, rfl⟩
      apply List.mem_filter.mpr
      constructor
      · apply List.mem_range.mpr
        have hvk : v = (k : Int) := by dsimp [progVal] at hkv; omega
        rw [hvk]
        simpa using hk
      · simpa [hvn] using hb.2
    · intro hv
      rcases List.mem_map.mp hv with ⟨k, hk, rfl⟩
      apply (intersection_contains (⟨0, 1, c.n⟩ : Progression) mask (k : Int)).mpr
      constructor
      · apply (contains_iff_mem (⟨0, 1, c.n⟩ : Progression) (k : Int)).mpr
        refine ⟨k, List.mem_range.mp (List.mem_filter.mp hk).1, ?_⟩
        simp [progVal]
      · simpa using (List.mem_filter.mp hk).2
  have hsumdel : ((valsList ov).map (fun v => c.base + v * c.step)).sum =
      (fl.map (fun v => c.base + v * c.step)).sum := list_sum_perm (hperm.map _)
  rw [hrows]
  simp only [layout, List.map_cons, List.map_nil]
  rw [hdel, hsumdel]
  rw [laneA_eq]
  simp only [List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
  rw [htotal]
  -- LHS is the complement filter sum, RHS is total minus deleted
  let lr := (List.range c.n).map (fun (i : Nat) => (i, c.base + c.step * (i : Int)))
  change ((lr.filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((List.range c.n).map (fun (i : Nat) => c.base + (i : Int) * c.step)).sum -
      (fl.map (fun v => c.base + v * c.step)).sum
  dsimp [lr]
  rw [map_pair_filter (List.range c.n) (fun (i : Nat) => i)
    (fun (i : Nat) => c.base + c.step * (i : Int)) (fun p : Nat => !contains mask p)]
  simp only [List.map_map]
  change (((List.range c.n).filter (fun i : Nat => !contains mask (i : Int))).map
      (fun i : Nat => c.base + c.step * (i : Int))).sum =
    ((List.range c.n).map (fun (i : Nat) => c.base + (i : Int) * c.step)).sum -
      (fl.map (fun v => c.base + v * c.step)).sum
  have hstep : ∀ i : Nat, c.base + c.step * (i : Int) = c.base + (i : Int) * c.step := by
    intro i; grind
  rw [show ((List.range c.n).filter (fun i : Nat => !contains mask (i : Int))).map
      (fun i : Nat => c.base + c.step * (i : Int)) =
      ((List.range c.n).filter (fun i : Nat => !contains mask (i : Int))).map
      (fun i : Nat => c.base + (i : Int) * c.step) by
    apply List.map_congr_left
    intro i hi
    exact hstep i]
  have hc := sum_filter_add_compl (List.range c.n) (fun i : Nat => contains mask (i : Int))
    (fun i : Nat => c.base + (i : Int) * c.step)
  dsimp [fl] at hc ⊢
  simp only [List.map_map]
  rw [show ((fun v : Int => c.base + v * c.step) ∘ (fun i : Nat => (i : Int))) =
      (fun i : Nat => c.base + (i : Int) * c.step) by rfl]
  omega

end TinylambOpt.ResearchProofs.Atlas.EncodedPages.LinearOrdinal.Remaining

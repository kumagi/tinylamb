import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.DictionaryCycle.Remaining

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryCycle

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


/-! ## Counting matches of a lane's value progression -/

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

private theorem sum_map_zero (l : List Nat) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l <;> simp [*]

private theorem sum_update (p r0 : Nat) (hr0 : r0 < p) (s : Nat → Int) (v : Int) :
    ((List.range p).map (fun r => if r == r0 then v + s r else s r)).sum =
      v + ((List.range p).map s).sum := by
  induction p generalizing r0 s v with
  | zero => omega
  | succ p ih =>
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

private theorem striped_sum_partition (p : Nat) (hp : 0 < p) (F : Nat → Int) :
    ∀ xs : List Nat, (xs.map F).sum =
      ((List.range p).map (fun r => ((xs.filter (fun i => i % p == r)).map F).sum)).sum := by
  intro xs
  induction xs with
  | nil =>
      rw [List.map_nil, List.sum_nil]
      exact (sum_map_zero (List.range p)).symm
  | cons x xs ih =>
      rw [List.map_cons, List.sum_cons, ih]
      rw [← sum_update p (x % p) (Nat.mod_lt x hp)]
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

private def stripedValue (p : Nat) (make : Nat → Int × Int) (i : Nat) : Int :=
  let (z, d) := make (i % p)
  z + d * ((i / p : Nat) : Int)

private theorem stripedValue_residue (p : Nat) (hp : 0 < p) (make : Nat → Int × Int)
    (r q : Nat) (hr : r < p) :
    stripedValue p make (r + q * p) =
      (make r).1 + (make r).2 * (q : Int) := by
  unfold stripedValue
  have hmod : (r + q * p) % p = r := by
    rw [Nat.add_mul_mod_self_right]
    exact Nat.mod_eq_of_lt hr
  have hdiv : (r + q * p) / p = q := by
    rw [Nat.mul_comm q p, Nat.add_mul_div_left r q hp, Nat.div_eq_of_lt hr]
    simp
  rw [hmod, hdiv]

private theorem striped_sum (p : Nat) (hp : 0 < p) (n : Nat) (make : Nat → Int × Int)
    (g : Nat → Int → Int) :
    ((List.range n).map (fun i => g i (stripedValue p make i))).sum =
      ((List.range p).map (fun r =>
        let (z, d) := make r
        ((List.range ((n - r + p - 1) / p)).map (fun q => g (r + q*p) (z + d*(q:Int)))).sum)).sum := by
  rw [striped_sum_partition p hp (fun i => g i (stripedValue p make i)) (List.range n)]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  have hrp : r < p := List.mem_range.mp hr
  rw [range_filter_mod_eq n p r hp hrp]
  simp only [List.map_map, Function.comp_apply]
  apply congrArg List.sum
  apply List.map_congr_left
  intro q hq
  change g (r + q * p) (stripedValue p make (r + q * p)) =
    g (r + q * p) ((make r).1 + (make r).2 * (q : Int))
  rw [stripedValue_residue p hp make r q hrp]

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

private theorem striped_sum_nat (p : Nat) (hp : 0 < p) (n : Nat) (make : Nat → Int × Int)
    (F : Int → Nat) :
    ((List.range n).map (fun i => F (stripedValue p make i))).sum =
      ((List.range p).map (fun r =>
        let (z, d) := make r
        ((List.range ((n - r + p - 1) / p)).map (fun q : Nat => F (z + d*(q:Int)))).sum)).sum := by
  rw [striped_sum_partition_nat p hp (fun i => F (stripedValue p make i)) (List.range n)]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  have hrp : r < p := List.mem_range.mp hr
  rw [range_filter_mod_eq n p r hp hrp]
  simp only [List.map_map]
  apply congrArg List.sum
  apply List.map_congr_left
  intro q hq
  change F (stripedValue p make (r + q * p)) = F ((make r).1 + (make r).2 * (q : Int))
  rw [stripedValue_residue p hp make r q hrp]

private theorem sum_range_ite_nat (m : Nat) (b : Bool) (v : Nat) :
    ((List.range m).map (fun _ => if b then v else 0)).sum = if b then m * v else 0 := by
  by_cases h : b <;> simp [h, sum_range_const_nat, sum_map_zero_nat]

private theorem countP_eq_sum (l : List (Nat × Int)) (f : Int → Bool) :
    l.countP (fun x => f x.2) = ((l.map (fun x => if f x.2 then 1 else 0)).sum : Nat) := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : f x.2 <;> simp [h, ih] <;> omega

private theorem sum_eq_single (p r : Nat) (hr : r < p) (f : Nat → Nat) :
    ((List.range p).map (fun x => if x == r then f x else 0)).sum = f r := by
  have hconst : ∀ x, (if x == r then f x else 0) = if x == r then f r else 0 := by
    intro x
    by_cases h : x == r
    · have h' : x = r := Nat.eq_of_beq_eq_true h
      simp [h, h']
    · simp [h]
  rw [show (List.range p).map (fun x => if x == r then f x else 0) =
      (List.range p).map (fun x => if x == r then f r else 0) from
    List.map_congr_left (fun x hx => hconst x)]
  rw [show (List.range p).map (fun x => if x == r then f r else 0) =
      (List.range p).map (fun x => if x == r then f r + 0 else 0) by
    apply List.map_congr_left
    intro x hx
    by_cases h : x == r <;> simp [h]]
  rw [sum_update_nat p r hr (fun _ : Nat => 0) (f r), sum_map_zero_nat]
  simp

private theorem range_succ_map' (f : Nat → Nat) (n : Nat) :
    (List.range (n+1)).map f = f 0 :: (List.range n).map (fun i => f (i+1)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 + 1 = Nat.succ (n+1) by omega, List.range_succ, List.map_append, ih]
      simp [List.range_succ]

private theorem sum_range_get_count : ∀ (l : List Int) (v : Int),
    ((List.range l.length).map (fun r => if l[r]?.getD 0 == v then 1 else 0)).sum = l.count v := by
  intro l
  induction l with
  | nil => intro v; simp
  | cons a as ih =>
      intro v
      rw [List.count_cons]
      simp only [List.length_cons]
      rw [range_succ_map']
      simp only [List.map_cons, List.sum_cons, List.getElem?_cons_zero, Option.getD_some,
        List.getElem?_cons_succ]
      rw [ih]
      omega

private theorem count_take_get : ∀ (k : Nat) (l : List Int) (v : Int),
    (l.take k).count v =
      ((List.range k).map (fun r => if r < l.length && l[r]?.getD 0 == v then 1 else 0)).sum := by
  intro k
  induction k with
  | zero => intro l v; simp
  | succ k ih =>
      intro l v
      cases l with
      | nil => simp [sum_map_zero_nat]
      | cons a as =>
          simp only [List.length_cons]
          rw [show k + 1 = Nat.succ k by omega, List.take_succ_cons]
          rw [List.count_cons]
          rw [ih as v]
          rw [range_succ_map']
          simp only [List.sum_cons, List.getElem?_cons_zero, Option.getD_some,
            List.getElem?_cons_succ]
          have hsimp : ∀ i : Nat,
              (if i + 1 < as.length + 1 && as[i]?.getD 0 == v then 1 else 0) =
              if i < as.length && as[i]?.getD 0 == v then 1 else 0 := by
            intro i
            by_cases h : i < as.length && as[i]?.getD 0 == v <;> simp [h] <;> omega
          rw [show (List.range k).map (fun i : Nat =>
              if i + 1 < as.length + 1 && as[i]?.getD 0 == v then 1 else 0) =
              (List.range k).map (fun i : Nat =>
              if i < as.length && as[i]?.getD 0 == v then 1 else 0) from
            List.map_congr_left (fun i hi => hsimp i)]
          simp
          omega

private theorem sum_range_trunc (f : Nat → Nat) (k p : Nat) (hk : k ≤ p) :
    ((List.range p).map (fun r => if r < k then f r else 0)).sum = ((List.range k).map f).sum := by
  obtain ⟨d, rfl⟩ := Nat.exists_eq_add_of_le hk
  rw [List.range_add, List.map_append, List.sum_append]
  have hzero : ((List.map (fun x => k + x) (List.range d)).map
      (fun r => if r < k then f r else 0)).sum = 0 := by
    rw [show (List.map (fun x => k + x) (List.range d)).map (fun r => if r < k then f r else 0) =
        (List.map (fun x => k + x) (List.range d)).map (fun _ => 0) by
      apply List.map_congr_left
      intro r hr
      simp only [List.mem_map, List.mem_range] at hr
      rcases hr with ⟨x, hx, rfl⟩
      by_cases hlt : k + x < k
      · omega
      · simp [hlt]]
    apply sum_map_zero_nat
  rw [hzero]
  simp only [Nat.add_zero]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  have hrlt : r < k := List.mem_range.mp hr
  simp [hrlt]

private theorem sum_range_add (f g : Nat → Nat) (n : Nat) :
    ((List.range n).map (fun i => f i + g i)).sum =
      ((List.range n).map f).sum + ((List.range n).map g).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append,
        List.map_append, List.sum_append, List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      rw [ih]
      omega

private theorem sum_range_mul_const (f : Nat → Nat) (n c : Nat) :
    ((List.range n).map (fun i => c * f i)).sum = c * ((List.range n).map f).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      rw [ih]
      simp only [List.map_append, List.sum_append, List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      rw [Nat.mul_add]
      simp

private theorem range_succ_map_sum (f : Nat → Int) (n : Nat) :
    ((List.range (n+1)).map f).sum = ((List.range n).map f).sum + f n := by
  rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
  simp

private theorem even_or_odd (n : Nat) : n % 2 = 0 ∨ n % 2 = 1 := by
  have h := Nat.mod_lt n (by omega : 0 < 2)
  omega

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

theorem sound_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryCycle.ResidueJoin.Goal := by
  intro c b
  by_cases hne : c.dict = []
  · cases c with
    | mk n split parameter base step bias dict =>
      simp only [Config.dict] at hne
      subst dict
      have hfm : (List.range n).filterMap (fun _ : Nat => (none : Option (Nat × Int))) = [] := by
        induction n with
        | zero => simp
        | succ n ih => simp [List.range_succ, ih]
      simp [rows, layout]
      rw [hfm]
      simp
  · let p := c.dict.length
    have hp : 0 < p := by dsimp [p]; exact List.length_pos_iff.mpr hne
    have hrows : rows layout c = (List.range c.n).map (fun i => (i, c.dict[i % p]?.getD 0)) := by
      apply rows_range_map
      intro i hi
      have hlt : i % p < p := Nat.mod_lt i hp
      have hget : c.dict[i % p]? = some (c.dict[i % p]) := by
        rw [List.getElem?_eq_some_iff]
        exact ⟨by simpa [p] using hlt, rfl⟩
      simp [layout, hget, p]
    rw [hrows]
    have hhit : (((List.range c.n).map (fun i : Nat => (i, c.dict[i % p]?.getD 0))).map
        (fun r : Nat × Int =>
          (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum =
        ((List.range c.n).map (fun i : Nat =>
          hit b (c.dict[i % p]?.getD 0))).sum := by
      rw [List.map_map]
      rfl
    rw [hhit]
    rw [striped_sum_partition_nat p hp
      (fun i : Nat => hit b (c.dict[i % p]?.getD 0)) (List.range c.n)]
    let col (r : Nat) : Nat := (((List.range c.n).filter (fun i => i % p == r)).map
      (fun i => hit b (c.dict[i % p]?.getD 0))).sum
    change ((List.range p).map col).sum = ((layout.lanes c).map (joinCount · b)).sum
    have hcol : ∀ r, r < p → col r =
        hit b (c.dict[r]?.getD 0) * (lane c.n r p (c.dict[r]?.getD 0) 0).n := by
      intro r hr
      have hconst : ∀ i ∈ (List.range c.n).filter (fun i => i % p == r),
          c.dict[i % p]?.getD 0 = c.dict[r]?.getD 0 := by
        intro i himem
        simp only [List.mem_filter, List.mem_range] at himem
        have heq : i % p = r := Nat.eq_of_beq_eq_true himem.2
        rw [heq]
      dsimp [col]
      rw [show ((List.range c.n).filter (fun i => i % p == r)).map
            (fun i => hit b (c.dict[i % p]?.getD 0)) =
          ((List.range c.n).filter (fun i => i % p == r)).map
            (fun _ => hit b (c.dict[r]?.getD 0)) by
        apply List.map_congr_left
        intro i himem
        rw [hconst i himem]]
      rw [sum_map_const_nat]
      dsimp [lane]
      rw [← List.countP_eq_length_filter]
      have hcnt : (List.range c.n).countP (fun i => i % p == r) =
          (c.n - r + p - 1) / p := by
        rw [List.countP_eq_length_filter, range_filter_mod_eq c.n p r hp hr]
        simp
      rw [hcnt]
      rw [Nat.mul_comm]
    rw [show ((List.range p).map col) =
        ((List.range p).map (fun r => if r < min p c.n then col r else 0)) by
      apply List.map_congr_left
      intro r hrmem
      have hrp : r < p := List.mem_range.mp hrmem
      by_cases hlt : r < min p c.n
      · simp [hlt]
      · have hrn : c.n ≤ r := by
          have hmin : min p c.n ≤ r := by omega
          by_cases hpn : p ≤ c.n
          · have : min p c.n = p := Nat.min_eq_left hpn
            omega
          · have : min p c.n = c.n := Nat.min_eq_right (by omega)
            omega
        have hzero : c.n - r + p - 1 = p - 1 := by omega
        rw [hcol r hrp]
        dsimp [lane]
        rw [hzero, Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))]
        simp [hlt]]
    rw [sum_range_trunc (fun r => col r) (min p c.n) p (Nat.min_le_left _ _)]
    rw [show ((List.range (min p c.n)).map col) =
        ((List.range (min p c.n)).map (fun r =>
          hit b (c.dict[r]?.getD 0) * (lane c.n r p (c.dict[r]?.getD 0) 0).n)) by
      apply List.map_congr_left
      intro r hrmem
      have hrp : r < p := Nat.lt_of_lt_of_le (List.mem_range.mp hrmem) (Nat.min_le_left _ _)
      exact hcol r hrp]
    rw [show ((layout.lanes c).map (joinCount · b)) =
        ((List.range (min p c.n)).map (fun r =>
          joinCount (lane c.n r p (c.dict[r]?.getD 0) 0) b)) by
      dsimp [layout, p]
      rw [List.map_map]
      rfl]
    apply congrArg List.sum
    apply List.map_congr_left
    intro r hrmem
    exact hit_eq_joinCount (lane c.n r p (c.dict[r]?.getD 0) 0) b rfl

end TinylambOpt.ResearchProofs.Atlas.EncodedPages.DictionaryCycle.Remaining

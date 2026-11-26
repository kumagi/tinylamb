import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `LaneMajor.ResidueJoin` — `∀ c (b : Lane), ((rows layout c).map (fun r => (List.range
  b.n).countP (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes
  c).map (joinCount · b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.LaneMajorResidueJoin

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.LaneMajor

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



/-! ## Common list / progression helpers for the three goals -/

private def valsList (p : Progression) : List Int :=
  (List.range p.n).map (fun k : Nat => p.lo + (k : Int) * (p.step : Int))

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

private theorem nodup_filter {α : Type} (p : α → Bool) {l : List α}
    (h : l.Nodup) : (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons a l ih =>
      have hd := (List.nodup_cons.mp h)
      by_cases hp : p a = true
      · rw [List.filter_cons, if_pos hp]
        exact List.nodup_cons.mpr ⟨by
          intro hm
          exact hd.1 (List.mem_filter.mp hm).1, ih hd.2⟩
      · have hfilter : (a :: l).filter p = l.filter p := by simp [hp]
        rw [hfilter]
        exact ih hd.2

private theorem valsList_nodup_of_step_pos (p : Progression) (hp : 0 < p.step) :
    (valsList p).Nodup := by
  unfold valsList
  exact List.Pairwise.map (fun k : Nat => p.lo + (k : Int) * (p.step : Int))
    (fun a b hne heq => hne (by
      have hmul : (a : Int) * (p.step : Int) = (b : Int) * (p.step : Int) := by omega
      have hnz : (p.step : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hp)
      have hab : (a : Int) = (b : Int) := Int.eq_of_mul_eq_mul_right hnz hmul
      exact_mod_cast hab)) List.nodup_range

private theorem inter_step_pos_of_left_pos (A B : Progression) (hA : 0 < A.step) :
    0 < (intersection A B).step := by
  unfold intersection
  by_cases h : A.n == 0 || B.n == 0
  · simp [h]
  · rw [if_neg h]
    by_cases hA0 : A.step == 0
    · exact False.elim (by simpa [Nat.ne_of_gt hA] using hA0)
    · rw [if_neg hA0]
      by_cases hB0 : B.step == 0
      · simp [hB0]
      · rw [if_neg hB0]
        by_cases hg : (B.lo - A.lo) % (A.step.gcd B.step : Int) != 0
        · simp [hg]
        · rw [if_neg hg]
          dsimp
          exact Nat.mul_pos (Nat.div_gcd_pos_of_pos_left B.step hA)
            (Nat.pos_of_ne_zero (by simpa using hB0))

private theorem count_common_eq_inter (A B : Progression) (hA : 0 < A.step) :
    (List.range A.n).countP (fun i => contains B (progVal A i)) = (intersection A B).n := by
  let R := intersection A B
  have hR : 0 < R.step := by dsimp [R]; exact inter_step_pos_of_left_pos A B hA
  have hperm : List.Perm ((valsList A).filter (fun v => contains B v)) (valsList R) := by
    apply (List.perm_ext_iff_of_nodup
      (nodup_filter (fun v => contains B v) (valsList_nodup_of_step_pos A hA))
      (valsList_nodup_of_step_pos R hR)).mpr
    intro v
    rw [List.mem_filter, mem_valsList, mem_valsList]
    exact (intersection_contains A B v).symm
  have hlen := hperm.length_eq
  rw [← List.countP_eq_length_filter] at hlen
  have hlenR : (valsList R).length = R.n := by simp [valsList]
  rw [hlenR] at hlen
  dsimp [R] at hlen
  unfold valsList at hlen
  simpa [progVal, Function.comp_def] using hlen

private theorem countP_range_shift (n : Nat) (p : Nat → Bool) :
    (List.range n).countP (fun i => p (i+1)) + (if p 0 then 1 else 0) =
      (List.range n).countP p + (if p n then 1 else 0) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.countP_append, List.countP_append]
      simp only [List.countP_cons, List.countP_nil]
      by_cases h0 : p 0 <;> by_cases hn : p n <;> by_cases hn1 : p (n+1) <;>
        simp [h0, hn, hn1] at ih ⊢ <;> omega

private theorem countP_range_reflect (n : Nat) (p : Nat → Bool) :
    (List.range n).countP (fun i => p (n-1-i)) = (List.range n).countP p := by
  induction n generalizing p with
  | zero => simp
  | succ n ih =>
      cases n with
      | zero => simp
      | succ n =>
          rw [show n + 2 = (n+1)+1 by omega, List.range_succ, List.countP_append,
            List.countP_append]
          simp only [List.countP_cons, List.countP_nil]
          have hsub : ∀ i : Nat, n + 2 - 1 - i = n + 1 - i := by intro i; omega
          simp only [hsub]
          have hih := ih (fun j => p (j+1))
          have href : (List.range (n+1)).countP (fun i => p (n+1-i)) =
              (List.range (n+1)).countP (fun i => p (i+1)) := by
            rw [← hih]
            apply List.countP_congr
            intro i hi
            have hi' : i ≤ n := by
              have := List.mem_range.mp hi
              omega
            have heq : n - i + 1 = n + 1 - i := by omega
            simp [heq]
          rw [href]
          have hshift := countP_range_shift (n+1) p
          simp at hshift ⊢
          omega

private def hit (b : Lane) (v : Int) : Nat :=
  (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int))

private theorem countP_eq_sum_nat {α : Type} (l : List α) (p : α → Bool) :
    l.countP p = (l.map (fun x => if p x then 1 else 0)).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih] <;> omega

private theorem sum_range_const_nat (n k : Nat) :
    ((List.range n).map (fun _ => k)).sum = n * k := by
  induction n with
  | zero => simp
  | succ n ih => simp [List.range_succ, ih, Nat.succ_mul]

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
            have hine : i ≠ n := by intro heq; subst i; rw [hpnf] at hpi; cases hpi
            exact ⟨i, by omega, hpi⟩
          · rintro ⟨i, hi, hpi⟩
            exact ⟨i, by omega, hpi⟩
        by_cases hnone : ∃ i, i < n ∧ p i = true
        · have hS : (∃ i, i < n+1 ∧ p i = true) := hiff.mpr hnone
          simp [hpnf, hnone, hS]
        · have hS : ¬ (∃ i, i < n+1 ∧ p i = true) := by intro hh; exact hnone (hiff.mp hh)
          simp [hpnf, hnone, hS]

private theorem hit_step_ne (b : Lane) (v : Int) (hs : b.step ≠ 0) :
    hit b v = if (∃ i, i < b.n ∧ v = b.base + b.step * (i : Int)) then 1 else 0 := by
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
        intro hh; exact hex (hiff.mpr hh)
      simp [hex, hex']
  · intro i j hi hj hpi hpj
    have hv1 : v = b.base + b.step * (i : Int) := beq_iff_eq.mp hpi
    have hv2 : v = b.base + b.step * (j : Int) := beq_iff_eq.mp hpj
    have hm0 : b.step * (i : Int) = b.step * (j : Int) := by omega
    have hm : (i : Int) * b.step = (j : Int) * b.step := by simpa [Int.mul_comm] using hm0
    have hsint : (b.step : Int) ≠ 0 := by exact_mod_cast hs
    have hij : (i : Int) = (j : Int) := Int.eq_of_mul_eq_mul_right hsint hm
    exact Int.ofNat_inj.mp hij

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

private theorem hit_eq_contains_values (b : Lane) (v : Int) :
    hit b v = (if contains (values b) v then (if b.step == 0 then b.n else 1) else 0) := by
  by_cases hbz : b.step = 0
  · rw [hit_step_zero b v hbz]
    by_cases hbn : b.n = 0
    · simp [contains, hbn, hbz]
    · have hbnpos : 0 < b.n := by omega
      by_cases hveq : b.base = v
      · have hcv : contains (values b) v = true := by
          rw [contains_values_iff]
          exact ⟨0, hbnpos, by simp [hbz, hveq]⟩
        simp [hveq, hcv, hbz]
      · have hcv : ¬ contains (values b) v = true := by
          intro h
          rcases (contains_values_iff b v).mp h with ⟨i, hi, hv⟩
          exact hveq (by simpa [hbz] using hv.symm)
        have hbeq : (b.base == v) = false := beq_eq_false_iff_ne.mpr hveq
        simp [hbeq, hcv, hbz]
  · rw [hit_step_ne b v hbz]
    have hcv := contains_values_iff b v
    by_cases hex : ∃ i, i < b.n ∧ v = b.base + b.step * (i : Int)
    · have hc : contains (values b) v = true := hcv.mpr hex
      simp [hex, hc, hbz]
    · have hc : ¬ contains (values b) v = true := by intro h; exact hex (hcv.mp h)
      simp [hex, hc, hbz]

private theorem sum_range_ite_count (n k : Nat) (p : Nat → Bool) :
    ((List.range n).map (fun i => if p i then k else 0)).sum = k * (List.range n).countP p := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
      rw [List.countP_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, List.countP_cons,
        List.countP_nil]
      by_cases hp : p n
      · simp [hp, ih, Nat.mul_add, Nat.mul_one, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]
      · simp [hp, ih]

private theorem count_lane_values_contains (a b : Lane) (ha : a.step ≠ 0) :
    (List.range a.n).countP (fun i : Nat => contains (values b) (a.base + (i : Int) * a.step)) =
      (intersection (values a) (values b)).n := by
  by_cases hpos : 0 < a.step
  · have hnegfalse : ¬ a.step < 0 := by omega
    have hva : values a = ⟨a.base, a.step.natAbs, a.n⟩ := by
      simp [values, hnegfalse]
    rw [hva]
    have habs : (a.step.natAbs : Int) = a.step := Int.natAbs_of_nonneg (by omega)
    have habspos : 0 < a.step.natAbs := by
      have : (0 : Int) < (a.step.natAbs : Int) := by rw [habs]; exact hpos
      exact_mod_cast this
    have h := count_common_eq_inter
      (⟨a.base, a.step.natAbs, a.n⟩ : Progression) (values b) habspos
    simpa [progVal, habs, Int.mul_comm] using h
  · have hneg : a.step < 0 := by omega
    have habs : (a.step.natAbs : Int) = -a.step := by
      rw [← Int.natAbs_neg a.step]
      exact Int.natAbs_of_nonneg (by omega)
    have hva : values a = ⟨a.base + ((a.n - 1 : Nat) : Int) * a.step, a.step.natAbs, a.n⟩ := by
      simp [values, hneg]
    rw [hva]
    have href := countP_range_reflect a.n
      (fun i : Nat => contains (values b) (a.base + (i : Int) * a.step))
    rw [← href]
    have habspos : 0 < a.step.natAbs := Nat.pos_of_ne_zero (by omega : a.step.natAbs ≠ 0)
    have h := count_common_eq_inter
      (⟨a.base + ((a.n - 1 : Nat) : Int) * a.step, a.step.natAbs, a.n⟩ : Progression)
      (values b) habspos
    have hfun : ∀ i ∈ List.range a.n,
        contains (values b) (a.base + ((a.n - 1 - i : Nat) : Int) * a.step) =
        contains (values b) (a.base + ((a.n - 1 : Nat) : Int) * a.step +
          (i : Int) * (a.step.natAbs : Int)) := by
      intro i hi
      have hi' : i < a.n := List.mem_range.mp hi
      have hcast : ((a.n - 1 - i : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (i : Int) := by omega
      rw [hcast, habs]
      congr 1
      grind
    rw [show (List.range a.n).countP
          (fun i : Nat => contains (values b) (a.base + ((a.n - 1 - i : Nat) : Int) * a.step)) =
        (List.range a.n).countP (fun i : Nat =>
          contains (values b) (a.base + ((a.n - 1 : Nat) : Int) * a.step +
            (i : Int) * (a.step.natAbs : Int))) by
          apply List.countP_congr
          intro i hi
          rw [hfun i hi]]
    simpa [progVal] using h

private theorem hit_sum_eq_inter (a b : Lane) (ha : a.step ≠ 0) :
    ((List.range a.n).map (fun i : Nat => hit b (a.base + (i : Int) * a.step))).sum =
      (intersection (values a) (values b)).n * (if b.step == 0 then b.n else 1) := by
  have hcnt := count_lane_values_contains a b ha
  by_cases hbz : b.step = 0
  · have hbcond : (b.step == 0) = true := by simp [hbz]
    rw [if_pos hbcond]
    have hval : ∀ i : Nat, hit b (a.base + (i : Int) * a.step) =
        (if contains (values b) (a.base + (i : Int) * a.step) then b.n else 0) := by
      intro i
      rw [hit_eq_contains_values]
      simp [hbz]
    rw [show ((List.range a.n).map (fun i : Nat => hit b (a.base + (i : Int) * a.step))) =
        ((List.range a.n).map (fun i : Nat =>
          if contains (values b) (a.base + (i : Int) * a.step) then b.n else 0)) by
        apply List.map_congr_left
        intro i hi
        exact hval i]
    rw [sum_range_ite_count, hcnt]
    exact Nat.mul_comm _ _
  · have hbcond : ¬ (b.step == 0) = true := by simp [hbz]
    rw [if_neg hbcond]
    have hval : ∀ i : Nat, hit b (a.base + (i : Int) * a.step) =
        (if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0) := by
      intro i
      rw [hit_eq_contains_values]
      simp [hbz]
    rw [show ((List.range a.n).map (fun i : Nat => hit b (a.base + (i : Int) * a.step))) =
        ((List.range a.n).map (fun i : Nat =>
          if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0)) by
        apply List.map_congr_left
        intro i hi
        exact hval i]
    rw [sum_range_ite_count, hcnt]
    simp

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
      simp only [ha, ite_true]
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
          have hbase : b.base ≠ a.base := by
            intro heq
            apply hno
            refine ⟨0, by omega, ?_⟩
            simp [hbz, heq]
          simp [hbz, hbase, hc, hva, hvn, intersection, han]
        · rw [hit_step_ne b a.base hbz]
          simp [hbz, hno, hc]

private theorem joinCount_eq_raw (a b : Lane) :
    ((List.range a.n).map (fun i : Nat => hit b (a.base + (i : Int) * a.step))).sum =
      joinCount a b := by
  by_cases ha : a.step = 0
  · rw [show ((List.range a.n).map (fun i : Nat => hit b (a.base + (i : Int) * a.step))) =
        ((List.range a.n).map (fun _ : Nat => hit b a.base)) by
      apply List.map_congr_left
      intro i hi
      simp [ha]]
    rw [sum_range_const_nat]
    rw [Nat.mul_comm]
    exact hit_eq_joinCount a b ha
  · rw [hit_sum_eq_inter a b ha]
    unfold joinCount
    simp [ha]
private theorem range_succ_map_sum (f : Nat → Int) (n : Nat) :
    ((List.range (n+1)).map f).sum = ((List.range n).map f).sum + f n := by
  rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
  simp

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
    have hsub : n - r + p - 1 = n / p * p + (n % p + p - 1 - r) := by grind
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

private theorem residue_count_succ (n p r : Nat) (hp : 0 < p) (hr : r < p) :
    (n + 1 - r + p - 1) / p = (n - r + p - 1) / p + if n % p == r then 1 else 0 := by
  rw [mod_step_count (n+1) p r hp hr, mod_step_count n p r hp hr]
  rw [succ_mod_step n p hp, succ_div_step n p hp]
  by_cases hn : n % p == r
  · have hnp : n % p = r := Nat.eq_of_beq_eq_true hn
    have hltold : ¬ r < n % p := by omega
    by_cases hs : r + 1 < p
    · have hltnew : r < r + 1 := by omega
      simp [hnp, hs, hltnew, hltold]
    · have hltnew : ¬ r < 0 := by omega
      simp [hnp, hs, hltnew, hltold]
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
    · have heq : n % p + 1 = p := by
        have hs' := Nat.mod_lt n hp
        omega
      have hlt : r < n % p := by omega
      have hltnew : ¬ r < 0 := by omega
      simp [hn, h, hlt, hltnew]

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
      rw [mod_step_count (n+1) p r hp hr, mod_step_count n p r hp hr]
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
        · have heq : n % p + 1 = p := by
            have hs' := Nat.mod_lt n hp
            omega
          have hlt : r < n % p := by omega
          have hltnew : ¬ r < 0 := by omega
          simp [hn, h, hlt, hltnew]

private theorem residue_sum_map_add_int {α : Type} (l : List α) (f g : α → Int) :
    ((l.map fun x => f x + g x).sum) = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_ite_filter {α : Type} (l : List α) (p : α → Bool) (v : Int) :
    ((l.map (fun x => if p x then v else 0)).sum) =
      (((l.filter p).map (fun _ => v)).sum) := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem range_filter_eq_out (p a : Nat) (h : p ≤ a) :
    (List.range p).filter (fun r => r == a) = [] := by
  induction p with
  | zero => simp
  | succ p ih =>
      rw [List.range_succ, List.filter_append]
      have hne : (p == a) = false := beq_eq_false_iff_ne.mpr (by omega)
      rw [ih (by omega)]
      simp [hne]

private theorem range_filter_eq_singleton (p a : Nat) (ha : a < p) :
    (List.range p).filter (fun r => r == a) = [a] := by
  induction p with
  | zero => omega
  | succ p ih =>
      rw [List.range_succ, List.filter_append]
      by_cases hap : a < p
      · have htail : (List.filter (fun r => r == a) [p]) = [] := by
          have hne : (p == a) = false := beq_eq_false_iff_ne.mpr (by omega)
          simp [hne]
        rw [ih hap, htail]
        simp
      · have haeq : a = p := by omega
        subst a
        rw [range_filter_eq_out p p (by omega)]
        simp

private theorem range_filter_mod_singleton (n p : Nat) (hp : 0 < p) :
    (List.range p).filter (fun r => n % p == r) = [n % p] := by
  have hlt : n % p < p := Nat.mod_lt n hp
  have hf : (List.range p).filter (fun r => n % p == r) =
      (List.range p).filter (fun r => r == n % p) := by
    apply filter_congr_bool
    intro r hr
    by_cases h : n % p == r
    · have h' : r == n % p := by
        have heq : n % p = r := Nat.eq_of_beq_eq_true h
        rw [heq]
        simp
      simp [h, h']
    · have hne : n % p ≠ r := by intro he; exact h (by rw [he]; simp)
      have h' : ¬ r == n % p := by
        intro hh
        have heq : r = n % p := Nat.eq_of_beq_eq_true hh
        exact hne heq.symm
      simp [h, h']
  rw [hf, range_filter_eq_singleton p (n % p) hlt]

private theorem map_congr {α β : Type} (l : List α) (f g : α → β)
    (h : ∀ x ∈ l, f x = g x) : l.map f = l.map g := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem sum_congr_int {α : Type} (l : List α) (f g : α → Int)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem residue_partition_sum (n p : Nat) (hp : 0 < p) (f : Nat → Int) :
  ((List.range (min p n)).map (fun r =>
    ((List.range ((n - r + p - 1) / p)).map (fun q => f (r + q*p))).sum)).sum =
  ((List.range n).map f).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ← ih]
      simp
      by_cases hnp : n < p
      · have hminn : min p n = n := Nat.min_eq_right (by omega)
        have hmins : min p (n+1) = n+1 := Nat.min_eq_right (by omega)
        rw [hminn, hmins]
        rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
        have hsum : ((List.range n).map (fun r =>
            ((List.range ((n+1-r+p-1)/p)).map (fun q => f (r+q*p))).sum)).sum =
            ((List.range n).map (fun r =>
            ((List.range ((n-r+p-1)/p)).map (fun q => f (r+q*p))).sum)).sum := by
          apply sum_congr_int
          intro r hr
          have hrp : r < p := by
            have := List.mem_range.mp hr
            omega
          have hnmod : n % p = n := Nat.mod_eq_of_lt hnp
          have hne : ¬ n % p == r := by
            intro hh
            have heq : n % p = r := Nat.eq_of_beq_eq_true hh
            have hmem := List.mem_range.mp hr
            omega
          rw [residue_count_succ n p r hp hrp]
          simp [hne]
        rw [hsum]
        have hlast : ((List.range ((n+1-n+p-1)/p)).map (fun q => f (n+q*p))).sum = f n := by
          have hcnt : (n+1-n+p-1)/p = 1 := by
            have hn1 : n + 1 - n = 1 := by omega
            rw [hn1]
            have hp1 : 1 + p - 1 = p := by omega
            rw [hp1, Nat.div_self hp]
          rw [hcnt]
          simp
        simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
        rw [hlast]
        omega
      · have hminn : min p n = p := Nat.min_eq_left (by omega)
        have hmins : min p (n+1) = p := Nat.min_eq_left (by omega)
        rw [hminn, hmins]
        have hsum : ((List.range p).map (fun r =>
            ((List.range ((n+1-r+p-1)/p)).map (fun q => f (r+q*p))).sum)).sum =
            ((List.range p).map (fun r =>
              ((List.range ((n-r+p-1)/p)).map (fun q => f (r+q*p))).sum +
                if n % p == r then f n else 0)).sum := by
          apply sum_congr_int
          intro r hr
          have hrp : r < p := List.mem_range.mp hr
          rw [residue_count_succ n p r hp hrp]
          by_cases hnr : n % p == r
          · rw [if_pos hnr, range_succ_map_sum]
            have hcnt : (n - r + p - 1) / p = n / p := by
              rw [mod_step_count n p r hp hrp]
              have hlt : ¬ r < n % p := by
                have heq : n % p = r := Nat.eq_of_beq_eq_true hnr
                omega
              simp [hlt]
            rw [hcnt]
            have hval : r + (n / p) * p = n := by
              have hd := Nat.div_add_mod n p
              rw [Nat.mul_comm p (n / p)] at hd
              have heq : n % p = r := Nat.eq_of_beq_eq_true hnr
              omega
            rw [hval]
            simp [hnr]
          · simp [hnr]
        rw [hsum, residue_sum_map_add_int]
        have hif : ((List.range p).map (fun r => if n % p == r then f n else 0)).sum = f n := by
          rw [sum_ite_filter, range_filter_mod_singleton n p hp]
          simp
        rw [hif]



/-! ## LaneMajor specific lemmas and goals -/

private def lmValue (c : Config) (i : Nat) : Int :=
  c.base + c.step * (i / width c : Nat) + c.bias * (i % width c : Nat)

private theorem rows_lm (c : Config) :
    rows layout c = (List.range c.n).map (fun i : Nat => (i, lmValue c i)) := by
  unfold rows
  apply range_filterMap_all
  intro i hi
  simp [layout, lmValue]

private theorem lmValue_residue (c : Config) (r q : Nat) (hr : r < width c) :
    lmValue c (r + q * width c) = (c.base + c.bias * r) + (q : Int) * c.step := by
  have hp : 0 < width c := by dsimp [width]; omega
  have hmod : (r + q * width c) % width c = r := by
    rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hr]
  have hdiv : (r + q * width c) / width c = q := by
    rw [Nat.add_mul_div_right r q hp, Nat.div_eq_of_lt hr]
    simp
  dsimp [lmValue]
  have hcast : ((r + q * width c : Nat) : Int) = (r : Int) + (q : Int) * (width c : Int) := by simp
  rw [← hcast]
  have hdivI : ((r + q * width c : Nat) : Int) / (width c : Int) = (q : Int) := by
    exact_mod_cast hdiv
  have hmodI : ((r + q * width c : Nat) : Int) % (width c : Int) = (r : Int) := by
    exact_mod_cast hmod
  rw [hdivI, hmodI]
  grind

private theorem lm_lane_count (c : Config) (r : Nat) :
    (lane c.n r (width c) (c.base + c.bias * r) c.step).n =
      (c.n - r + width c - 1) / width c := by
  rfl

private theorem lm_lanes (c : Config) :
    layout.lanes c = (List.range (min (width c) c.n)).map (fun r : Nat =>
      lane c.n r (width c) (c.base + c.bias * r) c.step) := by
  rfl

private theorem sum_natCast_list {α : Type} (l : List α) (f : α → Nat) :
    ((l.map (fun x => (f x : Int))).sum) = (((l.map f).sum : Nat) : Int) := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.natCast_add]

private theorem residue_partition_sum_nat (n p : Nat) (hp : 0 < p) (f : Nat → Nat) :
    ((List.range n).map f).sum =
      ((List.range (min p n)).map (fun r : Nat =>
        ((List.range ((n - r + p - 1) / p)).map (fun q : Nat => f (r + q * p))).sum)).sum := by
  let g : Nat → Nat := fun r => ((List.range ((n - r + p - 1) / p)).map
    (fun q : Nat => f (r + q * p))).sum
  apply Int.ofNat_inj.mp
  rw [← sum_natCast_list (List.range n) f]
  rw [← residue_partition_sum n p hp (fun i => (f i : Int))]
  have hinner : ∀ r : Nat,
      ((List.range ((n - r + p - 1) / p)).map (fun q : Nat => (f (r + q * p) : Int))).sum =
        (g r : Int) := by
    intro r
    dsimp [g]
    exact sum_natCast_list (List.range ((n - r + p - 1) / p)) (fun q => f (r + q * p))
  rw [show (List.range (min p n)).map (fun r : Nat =>
        ((List.range ((n - r + p - 1) / p)).map (fun q : Nat => (f (r + q * p) : Int))).sum) =
      (List.range (min p n)).map (fun r : Nat => (g r : Int)) by
      apply List.map_congr_left
      intro r hr
      exact hinner r]
  exact sum_natCast_list (List.range (min p n)) g

theorem sound_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.LaneMajor.ResidueJoin.Goal := by
  intro c b
  let p := width c
  have hp : 0 < p := by dsimp [p, width]; omega
  rw [rows_lm]
  simp only [List.map_map, Function.comp_apply]
  change ((List.range c.n).map (fun i : Nat => hit b (lmValue c i))).sum =
    ((layout.lanes c).map (joinCount · b)).sum
  rw [residue_partition_sum_nat c.n p hp (fun i : Nat => hit b (lmValue c i))]
  rw [lm_lanes]
  simp only [List.map_map, Function.comp_apply]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  have hrp : r < width c := by
    have := List.mem_range.mp hr
    omega
  rw [show ((List.range ((c.n - r + width c - 1) / width c)).map (fun q : Nat =>
        hit b (lmValue c (r + q * width c)))).sum =
      ((List.range ((c.n - r + width c - 1) / width c)).map (fun q : Nat =>
        hit b ((c.base + c.bias * r) + (q : Int) * c.step))).sum by
    apply congrArg List.sum
    apply List.map_congr_left
    intro q hq
    rw [lmValue_residue c r q hrp]]
  change ((List.range ((c.n - r + width c - 1) / width c)).map (fun q : Nat =>
      hit b ((c.base + c.bias * r) + (q : Int) * c.step))).sum =
    joinCount (lane c.n r (width c) (c.base + c.bias * r) c.step) b
  exact joinCount_eq_raw (lane c.n r (width c) (c.base + c.bias * r) c.step) b


private theorem perm_valsList_inter (p q : Progression) (hp : 0 < p.step) :
    List.Perm (valsList (intersection p q)) ((valsList p).filter (contains q)) := by
  apply (List.perm_ext_iff_of_nodup
    (valsList_nodup_of_step_pos _ (inter_step_pos_of_left_pos p q hp))
    (nodup_filter (contains q) (valsList_nodup_of_step_pos p hp))).mpr
  intro v
  rw [mem_valsList, List.mem_filter, mem_valsList]
  exact intersection_contains p q v

private theorem sum_valsList_filter (p q : Progression) (hp : 0 < p.step) (F : Int → Int) :
    ((valsList (intersection p q)).map F).sum = (((valsList p).filter (contains q)).map F).sum := by
  exact list_sum_perm ((perm_valsList_inter p q hp).map F)

private theorem sum_affine (n : Nat) (x dx : Int) :
    ((List.range n).map (fun k : Nat => x + (k : Int) * dx)).sum =
      (n : Int) * x + tri n * dx := by
  induction n with
  | zero => simp [tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih, tri_succ]
      simp [Int.natCast_add]
      grind

private theorem pos_index (a : Lane) (ha : 0 < a.stride) {pos : Int}
    (hpos : contains (positions a) pos = true) :
    ∃ i : Nat, i < a.n ∧ pos = (a.start : Int) + (i : Int) * (a.stride : Int) ∧
      (pos - (a.start : Int)) / (a.stride : Int) = (i : Int) := by
  rcases (contains_iff_mem (positions a) pos).mp hpos with ⟨i, hi, hival⟩
  dsimp [progVal, positions] at hival
  refine ⟨i, hi, hival, ?_⟩
  rw [show pos - (a.start : Int) = (i : Int) * (a.stride : Int) by omega]
  exact Int.mul_ediv_cancel (i : Int) (by exact_mod_cast (Nat.ne_of_gt ha))

private theorem filter_map_eq_map_filter {α β : Type} (f : α → β) (l : List α) (p : β → Bool) :
    (l.map f).filter p = (l.filter (fun x => p (f x))).map f := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p (f x) <;> simp [h, ih]


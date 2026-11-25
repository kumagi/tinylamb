import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages_WorkC

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-! ## Basic list helpers -/

private theorem list_sum_append_int (l₁ l₂ : List Int) :
    (l₁ ++ l₂).sum = l₁.sum + l₂.sum := by
  induction l₁ with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_map_append {α β : Type} (l₁ l₂ : List α) (f : α → List β) (g : β → Int) :
    ((l₁ ++ l₂).flatMap f |>.map g).sum = ((l₁.flatMap f).map g).sum + ((l₂.flatMap f).map g).sum := by
  simp [List.flatMap_append, List.map_append, list_sum_append_int]

private theorem map_flatMap_sum {α β : Type} (l : List α) (f : α → List β) (g : β → Int) :
    ((l.flatMap f).map g).sum = (l.map (fun a => ((f a).map g).sum)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp [List.flatMap_cons, List.map_append, list_sum_append_int, ih]

private theorem map_flatMap_sum_nat {α β : Type} (l : List α) (f : α → List β) (g : β → Nat) :
    ((l.flatMap f).map g).sum = (l.map (fun a => ((f a).map g).sum)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp [List.flatMap_cons, List.map_append, List.sum_append, ih]

private theorem length_flatMap {α β : Type} (l : List α) (f : α → List β) :
    (l.flatMap f).length = (l.map (fun a => (f a).length)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [List.flatMap_cons, ih]

private theorem filter_flatMap {α β : Type} (l : List α) (f : α → List β) (p : β → Bool) :
    (l.flatMap f).filter p = l.flatMap (fun a => (f a).filter p) := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [List.flatMap_cons, List.filter_append, ih]

private theorem sum_map_congr {α : Type} (l : List α) (f g : α → Int)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem countP_eq_length_filter {α : Type} (p : α → Bool) (l : List α) :
    l.countP p = (l.filter p).length := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem sum_natCast_eq {α : Type} (l : List α) (g : α → Nat) :
    ((l.map (fun x => (g x : Int))).sum) = (((l.map g).sum : Nat) : Int) := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.natCast_add]

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

private theorem rows_range_map (lay : Layout) (c : Config) (f : Nat → Int)
    (h : ∀ i, i < c.n → lay.read c i = some (f i)) :
    rows lay c = (List.range c.n).map (fun i => (i, f i)) := by
  unfold rows
  rw [← filterMap_some_eq_map (List.range c.n) (fun i => (i, f i))]
  apply filterMap_congr'
  intro i hi
  have hi' : i < c.n := List.mem_range.mp hi
  simp [h i hi']

/-! ## Lane points -/

private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun i : Nat => (a.start + i * a.stride, a.base + (i : Int) * a.step))

private def allPoints (ls : List Lane) : List (Nat × Int) := ls.flatMap lanePoints

private theorem allPoints_cons (a : Lane) (ls : List Lane) :
    allPoints (a :: ls) = lanePoints a ++ allPoints ls := by
  simp [allPoints, List.flatMap_cons]

private theorem allPoints_map_sum (ls : List Lane) (f : Nat × Int → Int) :
    ((allPoints ls).map f).sum = (ls.map (fun a => ((lanePoints a).map f).sum)).sum := by
  unfold allPoints
  rw [map_flatMap_sum]

/-! ## Arithmetic helpers -/

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
    have hle : k ≤ k * k := by simpa using Nat.mul_le_mul_left k (show 1 ≤ k by omega)
    have hEq : (k + 1) * k = k * (k - 1) + 2 * k := by rw [hmul1, hmul2]; omega
    rw [hEq]
    exact Nat.add_mul_div_left (k * (k - 1)) k (by decide : 0 < 2)

private theorem tri_succ (k : Nat) : tri (k + 1) = tri k + (k : Int) := by
  dsimp [tri]
  rw [choose2_succ]
  simp [Int.natCast_add]

private theorem squares_succ (k : Nat) :
    squares (k + 1) = squares k + (k : Int) * (k : Int) := by
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
  | succ k ih => rw [partialSum_succ, ih, List.range_succ, List.map_append, List.sum_append]; simp


/-! ## Progression membership and intersection -/

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
  constructor
  · intro h
    have h' : (0 < n ∧ lo ≤ v) ∧ v ≤ lo := by
      simpa [contains, lastVal] using h
    exact ⟨h'.1.1, by omega⟩
  · rintro ⟨hn, rfl⟩
    simp [contains, lastVal, hn]

private theorem contains_bounds (a : Progression) (v : Int) (h : contains a v = true) :
    a.lo ≤ v ∧ v ≤ lastVal a := by
  have hm := (contains_iff_mem a v).mp h
  obtain ⟨k, hk, hval⟩ := hm
  unfold progVal at hval
  have hkint : (k : Int) ≤ ((a.n - 1 : Nat) : Int) := by
    exact_mod_cast (Nat.le_pred_of_lt hk)
  have hmul := Int.mul_le_mul_of_nonneg_left hkint (by omega : 0 ≤ (a.step : Int))
  have hmul' : (k : Int) * a.step ≤ ((a.n - 1 : Nat) : Int) * a.step := by
    simpa only [Int.mul_comm] using hmul
  unfold lastVal
  have hknon : (0 : Int) ≤ k := by omega
  have hstepnon : (0 : Int) ≤ (a.step : Int) := Int.natCast_nonneg _
  have hkmulnon : (0 : Int) ≤ (k : Int) * a.step := Int.mul_nonneg hknon hstepnon
  constructor
  · rw [hval]
    omega
  · rw [hval]
    exact Int.add_le_add_left hmul' a.lo

private theorem contains_dvd (a : Progression) (v : Int) (h : contains a v = true) :
    (a.step : Int) ∣ v - a.lo := by
  obtain ⟨k, _, hval⟩ := (contains_iff_mem a v).mp h
  unfold progVal at hval
  refine ⟨(k : Int), ?_⟩
  rw [hval]
  rw [Int.mul_comm (k : Int) (a.step : Int)]
  omega

private theorem common_gap_dvd (a b : Progression) (v : Int)
    (ha : contains a v = true) (hb : contains b v = true) :
    (a.step.gcd b.step : Int) ∣ b.lo - a.lo := by
  obtain ⟨i, _, hi⟩ := (contains_iff_mem a v).mp ha
  obtain ⟨j, _, hj⟩ := (contains_iff_mem b v).mp hb
  unfold progVal at hi hj
  have hgd : ((a.step.gcd b.step : Nat) : Int) ∣ (b.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_right a.step b.step)
  have hgl : ((a.step.gcd b.step : Nat) : Int) ∣ (a.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_left a.step b.step)
  obtain ⟨tg, htg⟩ := hgd
  obtain ⟨tl, htl⟩ := hgl
  have h1 : (a.step.gcd b.step : Int) ∣ (b.step : Int) * (j : Int) := by
    refine ⟨tg * (j : Int), ?_⟩
    rw [htg, Int.mul_assoc]
  have h2 : (a.step.gcd b.step : Int) ∣ (a.step : Int) * (i : Int) := by
    refine ⟨tl * (i : Int), ?_⟩
    rw [htl, Int.mul_assoc]
  have hi' : (i : Int) * a.step = v - a.lo := by omega
  have hj' : (j : Int) * b.step = v - b.lo := by omega
  have heq : (b.step : Int) * (j : Int) - (a.step : Int) * (i : Int) = a.lo - b.lo := by
    have h1' : (b.step : Int) * (j : Int) = (j : Int) * b.step := Int.mul_comm _ _
    have h2' : (a.step : Int) * (i : Int) = (i : Int) * a.step := Int.mul_comm _ _
    rw [h1', h2', hj', hi']
    omega
  have hdvd : (a.step.gcd b.step : Int) ∣ a.lo - b.lo := by
    rw [← heq]
    exact Int.dvd_sub h1 h2
  have hneg : (a.step.gcd b.step : Int) ∣ -(a.lo - b.lo) := Int.dvd_neg.mpr hdvd
  simpa [Int.neg_sub] using hneg

private theorem contains_unit_one (lo : Int) (v : Int) :
    contains ⟨lo, 1, 1⟩ v = true ↔ v = lo := by
  rw [contains_iff_mem]
  constructor
  · rintro ⟨k, hk, hv⟩
    have hk1 : k < 1 := by simpa using hk
    have hk0 : k = 0 := by omega
    subst k
    unfold progVal at hv
    simpa using hv
  · intro hv
    subst v
    refine ⟨0, ?_, ?_⟩
    · change 0 < (1 : Nat)
      decide
    · unfold progVal
      simp

private theorem contains_unit_zero (lo : Int) (v : Int) :
    contains ⟨lo, 1, 0⟩ v = true ↔ False := by
  rw [contains_iff_mem]
  constructor
  · rintro ⟨k, hk, _⟩
    have hk0 : k < 0 := by simpa using hk
    omega
  · intro h
    exact h.elim

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
  · cases a with
    | mk alo astep an =>
        simp at han
        subst an
        simp [intersection, contains]
  · by_cases hbn : b.n = 0
    · cases b with
      | mk blo bstep bn =>
          simp at hbn
          subst bn
          simp [intersection, contains]
    · have hanp : 0 < a.n := by omega
      have hbnp : 0 < b.n := by omega
      by_cases has : a.step = 0
      · cases a with
        | mk alo astep an =>
          simp at has han hanp
          subst astep
          have hInt : intersection ⟨alo, 0, an⟩ b =
              ⟨alo, 1, if contains b alo then 1 else 0⟩ := by
            simp [intersection, han, hbn]
          rw [hInt]
          by_cases hb : contains b alo = true
          · rw [if_pos hb]
            constructor
            · intro h
              have hvalo : v = alo := (contains_unit_one alo v).mp h
              subst v
              exact ⟨(contains_const_iff alo an alo).mpr ⟨hanp, rfl⟩, hb⟩
            · rintro ⟨ha, hbv⟩
              have hva := (contains_const_iff alo an v).mp ha
              have hvalo : v = alo := hva.2
              subst v
              exact (contains_unit_one alo alo).mpr rfl
          · rw [if_neg hb]
            constructor
            · intro h
              exact (contains_unit_zero alo v).mp h |>.elim
            · rintro ⟨ha, hbv⟩
              have hva := (contains_const_iff alo an v).mp ha
              have hvalo : v = alo := hva.2
              subst v
              exact (hb hbv).elim
      · by_cases hbs : b.step = 0
        · cases b with
          | mk blo bstep bn =>
            simp at hbs hbn hbnp
            subst bstep
            have hInt : intersection a ⟨blo, 0, bn⟩ =
                ⟨blo, 1, if contains a blo then 1 else 0⟩ := by
              simp [intersection, han, hbn, has]
            rw [hInt]
            by_cases ha' : contains a blo = true
            · rw [if_pos ha']
              constructor
              · intro h
                have hvlo : v = blo := (contains_unit_one blo v).mp h
                subst v
                exact ⟨ha', (contains_const_iff blo bn blo).mpr ⟨hbnp, rfl⟩⟩
              · rintro ⟨ha, hbv⟩
                have hvb := (contains_const_iff blo bn v).mp hbv
                have hvlo : v = blo := hvb.2
                subst v
                exact (contains_unit_one blo blo).mpr rfl
            · rw [if_neg ha']
              constructor
              · intro h
                exact (contains_unit_zero blo v).mp h |>.elim
              · rintro ⟨ha, hbv⟩
                have hvb := (contains_const_iff blo bn v).mp hbv
                have hvlo : v = blo := hvb.2
                subst v
                exact (ha' ha).elim
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
          have hgpos : 0 < a.step.gcd b.step := Nat.gcd_pos_of_pos_left b.step hasp
          have hApos : 0 < a.step / a.step.gcd b.step := Nat.div_gcd_pos_of_pos_left b.step hasp
          have hpNat : 0 < period := by
            dsimp [period]
            exact Nat.mul_pos hApos hbsp
          have hp : (0 : Int) < period := by exact_mod_cast hpNat
          have hgapdef : gap = b.lo - a.lo := rfl
          by_cases hgapmod : gap % (g : Int) = 0
          · have hgap : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgapmod
            have hroot := root_dvd a.step b.step hasp hbsp a.lo b.lo gap hgapdef hgap
            have hfirst : first = low + (root - low) % (period : Int) := rfl
            have hfroot := first_root_dvd low root first period hp hfirst
            have hpaEq : period = a.step * (b.step / g) := by
              dsimp [period, g]
              have hbdvd : a.step.gcd b.step ∣ b.step := Nat.gcd_dvd_right a.step b.step
              have hadvd : a.step.gcd b.step ∣ a.step := Nat.gcd_dvd_left a.step b.step
              calc (a.step / a.step.gcd b.step) * b.step
                  = (a.step / a.step.gcd b.step) * (a.step.gcd b.step * (b.step / a.step.gcd b.step)) := by
                      rw [Nat.mul_div_cancel' hbdvd]
                _ = (a.step / a.step.gcd b.step * a.step.gcd b.step) * (b.step / a.step.gcd b.step) := by
                      rw [Nat.mul_assoc]
                _ = a.step * (b.step / a.step.gcd b.step) := by
                      rw [Nat.div_mul_cancel hadvd]
            have hpbEq : period = b.step * (a.step / g) := by
              dsimp [period, g]
              rw [Nat.mul_comm]
            have hpa : (a.step : Int) ∣ (period : Int) := by
              refine ⟨((b.step / g : Nat) : Int), ?_⟩
              exact_mod_cast hpaEq
            have hpb : (b.step : Int) ∣ (period : Int) := by
              refine ⟨((a.step / g : Nat) : Int), ?_⟩
              exact_mod_cast hpbEq
            simp [intersection, han, hbn, has, hbs, hgapmod, Id.run, pure, g, gap]
            change contains ⟨first, period, if first ≤ high then ((high - first) / (↑period : Int)).toNat + 1 else 0⟩ v = true ↔
              contains a v = true ∧ contains b v = true
            have hsp := span_contains_iff first (↑period) high v hp
            simp only [Int.toNat_natCast] at hsp
            rw [hsp]
            constructor
            · rintro ⟨hfirstx, hxhigh, hxfirst⟩
              have hxroot : (period : Int) ∣ v - root := by
                have heq : v - root = (v - first) + (first - root) := by omega
                rw [heq]
                exact Int.dvd_add hxfirst hfroot
              have hxalo : (a.step : Int) ∣ v - a.lo := by
                have h1 : (a.step : Int) ∣ v - root := Int.dvd_trans hpa hxroot
                have h2 : (a.step : Int) ∣ (v - root) + (root - a.lo) := Int.dvd_add h1 hroot.1
                simpa [show (v - root) + (root - a.lo) = v - a.lo by omega] using h2
              have hxblo : (b.step : Int) ∣ v - b.lo := by
                have h1 : (b.step : Int) ∣ v - root := Int.dvd_trans hpb hxroot
                have h2 : (b.step : Int) ∣ (v - root) + (root - b.lo) := Int.dvd_add h1 hroot.2
                simpa [show (v - root) + (root - b.lo) = v - b.lo by omega] using h2
              have hxlow : low ≤ v := by
                have hflow : low ≤ first := by
                  dsimp [first]
                  have := Int.emod_nonneg (root - low) (show (↑period : Int) ≠ 0 by omega)
                  omega
                omega
              constructor
              · exact mem_of_bounds_dvd a v hanp hasp (by omega) (by omega) hxalo
              · exact mem_of_bounds_dvd b v hbnp hbsp (by omega) (by omega) hxblo
            · rintro ⟨ha, hb⟩
              have hab := contains_bounds a v ha
              have hbb := contains_bounds b v hb
              have hxalo0 := contains_dvd a v ha
              have hxblo0 := contains_dvd b v hb
              have hvar : (a.step : Int) ∣ v - root := by
                have heq : v - root = (v - a.lo) - (root - a.lo) := by omega
                rw [heq]
                exact Int.dvd_sub hxalo0 hroot.1
              have hvbr : (b.step : Int) ∣ v - root := by
                have heq : v - root = (v - b.lo) - (root - b.lo) := by omega
                rw [heq]
                exact Int.dvd_sub hxblo0 hroot.2
              have hperLcmNat : period = Nat.lcm a.step b.step := by
                rw [hpaEq]
                exact mul_div_gcd_eq_lcm a.step b.step hasp
              have hxroot : (period : Int) ∣ v - root := by
                have hl := lcm_dvd_of_dvd_of_dvd (a.step : Int) (b.step : Int) (v - root) hvar hvbr
                rw [hperLcmNat]
                simpa using hl
              have hxfirst : (period : Int) ∣ v - first := by
                have heq : v - first = (v - root) - (first - root) := by omega
                rw [heq]
                exact Int.dvd_sub hxroot hfroot
              have hxlow : low ≤ v := by
                dsimp [low]
                omega
              have hfirstle : first ≤ v :=
                first_le_of_congruence low root first v (period : Int) hp hfirst hxlow hxroot
              have hxhigh : v ≤ high := by
                dsimp [high]
                omega
              exact ⟨hfirstle, hxhigh, hxfirst⟩
          · have hgapne : gap % (g : Int) ≠ 0 := by simpa using hgapmod
            simp [intersection, han, hbn, has, hbs, hgapmod, Id.run, pure, g, gap]
            simp only [contains]
            constructor
            · intro h; simp at h
            · intro ⟨ha, hb⟩
              have hg := common_gap_dvd a b v ha hb
              exact (hgapne (Int.emod_eq_zero_of_dvd hg)).elim

/-! ## Per-lane generic identities -/

private theorem list_sum_congr {α : Type} (l : List α) (f g : α → Int)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem countP_range_eq_of_step0 (b : Lane) (hb : b.step = 0) (v : Int) :
    (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)) =
      if v == b.base then b.n else 0 := by
  simp only [hb, Int.mul_zero, Int.add_zero]
  by_cases hv : v == b.base
  · simp [hv, List.countP_true]
  · simp [hv]

-- Obstacle: needs careful case split on a.step < 0 vs >= 0 to map the
-- reversed `values` progression back to lane indices. True but proof-engineering heavy.
private theorem contains_values_iff (a : Lane) (v : Int) :
    contains (values a) v = true ↔ ∃ i < a.n, v = a.base + (i : Int) * a.step := by
  have hlo : (values a).lo =
      if a.step < 0 then a.base + ((a.n - 1 : Nat) : Int) * a.step else a.base := rfl
  have hst : ((values a).step : Int) = (a.step.natAbs : Int) := rfl
  have hn : (values a).n = a.n := rfl
  constructor
  · intro h
    obtain ⟨k, hk, hv⟩ := (contains_iff_mem (values a) v).mp h
    unfold progVal at hv
    have hkn : k < a.n := by simpa [hn] using hk
    by_cases hs : a.step < 0
    · have hlo' : (values a).lo = a.base + ((a.n - 1 : Nat) : Int) * a.step := if_pos hs
      have hst' : ((values a).step : Int) = -a.step := by
        show (a.step.natAbs : Int) = -a.step
        omega
      rw [hlo', hst'] at hv
      rw [Int.mul_neg] at hv
      refine ⟨a.n - 1 - k, by omega, ?_⟩
      have h1 : ((a.n - 1 - k : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (k : Int) := by omega
      rw [h1, Int.sub_mul]
      omega
    · have hlo' : (values a).lo = a.base := if_neg hs
      have hst' : ((values a).step : Int) = a.step := by
        show (a.step.natAbs : Int) = a.step
        omega
      rw [hlo', hst'] at hv
      exact ⟨k, hkn, hv⟩
  · rintro ⟨i, hi, rfl⟩
    apply (contains_iff_mem (values a) _).mpr
    refine ⟨if a.step < 0 then a.n - 1 - i else i, ?_, ?_⟩
    · by_cases hs : a.step < 0 <;> simp [hs, hn] <;> omega
    · unfold progVal
      by_cases hs : a.step < 0
      · have hlo' : (values a).lo = a.base + ((a.n - 1 : Nat) : Int) * a.step := if_pos hs
        have hst' : ((values a).step : Int) = -a.step := by
          show (a.step.natAbs : Int) = -a.step
          omega
        rw [hlo', hst']
        simp only [hs, if_true]
        rw [Int.mul_neg]
        have h1 : ((a.n - 1 - i : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (i : Int) := by omega
        rw [h1, Int.sub_mul]
        omega
      · have hlo' : (values a).lo = a.base := if_neg hs
        have hst' : ((values a).step : Int) = a.step := by
          show (a.step.natAbs : Int) = a.step
          omega
        rw [hlo', hst']
        simp only [hs, if_false]

private theorem contains_values_of_lanePoint (a : Lane) (i : Nat) (hi : i < a.n) :
    contains (values a) (a.base + (i : Int) * a.step) = true := by
  rw [contains_values_iff]
  exact ⟨i, hi, rfl⟩


-- BEGIN ep-helpers
private theorem ep_contains_step_one_ite (lo : Int) (c : Bool) (v : Int) :
    contains ⟨lo, 1, if c then 1 else 0⟩ v = true ↔ c = true ∧ v = lo := by
  cases c <;> simp [contains, lastVal] <;> omega

private theorem ep_intersection_step0_left (a b : Progression) (han : a.n ≠ 0) (hbn : b.n ≠ 0)
    (has : a.step = 0) :
    intersection a b = ⟨a.lo, 1, if contains b a.lo then 1 else 0⟩ := by
  unfold intersection
  simp [han, hbn, has]

private theorem ep_intersection_step0_right (a b : Progression) (han : a.n ≠ 0) (hbn : b.n ≠ 0)
    (has : a.step ≠ 0) (hbs : b.step = 0) :
    intersection a b = ⟨b.lo, 1, if contains a b.lo then 1 else 0⟩ := by
  unfold intersection
  simp [han, hbn, has, hbs]

private theorem ep_intersection_main_form (a b : Progression) (han : a.n ≠ 0) (hbn : b.n ≠ 0)
    (has : a.step ≠ 0) (hbs : b.step ≠ 0) :
    let g := a.step.gcd b.step
    let gap := b.lo - a.lo
    let period := (a.step / g) * b.step
    let unit := (bezout (a.step / g) (b.step / g)).1
    let root := a.lo + (a.step : Int) * ((gap / (g : Int) * unit) % (b.step / g : Nat))
    let low := max a.lo b.lo
    let first := low + (root - low) % (period : Int)
    let high := min (lastVal a) (lastVal b)
    intersection a b = if gap % (g : Int) != 0 then ⟨0, 1, 0⟩ else
      ⟨first, period, if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ := by
  dsimp only
  unfold intersection
  simp [han, hbn, has, hbs]
  by_cases hgap : (b.lo - a.lo) % (a.step.gcd b.step : Int) = 0 <;> simp [hgap]

private theorem ep_intersection_step_pos (a b : Progression) : 0 < (intersection a b).step := by
  by_cases han : a.n = 0
  · simp [intersection, han]
  · by_cases hbn : b.n = 0
    · simp [intersection, han, hbn]
    · by_cases has : a.step = 0
      · simp [intersection, han, hbn, has]
      · by_cases hbs : b.step = 0
        · simp [intersection, han, hbn, has, hbs]
        · have hg : 0 < a.step.gcd b.step := Nat.gcd_pos_of_pos_left b.step (Nat.pos_of_ne_zero has)
          have hdiv : 0 < a.step / a.step.gcd b.step :=
            Nat.div_pos (Nat.le_of_dvd (Nat.pos_of_ne_zero has) (Nat.gcd_dvd_left a.step b.step)) hg
          have hp : 0 < (a.step / a.step.gcd b.step) * b.step := Nat.mul_pos hdiv (Nat.pos_of_ne_zero hbs)
          rw [ep_intersection_main_form a b han hbn has hbs]
          by_cases hgap : (b.lo - a.lo) % (a.step.gcd b.step : Int) = 0 <;> simp [hgap, hp]

private theorem ep_nodup_map_of_injective {α β : Type} (l : List α) (f : α → β)
    (h : ∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) (hl : l.Nodup) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at hl
      rw [List.map_cons, List.nodup_cons]
      constructor
      · intro hm
        rcases List.mem_map.mp hm with ⟨b, hb, he⟩
        exact hl.1 (by rw [← h b (by simp [hb]) a (by simp) he]; exact hb)
      · exact ih (fun x hx y hy => h x (by simp [hx]) y (by simp [hy])) hl.2

private theorem ep_laneVal_inj (a : Lane) (ha : a.step ≠ 0) (i j : Nat)
    (hi : i < a.n) (hj : j < a.n) (h : a.base + (i : Int) * a.step = a.base + (j : Int) * a.step) :
    i = j := by
  have hm0 : (i : Int) * a.step = (j : Int) * a.step := by omega
  have hm : a.step * (i : Int) = a.step * (j : Int) := by simpa [Int.mul_comm] using hm0
  have hij : (i : Int) = (j : Int) := (Int.mul_eq_mul_left_iff (by simpa using ha)).mp hm
  exact_mod_cast hij

private theorem ep_count_values_mem (a : Lane) (q : Progression) :
    (List.range a.n).countP (fun i : Nat => contains q (a.base + (i : Int) * a.step)) =
      (intersection (values a) q).n * (if a.step = 0 then a.n else 1) := by
  by_cases ha0 : a.step = 0
  · rw [if_pos ha0]
    by_cases han : a.n = 0
    · simp [han, contains]
    · by_cases hqn : q.n = 0
      · have hleft : (List.range a.n).countP
            (fun i : Nat => contains q (a.base + (i : Int) * a.step)) = 0 := by
          apply List.countP_eq_zero.mpr
          intro i hi
          simp [contains, hqn]
        rw [hleft]
        simp [intersection, hqn, contains]
      · rw [ep_intersection_step0_left (values a) q
            (by simpa [values, ha0] using han) hqn (by simp [values, ha0])]
        have hvalo : (values a).lo = a.base := by simp [values, ha0]
        rw [hvalo]
        by_cases hc : contains q a.base
        · have hleft : (List.range a.n).countP
              (fun i : Nat => contains q (a.base + (i : Int) * a.step)) = a.n := by
            rw [show (List.range a.n).countP (fun i : Nat => contains q (a.base + (i : Int) * a.step)) =
                (List.range a.n).countP (fun _ : Nat => true) by
              apply List.countP_congr
              intro i hi
              simp [ha0, hc]]
            simp
          rw [hleft]
          simp [ep_contains_step_one_ite, hc, Nat.mul_comm]
        · have hleft : (List.range a.n).countP
              (fun i : Nat => contains q (a.base + (i : Int) * a.step)) = 0 := by
            apply List.countP_eq_zero.mpr
            intro i hi
            simp [ha0, hc]
          rw [hleft]
          simp [ep_contains_step_one_ite, hc]
  · rw [if_neg ha0, Nat.mul_one]
    let R := intersection (values a) q
    let L : List Int := ((List.range a.n).filter
      (fun i : Nat => contains q (a.base + (i : Int) * a.step))).map
        (fun i : Nat => a.base + (i : Int) * a.step)
    let L2 : List Int := (List.range R.n).map (progVal R)
    have hnodL : L.Nodup := by
      dsimp [L]
      apply ep_nodup_map_of_injective
      · intro i hi j hj h
        exact ep_laneVal_inj a ha0 i j
          (List.mem_range.mp (List.mem_filter.mp hi).1)
          (List.mem_range.mp (List.mem_filter.mp hj).1) h
      · exact List.Sublist.nodup (List.filter_sublist (p := fun i : Nat =>
          contains q (a.base + (i : Int) * a.step)) (l := List.range a.n)) List.nodup_range
    have hnodL2 : L2.Nodup := by
      dsimp [L2]
      apply ep_nodup_map_of_injective
      · intro i hi j hj h
        have hm0 : (i : Int) * (R.step : Int) = (j : Int) * (R.step : Int) := by
          unfold progVal at h
          omega
        have hm : R.step * (i : Int) = R.step * (j : Int) := by simpa [Int.mul_comm] using hm0
        have hRnz : (R.step : Int) ≠ 0 := by
          exact_mod_cast (Nat.ne_of_gt (ep_intersection_step_pos (values a) q))
        have hij : (i : Int) = (j : Int) := (Int.mul_eq_mul_left_iff hRnz).mp hm
        exact_mod_cast hij
      · exact List.nodup_range
    have hmem : ∀ x : Int, x ∈ L ↔ x ∈ L2 := by
      intro x
      constructor
      · intro hx
        rcases List.mem_map.mp hx with ⟨i, hi, rfl⟩
        have hxi : i < a.n := List.mem_range.mp (List.mem_filter.mp hi).1
        have hxv : contains q (a.base + (i : Int) * a.step) = true := (List.mem_filter.mp hi).2
        have hxa : contains (values a) (a.base + (i : Int) * a.step) = true :=
          contains_values_of_lanePoint a i hxi
        have hxR : contains R (a.base + (i : Int) * a.step) = true :=
          (intersection_contains (values a) q _).mpr ⟨hxa, hxv⟩
        obtain ⟨k, hk, hv⟩ := (contains_iff_mem R _).mp hxR
        apply List.mem_map.mpr
        refine ⟨k, List.mem_range.mpr hk, ?_⟩
        exact hv.symm
      · intro hx
        rcases List.mem_map.mp hx with ⟨k, hk, hv⟩
        have hR : contains R (progVal R k) = true := by
          apply (contains_iff_mem R _).mpr
          exact ⟨k, List.mem_range.mp hk, rfl⟩
        have hboth := (intersection_contains (values a) q _).mp hR
        obtain ⟨i, hi, hival⟩ := (contains_values_iff a _).mp hboth.1
        apply List.mem_map.mpr
        refine ⟨i, ?_, ?_⟩
        · apply List.mem_filter.mpr
          refine ⟨List.mem_range.mpr hi, ?_⟩
          rw [hival] at hboth
          exact hboth.2
        · rw [← hival]
          exact hv
    have hperm : L.Perm L2 := (List.perm_ext_iff_of_nodup hnodL hnodL2).mpr hmem
    rw [List.countP_eq_length_filter]
    dsimp [L] at hperm
    dsimp [L2] at hperm
    have hlen := hperm.length_eq
    rw [List.length_map, List.length_map, List.length_range] at hlen
    exact hlen

private theorem ep_sum_congr_nat {α : Type} (l : List α) (f g : α → Nat)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem ep_sum_eq_zero_nat {α : Type} (l : List α) (f : α → Nat)
    (h : ∀ x ∈ l, f x = 0) : (l.map f).sum = 0 := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem ep_ite_sum_eq_length_filter {α : Type} (l : List α) (p : α → Bool) :
    (l.map (fun x => if p x then 1 else 0)).sum = (l.filter p).length := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih] <;> omega

private theorem ep_countP_eq_ite_sum {α : Type} (l : List α) (p : α → Bool) :
    (l.map (fun x => if p x then 1 else 0)).sum = l.countP p := by
  rw [ep_ite_sum_eq_length_filter, countP_eq_length_filter]

private theorem ep_sum_range_eq_one : ∀ n : Nat, ∀ i < n,
    ((List.range n).map (fun k => if k = i then (1 : Nat) else 0)).sum = 1 := by
  intro n
  induction n with
  | zero => intro i hi; exact absurd hi (by simp)
  | succ q ih =>
      intro i hi
      rw [List.range_succ, List.map_append, List.sum_append]
      rcases Nat.lt_or_ge i q with hij | hij
      · have hz : (if q = i then (1 : Nat) else 0) = 0 := by
          by_cases heq : q = i
          · exact absurd heq (by omega)
          · rw [if_neg heq]
        have ht : (([q] : List Nat).map (fun m => if m = i then (1 : Nat) else 0)).sum = 0 := by
          simp [hz]
        rw [ih i hij, ht]
      · have hq : i = q := by omega
        rw [hq]
        have hz : ∀ m ∈ List.range q, (if m = q then (1 : Nat) else 0) = 0 := by
          intro m hm
          rw [if_neg (by
            have hm' : m < q := List.mem_range.mp hm
            omega)]
        rw [ep_sum_congr_nat (List.range q) (fun m => if m = q then 1 else 0) (fun _ => 0) hz,
          ep_sum_eq_zero_nat (List.range q) (fun _ => 0) (by simp)]
        simp

private theorem ep_sum_map_mul_nat {α : Type} (l : List α) (c : Nat) (f : α → Nat) :
    ((l.map (fun x => c * f x)).sum) = c * (l.map f).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, Nat.mul_add, ih]

private theorem ep_countP_range_unique (b : Lane) (hb : b.step ≠ 0) (v : Int) (i : Nat)
    (hi : i < b.n) (hv : v = b.base + (i : Int) * b.step) :
    (List.range b.n).countP (fun k : Nat => v == b.base + b.step * (k : Int)) = 1 := by
  rw [← ep_countP_eq_ite_sum]
  have hel : ∀ k ∈ List.range b.n,
      (if v == b.base + b.step * (k : Int) then (1 : Nat) else 0) = (if k = i then 1 else 0) := by
    intro k hk
    have hk' : k < b.n := List.mem_range.mp hk
    by_cases hkeq : k = i
    · subst k
      simp [hv, Int.mul_comm]
    · rw [if_neg hkeq]
      rw [if_neg (by
        intro hbe
        have h2 := eq_of_beq hbe
        rw [Int.mul_comm] at h2
        rw [hv] at h2
        exact hkeq (ep_laneVal_inj b hb i k hi hk' h2).symm)]
  rw [ep_sum_congr_nat (List.range b.n)
    (fun k => if v == b.base + b.step * (k : Int) then 1 else 0) (fun k => if k = i then 1 else 0) hel]
  exact ep_sum_range_eq_one b.n i hi

private theorem ep_countP_range_none (b : Lane) (v : Int)
    (h : ∀ i < b.n, ¬(v = b.base + (i : Int) * b.step)) :
    (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)) = 0 := by
  rw [← ep_countP_eq_ite_sum]
  have hel : ∀ k ∈ List.range b.n,
      (if v == b.base + b.step * (k : Int) then (1 : Nat) else 0) = 0 := by
    intro k hk
    rw [if_neg (by
      intro hbe
      have h2 := eq_of_beq hbe
      rw [Int.mul_comm] at h2
      exact h k (List.mem_range.mp hk) h2)]
  rw [ep_sum_congr_nat (List.range b.n)
    (fun k => if v == b.base + b.step * (k : Int) then 1 else 0) (fun _ => 0) hel]
  exact ep_sum_eq_zero_nat (List.range b.n) (fun _ => 0) (by simp)
-- END ep-helpers

-- Obstacle: injectivity of i ↦ b.base+i*b.step for b.step!=0 plus
-- contains_values_iff; needs list counting/uniqueness plumbing.
private theorem countP_range_eq_of_step_ne (b : Lane) (hb : b.step ≠ 0) (v : Int) :
    (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)) =
      if contains (values b) v then 1 else 0 := by
  by_cases hcv : contains (values b) v = true
  · rw [if_pos hcv]
    obtain ⟨i, hi, hv⟩ := (contains_values_iff b v).mp hcv
    exact ep_countP_range_unique b hb v i hi hv
  · rw [if_neg hcv]
    refine ep_countP_range_none b v (fun i hi h2 => hcv ((contains_values_iff b v).mpr ⟨i, hi, h2⟩))

-- Obstacle: reduce per-point count to contains(values b) with multiplicity
-- (b.step==0 => b.n else 1, a.step==0 => a.n else 1) and sum over intersection.
-- Depends on contains_values_iff + intersection_contains.
private theorem joinCount_lane (a b : Lane) :
    ((lanePoints a).map (fun r => (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum =
      joinCount a b := by
  unfold lanePoints
  rw [List.map_map]
  show ((List.range a.n).map (fun i : Nat => (List.range b.n).countP
      (fun j : Nat => (a.base + (i : Int) * a.step) == b.base + b.step * (j : Int)))).sum = joinCount a b
  by_cases hbn : b.n = 0
  · rw [hbn, List.range_zero]
    have hsum : ((List.range a.n).map (fun i : Nat => List.countP
        (fun j : Nat => (a.base + (i : Int) * a.step) == b.base + b.step * (j : Int)) [])).sum = 0 := by
      apply ep_sum_eq_zero_nat
      intro i hi
      rfl
    rw [hsum]
    unfold joinCount
    have hn : (intersection (values a) (values b)).n = (0 : Nat) := by
      unfold intersection
      rw [show ((values b).n) = (0 : Nat) from by simp [values, hbn]]
      simp
    rw [hn, Nat.zero_mul]
    simp
  · by_cases hb0 : b.step = 0
    · have hper : ∀ i : Nat, (List.range b.n).countP
          (fun j : Nat => (a.base + (i : Int) * a.step) == b.base + b.step * (j : Int)) =
            b.n * (if (a.base + (i : Int) * a.step) == b.base then 1 else 0) := by
        intro i
        rw [countP_range_eq_of_step0 b hb0]
        by_cases he : a.base + (i : Int) * a.step = b.base
        · simp [he]
        · simp [he]
      rw [ep_sum_congr_nat (List.range a.n)
        (fun i => (List.range b.n).countP (fun j : Nat => (a.base + (i : Int) * a.step) == b.base + b.step * (j : Int)))
        (fun i => b.n * (if (a.base + (i : Int) * a.step) == b.base then 1 else 0)) (fun i _ => hper i)]
      rw [ep_sum_map_mul_nat, ep_countP_eq_ite_sum]
      have hc : ∀ i ∈ List.range a.n,
          ((a.base + (i : Int) * a.step) == b.base) = contains (values b) (a.base + (i : Int) * a.step) := by
        intro i hi
        have hin : i < a.n := List.mem_range.mp hi
        have hbn' : 0 < b.n := Nat.pos_of_ne_zero hbn
        have hx : ((a.base + (i : Int) * a.step) == b.base) =
            if a.base + (i : Int) * a.step = b.base then true else false := by
          by_cases he : a.base + (i : Int) * a.step = b.base <;> simp [he]
        have hy : contains (values b) (a.base + (i : Int) * a.step) =
            if a.base + (i : Int) * a.step = b.base then true else false := by
          by_cases he : a.base + (i : Int) * a.step = b.base
          · rw [if_pos he]
            refine (contains_values_iff b _).mpr ⟨0, by omega, ?_⟩
            rw [hb0, Int.mul_zero]
            omega
          · rw [if_neg he]
            cases hT : contains (values b) (a.base + (i : Int) * a.step)
            · rfl
            · obtain ⟨j, _, hj⟩ := (contains_values_iff b _).mp hT
              rw [hb0, Int.mul_zero, Int.add_zero] at hj
              exact (he hj).elim
        rw [hx, hy]
      rw [List.countP_congr (fun i hi => Iff.intro
            (fun h => (hc i hi).symm.trans h) (fun h => (hc i hi).trans h)),
        ep_count_values_mem a (values b)]
      unfold joinCount
      rw [show (if b.step == (0 : Int) then b.n else 1) = b.n from if_pos (by simpa using hb0)]
      by_cases has : a.step = 0
      · rw [if_pos has,
          show (if a.step == (0 : Int) then a.n else 1) = a.n from if_pos (by simpa using has)]
        exact Nat.mul_comm _ _
      · rw [if_neg has,
          show (if a.step == (0 : Int) then a.n else 1) = (1 : Nat) from
            if_neg (by intro h; exact has (by simpa using h)), Nat.mul_one]
        exact Nat.mul_comm _ _
    · have hper : ∀ i : Nat, (List.range b.n).countP
          (fun j : Nat => (a.base + (i : Int) * a.step) == b.base + b.step * (j : Int)) =
            if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0 := by
        intro i
        exact countP_range_eq_of_step_ne b hb0 _
      rw [ep_sum_congr_nat (List.range a.n)
        (fun i => (List.range b.n).countP (fun j : Nat => (a.base + (i : Int) * a.step) == b.base + b.step * (j : Int)))
        (fun i => if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0) (fun i _ => hper i)]
      rw [ep_countP_eq_ite_sum, ep_count_values_mem a (values b)]
      unfold joinCount
      rw [show (if b.step == (0 : Int) then b.n else 1) = (1 : Nat) from
        if_neg (by intro h; exact hb0 (by simpa using h))]
      rw [Nat.mul_one]
      by_cases has : a.step = 0 <;> simp [has]

/-! ## Deleted sums over a non-degenerate lane -/

-- The identity needs 0 < a.stride: otherwise a.stride = 0 duplicates a single
-- position while intersection (positions a) mask collapses to one point, so
-- the closed form for the deleted positions no longer matches the filtered
-- lanePoints sum (counterexample: a = ⟨0, 0, 2, 0, 1⟩, mask = ⟨0, 1, 1⟩).

private theorem sum_filter_complement {α : Type} (l : List α) (p : α → Bool) (f : α → Int) :
    ((l.filter p).map f).sum + ((l.filter (fun x => !p x)).map f).sum = (l.map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true
      · simp [h, ih]
        omega
      · simp [h, ih]
        omega

private theorem list_sum_perm {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private def progVals (p : Progression) : List Int :=
  (List.range p.n).map (progVal p)

private theorem progMem_iff_mem_progVals (p : Progression) (v : Int) :
    progMem p v ↔ v ∈ progVals p := by
  simp [progMem, progVals, progVal, eq_comm]

private theorem contains_mem (p : Progression) (v : Int) :
    contains p v = true ↔ v ∈ progVals p :=
  (contains_iff_mem p v).trans (progMem_iff_mem_progVals p v)

private theorem progVals_nodup_of_pos (p : Progression) (hs : 0 < p.step) :
    (progVals p).Nodup := by
  unfold progVals
  apply ep_nodup_map_of_injective
  · intro a ha b hb hEq
    unfold progVal at hEq
    have hm' : (a : Int) * (p.step : Int) = (b : Int) * (p.step : Int) := by omega
    have hm : (p.step : Int) * (a : Int) = (p.step : Int) * (b : Int) := by
      simpa [Int.mul_comm] using hm'
    have hab : (a : Int) = (b : Int) :=
      (Int.mul_eq_mul_left_iff (by exact_mod_cast (Nat.ne_of_gt hs) : (p.step : Int) ≠ 0)).mp hm
    exact_mod_cast hab
  · exact List.nodup_range

private def lanePosVals (a : Lane) : List Int :=
  (List.range a.n).map (fun j : Nat => ((a.start + j * a.stride : Nat) : Int))

private def posF (a : Lane) (v : Int) : Int :=
  a.base + (((v - (a.start : Int)) / (a.stride : Int)).toNat : Int) * a.step

private theorem posF_at (a : Lane) (ha : 0 < a.stride) (j : Nat) :
    posF a ((a.start + j * a.stride : Nat) : Int) = a.base + (j : Int) * a.step := by
  unfold posF
  have hsub : ((a.start + j * a.stride : Nat) : Int) - (a.start : Int) =
      (j : Int) * (a.stride : Int) := by
    push_cast
    omega
  rw [hsub]
  rw [Int.mul_ediv_cancel (j : Int) (by exact_mod_cast (Nat.ne_of_gt ha))]
  rfl

private theorem inter_positions_perm (a : Lane) (ha : 0 < a.stride) (m : Progression) :
    (progVals (intersection (positions a) m)).Perm
      ((lanePosVals a).filter (fun v => contains m v)) := by
  have hleft : (progVals (intersection (positions a) m)).Nodup :=
    progVals_nodup_of_pos _ (ep_intersection_step_pos (positions a) m)
  have hright : ((lanePosVals a).filter (fun v => contains m v)).Nodup := by
    unfold lanePosVals
    exact (ep_nodup_map_of_injective _ _ (by
      intro i _ j _ hEq
      have hEq' : a.start + i * a.stride = a.start + j * a.stride := by exact_mod_cast hEq
      have hij : i * a.stride = j * a.stride := Nat.add_left_cancel hEq'
      exact Nat.eq_of_mul_eq_mul_left ha (by simpa [Nat.mul_comm] using hij)) List.nodup_range).filter _
  apply (List.perm_ext_iff_of_nodup hleft hright).mpr
  intro v
  unfold lanePosVals
  simp only [List.mem_filter]
  constructor
  · intro hz
    have hq := (contains_mem (intersection (positions a) m) v).mpr hz
    have hb := (intersection_contains (positions a) m v).mp hq
    rcases List.mem_map.mp ((contains_mem (positions a) v).mp hb.1) with ⟨j, hj, hjv⟩
    have hj' : j ∈ List.range a.n := by simpa [positions] using hj
    have hjv' : ((a.start + j * a.stride : Nat) : Int) = v := by
      simpa [progVal, positions] using hjv
    exact ⟨List.mem_map.mpr ⟨j, hj', hjv'⟩, hb.2⟩
  · rintro ⟨hz, hm⟩
    rcases List.mem_map.mp hz with ⟨j, hj, hjv⟩
    have hj' : j ∈ List.range (positions a).n := by simpa [positions] using hj
    have hjv' : progVal (positions a) j = v := by
      simpa [progVal, positions] using hjv
    apply (contains_mem (intersection (positions a) m) v).mp
    apply (intersection_contains (positions a) m v).mpr
    refine ⟨(contains_mem (positions a) v).mpr ?_, hm⟩
    exact List.mem_map.mpr ⟨j, hj', hjv'⟩

private theorem posF_inter_of (a : Lane) (ha : 0 < a.stride) (q : Progression)
    (hqa : ∀ v, contains q v = true → contains (positions a) v = true)
    (v : Int) (hv : v ∈ progVals q) :
    ∃ k, k < q.n ∧
      v = q.lo + (k : Int) * (q.step : Int) ∧
      posF a v = firstAt a q + (k : Int) * slopeAt a q := by
  have hqmem : v ∈ progVals q := hv
  rcases List.mem_map.mp hqmem with ⟨k, hk, hkv⟩
  have hklt : k < q.n := List.mem_range.mp hk
  have hkval : v = q.lo + (k : Int) * (q.step : Int) := by
    simpa [progVal] using hkv.symm
  have hqlo : q.lo ∈ progVals q := by
    exact List.mem_map.mpr ⟨0, List.mem_range.mpr (by omega), by simp [progVal]⟩
  have hqloPos : contains (positions a) q.lo = true :=
    hqa q.lo ((contains_mem q q.lo).mpr hqlo)
  rcases List.mem_map.mp ((contains_mem (positions a) q.lo).mp hqloPos) with ⟨j0, hj0, hj0v⟩
  have hj0val : q.lo = (a.start : Int) + (j0 : Int) * (a.stride : Int) := by
    have htmp : q.lo = (positions a).lo + (j0 : Int) * (positions a).step := by
      simpa [progVal] using hj0v.symm
    change q.lo = (a.start : Int) + (j0 : Int) * (a.stride : Int) at htmp
    exact htmp
  have hsub0 : q.lo - (a.start : Int) = (j0 : Int) * (a.stride : Int) := by omega
  have hdiv0 : (((q.lo - (a.start : Int)) / (a.stride : Int)).toNat) = j0 := by
    rw [hsub0, Int.mul_ediv_cancel (j0 : Int) (by exact_mod_cast (Nat.ne_of_gt ha))]
    simp
  by_cases hk0 : k = 0
  · subst k
    refine ⟨0, hklt, by simpa using hkval, ?_⟩
    have hnon : 0 ≤ (q.lo - (a.start : Int)) / (a.stride : Int) := by
      apply Int.ediv_nonneg
      · rw [hsub0]
        exact Int.mul_nonneg (Int.natCast_nonneg _) (Int.natCast_nonneg _)
      · exact_mod_cast (Nat.zero_le a.stride)
    have hdiv0' : (q.lo - (a.start : Int)) / (a.stride : Int) = (j0 : Int) := by
      rw [← Int.toNat_of_nonneg hnon, hdiv0]
    have hkval0 : v = q.lo := by simpa using hkval
    unfold posF firstAt slopeAt
    rw [hkval0]
    rw [Int.toNat_of_nonneg hnon, hdiv0']
    simp
  · have hkpos : 0 < k := by omega
    have hq1mem : q.lo + (q.step : Int) ∈ progVals q := by
      exact List.mem_map.mpr ⟨1, List.mem_range.mpr (by omega), by simp [progVal]⟩
    have hq1Pos : contains (positions a) (q.lo + (q.step : Int)) = true :=
      hqa (q.lo + (q.step : Int)) ((contains_mem q (q.lo + (q.step : Int))).mpr hq1mem)
    rcases List.mem_map.mp ((contains_mem (positions a) (q.lo + (q.step : Int))).mp hq1Pos) with
      ⟨j1, hj1, hj1v⟩
    have hj1val : q.lo + (q.step : Int) = (a.start : Int) + (j1 : Int) * (a.stride : Int) := by
      have htmp : q.lo + (q.step : Int) = (positions a).lo + (j1 : Int) * (positions a).step := by
        simpa [progVal] using hj1v.symm
      change q.lo + (q.step : Int) = (a.start : Int) + (j1 : Int) * (a.stride : Int) at htmp
      exact htmp
    have hqstep_nonneg : 0 ≤ (q.step : Int) := by exact_mod_cast (Nat.zero_le q.step)
    have hj0le1 : j0 ≤ j1 := by
      by_cases h : j0 ≤ j1
      · exact h
      · have hjlt : j1 < j0 := by omega
        have hmul : j1 * a.stride < j0 * a.stride := Nat.mul_lt_mul_of_pos_right hjlt ha
        omega
    have hstepEq : (q.step : Int) = ((j1 - j0 : Nat) : Int) * (a.stride : Int) := by
      have hsubcast : ((j1 - j0 : Nat) : Int) = (j1 : Int) - (j0 : Int) := by
        exact Int.ofNat_sub hj0le1
      rw [hsubcast, Int.sub_mul]
      grind
    have hstepNatEq : q.step = (j1 - j0) * a.stride := by exact_mod_cast hstepEq
    have hstepNat : q.step / a.stride = j1 - j0 := by
      rw [hstepNatEq]
      exact Nat.mul_div_cancel _ ha
    have hsubv : v - (a.start : Int) = ((j0 + k * (j1 - j0) : Nat) : Int) * (a.stride : Int) := by
      rw [hkval, hstepEq]
      grind
    have hdivv : (((v - (a.start : Int)) / (a.stride : Int)).toNat) = j0 + k * (j1 - j0) := by
      rw [hsubv, Int.mul_ediv_cancel ((j0 + k * (j1 - j0) : Nat) : Int)
        (by exact_mod_cast (Nat.ne_of_gt ha))]
      rw [Int.toNat_natCast]
    refine ⟨k, hklt, hkval, ?_⟩
    change posF a v = firstAt a q + (k : Int) * slopeAt a q
    have hnon0 : 0 ≤ (q.lo - (a.start : Int)) / (a.stride : Int) := by
      apply Int.ediv_nonneg
      · rw [hsub0]
        exact Int.mul_nonneg (Int.natCast_nonneg _) (Int.natCast_nonneg _)
      · exact_mod_cast (Nat.zero_le a.stride)
    have hnonv : 0 ≤ (v - (a.start : Int)) / (a.stride : Int) := by
      apply Int.ediv_nonneg
      · rw [hsubv]
        exact Int.mul_nonneg (Int.natCast_nonneg _) (Int.natCast_nonneg _)
      · exact_mod_cast (Nat.zero_le a.stride)
    have hdiv0' : (q.lo - (a.start : Int)) / (a.stride : Int) = (j0 : Int) := by
      rw [← Int.toNat_of_nonneg hnon0, hdiv0]
    have hdivv' : (v - (a.start : Int)) / (a.stride : Int) =
        ((j0 + k * (j1 - j0) : Nat) : Int) := by
      rw [← Int.toNat_of_nonneg hnonv, hdivv]
    unfold posF firstAt slopeAt
    change a.base + (((v - (a.start : Int)) / (a.stride : Int)).toNat : Int) * a.step =
      a.base + (q.lo - (a.start : Int)) / (a.stride : Int) * a.step +
        (k : Int) * (((q.step / a.stride : Nat) : Int) * a.step)
    rw [Int.toNat_of_nonneg hnonv, hdivv', hdiv0', hstepNat]
    grind

private theorem posF_inter (a : Lane) (ha : 0 < a.stride) (m : Progression)
    (v : Int) (hv : v ∈ progVals (intersection (positions a) m)) :
    ∃ k, k < (intersection (positions a) m).n ∧
      v = (intersection (positions a) m).lo + (k : Int) * ((intersection (positions a) m).step : Int) ∧
      posF a v = firstAt a (intersection (positions a) m) +
        (k : Int) * slopeAt a (intersection (positions a) m) := by
  exact posF_inter_of a ha (intersection (positions a) m)
    (fun v h => ((intersection_contains (positions a) m v).mp h).1) v hv

private theorem progVal_inj_of_pos (p : Progression) (hs : 0 < p.step)
    (i j : Nat) (hi : i < p.n) (hj : j < p.n)
    (hEq : progVal p i = progVal p j) : i = j := by
  unfold progVal at hEq
  have hm' : (i : Int) * (p.step : Int) = (j : Int) * (p.step : Int) := by omega
  have hm : (p.step : Int) * (i : Int) = (p.step : Int) * (j : Int) := by
    rw [Int.mul_comm (p.step : Int) (i : Int), Int.mul_comm (p.step : Int) (j : Int)]
    exact hm'
  have hij : (i : Int) = (j : Int) :=
    (Int.mul_eq_mul_left_iff (by exact_mod_cast (Nat.ne_of_gt hs) : (p.step : Int) ≠ 0)).mp hm
  exact_mod_cast hij

private theorem sum_posF_inter (a : Lane) (ha : 0 < a.stride) (m : Progression) :
    ((progVals (intersection (positions a) m)).map (posF a)).sum = deletedSum a m := by
  unfold deletedSum
  let q := intersection (positions a) m
  have hqstep : 0 < q.step := ep_intersection_step_pos (positions a) m
  change ((progVals q).map (posF a)).sum = (q.n : Int) * firstAt a q + tri q.n * slopeAt a q
  have hval : ∀ k, k < q.n →
      posF a (progVal q k) = firstAt a q + (k : Int) * slopeAt a q := by
    intro k hk
    obtain ⟨k', hk', hvk', heq⟩ := posF_inter a ha m (progVal q k)
      (List.mem_map.mpr ⟨k, List.mem_range.mpr hk, rfl⟩)
    have hvk'' : progVal q k = q.lo + (k' : Int) * (q.step : Int) := by
      change progVal q k = (intersection (positions a) m).lo + (k' : Int) * ((intersection (positions a) m).step : Int) at hvk'
      exact hvk'
    have heq' : posF a (progVal q k) = firstAt a q + (k' : Int) * slopeAt a q := by
      change posF a (progVal q k) = firstAt a q + (k' : Int) * slopeAt a q at heq
      exact heq
    have hEqv : progVal q k' = progVal q k := by
      change q.lo + (k' : Int) * (q.step : Int) = progVal q k
      rw [hvk'']
    have hk'eq : k' = k := progVal_inj_of_pos q hqstep k' k hk' hk hEqv
    rw [hk'eq] at heq'
    exact heq'
  let L : Lane := ⟨0, 1, q.n, firstAt a q, slopeAt a q⟩
  calc
    ((progVals q).map (posF a)).sum =
        ((List.range q.n).map (fun k : Nat => firstAt a q + (k : Int) * slopeAt a q)).sum := by
      rw [show progVals q = (List.range q.n).map (fun k : Nat => progVal q k) by rfl, List.map_map]
      change ((List.range q.n).map (fun k : Nat => posF a (progVal q k))).sum =
        ((List.range q.n).map (fun k : Nat => firstAt a q + (k : Int) * slopeAt a q)).sum
      apply congrArg List.sum
      apply List.map_congr_left
      intro k hk
      exact hval k (List.mem_range.mp hk)
    _ = partialSum L q.n := (partialSum_eq_sum L q.n).symm
    _ = (q.n : Int) * firstAt a q + tri q.n * slopeAt a q := rfl

private theorem filter_map_eq {α β : Type} (l : List α) (f : α → β) (p : β → Bool) :
    (l.map f).filter p = (l.filter (fun x => p (f x))).map f := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p (f x) <;> simp [h, ih]

private theorem filter_lanePosVals (a : Lane) (m : Progression) :
    (lanePosVals a).filter (fun v => contains m v) =
      ((List.range a.n).filter (fun j : Nat => contains m ((a.start + j * a.stride : Nat) : Int))).map
        (fun j : Nat => ((a.start + j * a.stride : Nat) : Int)) := by
  unfold lanePosVals
  rw [filter_map_eq]

private theorem lanePoints_filter_snd (a : Lane) (m : Progression) :
    ((lanePoints a).filter (fun r => !contains m r.1)).map Prod.snd =
      ((List.range a.n).filter (fun j : Nat => !contains m ((a.start + j * a.stride : Nat) : Int))).map
        (fun j : Nat => a.base + (j : Int) * a.step) := by
  unfold lanePoints
  rw [filter_map_eq]
  simp

private theorem deletedSum_lane (a : Lane) (ha : 0 < a.stride) (mask : Progression) :
    (((lanePoints a).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
      partialSum a a.n - deletedSum a mask := by
  rw [lanePoints_filter_snd]
  let p : Nat → Bool := fun j : Nat => contains mask ((a.start + j * a.stride : Nat) : Int)
  let val : Nat → Int := fun j => a.base + (j : Int) * a.step
  have hcomp := sum_filter_complement (List.range a.n) p val
  have hdel : (((List.range a.n).filter p).map val).sum = deletedSum a mask := by
    rw [show ((List.range a.n).filter p).map val =
        (((lanePosVals a).filter (fun v => contains mask v)).map (posF a)) by
      rw [filter_lanePosVals, List.map_map]
      apply List.map_congr_left
      intro j hj
      exact (posF_at a ha j).symm]
    exact (list_sum_perm ((inter_positions_perm a ha mask).map (posF a))).symm.trans (sum_posF_inter a ha mask)
  have htot : ((List.range a.n).map val).sum = partialSum a a.n := (partialSum_eq_sum a a.n).symm
  have hcomp' : (((List.range a.n).filter p).map val).sum +
      (((List.range a.n).filter (fun j : Nat => !p j)).map val).sum =
      ((List.range a.n).map val).sum := by simpa using hcomp
  have hgoal : (((List.range a.n).filter (fun j : Nat => !p j)).map val).sum =
      partialSum a a.n - deletedSum a mask := by omega
  simpa [p, val] using hgoal


private theorem intersection_step_pos (p q : Progression) (hp : 0 < p.step) :
    0 < (intersection p q).step := ep_intersection_step_pos p q
private def bvalAt (b : Lane) (v : Int) : Int :=
  if contains (positions b) v then
    b.base + b.step * (((v - (b.start : Int)) / (b.stride : Int)).toNat : Int)
  else 0

private theorem sum_map_zero_int {α : Type} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]
private theorem sum_eq_single_int (p r : Nat) (hr : r < p) (f : Nat → Int) :
    ((List.range p).map (fun x => if x == r then f x else 0)).sum = f r := by
  induction p with
  | zero => omega
  | succ p ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      by_cases heq : p = r
      · subst r
        have hpre : ((List.range p).map (fun x => if x == p then f x else 0)) =
            (List.range p).map (fun _ => (0 : Int)) := by
          apply List.map_congr_left
          intro x hx
          have hxp : x < p := List.mem_range.mp hx
          rw [beq_eq_false_iff_ne.mpr (by omega)]
          simp
        rw [hpre, sum_map_zero_int]
        simp
      · rw [if_neg (by simp [heq])]
        rw [ih (by omega)]
        omega
private theorem inner_aligned (a b : Lane) (hb : 0 < b.stride) (j : Nat) :
    ((List.range b.n).map (fun i : Nat =>
      if a.start + j * a.stride == b.start + i * b.stride then
        (a.base + (j : Int) * a.step) * (b.base + b.step * (i : Int))
      else 0)).sum =
    (a.base + (j : Int) * a.step) * bvalAt b ((a.start + j * a.stride : Nat) : Int) := by
  let pos : Int := ((a.start + j * a.stride : Nat) : Int)
  let aval : Int := a.base + (j : Int) * a.step
  by_cases hc : contains (positions b) pos = true
  · rcases List.mem_map.mp ((contains_mem (positions b) pos).mp hc) with ⟨i, hi, hival⟩
    have hival' : pos = (b.start : Int) + (i : Int) * (b.stride : Int) := by
      simpa [progVal, positions] using hival.symm
    have huniq : ∀ k < b.n, pos == b.start + k * b.stride → k = i := by
      intro k hk heqk
      have hEq : (b.start : Int) + (k : Int) * (b.stride : Int) =
          (b.start : Int) + (i : Int) * (b.stride : Int) := by
        have hcast : ((b.start + k * b.stride : Nat) : Int) = (b.start : Int) + (k : Int) * (b.stride : Int) := by
          push_cast
          rfl
        rw [← hcast]
        exact (beq_iff_eq.mp heqk).symm.trans hival'
      have hmul : k * b.stride = i * b.stride := by exact_mod_cast (by omega : (k : Int) * (b.stride : Int) = (i : Int) * (b.stride : Int))
      exact Nat.eq_of_mul_eq_mul_left hb (by simpa [Nat.mul_comm] using hmul)
    have hsum : ((List.range b.n).map (fun k : Nat =>
        if a.start + j * a.stride == b.start + k * b.stride then
          aval * (b.base + b.step * (k : Int))
        else 0)).sum = aval * (b.base + b.step * (i : Int)) := by
      rw [show (List.range b.n).map (fun k : Nat =>
          if a.start + j * a.stride == b.start + k * b.stride then
            aval * (b.base + b.step * (k : Int))
          else 0) =
          (List.range b.n).map (fun k : Nat => if k == i then
            aval * (b.base + b.step * (i : Int)) else 0) by
        apply List.map_congr_left
        intro k hk
        by_cases hkEq : k == i
        · have hki : k = i := beq_iff_eq.mp hkEq
          subst k
          have hnat : a.start + j * a.stride = b.start + i * b.stride := by omega
          rw [if_pos (beq_iff_eq.mpr hnat)]
          simp
        · have hne : a.start + j * a.stride ≠ b.start + k * b.stride := by
            intro hEq
            have hbeq : pos == b.start + k * b.stride := by
              apply beq_iff_eq.mpr
              change ((a.start + j * a.stride : Nat) : Int) = ((b.start + k * b.stride : Nat) : Int)
              exact_mod_cast hEq
            have hki := huniq k (List.mem_range.mp hk) hbeq
            exact hkEq (by rw [hki]; simp)
          rw [beq_eq_false_iff_ne.mpr hne]
          by_cases hki : k = i
          · subst k
            exact (hkEq (by simp)).elim
          · simp [hki]]
      rw [sum_eq_single_int b.n i (List.mem_range.mp hi)]
    have hbval : bvalAt b pos = b.base + b.step * (i : Int) := by
      unfold bvalAt
      rw [if_pos hc]
      have hdiv : (((pos - (b.start : Int)) / (b.stride : Int)).toNat) = i := by
        rw [hival']
        have hsub : ((b.start : Int) + (i : Int) * (b.stride : Int) - (b.start : Int)) =
            (i : Int) * (b.stride : Int) := by omega
        rw [hsub, Int.mul_ediv_cancel (i : Int) (by exact_mod_cast (Nat.ne_of_gt hb))]
        simp
      rw [hdiv]
    rw [hsum, hbval]
  · have hzero : ((List.range b.n).map (fun k : Nat =>
        if a.start + j * a.stride == b.start + k * b.stride then
          aval * (b.base + b.step * (k : Int))
        else 0)).sum = 0 := by
      rw [show (List.range b.n).map (fun k : Nat =>
          if a.start + j * a.stride == b.start + k * b.stride then
            aval * (b.base + b.step * (k : Int))
          else 0) = (List.range b.n).map (fun _ : Nat => (0 : Int)) by
        apply List.map_congr_left
        intro k hk
        by_cases hEq : a.start + j * a.stride == b.start + k * b.stride
        · have hEq' : a.start + j * a.stride = b.start + k * b.stride := beq_iff_eq.mp hEq
          have hc' : contains (positions b) pos = true := by
            apply (contains_mem (positions b) pos).mpr
            unfold progVals
            apply List.mem_map.mpr
            refine ⟨k, hk, ?_⟩
            have hcast : ((b.start + k * b.stride : Nat) : Int) = pos := by
              change ((b.start + k * b.stride : Nat) : Int) = ((a.start + j * a.stride : Nat) : Int)
              exact_mod_cast hEq'.symm
            change ((b.start + k * b.stride : Nat) : Int) = pos
            exact hcast
          exact (hc hc').elim
        · rw [if_neg hEq]]
      rw [sum_map_zero_int]
    have hbval : bvalAt b pos = 0 := by
      unfold bvalAt
      rw [if_neg hc]
    rw [hzero, hbval]
    simp
private theorem moment_sum (n : Nat) (x y dx dy : Int) :
    ((List.range n).map (fun k : Nat => (x + (k : Int) * dx) * (y + (k : Int) * dy))).sum =
      (n : Int) * x * y + tri n * (x * dy + y * dx) + squares n * dx * dy := by
  induction n with
  | zero => simp [tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      rw [tri_succ, squares_succ]
      simp [Int.natCast_add, Int.natCast_mul]
      grind
private theorem aligned_lane (a b : Lane) (ha : 0 < a.stride) (hb : 0 < b.stride) :
    ((lanePoints a).map (fun r => ((List.range b.n).map (fun i : Nat =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)).sum =
      aligned a b := by
  -- Reduce to the range-indexed form, then mirror the corrected LaneMajor proof.
  unfold lanePoints
  rw [List.map_map]
  change ((List.range a.n).map (fun j : Nat => ((List.range b.n).map (fun i : Nat =>
      if a.start + j * a.stride == b.start + i * b.stride then
        (a.base + (j : Int) * a.step) * (b.base + b.step * (i : Int))
      else 0)).sum)).sum = aligned a b
  rw [show (List.range a.n).map (fun j : Nat =>
      ((List.range b.n).map (fun i : Nat =>
        if a.start + j * a.stride == b.start + i * b.stride then
          (a.base + (j : Int) * a.step) * (b.base + b.step * (i : Int))
        else 0)).sum) =
      (List.range a.n).map (fun j : Nat =>
        (a.base + (j : Int) * a.step) * bvalAt b ((a.start + j * a.stride : Nat) : Int)) by
    apply List.map_congr_left
    intro j hj
    exact inner_aligned a b hb j]
  rw [show ((List.range a.n).map (fun j : Nat =>
      (a.base + (j : Int) * a.step) * bvalAt b ((a.start + j * a.stride : Nat) : Int))).sum =
      ((lanePosVals a).map (fun v => posF a v * bvalAt b v)).sum by
    rw [lanePosVals, List.map_map]
    apply congrArg List.sum
    apply List.map_congr_left
    intro j hj
    change (a.base + (j : Int) * a.step) * bvalAt b ((a.start + j * a.stride : Nat) : Int) =
      posF a ((a.start + j * a.stride : Nat) : Int) * bvalAt b ((a.start + j * a.stride : Nat) : Int)
    rw [posF_at a ha j]]
  let q := intersection (positions a) (positions b)
  have hqstep : 0 < q.step := intersection_step_pos (positions a) (positions b) (by exact ha)
  have hsplit : ((lanePosVals a).map (fun v => posF a v * bvalAt b v)).sum =
      (((lanePosVals a).filter (fun v => contains (positions b) v)).map
        (fun v => posF a v * bvalAt b v)).sum := by
    have hc := sum_filter_complement (lanePosVals a) (fun v => contains (positions b) v)
      (fun v => posF a v * bvalAt b v)
    have hz : (((lanePosVals a).filter (fun v => !contains (positions b) v)).map
        (fun v => posF a v * bvalAt b v)).sum = 0 := by
      rw [show ((lanePosVals a).filter (fun v => !contains (positions b) v)).map
          (fun v => posF a v * bvalAt b v) =
          ((lanePosVals a).filter (fun v => !contains (positions b) v)).map (fun _ => (0 : Int)) by
        apply List.map_congr_left
        intro v hv
        have hnv : ¬ contains (positions b) v := by
          have hb := (List.mem_filter.mp hv).2
          simpa using hb
        unfold bvalAt
        rw [if_neg hnv]
        simp]
      exact sum_map_zero_int _
    omega
  rw [hsplit]
  rw [← list_sum_perm ((inter_positions_perm a ha (positions b)).map
    (fun v => posF a v * bvalAt b v))]
  have hbval_inter : ∀ v ∈ progVals q, bvalAt b v = posF b v := by
    intro v hv
    have hc := (contains_mem q v).mpr hv
    have hbboth := (intersection_contains (positions a) (positions b) v).mp hc
    unfold bvalAt posF
    rw [if_pos hbboth.2]
    grind
  rw [show ((progVals q).map (fun v => posF a v * bvalAt b v)).sum =
      ((progVals q).map (fun v => posF a v * posF b v)).sum by
    apply congrArg List.sum
    apply List.map_congr_left
    intro v hv
    rw [hbval_inter v hv]]
  have hval : ∀ k, k < q.n →
      posF a (progVal q k) * posF b (progVal q k) =
        (firstAt a q + (k : Int) * slopeAt a q) *
          (firstAt b q + (k : Int) * slopeAt b q) := by
    intro k hk
    obtain ⟨ka, hka, hvka, heqa⟩ := posF_inter a ha (positions b) (progVal q k)
      (List.mem_map.mpr ⟨k, List.mem_range.mpr hk, rfl⟩)
    change progVal q k = q.lo + (ka : Int) * (q.step : Int) at hvka
    change posF a (progVal q k) = firstAt a q + (ka : Int) * slopeAt a q at heqa
    change ka < q.n at hka
    obtain ⟨kb, hkb, hvkb, heqb⟩ := posF_inter_of b hb q
      (fun v h => ((intersection_contains (positions a) (positions b) v).mp h).2)
      (progVal q k) (List.mem_map.mpr ⟨k, List.mem_range.mpr hk, rfl⟩)
    have hkab : ka = kb := by
      have hEq : progVal q ka = progVal q kb := by
        change q.lo + (ka : Int) * (q.step : Int) = q.lo + (kb : Int) * (q.step : Int)
        rw [← hvka, ← hvkb]
      exact progVal_inj_of_pos q hqstep ka kb hka hkb hEq
    have heqb' : posF b (progVal q k) = firstAt b q + (ka : Int) * slopeAt b q := by
      rw [← hkab] at heqb
      exact heqb
    have hkaka : ka = k := by
      apply progVal_inj_of_pos q hqstep ka k hka hk
      change q.lo + (ka : Int) * (q.step : Int) = q.lo + (k : Int) * (q.step : Int)
      rw [← hvka]
      rfl
    rw [hkaka] at heqa heqb'
    rw [heqa, heqb']
  unfold aligned
  change ((progVals q).map (fun v => posF a v * posF b v)).sum =
    (q.n : Int) * firstAt a (intersection (positions a) (positions b)) *
        firstAt b (intersection (positions a) (positions b)) +
      tri q.n * (firstAt a q * slopeAt b q + firstAt b q * slopeAt a q) +
      squares q.n * slopeAt a q * slopeAt b q
  rw [show progVals q = (List.range q.n).map (fun k : Nat => progVal q k) by rfl, List.map_map]
  change ((List.range q.n).map (fun k : Nat =>
    posF a (progVal q k) * posF b (progVal q k))).sum = _
  rw [show (List.range q.n).map (fun k : Nat =>
      posF a (progVal q k) * posF b (progVal q k)) =
      (List.range q.n).map (fun k : Nat =>
        (firstAt a q + (k : Int) * slopeAt a q) *
          (firstAt b q + (k : Int) * slopeAt b q)) by
    apply List.map_congr_left
    intro k hk
    exact hval k (List.mem_range.mp hk)]
  exact moment_sum q.n (firstAt a q) (firstAt b q) (slopeAt a q) (slopeAt b q)

/-! ## Layout-to-lanes correspondence -/

-- For each target layout we prove rows = allPoints lanes directly.


/-! ## Residue enumeration helpers (LaneMajor rows/lanes permutation) -/

private theorem nodup_map_inj {α β : Type} (l : List α) (f : α → β)
    (h : ∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) (hl : l.Nodup) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at hl
      rw [List.map_cons, List.nodup_cons]
      constructor
      · intro hm
        rcases List.mem_map.mp hm with ⟨b, hb, he⟩
        exact hl.1 (by rw [← h b (by simp [hb]) a (by simp) he]; exact hb)
      · exact ih (fun x hx y hy => h x (by simp [hx]) y (by simp [hy])) hl.2

private theorem pos_lt_iff (start stride i p : Nat) (hs : 0 < stride) :
    start + i * stride < p ↔ i < (p - start + stride - 1) / stride := by
  by_cases hps : p ≤ start
  · have hp0 : p - start = 0 := by omega
    have hzero : (p - start + stride - 1) / stride = 0 := by
      rw [hp0, Nat.zero_add]
      exact Nat.div_eq_of_lt (by omega)
    rw [hzero]
    omega
  · rw [Nat.lt_div_iff_mul_lt hs]
    have hrewrite : p - start + stride - 1 - (stride - 1) = p - start := by omega
    rw [hrewrite]
    omega

private theorem residue_count_eq_zero_of_ge (N r p : Nat) (hr : N ≤ r) (hp : 0 < p) :
    (N - r + p - 1) / p = 0 := by
  have hsub : N - r = 0 := by omega
  rw [hsub, Nat.zero_add]
  exact Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))

private theorem residue_lt_of_count (N p r q : Nat) (hp : 0 < p) (hr : r < p)
    (hq : q < (N - r + p - 1) / p) : r + q * p < N := by
  by_cases hle : r ≤ N
  · have hmul : q * p < N - r + p - 1 - (p - 1) := (Nat.lt_div_iff_mul_lt hp).mp hq
    have hdiff : N - r + p - 1 - (p - 1) = N - r := by omega
    rw [hdiff] at hmul
    omega
  · have hz : (N - r + p - 1) / p = 0 := residue_count_eq_zero_of_ge N r p (by omega) hp
    omega

private theorem mem_of_mem_filter {α : Type} {l : List α} {p : α → Bool} {a : α}
    (h : a ∈ l.filter p) : a ∈ l := (List.mem_filter.mp h).1

private theorem mem_of_mem_flatMap {α β : Type} {l : List α} {f : α → List β} {b : β}
    (h : b ∈ l.flatMap f) : ∃ a ∈ l, b ∈ f a := List.mem_flatMap.mp h

private theorem nodup_filter {α : Type} (l : List α) (p : α → Bool) (hl : l.Nodup) :
    (l.filter p).Nodup := hl.filter _

private theorem pairwise_range_rel (p : Nat) (R : Nat → Nat → Prop)
    (h : ∀ a < p, ∀ b < p, a ≠ b → R a b) : (List.range p).Pairwise R := by
  induction p with
  | zero => simp
  | succ p ih =>
      rw [show p + 1 = Nat.succ p by omega, List.range_succ, List.pairwise_append]
      refine ⟨ih (fun a ha b hb hab => h a (by omega) b (by omega) hab), by simp, ?_⟩
      intro a ha
      have ha' : a < p := List.mem_range.mp ha
      intro b hb
      have hb' : b = p := by simpa using hb
      subst b
      exact h a (by omega) p (by omega) (by omega)

private theorem residue_flatMap_nodup_gen (n p k : Nat) (hp : 0 < p) (hk : k ≤ p)
    (f : Nat → Int) :
    (((List.range (min k n)).flatMap fun r =>
      (List.range ((n - r + p - 1) / p)).map fun q =>
        (r + q*p, f (r + q*p)))).Nodup := by
  let F : Nat → List (Nat × Int) := fun r =>
    (List.range ((n - r + p - 1) / p)).map fun q => (r + q*p, f (r+q*p))
  change List.Pairwise (fun a b : Nat × Int => a ≠ b) (List.flatMap F (List.range (min k n)))
  rw [List.pairwise_flatMap]
  constructor
  · intro r _
    unfold F
    apply nodup_map_inj
    · intro q _ q' _ he
      have hh : r + q*p = r + q'*p := congrArg Prod.fst he
      exact Nat.eq_of_mul_eq_mul_left hp (by simpa [Nat.mul_comm] using Nat.add_left_cancel hh)
    · exact List.nodup_range
  · apply pairwise_range_rel (min k n)
    intro a ha b hb hab x hx y hy he
    rcases List.mem_map.mp hx with ⟨qx, hqx, rfl⟩
    rcases List.mem_map.mp hy with ⟨qy, hqy, rfl⟩
    have ha' : a < p := by omega
    have hb' : b < p := by omega
    have hpos : a + qx*p = b + qy*p := congrArg Prod.fst he
    have hamod : (a + qx*p) % p = a := by
      rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt ha']
    have hbmod : (b + qy*p) % p = b := by
      rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hb']
    have hab' : a = b := by rw [← hamod, ← hbmod, hpos]
    exact hab hab'

private theorem residue_rows_perm_gen (n p k : Nat) (hp : 0 < p) (hk : k ≤ p)
    (f : Nat → Int) :
    (((List.range n).filter (fun i => i % p < k)).map (fun i => (i, f i))).Perm
      (((List.range (min k n)).flatMap fun r =>
        (List.range ((n - r + p - 1) / p)).map fun q =>
          (r + q*p, f (r + q*p)))) := by
  refine (List.perm_ext_iff_of_nodup ?_ ?_).mpr ?_
  · exact nodup_map_inj _ _ (by intro a _ b _ he; exact congrArg Prod.fst he)
      (nodup_filter _ _ List.nodup_range)
  · exact residue_flatMap_nodup_gen n p k hp hk f
  · intro z
    constructor
    · intro hz
      rcases List.mem_map.mp hz with ⟨i, hi, rfl⟩
      have hin : i < n := List.mem_range.mp (mem_of_mem_filter hi)
      have hik : i % p < k := by
        have hb := (List.mem_filter.mp hi).2
        exact Nat.lt_of_lt_of_le (by simpa using hb) (by omega)
      have hrK : i % p < min k n := Nat.lt_min.mpr ⟨hik, by
        have := Nat.mod_le i p
        omega⟩
      have hq : i / p < (n - i % p + p - 1) / p := by
        have hdecomp : i % p + (i / p) * p = i := by
          have hd := Nat.div_add_mod i p
          rw [Nat.mul_comm p (i / p)] at hd
          omega
        exact (pos_lt_iff (i % p) p (i / p) n hp).mp (by rw [hdecomp]; exact hin)
      apply List.mem_flatMap.mpr
      refine ⟨i % p, List.mem_range.mpr hrK, ?_⟩
      apply List.mem_map.mpr
      refine ⟨i / p, List.mem_range.mpr hq, ?_⟩
      have hdecomp : i % p + (i / p) * p = i := by
        have hd := Nat.div_add_mod i p
        rw [Nat.mul_comm p (i / p)] at hd
        omega
      simp [hdecomp]
    · intro hz
      rcases mem_of_mem_flatMap hz with ⟨r, hr, hz⟩
      rcases List.mem_map.mp hz with ⟨q, hq, rfl⟩
      have hrp : r < p := by have := List.mem_range.mp hr; omega
      have hrk : r < k := by have := List.mem_range.mp hr; omega
      have hrn : r + q*p < n := residue_lt_of_count n p r q hp hrp (List.mem_range.mp hq)
      apply List.mem_map.mpr
      refine ⟨r + q*p, List.mem_filter.mpr ⟨List.mem_range.mpr hrn, ?_⟩, rfl⟩
      have hmod : (r + q*p) % p = r := by
        rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hrp]
      rw [hmod]
      simp [hrk]

private theorem flatMap_congr_left {α β : Type} (l : List α) (f g : α → List β)
    (h : ∀ x ∈ l, f x = g x) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private def lmValue (c : Config) (i : Nat) : Int :=
  c.base + c.step * (i / width c : Nat) + c.bias * (i % width c : Nat)

/-! ## Layout-to-lanes correspondence -/

-- rows and the lane point list agree up to permutation, not pointwise: stripe
-- lanes enumerate points residue-major whereas rows enumerates them in ordinal
-- index order. (Equality is false: c = ⟨3, 0, 1, 0, 10, 1, []⟩.)
private theorem LaneMajor_rows_lanes (c : Config) :
    (rows LaneMajor.layout c).Perm (allPoints (LaneMajor.layout.lanes c)) := by
  let p := width c
  have hp : 0 < p := by dsimp [p, width]; omega
  have hrows : rows LaneMajor.layout c =
      (List.range c.n).map (fun i : Nat => (i, lmValue c i)) := by
    apply rows_range_map
    intro i hi
    simp [LaneMajor.layout, lmValue]
  rw [hrows]
  have hperm := residue_rows_perm_gen c.n p (min p c.n) hp (Nat.min_le_left _ _) (lmValue c)
  have hfilter : (List.range c.n).filter (fun i : Nat => i % p < min p c.n) = List.range c.n := by
    apply List.filter_eq_self.mpr
    intro i hi
    have hiN : i < c.n := List.mem_range.mp hi
    by_cases hpn : p ≤ c.n
    · rw [Nat.min_eq_left hpn]
      simp [Nat.mod_lt i hp]
    · rw [Nat.min_eq_right (by omega)]
      simp [Nat.lt_of_le_of_lt (Nat.mod_le i p) hiN]
  have hRHS :
      ((List.range (min p c.n)).flatMap (fun r : Nat =>
        (List.range ((c.n - r + p - 1) / p)).map (fun q : Nat =>
          (r + q * p, lmValue c (r + q * p))))) =
      ((LaneMajor.layout.lanes c).flatMap lanePoints) := by
    dsimp [p]
    dsimp [LaneMajor.layout, striped]
    rw [List.flatMap_map]
    apply flatMap_congr_left
    intro r hr
    have hrp : r < width c := by have := List.mem_range.mp hr; omega
    unfold lanePoints lane
    dsimp
    congr 1
    funext q
    have hw : 0 < width c := by dsimp [width]; omega
    have hmod : (r + q * width c) % width c = r := by
      rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hrp]
    have hdiv : (r + q * width c) / width c = q := by
      rw [Nat.add_mul_div_right r q hw, Nat.div_eq_of_lt hrp]
      simp
    dsimp [lmValue]
    have hcast : ((r + q * width c : Nat) : Int) =
        (r : Int) + (q : Int) * (width c : Int) := by push_cast; rfl
    have hdivI : ((r : Int) + (q : Int) * (width c : Int)) / (width c : Int) = (q : Int) := by
      rw [← hcast]
      exact_mod_cast hdiv
    have hmodI : ((r : Int) + (q : Int) * (width c : Int)) % (width c : Int) = (r : Int) := by
      rw [← hcast]
      exact_mod_cast hmod
    rw [hdivI, hmodI]
    rw [Int.mul_comm c.step (q : Int)]
    ac_rfl
  have hperm' :
      ((List.range c.n).map (fun i : Nat => (i, lmValue c i))).Perm
        ((List.range (min p c.n)).flatMap (fun r : Nat =>
          (List.range ((c.n - r + p - 1) / p)).map (fun q : Nat =>
            (r + q * p, lmValue c (r + q * p))))) := by
    rw [← hfilter]
    simpa [Nat.min_assoc] using hperm
  unfold allPoints
  rw [← hRHS]
  exact hperm'

private def boustIndex (w r : Nat) : Nat :=
  if r < w then r else 2*w - 1 - (r - w)

private def boustVal (c : Config) (i : Nat) : Int :=
  let w := width c
  let k := i / w
  let r := i % w
  c.base + c.step * (k*w + (if k % 2 == 0 then r else w - 1 - r) : Nat)

private theorem boust_rows (c : Config) :
    rows Boustrophedon.layout c = (List.range c.n).map (fun i => (i, boustVal c i)) := by
  apply rows_range_map
  intro i hi
  simp [Boustrophedon.layout, boustVal]

private theorem boust_lanes (c : Config) :
    Boustrophedon.layout.lanes c =
      (List.range (min (2*width c) c.n)).map (fun r =>
        lane c.n r (2*width c) (c.base + c.step * boustIndex (width c) r)
          (c.step * (2*width c))) := by
  rfl

private theorem boust_shift (c : Config) (r q : Nat) (hr : r < 2*width c) :
    boustVal c (r + q * (2*width c)) =
      c.base + c.step * boustIndex (width c) r + (q : Int) * (c.step * (2*width c)) := by
  let w := width c
  have hw : 0 < w := by dsimp [w, width]; omega
  have hrp : r < 2*w := by simpa [w] using hr
  by_cases hrw : r < w
  · have hdiv : (r + q * (2*w)) / w = 2*q := by
      have hrewrite : r + q * (2*w) = r + w * (2*q) := by grind
      rw [hrewrite, Nat.add_mul_div_left r (2*q) hw, Nat.div_eq_of_lt hrw]
      omega
    have hmod : (r + q * (2*w)) % w = r := by
      have hrewrite : r + q * (2*w) = r + w * (2*q) := by grind
      rw [hrewrite, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hrw]
    have hb : boustIndex w r = r := by simp [boustIndex, hrw]
    dsimp [boustVal]
    rw [show width c = w by rfl]
    rw [hdiv, hmod]
    simp [hrw]
    rw [hb]
    have hnum : (↑r + ↑q * (2 * (↑w : Int)) : Int) = ↑(r + q * (2*w)) := by simp [Int.natCast_add, Int.natCast_mul]
    have hdivI : (↑r + ↑q * (2 * (↑w : Int))) / ↑w = (2*q : Nat) := by
      rw [hnum, ← Int.natCast_ediv]
      exact congrArg Int.ofNat hdiv
    rw [hdivI]
    grind
  · have hwle : w ≤ r := by omega
    let r0 := r - w
    have hr0 : r0 < w := by dsimp [r0]; omega
    have hr_eq : r = w + r0 := by dsimp [r0]; omega
    have hdiv : (r + q * (2*w)) / w = 2*q + 1 := by
      have hrewrite : r + q * (2*w) = r0 + w * (2*q + 1) := by rw [hr_eq]; grind
      rw [hrewrite, Nat.add_mul_div_left r0 (2*q+1) hw, Nat.div_eq_of_lt hr0]
      omega
    have hmod : (r + q * (2*w)) % w = r0 := by
      have hrewrite : r + q * (2*w) = r0 + w * (2*q + 1) := by rw [hr_eq]; grind
      rw [hrewrite, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hr0]
    have hb : boustIndex w r = 2*w - 1 - r0 := by
      simp [boustIndex, hrw, r0]
    dsimp [boustVal]
    rw [show width c = w by rfl]
    rw [hdiv, hmod]
    simp
    rw [hb]
    have hnum : (↑r + ↑q * (2 * (↑w : Int)) : Int) = ↑(r + q * (2*w)) := by simp [Int.natCast_add, Int.natCast_mul]
    have hdivI : (↑r + ↑q * (2 * (↑w : Int))) / ↑w = (2*q + 1 : Nat) := by
      rw [hnum, ← Int.natCast_ediv]
      exact congrArg Int.ofNat hdiv
    rw [hdivI]
    grind

private theorem Boustrophedon_rows_lanes (c : Config) :
    (rows Boustrophedon.layout c).Perm (allPoints (Boustrophedon.layout.lanes c)) := by
  rw [boust_rows c]
  let p := 2 * width c
  have hw : 0 < width c := by dsimp [width]; omega
  have hp : 0 < p := by dsimp [p]; omega
  have hk : min p c.n ≤ p := Nat.min_le_left _ _
  have hperm := residue_rows_perm_gen c.n p (min p c.n) hp hk (boustVal c)
  have hfilter : (List.range c.n).filter (fun i : Nat => i % p < min p c.n) = List.range c.n := by
    apply List.filter_eq_self.mpr
    intro i hi
    have hin : i < c.n := List.mem_range.mp hi
    have himodlt : i % p < p := Nat.mod_lt i hp
    have himodle : i % p ≤ i := Nat.mod_le _ _
    simpa using (Nat.lt_min.mpr ⟨himodlt, by omega⟩ : i % p < min p c.n)
  rw [hfilter] at hperm
  have hR : ((List.range (min (min p c.n) c.n)).flatMap fun r =>
        (List.range ((c.n - r + p - 1) / p)).map fun q =>
          (r + q*p, boustVal c (r + q*p))) =
      allPoints (Boustrophedon.layout.lanes c) := by
    rw [boust_lanes c]
    unfold allPoints
    rw [List.flatMap_map]
    dsimp [p]
    rw [show min (min (2*width c) c.n) c.n = min (2*width c) c.n by
      exact Nat.min_eq_left (Nat.min_le_right _ _)]
    apply flatMap_congr_left
    intro r hr
    have hrp : r < 2*width c := by
      have h := List.mem_range.mp hr
      exact Nat.lt_of_lt_of_le h (Nat.min_le_left _ _)
    have hlen : (c.n - r + (2*width c) - 1) / (2*width c) =
        (lane c.n r (2*width c) (c.base + c.step * boustIndex (width c) r)
          (c.step * (2*width c))).n := by
      simp [lane]
    rw [hlen]
    apply List.map_congr_left
    intro q hq
    apply Prod.ext
    · simp [lane]
    · simpa [lane] using boust_shift c r q hrp
  rw [← hR]
  exact hperm

private theorem stride_boust (c : Config) :
    ∀ a ∈ Boustrophedon.layout.lanes c, 0 < a.stride := by
  intro a ha
  rw [boust_lanes c] at ha
  simp only [List.mem_map] at ha
  rcases ha with ⟨r, hr, rfl⟩
  dsimp [lane]
  dsimp [width]
  omega


private theorem rs_filterMap_ite_none_some {α β : Type} (l : List α) (p : α → Bool) (f : α → β) :
    l.filterMap (fun x => if p x then none else some (f x)) = (l.filter (fun x => !p x)).map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem rs_rows_ragged_filter (c : Config) :
    rows RaggedStripe.layout c =
      ((List.range c.n).filter (fun i => !(i % width c == width c - 1))).map
        (fun i : Nat => (i, c.base + c.step * (i : Int))) := by
  unfold rows
  have hFM : (List.range c.n).filterMap (fun i => (RaggedStripe.layout.read c i).map (fun v => (i, v))) =
      (List.range c.n).filterMap (fun i : Nat =>
        if i % width c == width c - 1 then none else some (i, c.base + c.step * (i : Int))) := by
    apply filterMap_congr'
    intro i hi
    by_cases h : (i % width c == width c - 1) = true
    · have heq : i % width c = width c - 1 := beq_iff_eq.mp h
      simp [RaggedStripe.layout, h, heq]
    · have hf : (i % width c == width c - 1) = false := Bool.eq_false_of_ne_true h
      have heq : i % width c ≠ width c - 1 := by intro he; exact h (by rw [he]; simp)
      simp [RaggedStripe.layout, h, hf, heq]
  rw [hFM]
  rw [rs_filterMap_ite_none_some]

private theorem rs_ceilDiv_eq (d s : Nat) (hs : 0 < s) : d + s - 1 - (s - 1) = d := by omega

private theorem rs_pos_lt_iff (start stride i p : Nat) (hs : 0 < stride) :
    start + i * stride < p ↔ i < (p - start + stride - 1) / stride := by
  by_cases hps : p ≤ start
  · have hp0 : p - start = 0 := by omega
    have hzero : (p - start + stride - 1) / stride = 0 := by
      rw [hp0, Nat.zero_add]
      exact Nat.div_eq_of_lt (by omega)
    rw [hzero]
    omega
  · have hps' : start < p := by omega
    rw [Nat.lt_div_iff_mul_lt hs]
    have hrewrite : p - start + stride - 1 - (stride - 1) = p - start :=
      rs_ceilDiv_eq (p - start) stride hs
    rw [hrewrite]
    omega

private theorem rs_mem_lanePoints (a : Lane) (x : Nat × Int) :
    x ∈ lanePoints a ↔ ∃ q, q < a.n ∧ x = (a.start + q * a.stride, a.base + (q : Int) * a.step) := by
  unfold lanePoints
  rw [List.mem_map]
  constructor
  · rintro ⟨q, hq, hval⟩
    exact ⟨q, List.mem_range.mp hq, hval.symm⟩
  · rintro ⟨q, hq, hval⟩
    exact ⟨q, List.mem_range.mpr hq, hval.symm⟩

private theorem rs_lanePoints_nodup (a : Lane) (hs : 0 < a.stride) : (lanePoints a).Nodup := by
  unfold lanePoints
  apply ep_nodup_map_of_injective
  · intro i hi j hj h
    have hfst := congrArg Prod.fst h
    have hmul : i * a.stride = j * a.stride := Nat.add_left_cancel hfst
    exact Nat.eq_of_mul_eq_mul_right hs hmul
  · exact List.nodup_range

private theorem rs_perm_sum_nat {l₁ l₂ : List Nat} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem rs_perm_sum_int {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem RaggedStripe_rows_lanes (c : Config) :
    (rows RaggedStripe.layout c).Perm (allPoints (RaggedStripe.layout.lanes c)) := by
  rw [rs_rows_ragged_filter c]
  have hw : 0 < width c := by dsimp [width]; omega
  let laneAt : Nat → Lane := fun r =>
    lane c.n r (width c) (c.base + c.step * (r : Int)) (c.step * width c)
  have hlanes : RaggedStripe.layout.lanes c = (List.range (min c.n (width c - 1))).map laneAt := by
    dsimp [RaggedStripe.layout, laneAt]
  rw [hlanes]
  have hL : (((List.range c.n).filter (fun i => !(i % width c == width c - 1))).map
      (fun i : Nat => (i, c.base + c.step * (i : Int)))).Nodup := by
    apply ep_nodup_map_of_injective
    · intro i hi j hj h
      exact congrArg Prod.fst h
    · exact List.nodup_range.filter _
  have hcross : ∀ r1 r2, r1 < min c.n (width c - 1) → r2 < min c.n (width c - 1) →
      r1 ≠ r2 → ∀ x ∈ lanePoints (laneAt r1), ∀ y ∈ lanePoints (laneAt r2), x ≠ y := by
    intro r1 r2 hr1 hr2 hrne x hx y hy heq
    rcases (rs_mem_lanePoints _ x).mp hx with ⟨j1, hj1, hxv⟩
    rcases (rs_mem_lanePoints _ y).mp hy with ⟨j2, hj2, hyv⟩
    have hfst := congrArg Prod.fst (hxv.symm.trans (heq.trans hyv))
    dsimp [laneAt, lane] at hfst
    have hm1 : (r1 + j1 * width c) % width c = r1 := by
      rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt (by omega)]
    have hm2 : (r2 + j2 * width c) % width c = r2 := by
      rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt (by omega)]
    have hr : r1 = r2 := by
      rw [← hm1, ← hm2, hfst]
    exact hrne hr
  have hR2gen : ∀ xs : List Nat, xs.Nodup → (∀ a ∈ xs, a < min c.n (width c - 1)) →
      xs.Pairwise (fun r1 r2 => ∀ x ∈ lanePoints (laneAt r1),
        ∀ y ∈ lanePoints (laneAt r2), x ≠ y) := by
    intro xs
    induction xs with
    | nil => simp
    | cons a as ih =>
        intro hnd hbound
        rw [List.nodup_cons] at hnd
        rcases hnd with ⟨ha_not, hnd_as⟩
        rw [List.pairwise_cons]
        constructor
        · intro b hb
          apply hcross a b (hbound a (by simp)) (hbound b (by simp [hb]))
          intro hr
          subst b
          exact ha_not hb
        · apply ih hnd_as
          intro b hb
          exact hbound b (by simp [hb])
  have hR2 := hR2gen (List.range (min c.n (width c - 1))) List.nodup_range
    (fun a ha => List.mem_range.mp ha)
  have hall : allPoints ((List.range (min c.n (width c - 1))).map laneAt) =
      (List.range (min c.n (width c - 1))).flatMap (fun r => lanePoints (laneAt r)) := by
    simp [allPoints, List.flatMap_map]
  have hR : ((List.range (min c.n (width c - 1))).flatMap (fun r => lanePoints (laneAt r))).Nodup := by
    rw [List.nodup_iff_pairwise_ne, List.pairwise_flatMap]
    constructor
    · intro r hr
      apply rs_lanePoints_nodup
      dsimp [laneAt, lane]
      simp [hw]
    · exact hR2
  rw [hall]
  apply (List.perm_ext_iff_of_nodup hL hR).mpr
  intro x
  constructor
  · intro hx
    rcases List.mem_map.mp hx with ⟨i, hi, rfl⟩
    have hin : i < c.n := (List.mem_range.mp (List.mem_filter.mp hi).1)
    have hbeq : (i % width c == width c - 1) = false := by
      have hf : (!(i % width c == width c - 1)) = true := (List.mem_filter.mp hi).2
      simpa using hf
    have hneq : i % width c ≠ width c - 1 := by
      intro h
      rw [h] at hbeq
      simp at hbeq
    let r := i % width c
    let j := i / width c
    have hw2 : 2 ≤ width c := by
      by_cases hw1 : width c = 1
      · have hn : i % width c = width c - 1 := by
          rw [hw1]
          simpa using Nat.mod_one i
        exact False.elim (hneq hn)
      · omega
    have hr : r < min c.n (width c - 1) := by
      apply Nat.lt_min.mpr
      constructor
      · exact Nat.lt_of_le_of_lt (Nat.mod_le i (width c)) hin
      · have hmodlt : i % width c < width c := Nat.mod_lt i hw
        omega
    have hi_decomp : i = r + j * width c := by
      dsimp [r, j]
      have hd := Nat.div_add_mod i (width c)
      rw [Nat.mul_comm (width c) (i / width c)] at hd
      omega
    have hj : j < (lane c.n r (width c) (c.base + c.step * (r : Int)) (c.step * width c)).n := by
      unfold lane
      apply (rs_pos_lt_iff r (width c) j c.n hw).mp
      omega
    apply List.mem_flatMap.mpr
    refine ⟨r, List.mem_range.mpr hr, ?_⟩
    apply (rs_mem_lanePoints (laneAt r) _).mpr
    refine ⟨j, by simpa [laneAt, lane] using hj, ?_⟩
    apply Prod.ext
    · dsimp [laneAt, lane]
      omega
    · dsimp [laneAt, lane]
      have hi_decomp' : (i : Int) = (r : Int) + (j : Int) * (width c : Int) := by
        exact_mod_cast hi_decomp
      rw [hi_decomp']
      grind
  · intro hx
    rcases List.mem_flatMap.mp hx with ⟨r, hr, hx⟩
    rcases (rs_mem_lanePoints (laneAt r) x).mp hx with ⟨j, hj, rfl⟩
    have hr' : r < min c.n (width c - 1) := List.mem_range.mp hr
    have hjlt : j < (lane c.n r (width c) (c.base + c.step * (r : Int)) (c.step * width c)).n := by
      simpa [laneAt] using hj
    have hi : r + j * width c < c.n := by
      dsimp [lane] at hjlt
      exact (rs_pos_lt_iff r (width c) j c.n hw).mpr hjlt
    have hmod : (r + j * width c) % width c = r := by
      rw [Nat.mul_comm j (width c), Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt (by omega)]
    have hbeq : ((r + j * width c) % width c == width c - 1) = false := by
      rw [hmod]
      apply beq_eq_false_iff_ne.mpr
      omega
    apply List.mem_map.mpr
    refine ⟨r + j * width c, ?_, ?_⟩
    · apply List.mem_filter.mpr
      exact ⟨List.mem_range.mpr hi, by simpa using hbeq⟩
    · apply Prod.ext
      · dsimp [laneAt, lane]
      · dsimp [laneAt, lane]
        push_cast
        grind


-- PeriodicPatch: rows and lanes agree only up to permutation (lanes are
-- residue-major, rows are ordinal). The proof is below after the ep-helpers.

-- BEGIN ep-helpers
private theorem ep_div_eq_one_of_bounds (t p : Nat) (hp : 0 < p)
    (hge : p ≤ t) (hlt : t < 2*p) : t / p = 1 := by
  have hlt2 : t / p < 2 := (Nat.div_lt_iff_lt_mul hp).mpr hlt
  have hnle : ¬ t / p ≤ 0 := by
    intro h
    have hh := (Nat.div_le_iff_le_mul hp).mp h
    omega
  omega

private theorem ep_mod_step_count (n p r : Nat) (hp : 0 < p) (hr : r < p) :
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
      have hdiv2 : (n % p + p - 1 - r) / p = 1 := ep_div_eq_one_of_bounds _ _ hp hge hlt2
      simp [hlt, hdiv2]
    · have hle : n % p ≤ r := by omega
      have hlt2 : n % p + p - 1 - r < p := by omega
      have hdiv2 : (n % p + p - 1 - r) / p = 0 := Nat.div_eq_of_lt hlt2
      simp [hlt, hdiv2]

private theorem ep_succ_mod_step (n p : Nat) (hp : 0 < p) :
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

private theorem ep_succ_div_step (n p : Nat) (hp : 0 < p) :
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

private theorem ep_range_filter_mod_eq (n p r : Nat) (hp : 0 < p) (hr : r < p) :
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
      rw [ep_mod_step_count n p r hp hr, ep_mod_step_count (n+1) p r hp hr]
      rw [ep_succ_mod_step n p hp, ep_succ_div_step n p hp]
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

private theorem ep_filterMap_ite_some {α β : Type} (l : List α) (p : α → Bool) (f : α → β) :
    l.filterMap (fun x => if p x then some (f x) else none) = (l.filter p).map f := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : p x
      · simp [h, ih]
      · simp [h, ih]
-- END ep-helpers

-- Obstacle: structural rows=allPoints(lanes) correspondence; provable by residue/stripe enumeration
-- as in EncodedPages3 slice proofs, but needs per-layout unfolding.
private theorem SparseSample_rows_lanes (c : Config) :
    rows SparseSample.layout c = allPoints (SparseSample.layout.lanes c) := by
  have hw : 0 < width c := by
    simp only [width]
    omega
  have hr : c.split % width c < width c := Nat.mod_lt _ hw
  have hrows : rows SparseSample.layout c =
      (List.range c.n).filterMap (fun i =>
        if i % width c == c.split % width c then
          some (i, c.base + c.step * (i / width c : Nat)) else none) := by
    unfold rows
    refine filterMap_congr' (fun i _ => ?_)
    show Option.map (fun x => (i, x))
        (if i % width c == c.split % width c then
          some (c.base + c.step * (i / width c : Nat)) else none) =
      if i % width c == c.split % width c then
        some (i, c.base + c.step * (i / width c : Nat)) else none
    by_cases h : i % width c = c.split % width c
    · simp [h]
    · simp [h]
  have hrhs : allPoints (SparseSample.layout.lanes c) =
      (List.range ((c.n - (c.split % width c) + width c - 1) / width c)).map
        (fun j : Nat => (c.split % width c + j * width c, c.base + (j : Int) * c.step)) := by
    show (([lane c.n (c.split % width c) (width c) c.base c.step]).flatMap lanePoints) = _
    simp only [List.flatMap_cons, List.flatMap_nil, lanePoints, lane]
    rw [List.append_nil]
  rw [hrows, ep_filterMap_ite_some,
    ep_range_filter_mod_eq c.n (width c) (c.split % width c) hw hr, hrhs]
  rw [List.map_map]
  refine List.map_congr_left (fun q _ => ?_)
  have hpos : (0 : Int) < (width c : Int) := by
    simp only [width]
    omega
  have hne : (width c : Int) ≠ 0 := by
    simp only [width]
    omega
  have key : ((c.split : Int) % (width c : Int) + (q : Int) * (width c : Int)) /
      (width c : Int) = (q : Int) := by
    rw [Int.mul_comm (q : Int) (width c : Int), Int.add_mul_ediv_left _ _ hne,
      Int.ediv_eq_zero_of_lt (Int.emod_nonneg _ hne) (Int.emod_lt_of_pos _ hpos)]
    simp
  simp
  rw [key, Int.mul_comm]

/-! ## Generic lifting -/

private theorem perm_map_sum_int {α : Type} (l₁ l₂ : List α) (f : α → Int)
    (p : l₁.Perm l₂) : (l₁.map f).sum = (l₂.map f).sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem perm_map_sum_nat {α : Type} (l₁ l₂ : List α) (f : α → Nat)
    (p : l₁.Perm l₂) : (l₁.map f).sum = (l₂.map f).sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem ResidueJoin_of_rows (lay : Layout) (c : Config) (b : Lane)
    (hrows : rows lay c = allPoints (lay.lanes c)) :
    ((rows lay c).map (fun r => (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum =
      ((lay.lanes c).map (joinCount · b)).sum := by
  rw [hrows]
  unfold allPoints
  change ((allPoints (lay.lanes c)).map (fun r => (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum = _
  unfold allPoints
  rw [map_flatMap_sum_nat]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  exact joinCount_lane a b

private theorem DeleteProgression_of_rows (lay : Layout) (c : Config) (mask : Progression)
    (hrows : rows lay c = allPoints (lay.lanes c))
    (hs : ∀ a ∈ lay.lanes c, 0 < a.stride) :
    (((rows lay c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
      ((lay.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum := by
  rw [hrows]
  unfold allPoints
  rw [filter_flatMap]
  rw [map_flatMap_sum]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  exact deletedSum_lane a (hs a ha) mask

private theorem AlignedProduct_of_rows (lay : Layout) (c : Config) (b : Lane) (hb : 0 < b.stride)
    (hrows : rows lay c = allPoints (lay.lanes c))
    (hs : ∀ a ∈ lay.lanes c, 0 < a.stride) :
    ((rows lay c).map (fun r => ((List.range b.n).map (fun i : Nat =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)).sum =
      ((lay.lanes c).map (aligned · b)).sum := by
  rw [hrows]
  unfold allPoints
  have key := map_flatMap_sum (lay.lanes c) lanePoints (fun r => ((List.range b.n).map (fun i : Nat =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)
  change ((lay.lanes c).flatMap lanePoints |>.map (fun r => ((List.range b.n).map (fun i : Nat =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)).sum = _
  rw [key]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  exact aligned_lane a b (hs a ha) hb

private theorem ResidueJoin_of_rows_perm (lay : Layout) (c : Config) (b : Lane)
    (hrows : (rows lay c).Perm (allPoints (lay.lanes c))) :
    ((rows lay c).map (fun r => (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum =
      ((lay.lanes c).map (joinCount · b)).sum := by
  calc
    ((rows lay c).map (fun r => (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum =
        ((allPoints (lay.lanes c)).map (fun r => (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum := by
      apply perm_map_sum_nat
      exact hrows
    _ = ((lay.lanes c).map (joinCount · b)).sum := by
      unfold allPoints
      rw [map_flatMap_sum_nat]
      apply congrArg List.sum
      apply List.map_congr_left
      intro a ha
      exact joinCount_lane a b

private theorem DeleteProgression_of_rows_perm (lay : Layout) (c : Config) (mask : Progression)
    (hrows : (rows lay c).Perm (allPoints (lay.lanes c)))
    (hs : ∀ a ∈ lay.lanes c, 0 < a.stride) :
    (((rows lay c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
      ((lay.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum := by
  have hf : ((rows lay c).filter (fun r => !contains mask r.1)).Perm
      ((allPoints (lay.lanes c)).filter (fun r => !contains mask r.1)) :=
    List.Perm.filter _ hrows
  calc
    (((rows lay c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
        (((allPoints (lay.lanes c)).filter (fun r => !contains mask r.1)).map Prod.snd).sum := by
      apply perm_map_sum_int
      exact hf
    _ = ((lay.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum := by
      unfold allPoints
      rw [filter_flatMap]
      rw [map_flatMap_sum]
      apply congrArg List.sum
      apply List.map_congr_left
      intro a ha
      exact deletedSum_lane a (hs a ha) mask

private theorem AlignedProduct_of_rows_perm (lay : Layout) (c : Config) (b : Lane) (hb : 0 < b.stride)
    (hrows : (rows lay c).Perm (allPoints (lay.lanes c)))
    (hs : ∀ a ∈ lay.lanes c, 0 < a.stride) :
    ((rows lay c).map (fun r => ((List.range b.n).map (fun i : Nat =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)).sum =
      ((lay.lanes c).map (aligned · b)).sum := by
  let g := fun r : Nat × Int => ((List.range b.n).map (fun i : Nat =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum
  calc
    ((rows lay c).map g).sum = ((allPoints (lay.lanes c)).map g).sum := by
      apply perm_map_sum_int
      exact hrows
    _ = ((lay.lanes c).map (aligned · b)).sum := by
      unfold allPoints
      rw [map_flatMap_sum]
      apply congrArg List.sum
      apply List.map_congr_left
      intro a ha
      change ((lanePoints a).map g).sum = aligned a b
      exact aligned_lane a b (hs a ha) hb

/-! ## Residue-stripe permutation (PeriodicPatch) -/

private theorem ep_flatMap_congr_left {α β : Type} (l : List α) (f g : α → List β)
    (h : ∀ x ∈ l, f x = g x) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem ep_residue_flatMap_nodup (n p : Nat) (hp : 0 < p) (V : Nat → Int) :
    (((List.range (min p n)).flatMap fun r =>
      (List.range ((n - r + p - 1) / p)).map fun q =>
        (r + q * p, V (r + q * p)))).Nodup := by
  let F : Nat → List (Nat × Int) := fun r =>
    (List.range ((n - r + p - 1) / p)).map fun q => (r + q * p, V (r + q * p))
  change List.Pairwise (fun a b : Nat × Int => a ≠ b) (List.flatMap F (List.range (min p n)))
  rw [List.pairwise_flatMap]
  constructor
  · intro r _
    unfold F
    apply ep_nodup_map_of_injective _ _ (fun a _ b _ he => ?_) List.nodup_range
    have hh : r + a * p = r + b * p := congrArg Prod.fst he
    exact Nat.eq_of_mul_eq_mul_left hp (by simpa [Nat.mul_comm] using Nat.add_left_cancel hh)
  · apply pairwise_range_rel
    intro a ha b hb hab x hx y hy he
    rcases List.mem_map.mp hx with ⟨qx, hqx, rfl⟩
    rcases List.mem_map.mp hy with ⟨qy, hqy, rfl⟩
    have ha' : a < p := by omega
    have hb' : b < p := by omega
    have hpos : a + qx * p = b + qy * p := congrArg Prod.fst he
    have hamod : (a + qx * p) % p = a := by
      rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt ha']
    have hbmod : (b + qy * p) % p = b := by
      rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hb']
    have hab' : a = b := by rw [← hamod, ← hbmod, hpos]
    exact hab hab'

private theorem ep_residue_partition_perm (n p : Nat) (hp : 0 < p) (V : Nat → Int) :
    ((List.range n).map (fun i => (i, V i))).Perm
      ((List.range (min p n)).flatMap fun r =>
        (List.range ((n - r + p - 1) / p)).map fun q =>
          (r + q * p, V (r + q * p))) := by
  refine (List.perm_ext_iff_of_nodup ?_ ?_).mpr ?_
  · apply ep_nodup_map_of_injective _ _ (fun a _ b _ h => ?_) List.nodup_range
    exact congrArg Prod.fst h
  · exact ep_residue_flatMap_nodup n p hp V
  intro z
  constructor
  · intro hz
    rcases List.mem_map.mp hz with ⟨i, hi, rfl⟩
    have hin : i < n := List.mem_range.mp hi
    let r := i % p
    have hrp : r < p := Nat.mod_lt i hp
    have hrn : r < min p n := Nat.lt_min.mpr ⟨hrp, by
      have hle : i % p ≤ i := Nat.mod_le i p
      omega⟩
    have hmemf : i ∈ (List.range n).filter (fun j => j % p == r) := by
      apply List.mem_filter.mpr
      exact ⟨List.mem_range.mpr hin, by simp [r]⟩
    rw [ep_range_filter_mod_eq n p r hp hrp] at hmemf
    rcases List.mem_map.mp hmemf with ⟨q, hq, hqi⟩
    apply List.mem_flatMap.mpr
    refine ⟨r, List.mem_range.mpr hrn, ?_⟩
    apply List.mem_map.mpr
    refine ⟨q, hq, ?_⟩
    apply Prod.ext
    · exact hqi
    · rw [hqi]
  · intro hz
    rcases mem_of_mem_flatMap hz with ⟨r, hr, hz⟩
    rcases List.mem_map.mp hz with ⟨q, hq, rfl⟩
    have hrp : r < p := by
      have := List.mem_range.mp hr
      omega
    have hmemf : (r + q * p) ∈ (List.range n).filter (fun j => j % p == r) := by
      rw [ep_range_filter_mod_eq n p r hp hrp]
      exact List.mem_map.mpr ⟨q, hq, rfl⟩
    apply List.mem_map.mpr
    refine ⟨r + q * p, List.mem_range.mpr (List.mem_range.mp (mem_of_mem_filter hmemf)), rfl⟩

private theorem ep_perm_sum_int {l₁ l₂ : List Int} (h : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction h with
  | nil => rfl
  | cons a h ih => simp [ih]
  | swap a b l => simp [List.sum_cons]; omega
  | trans h1 h2 ih1 ih2 => exact ih1.trans ih2

private theorem ep_perm_sum_nat {l₁ l₂ : List Nat} (h : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction h with
  | nil => rfl
  | cons a h ih => simp [ih]
  | swap a b l => simp [List.sum_cons]; omega
  | trans h1 h2 ih1 ih2 => exact ih1.trans ih2

-- Correct statement: periodic patch lanes are residue-major while rows are
-- ordinal, so the two lists agree only up to permutation (counterexample to
-- equality: c = ⟨3, 0, 1, 0, 10, 0, []⟩).
private theorem PeriodicPatch_rows_lanes (c : Config) :
    (rows PeriodicPatch.layout c).Perm (allPoints (PeriodicPatch.layout.lanes c)) := by
  let p := width c
  have hp : 0 < p := by dsimp [p, width]; omega
  let V : Nat → Int := fun i => c.base + c.step * (i : Int) + if i % p == 0 then c.bias else 0
  have hrows : rows PeriodicPatch.layout c = (List.range c.n).map (fun i => (i, V i)) := by
    apply rows_range_map
    intro i hi
    dsimp [V, p]
    simp [PeriodicPatch.layout]
  rw [hrows]
  refine (ep_residue_partition_perm c.n p hp V).trans ?_
  apply List.Perm.of_eq
  unfold allPoints
  dsimp [PeriodicPatch.layout, striped, lane, p, V]
  rw [List.flatMap_map]
  dsimp [lanePoints]
  apply ep_flatMap_congr_left
  intro r hr
  have hrp : r < p := by
    have := List.mem_range.mp hr
    omega
  apply List.map_congr_left
  intro q hq
  apply Prod.ext
  · rfl
  · have hmod : (r + q * p) % p = r := by
      rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hrp]
    rw [hmod]
    by_cases hr0 : r = 0 <;> simp [hr0] <;> grind

-- Obstacle: structural rows=allPoints(lanes) correspondence; provable by residue/stripe enumeration
-- as in EncodedPages3 slice proofs, but needs per-layout unfolding.

/-! ## The 15 target goals -/

namespace LaneMajor
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.LaneMajor
theorem sound_LaneMajor_ResidueJoin : ResidueJoin.Goal := by
  intro c b
  exact ResidueJoin_of_rows_perm layout c b (LaneMajor_rows_lanes c)
theorem sound_LaneMajor_DeleteProgression : DeleteProgression.Goal := by
  intro c mask
  have hs : ∀ a ∈ layout.lanes c, 0 < a.stride := by
    intro a ha
    dsimp [LaneMajor.layout, striped] at ha
    rcases List.mem_map.mp ha with ⟨r, hr, rfl⟩
    dsimp [lane, width]
    omega
  exact DeleteProgression_of_rows_perm layout c mask (LaneMajor_rows_lanes c) hs
theorem sound_LaneMajor_AlignedProduct : AlignedProduct.Goal := by
  intro c b hb
  have hs : ∀ a ∈ layout.lanes c, 0 < a.stride := by
    intro a ha
    dsimp [LaneMajor.layout, striped] at ha
    rcases List.mem_map.mp ha with ⟨r, hr, rfl⟩
    dsimp [lane, width]
    omega
  exact AlignedProduct_of_rows_perm layout c b hb (LaneMajor_rows_lanes c) hs
end LaneMajor

namespace Boustrophedon
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.Boustrophedon
theorem sound_Boustrophedon_ResidueJoin : ResidueJoin.Goal := by
  intro c b
  exact ResidueJoin_of_rows_perm layout c b (Boustrophedon_rows_lanes c)
theorem sound_Boustrophedon_DeleteProgression : DeleteProgression.Goal := by
  intro c mask
  have hs : ∀ a ∈ layout.lanes c, 0 < a.stride := by
    intro a ha
    dsimp [Boustrophedon.layout, striped] at ha
    rcases List.mem_map.mp ha with ⟨r, hr, rfl⟩
    dsimp [lane, width]
    omega
  exact DeleteProgression_of_rows_perm layout c mask (Boustrophedon_rows_lanes c) hs
theorem sound_Boustrophedon_AlignedProduct : AlignedProduct.Goal := by
  intro c b hb
  have hs : ∀ a ∈ layout.lanes c, 0 < a.stride := by
    intro a ha
    dsimp [Boustrophedon.layout, striped] at ha
    rcases List.mem_map.mp ha with ⟨r, hr, rfl⟩
    dsimp [lane, width]
    omega
  exact AlignedProduct_of_rows_perm layout c b hb (Boustrophedon_rows_lanes c) hs
end Boustrophedon

namespace RaggedStripe
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RaggedStripe
theorem sound_RaggedStripe_ResidueJoin : ResidueJoin.Goal := by
  intro c b
  exact ResidueJoin_of_rows_perm layout c b (RaggedStripe_rows_lanes c)
theorem sound_RaggedStripe_DeleteProgression : DeleteProgression.Goal := by
  intro c mask
  have hs : ∀ a ∈ layout.lanes c, 0 < a.stride := by
    intro a ha
    dsimp [RaggedStripe.layout] at ha
    rcases List.mem_map.mp ha with ⟨r, hr, rfl⟩
    dsimp [lane, width]
    omega
  exact DeleteProgression_of_rows_perm layout c mask (RaggedStripe_rows_lanes c) hs
theorem sound_RaggedStripe_AlignedProduct : AlignedProduct.Goal := by
  intro c b hb
  have hs : ∀ a ∈ layout.lanes c, 0 < a.stride := by
    intro a ha
    dsimp [RaggedStripe.layout] at ha
    rcases List.mem_map.mp ha with ⟨r, hr, rfl⟩
    dsimp [lane, width]
    omega
  exact AlignedProduct_of_rows_perm layout c b hb (RaggedStripe_rows_lanes c) hs
end RaggedStripe

namespace PeriodicPatch
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.PeriodicPatch
theorem sound_PeriodicPatch_ResidueJoin : ResidueJoin.Goal := by
  intro c b
  have hs : ∀ a ∈ layout.lanes c, 0 < a.stride := by
    intro a ha
    dsimp [PeriodicPatch.layout, striped] at ha
    rcases List.mem_map.mp ha with ⟨r, hr, rfl⟩
    dsimp [lane, width]
    omega
  exact ResidueJoin_of_rows_perm layout c b (PeriodicPatch_rows_lanes c)
theorem sound_PeriodicPatch_DeleteProgression : DeleteProgression.Goal := by
  intro c mask
  have hs : ∀ a ∈ layout.lanes c, 0 < a.stride := by
    intro a ha
    dsimp [PeriodicPatch.layout, striped] at ha
    rcases List.mem_map.mp ha with ⟨r, hr, rfl⟩
    dsimp [lane, width]
    omega
  exact DeleteProgression_of_rows_perm layout c mask (PeriodicPatch_rows_lanes c) hs
theorem sound_PeriodicPatch_AlignedProduct : AlignedProduct.Goal := by
  intro c b hb
  have hs : ∀ a ∈ layout.lanes c, 0 < a.stride := by
    intro a ha
    dsimp [PeriodicPatch.layout, striped] at ha
    rcases List.mem_map.mp ha with ⟨r, hr, rfl⟩
    dsimp [lane, width]
    omega
  exact AlignedProduct_of_rows_perm layout c b hb (PeriodicPatch_rows_lanes c) hs
end PeriodicPatch

namespace SparseSample
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample
theorem sound_SparseSample_ResidueJoin : ResidueJoin.Goal := by
  intro c b
  exact ResidueJoin_of_rows layout c b (SparseSample_rows_lanes c)
theorem sound_SparseSample_DeleteProgression : DeleteProgression.Goal := by
  intro c mask
  have hs : ∀ a ∈ layout.lanes c, 0 < a.stride := by
    intro a ha
    dsimp [SparseSample.layout] at ha
    simp at ha
    subst a
    dsimp [lane, width]
    omega
  exact DeleteProgression_of_rows layout c mask (SparseSample_rows_lanes c) hs
theorem sound_SparseSample_AlignedProduct : AlignedProduct.Goal := by
  intro c b hb
  have hs : ∀ a ∈ layout.lanes c, 0 < a.stride := by
    intro a ha
    dsimp [SparseSample.layout] at ha
    simp at ha
    subst a
    dsimp [lane, width]
    omega
  exact AlignedProduct_of_rows layout c b hb (SparseSample_rows_lanes c) hs
end SparseSample

end TinylambOpt.ResearchProofs.Atlas.EncodedPages_WorkC

import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.TriangleWave.Remaining

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

-- Generic list algebra (adapted from a verified sibling proof).
private theorem countP_eq_sum {α : Type} (l : List α) (p : α → Bool) :
    l.countP p = (l.map (fun x => if p x then 1 else 0)).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih] <;> omega

private theorem countP_between_eq_sub {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, q x = true → p x = true) :
    l.countP (fun x => p x && !q x) = l.countP p - l.countP q := by
  have hqlen : l.countP q = (l.filter q).length := List.countP_eq_length_filter
  have hqa : l.countP q = (l.filter q).countP p := by
    rw [hqlen]
    exact (List.countP_eq_length.mpr (fun x hx =>
      h x (List.mem_filter.mp hx).1 (List.mem_filter.mp hx).2)).symm
  have hleft : l.countP (fun x => p x && !q x) = (l.filter fun a => !q a).countP p := by
    rw [← List.countP_filter (p := p) (q := fun a => !q a)]
  have hpart := List.countP_eq_countP_filter_add l p q
  rw [hleft, hpart, hqa]
  exact (Nat.add_sub_cancel_left _ _).symm

private theorem sum_map_ite_zero {α : Type} (l : List α) (p : α → Bool) (f : α → Int) :
    ((l.filter p).map f).sum = (l.map (fun x => if p x then f x else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

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
    have hcan : k * k - k + k = k * k := Nat.sub_add_cancel hle
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


-- Finite-progression foundations (adapted from a verified sibling proof).
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
  have hkint : (k : Int) ≤ (a.n - 1 : Nat) := by exact_mod_cast (by omega)
  have hmul := Int.mul_le_mul_of_nonneg_left hkint (by omega : 0 ≤ (a.step : Int))
  have hmul' : (k : Int) * a.step ≤ ((a.n - 1 : Nat) : Int) * a.step := by
    simpa only [Int.mul_comm] using hmul
  unfold lastVal
  constructor
  · rw [hval]
    exact Int.le_add_of_nonneg_right (Int.mul_nonneg (Int.natCast_nonneg _) (Int.natCast_nonneg _))
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
  have hd1 : (a.step.gcd b.step : Int) ∣ (b.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_right a.step b.step)
  have hd2 : (a.step.gcd b.step : Int) ∣ (a.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_left a.step b.step)
  have hi' : (a.step : Int) * (i : Int) = v - a.lo := by rw [hi]; grind
  have hj' : (b.step : Int) * (j : Int) = v - b.lo := by rw [hj]; grind
  have heq : (a.step : Int) * (i : Int) - (b.step : Int) * (j : Int) = b.lo - a.lo := by
    rw [hi', hj']
    clear hi hj ha hb
    omega
  have h1' : (a.step.gcd b.step : Int) ∣ (j : Int) * (b.step : Int) :=
    Int.dvd_mul_of_dvd_right hd1
  have h2' : (a.step.gcd b.step : Int) ∣ (i : Int) * (a.step : Int) :=
    Int.dvd_mul_of_dvd_right hd2
  have h1 : (a.step.gcd b.step : Int) ∣ (b.step : Int) * (j : Int) := by simpa [Int.mul_comm] using h1'
  have h2 : (a.step.gcd b.step : Int) ∣ (a.step : Int) * (i : Int) := by simpa [Int.mul_comm] using h2'
  rw [← heq]
  exact Int.dvd_sub h2 h1

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
  obtain ⟨k1, hk1⟩ := hds
  obtain ⟨k2, hk2⟩ := hdt
  rcases hpair : bezout sa tb with ⟨u, vv⟩
  have hbez := bezout_spec sa tb
  rw [hpair] at hbez
  change (sa.gcd tb : Int) = (sa : Int) * u + (tb : Int) * vv at hbez
  have hcop : sa.gcd tb = 1 := Nat.coprime_div_gcd_div_gcd hg
  rw [hcop] at hbez
  simp only [Int.ofNat_one] at hbez
  have hgne : (g : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hg)
  have htbk : (tb : Int) * k2 = (sa : Int) * k1 := by
    apply (Int.mul_eq_mul_left_iff hgne).mp
    calc
      (g : Int) * ((tb : Int) * k2) = (t : Int) * k2 := by rw [htEq]; grind
      _ = delta := hk2.symm
      _ = (s : Int) * k1 := hk1
      _ = (g : Int) * ((sa : Int) * k1) := by rw [hsEq]; grind
  have hm : (tb : Int) * (u*k2 + vv*k1) = k1 := by
    calc
      (tb : Int) * (u*k2 + vv*k1) = (tb : Int)*u*k2 + (tb : Int)*vv*k1 := by grind
      _ = (sa : Int)*u*k1 + (tb : Int)*vv*k1 := by
        rw [show (tb : Int)*u*k2 = u*((tb : Int)*k2) by grind, htbk]
        grind
      _ = ((sa : Int)*u + (tb : Int)*vv)*k1 := by grind
      _ = k1 := by rw [← hbez]; simp
  refine ⟨u*k2 + vv*k1, ?_⟩
  change delta = ((sa * t : Nat) : Int) * (u*k2 + vv*k1)
  calc
    delta = (g : Int)*(sa : Int)*k1 := by rw [hk1, hsEq]
    _ = (g : Int)*(sa : Int)*((tb : Int)*(u*k2+vv*k1)) := by rw [hm]
    _ = ((sa * t : Nat) : Int) * (u*k2 + vv*k1) := by
      rw [show ((sa * t : Nat) : Int) = (sa : Int) * (t : Int) by simp]
      rw [htEq]
      grind

private theorem intersection_contains (a b : Progression) (v : Int) :
    contains (intersection a b) v = true ↔ contains a v = true ∧ contains b v = true := by
  by_cases han : a.n = 0
  · have hcf : contains a v = false := by simp [contains, han]
    simp [intersection, han, hcf, contains]
  · by_cases hbn : b.n = 0
    · have hcf : contains b v = false := by simp [contains, hbn]
      simp [intersection, han, hbn, hcf, contains]
    · have hanp : 0 < a.n := by omega
      have hbnp : 0 < b.n := by omega
      by_cases has : a.step = 0
      · simp [intersection, han, hbn, has]
        have hca : contains a v = true ↔ 0 < a.n ∧ v = a.lo := by
          simp [contains, lastVal, has]
          omega
        by_cases hb0 : contains b a.lo = true
        · simp [hb0]
          have hspan : contains ⟨a.lo, 1, 1⟩ v = true ↔ v = a.lo := by
            simp [contains, lastVal]
            omega
          rw [hspan, hca]
          constructor
          · intro hv
            subst v
            exact ⟨⟨hanp, rfl⟩, hb0⟩
          · rintro ⟨⟨_, hv⟩, _⟩
            exact hv
        · have hb0f : contains b a.lo = false := by cases h : contains b a.lo <;> simp_all
          simp [hb0f]
          have hspan : contains ⟨a.lo, 1, 0⟩ v = true ↔ False := by simp [contains]
          rw [hspan]
          constructor
          · intro h
            cases h
          · rintro ⟨hav, hbv⟩
            have hv : v = a.lo := (hca.mp hav).2
            subst v
            rw [hb0f] at hbv
            cases hbv
      · by_cases hbs : b.step = 0
        · simp [intersection, han, hbn, has, hbs]
          have hcb : contains b v = true ↔ 0 < b.n ∧ v = b.lo := by
            simp [contains, lastVal, hbs]
            omega
          by_cases ha0 : contains a b.lo = true
          · simp [ha0]
            have hspan : contains ⟨b.lo, 1, 1⟩ v = true ↔ v = b.lo := by
              simp [contains, lastVal]
              omega
            rw [hspan, hcb]
            constructor
            · intro hv
              subst v
              exact ⟨ha0, ⟨hbnp, rfl⟩⟩
            · rintro ⟨_, ⟨_, hv⟩⟩
              exact hv
          · have ha0f : contains a b.lo = false := by cases h : contains a b.lo <;> simp_all
            simp [ha0f]
            have hspan : contains ⟨b.lo, 1, 0⟩ v = true ↔ False := by simp [contains]
            rw [hspan]
            constructor
            · intro h
              cases h
            · rintro ⟨hav, hbv⟩
              have hv : v = b.lo := (hcb.mp hbv).2
              subst v
              rw [ha0f] at hav
              cases hav
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
          have hp_period_nat : 0 < period := by
            dsimp [period, g]
            exact Nat.mul_pos (Nat.div_gcd_pos_of_pos_left b.step hasp) hbsp
          have hp : (0 : Int) < period := by exact_mod_cast hp_period_nat
          have hgapdef : gap = b.lo - a.lo := rfl
          by_cases hgapmod : gap % (g : Int) = 0
          · have hgap : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgapmod
            have hroot := root_dvd a.step b.step hasp hbsp a.lo b.lo gap hgapdef hgap
            have hfirst : first = low + (root - low) % (period : Int) := rfl
            have hfroot := first_root_dvd low root first period hp hfirst
            have hnatA : period = a.step * (b.step / g) := by
              dsimp [period, g]
              have hdiv1 : a.step / a.step.gcd b.step * a.step.gcd b.step = a.step :=
                Nat.div_mul_cancel (Nat.gcd_dvd_left a.step b.step)
              have hdiv2 : a.step.gcd b.step * (b.step / a.step.gcd b.step) = b.step :=
                Nat.mul_div_cancel' (Nat.gcd_dvd_right a.step b.step)
              grind
            have hnatB : period = b.step * (a.step / g) := by
              dsimp [period]
              grind
            have hpa : (a.step : Int) ∣ (period : Int) := by
              refine ⟨((b.step / g : Nat) : Int), ?_⟩
              exact_mod_cast hnatA
            have hpb : (b.step : Int) ∣ (period : Int) := by
              refine ⟨((a.step / g : Nat) : Int), ?_⟩
              exact_mod_cast hnatB
            have hgmod0 : (b.lo - a.lo) % (a.step.gcd b.step : Int) = 0 := by
              simpa [gap, g] using hgapmod
            simp only [intersection]
            rw [if_neg (by simp [han, hbn]), if_neg (by simp [has]),
              if_neg (by simp [hbs]), if_neg (by simp [hgmod0, gap, g])]
            change contains ⟨first, period, if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ v = true ↔ _
            have hpercast : ((period : Int).toNat) = period := by simp
            have hspan := span_contains_iff first (period : Int) high v hp
            dsimp only at hspan
            rw [hpercast] at hspan
            rw [hspan]
            constructor
            · rintro ⟨hfirstx, hxhigh, hxfirst⟩
              have hxroot : (period : Int) ∣ v - root := by
                obtain ⟨k, hk⟩ := hxfirst
                obtain ⟨l, hl⟩ := hfroot
                refine ⟨k + l, ?_⟩
                calc
                  v - root = (v - first) + (first - root) := by grind
                  _ = period*k + period*l := by rw [hk, hl]
                  _ = period * (k + l) := by grind
              have hxalo : (a.step : Int) ∣ v - a.lo := by
                obtain ⟨k, hk⟩ := hpa
                obtain ⟨l, hl⟩ := hxroot
                obtain ⟨m, hm⟩ := hroot.1
                refine ⟨k*l + m, ?_⟩
                calc
                  v - a.lo = (v - root) + (root - a.lo) := by grind
                  _ = period*l + a.step*m := by rw [hl, hm]
                  _ = (a.step*k)*l + a.step*m := by rw [hk]
                  _ = a.step * (k*l + m) := by grind
              have hxblo : (b.step : Int) ∣ v - b.lo := by
                obtain ⟨k, hk⟩ := hpb
                obtain ⟨l, hl⟩ := hxroot
                obtain ⟨m, hm⟩ := hroot.2
                refine ⟨k*l + m, ?_⟩
                calc
                  v - b.lo = (v - root) + (root - b.lo) := by grind
                  _ = period*l + b.step*m := by rw [hl, hm]
                  _ = (b.step*k)*l + b.step*m := by rw [hk]
                  _ = b.step * (k*l + m) := by grind
              have hflow : low ≤ first := by
                dsimp [first]
                have hremnon : 0 ≤ (root - low) % (period : Int) :=
                  Int.emod_nonneg _ (by omega)
                omega
              have hxlow : low ≤ v := by omega
              have hal : a.lo ≤ v := by dsimp [low] at hxlow; omega
              have hbl : b.lo ≤ v := by dsimp [low] at hxlow; omega
              have hah : v ≤ lastVal a := by dsimp [high] at hxhigh; omega
              have hbh : v ≤ lastVal b := by dsimp [high] at hxhigh; omega
              constructor
              · exact mem_of_bounds_dvd a v hanp hasp hal hah hxalo
              · exact mem_of_bounds_dvd b v hbnp hbsp hbl hbh hxblo
            · rintro ⟨ha, hb⟩
              have hxalo := contains_dvd a v ha
              have hxblo := contains_dvd b v hb
              have hdeltaA : (a.step : Int) ∣ v - root := by
                obtain ⟨k, hk⟩ := hxalo
                obtain ⟨l, hl⟩ := hroot.1
                refine ⟨k - l, ?_⟩
                calc
                  v - root = (v - a.lo) - (root - a.lo) := by grind
                  _ = a.step*k - a.step*l := by rw [hk, hl]
                  _ = a.step * (k - l) := by grind
              have hdeltaB : (b.step : Int) ∣ v - root := by
                obtain ⟨k, hk⟩ := hxblo
                obtain ⟨l, hl⟩ := hroot.2
                refine ⟨k - l, ?_⟩
                calc
                  v - root = (v - b.lo) - (root - b.lo) := by grind
                  _ = b.step*k - b.step*l := by rw [hk, hl]
                  _ = b.step * (k - l) := by grind
              have hperdiv := period_dvd_delta a.step b.step hasp hbsp (v - root) hdeltaA hdeltaB
              have hxroot : (period : Int) ∣ v - root := by simpa [period, g] using hperdiv
              have hxfirst : (period : Int) ∣ v - first := by
                obtain ⟨k, hk⟩ := hxroot
                obtain ⟨l, hl⟩ := hfroot
                refine ⟨k - l, ?_⟩
                calc
                  v - first = (v - root) - (first - root) := by grind
                  _ = period*k - period*l := by rw [hk, hl]
                  _ = period * (k - l) := by grind
              have hflow : low ≤ first := by
                dsimp [first]
                have hremnon : 0 ≤ (root - low) % (period : Int) :=
                  Int.emod_nonneg _ (by omega)
                omega
              have hlo : low ≤ v := by
                have habounds := contains_bounds a v ha
                have hbbounds := contains_bounds b v hb
                dsimp [low]
                omega
              have hfirstv : first ≤ v :=
                first_le_of_congruence low root first v period hp rfl hlo hxroot
              have hbh : v ≤ high := by
                have habounds := contains_bounds a v ha
                have hbbounds := contains_bounds b v hb
                dsimp [high]
                omega
              exact ⟨hfirstv, hbh, hxfirst⟩
          · have hgapne : gap % (g : Int) ≠ 0 := by simpa using hgapmod
            have hgmodne : (b.lo - a.lo) % (a.step.gcd b.step : Int) ≠ 0 := by
              simpa [gap, g] using hgapmod
            simp only [intersection]
            rw [if_neg (by simp [han, hbn]), if_neg (by simp [has]),
              if_neg (by simp [hbs]), if_pos (by simp [hgmodne, gap, g])]
            change contains (⟨0, 1, 0⟩ : Progression) v = true ↔ contains a v = true ∧ contains b v = true
            have hleftfalse : contains (⟨0, 1, 0⟩ : Progression) v = true ↔ False := by simp [contains]
            rw [hleftfalse]
            constructor
            · intro h
              cases h
            · rintro ⟨ha, hb⟩
              have hg := common_gap_dvd a b v ha hb
              exact hgmodne (Int.emod_eq_zero_of_dvd hg)


private def progVals (p : Progression) : List Int :=
  (List.range p.n).map (fun k : Nat => p.lo + (k : Int) * p.step)

private theorem mem_progVals (p : Progression) (v : Int) :
    v ∈ progVals p ↔ contains p v = true := by
  unfold progVals
  rw [contains_iff_mem]
  unfold progMem progVal
  simp only [List.mem_map, List.mem_range]
  constructor
  · rintro ⟨k, hk, hkv⟩
    exact ⟨k, hk, hkv.symm⟩
  · rintro ⟨k, hk, hkv⟩
    exact ⟨k, hk, hkv.symm⟩

private theorem progVals_nodup_aux (lo : Int) (step : Int) (hs : 0 < step) :
    ∀ n : Nat, (List.range n).map (fun k : Nat => lo + (k : Int) * step) |>.Nodup := by
  intro n
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append]
      simp only [List.nodup_append]
      constructor
      · exact ih
      · constructor
        · simp
        · intro a ha b hb
          intro heq
          rw [List.mem_map] at ha hb
          rcases ha with ⟨k, hk, rfl⟩
          rcases hb with ⟨l, hl, rfl⟩
          simp only [List.mem_singleton] at hl
          subst l
          have hkn : k < n := List.mem_range.mp hk
          have heq' : (k : Int) * step = (n : Int) * step := by omega
          have hsne : step ≠ 0 := by omega
          have hkn' : (k : Int) = (n : Int) := (Int.mul_eq_mul_right_iff hsne).mp heq'
          omega

private theorem progVals_nodup (p : Progression) (hp : 0 < p.step) : (progVals p).Nodup := by
  unfold progVals
  exact progVals_nodup_aux p.lo p.step (by omega) p.n

private theorem intersection_step_pos (p q : Progression) (hp : 0 < p.step) :
    0 < (intersection p q).step := by
  by_cases h1 : p.n == 0 || q.n == 0
  · simpa [intersection, h1]
  · by_cases h2 : p.step == 0
    · simpa [intersection, h1, h2]
    · by_cases h3 : q.step == 0
      · simpa [intersection, h1, h2, h3]
      · by_cases h4p : (q.lo - p.lo) % (p.step.gcd q.step : Int) = 0
        · have hs : 0 < p.step / p.step.gcd q.step * q.step :=
            Nat.mul_pos (Nat.div_gcd_pos_of_pos_left q.step hp)
              (Nat.pos_of_ne_zero (by simpa using h3))
          simpa [intersection, h1, h2, h3, h4p] using hs
        · simpa [intersection, h1, h2, h3, h4p]

private theorem count_eq_one_of_mem_nodup {α : Type} [BEq α] [LawfulBEq α]
    {a : α} {l : List α} (hnd : l.Nodup) (hm : a ∈ l) : l.count a = 1 := by
  induction l with
  | nil => contradiction
  | cons x xs ih =>
      simp only [List.count_cons]
      have hnd' := hnd
      simp only [List.nodup_cons] at hnd'
      by_cases hxa : x == a
      · have hxeq : x = a := beq_iff_eq.mp hxa
        have hnotin : a ∉ xs := by
          intro hxs
          exact hnd'.1 (by simpa [hxeq] using hxs)
        simp [hxa, List.count_eq_zero.mpr hnotin]
      · have hne : x ≠ a := by intro he; exact hxa (by rw [he]; simp)
        have haxs : a ∈ xs := by
          rcases List.mem_cons.mp hm with h | h
          · exact False.elim (hne h.symm)
          · exact h
        simp [hxa, ih hnd'.2 haxs]

private theorem nodup_filter_bool {α : Type} (f : α → Bool) {l : List α}
    (h : l.Nodup) : (l.filter f).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.filter_cons]
      by_cases hx : f x = true
      · rw [if_pos hx]
        simp only [List.nodup_cons]
        constructor
        · intro hm
          have hmx : x ∈ xs := (List.mem_filter.mp hm).1
          exact (List.pairwise_cons.mp h).1 x hmx rfl
        · exact ih ((List.pairwise_cons.mp h).2)
      · have hxf : f x = false := by cases h' : f x <;> simp_all
        rw [if_neg (by simpa [hxf])]
        exact ih ((List.pairwise_cons.mp h).2)

private theorem list_sum_perm {l₁ l₂ : List Int} (h : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction h with
  | nil => rfl
  | cons a h ih => simpa using ih
  | swap a b l => simp; omega
  | trans h₁ h₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem perm_progVals_intersection (p q : Progression) (hp : 0 < p.step) :
    (progVals (intersection p q)).Perm ((progVals p).filter (contains q)) := by
  apply (List.perm_iff_count).mpr
  intro v
  have hnd1 : (progVals (intersection p q)).Nodup :=
    progVals_nodup _ (intersection_step_pos p q hp)
  have hnd2 : ((progVals p).filter (contains q)).Nodup :=
    nodup_filter_bool (contains q) (progVals_nodup p hp)
  have hmem : v ∈ progVals (intersection p q) ↔ v ∈ (progVals p).filter (contains q) := by
    rw [mem_progVals, List.mem_filter, mem_progVals]
    exact intersection_contains p q v
  by_cases hv : v ∈ progVals (intersection p q)
  · rw [count_eq_one_of_mem_nodup hnd1 hv,
        count_eq_one_of_mem_nodup hnd2 (hmem.mp hv)]
  · have hv2 : v ∉ (progVals p).filter (contains q) := by
      intro h
      exact hv (hmem.mpr h)
    rw [List.count_eq_zero.mpr hv, List.count_eq_zero.mpr hv2]

private theorem sum_progVals_filter (p q : Progression) (hp : 0 < p.step) (F : Int → Int) :
    ((progVals (intersection p q)).map F).sum =
      (((progVals p).filter (contains q)).map F).sum := by
  exact list_sum_perm ((perm_progVals_intersection p q hp).map F)

-- Striped-layout partitioning helpers (adapted from a verified sibling proof).
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

private theorem sum_range_const_nat (m v : Nat) :
    ((List.range m).map (fun _ => v)).sum = m * v := by
  induction m with
  | zero => simp
  | succ m ih =>
      rw [show m + 1 = Nat.succ m by omega, List.range_succ, List.map_append]
      rw [List.sum_append]
      simp [ih, Nat.succ_mul]

private theorem sum_range_ite_nat (m : Nat) (b : Bool) (v : Nat) :
    ((List.range m).map (fun _ => if b then v else 0)).sum = if b then m * v else 0 := by
  by_cases h : b <;> simp [h, sum_range_const_nat, sum_map_zero_nat]

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


open TinylambOpt.ResearchGoals.Atlas.EncodedPages.TriangleWave

private def twVal (c : Config) (i : Nat) : Int :=
  c.base + c.step * (min (i % (2 * width c)) (2 * width c - i % (2 * width c)) : Nat)

private theorem twVal_mod (c : Config) (i : Nat) :
    twVal c (i % (2 * width c)) = twVal c i := by
  unfold twVal
  rw [Nat.mod_mod]

private theorem striped_tw (c : Config) (i : Nat) :
    stripedValue (2 * width c) (fun r => (twVal c r, 0)) i = twVal c i := by
  unfold stripedValue
  simp only [Prod.fst, Prod.snd, Int.zero_mul, Int.add_zero]
  exact twVal_mod c i

private theorem rows_tw (c : Config) :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.rows layout c =
      (List.range c.n).map (fun i => (i, twVal c i)) := by
  unfold twVal
  simp [TinylambOpt.ResearchGoals.Atlas.EncodedPages.rows,
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.TriangleWave.layout]

private theorem lanes_tw (c : Config) :
    layout.lanes c = (List.range (min (2 * width c) c.n)).map (fun r =>
      lane c.n r (2 * width c) (twVal c r) 0) := by
  simp only [TinylambOpt.ResearchGoals.Atlas.EncodedPages.TriangleWave.layout, striped]
  apply List.map_congr_left
  intro r hr
  have hr : r < 2 * width c := by
    have := List.mem_range.mp hr
    omega
  simp [twVal, Nat.mod_eq_of_lt hr]

private theorem countP_map_eq {α : Type} (l : List α) (f : α → Int) (v : Int) :
    l.countP (fun x => v == f x) = (l.map f).countP (fun y => v == y) := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : v == f x <;> simp [h, ih]

private theorem countP_beq_eq_count {α : Type} [BEq α] [LawfulBEq α] (l : List α) (v : α) :
    l.countP (fun x => x == v) = l.count v := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : x == v
      · have hveq : x = v := beq_iff_eq.mp h
        have hv : v == x := by rw [hveq]; simp
        simp [h, hveq, hv, ih]
      · have hnev : x ≠ v := by intro he; exact h (by rw [he]; simp)
        have hv : ¬ v == x := by
          intro hh
          exact hnev (beq_iff_eq.mp hh).symm
        simp [h, hnev, hv, ih]

private theorem countP_eq_count_rev {α : Type} [BEq α] [LawfulBEq α] (l : List α) (v : α) :
    l.countP (fun x => v == x) = l.count v := by
  rw [show l.countP (fun x => v == x) = l.countP (fun x => x == v) by
    apply List.countP_congr
    intro x hx
    rw [beq_iff_eq, beq_iff_eq]
    exact eq_comm]
  exact countP_beq_eq_count l v

private theorem countP_const (n : Nat) (b : Bool) :
    (List.range n).countP (fun _ : Nat => b) = if b then n else 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.countP_append]
      by_cases h : b <;> simp [ih, h]

private theorem affine_nodup (base step : Int) (hs : step ≠ 0) : ∀ n : Nat,
    ((List.range n).map (fun k : Nat => base + step * (k : Int))).Nodup := by
  intro n
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append]
      simp only [List.nodup_append]
      constructor
      · exact ih
      · constructor
        · simp
        · intro a ha b hb
          rw [List.mem_map] at ha hb
          rcases ha with ⟨k, hk, hka⟩
          rcases hb with ⟨j, hj, hjb⟩
          simp only [List.mem_singleton] at hj
          subst j
          intro heq
          have heq0 : base + step * (k : Int) = base + step * (n : Int) := by
            rw [← hka, ← hjb] at heq
            exact heq
          have heq' : step * (k : Int) = step * (n : Int) := Int.add_left_cancel heq0
          have heq'' : (k : Int) * step = (n : Int) * step := by simpa [Int.mul_comm] using heq'
          have hkn : (k : Int) = (n : Int) := Int.eq_of_mul_eq_mul_right hs heq''
          have hklt : k < n := List.mem_range.mp hk
          omega

private theorem bvals_mem_values (b : Lane) (v : Int) :
    (∃ i, i < b.n ∧ v = b.base + b.step * (i : Int)) ↔ contains (values b) v = true := by
  rw [contains_iff_mem]
  constructor
  · rintro ⟨i, hi, hv⟩
    by_cases hneg : b.step < 0
    · have hnat : (b.step.natAbs : Int) = -b.step := by
        rw [← Int.natAbs_neg b.step]
        exact Int.ofNat_natAbs_of_nonneg (by omega)
      refine ⟨b.n - 1 - i, ?_, ?_⟩
      · simpa [values] using (show b.n - 1 - i < b.n by omega)
      dsimp [progVal]
      unfold values
      simp only [hneg, ↓reduceIte]
      rw [hnat]
      have hi_le : i ≤ b.n - 1 := by omega
      have hsubNat : b.n - 1 - i + i = b.n - 1 := Nat.sub_add_cancel hi_le
      have hsub : ((b.n - 1 - i : Nat) : Int) = ((b.n - 1 : Nat) : Int) - (i : Int) := by
        omega
      rw [hsub]
      grind
    · have hs0or : b.step = 0 ∨ 0 < b.step := by omega
      rcases hs0or with hs0 | hsp
      · have hs0eq : b.step = 0 := hs0
        simp [hs0eq, progVal, values] at hv
        refine ⟨i, ?_, ?_⟩
        · simpa [values] using hi
        · simpa [progVal, values, hs0eq] using hv
      · have hnat : (b.step.natAbs : Int) = b.step :=
          Int.ofNat_natAbs_of_nonneg (by omega)
        refine ⟨i, hi, ?_⟩
        dsimp [progVal]
        unfold values
        simp only [hneg, hsp, ↓reduceIte]
        rw [hnat]
        grind
  · rintro ⟨k, hk, hv⟩
    by_cases hneg : b.step < 0
    · have hnat : (b.step.natAbs : Int) = -b.step := by
        rw [← Int.natAbs_neg b.step]
        exact Int.ofNat_natAbs_of_nonneg (by omega)
      have hk' : k < b.n := by simpa [values] using hk
      have hnpos : 0 < b.n := by omega
      refine ⟨b.n - 1 - k, by omega, ?_⟩
      dsimp [progVal] at hv
      unfold values at hv
      simp only [hneg, ↓reduceIte] at hv
      rw [hnat] at hv
      have hk_le : k ≤ b.n - 1 := by omega
      have hsubNat : b.n - 1 - k + k = b.n - 1 := Nat.sub_add_cancel hk_le
      have hsub : ((b.n - 1 - k : Nat) : Int) = ((b.n - 1 : Nat) : Int) - (k : Int) := by
        omega
      rw [hsub]
      grind
    · have hs0or : b.step = 0 ∨ 0 < b.step := by omega
      rcases hs0or with hs0 | hsp
      · have hs0eq : b.step = 0 := hs0
        simp [hs0eq, progVal, values] at hv
        refine ⟨k, ?_, ?_⟩
        · simpa [values] using hk
        · simpa [progVal, values, hs0eq] using hv
      · have hnat : (b.step.natAbs : Int) = b.step :=
            Int.ofNat_natAbs_of_nonneg (by omega)
        dsimp [progVal] at hv
        unfold values at hv
        simp only [hneg, hsp, ↓reduceIte] at hv
        rw [hnat] at hv
        exact ⟨k, by simpa [values] using hk, by grind⟩

private theorem count_bvals (b : Lane) (v : Int) :
    List.countP (fun i : Nat => v == b.base + b.step * (i : Int)) (List.range b.n) =
      if contains (values b) v then (if b.step == 0 then b.n else 1) else 0 := by
  by_cases hs0 : b.step == 0
  · have hs0eq : b.step = 0 := by simpa using hs0
    rw [hs0eq]
    simp only [Int.zero_mul, Int.add_zero, ↓reduceIte]
    rw [countP_const]
    by_cases hn : b.n = 0
    · rw [hn]
      simp [values]
    · have hnp : 0 < b.n := by omega
      by_cases hv : v == b.base
      · have hveq : v = b.base := beq_iff_eq.mp hv
        rw [if_pos hv]
        have hc : contains (values b) v = true := by
          simp [values, hs0eq, hveq, hnp, contains, lastVal]
        rw [if_pos hc]
        simp
      · rw [if_neg hv]
        have hc : ¬ contains (values b) v := by
          simp [values, hs0eq, hv, hnp, contains, lastVal]
          intro hle
          have hvne : v ≠ b.base := by intro he; exact hv (by rw [he]; simp)
          omega
        rw [if_neg hc]
  · have hsne : b.step ≠ 0 := by simpa using hs0
    rw [countP_map_eq]
    rw [countP_eq_count_rev]
    have hnd : ((List.range b.n).map (fun i : Nat => b.base + b.step * (i : Int))).Nodup :=
      affine_nodup b.base b.step hsne b.n
    by_cases hmem : v ∈ (List.range b.n).map (fun i : Nat => b.base + b.step * (i : Int))
    · have hc : contains (values b) v = true := by
        rw [← bvals_mem_values]
        rcases List.mem_map.mp hmem with ⟨i, hi, hiv⟩
        exact ⟨i, List.mem_range.mp hi, hiv.symm⟩
      rw [if_pos hc, count_eq_one_of_mem_nodup hnd hmem]
      rw [if_neg hs0]
    · have hc : ¬ contains (values b) v := by
        intro hc
        have hex := (bvals_mem_values b v).mpr hc
        rcases hex with ⟨i, hi, hiv⟩
        exact hmem (List.mem_map.mpr ⟨i, List.mem_range.mpr hi, hiv.symm⟩)
      rw [if_neg hc, List.count_eq_zero.mpr hmem]


private theorem intersection_const_left (z : Int) (n : Nat) (q : Progression) :
    intersection ⟨z, 0, n⟩ q =
      if n == 0 || q.n == 0 then ⟨0, 1, 0⟩ else ⟨z, 1, if contains q z then 1 else 0⟩ := by
  by_cases h : n == 0 || q.n == 0
  · simp [intersection, h]
  · simp [intersection, h]

private theorem joinCount_const (z : Int) (nA : Nat) (b : Lane) :
    joinCount (⟨0, 1, nA, z, 0⟩ : Lane) b =
      nA * (if contains (values b) z then (if b.step == 0 then b.n else 1) else 0) := by
  unfold joinCount
  have hva : values (⟨0, 1, nA, z, 0⟩ : Lane) = ⟨z, 0, nA⟩ := by
    simp [values]
  rw [hva, intersection_const_left]
  by_cases hcond : nA = 0 ∨ (values b).n = 0
  · by_cases hn0 : nA = 0
    · subst nA
      simp
    · have hbvn0 : (values b).n = 0 := by omega
      have hc : ¬ contains (values b) z := by
        rw [contains_iff_mem]
        simp [progMem, hbvn0]
      simp [hcond, hn0, hbvn0, hc]
  · have hnA : nA ≠ 0 := by omega
    have hbvn : (values b).n ≠ 0 := by omega
    by_cases hc : contains (values b) z = true
    · simp [hcond, hnA, hbvn, hc, Nat.mul_comm, Nat.mul_left_comm, Nat.mul_assoc]
    · simp [hcond, hnA, hbvn, hc]

private theorem joinCount_lane_eq (start stride n : Nat) (z : Int) (b : Lane) :
    joinCount (⟨start, stride, n, z, 0⟩ : Lane) b = joinCount (⟨0, 1, n, z, 0⟩ : Lane) b := by
  simp [joinCount, values]

theorem sound_ResidueJoin : ResidueJoin.Goal := by
  intro c b
  let p := 2 * width c
  have hp : 0 < p := by dsimp [p, width]; omega
  let make : Nat → Int × Int := fun r => (twVal c r, 0)
  let F : Int → Nat := fun v => List.countP
    (fun i : Nat => v == b.base + b.step * (i : Int)) (List.range b.n)
  rw [rows_tw]
  rw [List.map_map]
  change ((List.range c.n).map (fun i => List.countP
    (fun j : Nat => twVal c i == b.base + b.step * (j : Int)) (List.range b.n))).sum = _
  have hstrip := striped_sum_nat p hp c.n make F
  rw [show (fun i : Nat => F (stripedValue p make i)) = (fun i : Nat => F (twVal c i)) by
    funext i; rw [striped_tw]] at hstrip
  dsimp [make, F] at hstrip
  simp only [Int.zero_mul, Int.add_zero] at hstrip
  rw [hstrip]
  rw [lanes_tw]
  have hinner : ∀ r : Nat,
      ((List.range ((c.n - r + p - 1) / p)).map
        (fun _ : Nat => List.countP
          (fun i : Nat => twVal c r == b.base + b.step * (i : Int)) (List.range b.n))).sum =
      ((c.n - r + p - 1) / p) *
        List.countP (fun i : Nat => twVal c r == b.base + b.step * (i : Int)) (List.range b.n) := by
    intro r
    exact sum_range_const_nat _ _
  simp only [hinner]
  have hcnt : ∀ r : Nat, List.countP
      (fun i : Nat => twVal c r == b.base + b.step * (i : Int)) (List.range b.n) =
      if contains (values b) (twVal c r) then (if b.step == 0 then b.n else 1) else 0 := by
    intro r
    exact count_bvals b (twVal c r)
  simp only [hcnt]
  -- reduce the lane-side sum over r < min p n
  have htrunc : ((List.range p).map (fun r =>
      ((c.n - r + p - 1) / p) *
        (if contains (values b) (twVal c r) then (if b.step == 0 then b.n else 1) else 0))).sum =
      ((List.range (min p c.n)).map (fun r =>
      ((c.n - r + p - 1) / p) *
        (if contains (values b) (twVal c r) then (if b.step == 0 then b.n else 1) else 0))).sum := by
    let f : Nat → Nat := fun r =>
      ((c.n - r + p - 1) / p) *
        (if contains (values b) (twVal c r) then (if b.step == 0 then b.n else 1) else 0)
    have htail : ∀ r ∈ List.range p, ¬ r < min p c.n → (c.n - r + p - 1) / p = 0 := by
      intro r hr hnot
      have hrp : r < p := List.mem_range.mp hr
      have hnle : c.n ≤ r := by
        by_cases hpn : p ≤ c.n
        · have hmin : min p c.n = p := Nat.min_eq_left hpn
          omega
        · have hmin : min p c.n = c.n := Nat.min_eq_right (Nat.le_of_not_ge hpn)
          omega
      have heq : c.n - r + p - 1 = p - 1 := by omega
      rw [heq, Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))]
    have hf : (List.range p).map f = (List.range p).map (fun r => if r < min p c.n then f r else 0) := by
      apply List.map_congr_left
      intro r hr
      by_cases hlt : r < min p c.n
      · simp [f, hlt]
      · simp [f, hlt, htail r hr hlt]
    change ((List.range p).map f).sum = ((List.range (min p c.n)).map f).sum
    rw [hf]
    exact sum_range_trunc f (min p c.n) p (Nat.min_le_left _ _)
  rw [htrunc]
  rw [show 2 * width c = p by rfl]
  rw [List.map_map]
  change ((List.range (min p c.n)).map (fun r =>
      ((c.n - r + p - 1) / p) *
        (if contains (values b) (twVal c r) then (if b.step == 0 then b.n else 1) else 0))).sum =
    ((List.range (min p c.n)).map (fun r =>
      joinCount (lane c.n r p (twVal c r) 0) b)).sum
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  simp only [lane]
  rw [joinCount_lane_eq, joinCount_const]


private theorem countP_neg (k : Nat) (P : Nat → Bool) :
    (List.range k).countP (fun q => !P q) = k - (List.range k).countP P := by
  induction k with
  | zero => simp
  | succ k ih =>
      rw [show k + 1 = Nat.succ k by omega, List.range_succ, List.countP_append, List.countP_append]
      have hle : (List.range k).countP P ≤ k := by
        rw [List.countP_eq_length_filter]
        simpa using (List.length_filter_le P (List.range k))
      by_cases h : P k <;> simp [h, ih] <;> omega

private theorem sum_not_contains (k : Nat) (z : Int) (P : Nat → Bool) :
    ((List.range k).map (fun q => if P q then 0 else z)).sum =
      ((k - (List.range k).countP P : Nat) : Int) * z := by
  rw [show (fun q => if P q then 0 else z) = (fun q => if (!P q) then z else 0) by
    funext q
    by_cases h : P q <;> simp [h]]
  rw [← sum_map_ite_zero (List.range k) (fun q => !P q) (fun _ => z)]
  rw [sum_map_const_int]
  rw [List.countP_eq_length_filter]
  rw [show ((List.range k).filter (fun q => !P q)).length = k - (List.range k).countP P by
    rw [← List.countP_eq_length_filter, countP_neg]]
  rw [List.countP_eq_length_filter]

private theorem countP_map_fun {α : Type} (l : List α) (f : α → Int) (P : Int → Bool) :
    (l.map f).countP P = l.countP (fun x => P (f x)) := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : P (f x) <;> simp [h, ih]

private theorem partialSum_const (start stride n : Nat) (z : Int) :
    partialSum (⟨start, stride, n, z, 0⟩ : Lane) n = (n : Int) * z := by
  unfold partialSum
  simp

private theorem deletedSum_const (n start stride : Nat) (z : Int) (m : Progression) :
    deletedSum (⟨start, stride, n, z, 0⟩ : Lane) m =
      ((intersection (⟨start, stride, n⟩ : Progression) m).n : Int) * z := by
  unfold deletedSum firstAt slopeAt positions
  simp

private theorem count_del_positions (r p n : Nat) (hp : 0 < p) (m : Progression) :
    (List.range ((n - r + p - 1) / p)).countP (fun q => contains m ((r + q * p : Nat) : Int)) =
      (intersection (⟨r, p, (n - r + p - 1) / p⟩ : Progression) m).n := by
  let A : Progression := ⟨r, p, (n - r + p - 1) / p⟩
  have hApos : 0 < A.step := by dsimp [A]; exact hp
  have hperm := perm_progVals_intersection A m hApos
  have hlen := hperm.length_eq
  have hprog : progVals A = (List.range ((n - r + p - 1) / p)).map (fun q => ((r + q * p : Nat) : Int)) := by
    unfold progVals
    dsimp [A]
  have hlenI : (progVals (intersection A m)).length = (intersection A m).n := by
    simp [progVals]
  rw [hprog] at hlen
  rw [← List.countP_eq_length_filter] at hlen
  rw [countP_map_fun] at hlen
  rw [hlenI] at hlen
  simpa [A] using hlen.symm

private theorem filter_range_lt (p k : Nat) (hk : k ≤ p) :
    (List.range p).filter (fun r => decide (r < k)) = List.range k := by
  induction p with
  | zero =>
      have : k = 0 := by omega
      subst k
      simp
  | succ p ih =>
      rw [show p + 1 = Nat.succ p by omega, List.range_succ, List.filter_append]
      by_cases hkp : k ≤ p
      · rw [ih hkp]
        simp [show ¬ p < k by omega]
      · have hkeq : k = p + 1 := by omega
        subst k
        have hf : (List.range p).filter (fun r => decide (r < p + 1)) = List.range p := by
          apply List.filter_eq_self.mpr
          intro r hr
          have hrp : r < p := List.mem_range.mp hr
          simp [show r < p + 1 by omega]
        rw [hf]
        simp [List.range_succ]

private theorem rows_filter_sum (c : Config) (P : Nat → Bool) :
    (((TinylambOpt.ResearchGoals.Atlas.EncodedPages.rows layout c).filter
      (fun r => P r.1)).map Prod.snd).sum =
    ((List.range c.n).map (fun i => if P i then twVal c i else 0)).sum := by
  rw [rows_tw]
  rw [show ((List.range c.n).map (fun i => (i, twVal c i))).filter (fun r => P r.1) =
      ((List.range c.n).filter (fun i => P i)).map (fun i => (i, twVal c i)) by
    induction List.range c.n with
    | nil => simp
    | cons x xs ih => by_cases h : P x <;> simp [h, ih]]
  simp only [List.map_map]
  rw [show (Prod.snd ∘ fun i => (i, twVal c i)) = twVal c by funext i; rfl]
  rw [← sum_map_ite_zero]

theorem sound_DeleteProgression : DeleteProgression.Goal := by
  intro c mask
  let p := 2 * width c
  have hp : 0 < p := by dsimp [p, width]; omega
  let make : Nat → Int × Int := fun r => (twVal c r, 0)
  let g : Nat → Int → Int := fun i v => if contains mask i then 0 else v
  rw [rows_filter_sum c (fun i => !contains mask i)]
  rw [show (fun i : Nat => if (!contains mask i) = true then twVal c i else 0) =
      (fun i : Nat => g i (twVal c i)) by
    funext i
    dsimp [g]
    by_cases h : contains mask i <;> simp [h]]
  have hstrip := striped_sum p hp c.n make g
  rw [show (fun i : Nat => g i (stripedValue p make i)) = (fun i : Nat => g i (twVal c i)) by
    funext i; rw [striped_tw]] at hstrip
  rw [hstrip]
  dsimp [make, g]
  simp only [Int.zero_mul, Int.add_zero]
  rw [lanes_tw]
  rw [show 2 * width c = p by rfl]
  rw [List.map_map]
  change _ = ((List.range (min p c.n)).map (fun r =>
    partialSum (lane c.n r p (twVal c r) 0) (lane c.n r p (twVal c r) 0).n -
      deletedSum (lane c.n r p (twVal c r) 0) mask)).sum
  have hinner : ∀ r : Nat,
      ((List.range ((c.n - r + p - 1) / p)).map
        (fun q => if contains mask (((r : Nat) : Int) + ((q : Nat) : Int) * ((p : Nat) : Int)) then 0 else twVal c r)).sum =
      ((twVal c r) * (((c.n - r + p - 1) / p) -
        (intersection (⟨r, p, (c.n - r + p - 1) / p⟩ : Progression) mask).n : Nat) : Int) := by
    intro r
    have hcast : ∀ q : Nat, ((r : Nat) : Int) + ((q : Nat) : Int) * ((p : Nat) : Int) = ((r + q * p : Nat) : Int) := by
      intro q
      omega
    rw [show (fun q : Nat => if contains mask (((r : Nat) : Int) + ((q : Nat) : Int) * ((p : Nat) : Int)) then 0 else twVal c r) =
        (fun q : Nat => if contains mask ((r + q * p : Nat) : Int) then 0 else twVal c r) by
      funext q
      rw [hcast]]
    rw [sum_not_contains]
    rw [count_del_positions r p c.n hp mask]
    rw [Int.mul_comm]
  simp only [hinner]
  have htrunc : ((List.range p).map (fun r =>
      (twVal c r * (((c.n - r + p - 1) / p) -
        (intersection (⟨r, p, (c.n - r + p - 1) / p⟩ : Progression) mask).n : Nat) : Int))).sum =
      ((List.range (min p c.n)).map (fun r =>
      (twVal c r * (((c.n - r + p - 1) / p) -
        (intersection (⟨r, p, (c.n - r + p - 1) / p⟩ : Progression) mask).n : Nat) : Int))).sum := by
    let f : Nat → Int := fun r =>
      twVal c r * (((c.n - r + p - 1) / p -
        (intersection (⟨r, p, (c.n - r + p - 1) / p⟩ : Progression) mask).n : Nat) : Int)
    have htail : ∀ r ∈ List.range p, ¬ r < min p c.n → f r = 0 := by
      intro r hr hnot
      have hrp : r < p := List.mem_range.mp hr
      have hnle : c.n ≤ r := by
        by_cases hpn : p ≤ c.n
        · have hmin : min p c.n = p := Nat.min_eq_left hpn
          omega
        · have hmin : min p c.n = c.n := Nat.min_eq_right (Nat.le_of_not_ge hpn)
          omega
      have hq : (c.n - r + p - 1) / p = 0 := by
        have heq : c.n - r + p - 1 = p - 1 := by omega
        rw [heq, Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))]
      dsimp [f]
      rw [hq]
      simp
    have hf : (List.range p).map f = (List.range p).map (fun r => if r < min p c.n then f r else 0) := by
      apply List.map_congr_left
      intro r hr
      by_cases hlt : r < min p c.n
      · simp [hlt]
      · simp [hlt, htail r hr hlt]
    change ((List.range p).map f).sum = ((List.range (min p c.n)).map f).sum
    rw [hf]
    rw [show (fun r : Nat => if r < min p c.n then f r else 0) =
        (fun r : Nat => if decide (r < min p c.n) then f r else 0) by
      funext r
      by_cases h : r < min p c.n <;> simp [h]]
    rw [← sum_map_ite_zero]
    rw [filter_range_lt p (min p c.n) (Nat.min_le_left _ _)]
  rw [htrunc]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  simp only [lane]
  rw [partialSum_const, deletedSum_const]
  let k := (intersection (⟨r, p, (c.n - r + p - 1) / p⟩ : Progression) mask).n
  have hk : k ≤ (c.n - r + p - 1) / p := by
    dsimp [k]
    rw [← count_del_positions r p c.n hp mask]
    rw [List.countP_eq_length_filter]
    simpa using (List.length_filter_le (fun q => contains mask ((r + q * p : Nat) : Int)) (List.range ((c.n - r + p - 1) / p)))
  have hcast : (((c.n - r + p - 1) / p - k : Nat) : Int) =
      (((c.n - r + p - 1) / p : Nat) : Int) - ((k : Nat) : Int) := by
    omega
  rw [hcast]
  rw [Int.mul_sub]
  rw [Int.mul_comm (twVal c r) (((c.n - r + p - 1) / p : Nat) : Int)]
  rw [Int.mul_comm (twVal c r) ((k : Nat) : Int)]


private def bvalAt (b : Lane) (pos : Int) : Int :=
  b.base + b.step * ((pos - (b.start : Int)) / (b.stride : Int))

private theorem bvalAt_progVal (b : Lane) (hstride : 0 < b.stride) (R : Progression)
    (hR : 0 < R.step) (hdvd : b.stride ∣ R.step)
    (hlo : contains (positions b) R.lo) : ∀ k : Nat,
    bvalAt b (progVal R k) =
      bvalAt b R.lo + (k : Int) * (((R.step / b.stride : Nat) : Int) * b.step) := by
  intro k
  have hstrne : (b.stride : Int) ≠ 0 := by omega
  obtain ⟨m, hm⟩ := hdvd
  have hmInt : (R.step : Int) = (b.stride : Int) * (m : Int) := by exact_mod_cast hm
  obtain ⟨j, hj, hjval⟩ := (contains_iff_mem (positions b) R.lo).mp hlo
  dsimp [positions, progVal] at hjval
  have hdivlo : (R.lo - (b.start : Int)) / (b.stride : Int) = (j : Int) := by
    apply Int.ediv_eq_of_eq_mul_right hstrne
    grind
  have harg : progVal R k - (b.start : Int) =
      (b.stride : Int) * ((j : Int) + (k : Int) * (m : Int)) := by
    unfold progVal
    rw [hmInt]
    grind
  have hdivk : (progVal R k - (b.start : Int)) / (b.stride : Int) =
      (j : Int) + (k : Int) * (m : Int) := by
    rw [harg]
    exact Int.mul_ediv_cancel_left ((j : Int) + (k : Int) * (m : Int)) hstrne
  have hmdvd : R.step / b.stride = m := by
    rw [hm]
    exact Nat.mul_div_right m hstride
  unfold bvalAt
  rw [hdivlo, hdivk, hmdvd]
  grind

private theorem intersection_step_dvd (A B : Progression) (hA : 0 < A.step) (hB : 0 < B.step)
    (hR : 1 < (intersection A B).n) : B.step ∣ (intersection A B).step := by
  let R := intersection A B
  have hRpos : 0 < R.step := intersection_step_pos A B hA
  have hRn : 1 < R.n := by simpa [R] using hR
  have hmemR : ∀ k < R.n, contains R (progVal R k) = true := by
    intro k hk
    rw [contains_iff_mem]
    exact ⟨k, hk, rfl⟩
  have hmemB : ∀ k < R.n, contains B (progVal R k) = true := by
    intro k hk
    exact ((intersection_contains A B (progVal R k)).mp (hmemR k hk)).2
  have h0 := (contains_iff_mem B R.lo).mp (by
    simpa [progVal] using hmemB 0 (by omega))
  have h1 := (contains_iff_mem B (R.lo + R.step)).mp (by
    have hh := hmemB 1 (by omega)
    simpa [progVal] using hh)
  rcases h0 with ⟨i0, hi0, hv0⟩
  rcases h1 with ⟨i1, hi1, hv1⟩
  unfold progVal at hv0 hv1
  have hi01 : i0 ≤ i1 := by
    have hsB : (0 : Int) < B.step := by exact_mod_cast hB
    have hle : (i0 : Int) * B.step ≤ (i1 : Int) * B.step := by grind
    exact_mod_cast (Int.le_of_mul_le_mul_right hle hsB)
  have hdiff : R.step = (i1 - i0) * B.step := by
    have hsub : ((i1 - i0 : Nat) : Int) = (i1 : Int) - (i0 : Int) := by omega
    grind
  change B.step ∣ R.step
  refine ⟨i1 - i0, ?_⟩
  rw [hdiff, Nat.mul_comm]

private theorem aligned_const (a b : Lane) (ha : a.step = 0) :
    aligned a b = a.base *
      (((intersection (positions a) (positions b)).n : Int) *
        firstAt b (intersection (positions a) (positions b)) +
        tri (intersection (positions a) (positions b)).n *
          (slopeAt b (intersection (positions a) (positions b)))) := by
  unfold aligned firstAt slopeAt
  simp [ha]
  grind



private theorem bvalAt_lo_eq_first (b : Lane) (hb : 0 < b.stride) (R : Progression)
    (hlo : contains (positions b) R.lo) : bvalAt b R.lo = firstAt b R := by
  obtain ⟨i, hi, hiv⟩ := (contains_iff_mem (positions b) R.lo).mp hlo
  dsimp [positions, progVal] at hiv
  have hdiv : (R.lo - (b.start : Int)) / (b.stride : Int) = (i : Int) := by
    apply Int.ediv_eq_of_eq_mul_right (by omega : (b.stride : Int) ≠ 0)
    grind
  unfold bvalAt firstAt
  rw [hdiv]
  rw [Int.mul_comm b.step (i : Int)]

private theorem sum_affine (n : Nat) (y dy : Int) :
    ((List.range n).map (fun k : Nat => y + (k : Int) * dy)).sum = (n : Int) * y + tri n * dy := by
  induction n with
  | zero => simp [tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      rw [ih, tri_succ]
      grind

private theorem aligned_sum_aux (b : Lane) (hb : 0 < b.stride) (R : Progression)
    (hRpos : 0 < R.step) (hmem : ∀ k < R.n, contains (positions b) (progVal R k) = true) :
    ((progVals R).map (bvalAt b)).sum =
      (R.n : Int) * firstAt b R + tri R.n * slopeAt b R := by
  by_cases h0 : R.n = 0
  · simp [progVals, h0, tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  by_cases h1 : R.n = 1
  · unfold progVals
    rw [h1]
    simp
    have hlo : contains (positions b) R.lo := by simpa [progVal] using hmem 0 (by omega)
    rw [bvalAt_lo_eq_first b hb R hlo]
    unfold slopeAt
    dsimp [positions]
    simp [tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  · have hn2 : 1 < R.n := by omega
    have hlo : contains (positions b) R.lo := by simpa [progVal] using hmem 0 (by omega)
    have hnext : contains (positions b) (R.lo + R.step) := by
      simpa [progVal] using hmem 1 (by omega)
    obtain ⟨i0, hi0, hv0⟩ := (contains_iff_mem (positions b) R.lo).mp hlo
    obtain ⟨i1, hi1, hv1⟩ := (contains_iff_mem (positions b) (R.lo + R.step)).mp hnext
    dsimp [positions, progVal] at hv0 hv1
    have hsB : (0 : Int) < b.stride := by exact_mod_cast hb
    have hi01 : i0 ≤ i1 := by
      have hle : (i0 : Int) * b.stride ≤ (i1 : Int) * b.stride := by grind
      exact_mod_cast (Int.le_of_mul_le_mul_right hle hsB)
    have hdiff : R.step = (i1 - i0) * b.stride := by
      grind
    have hdvd : b.stride ∣ R.step := ⟨i1 - i0, by rw [hdiff, Nat.mul_comm]⟩
    unfold progVals
    rw [List.map_map]
    rw [show (bvalAt b ∘ fun k : Nat => R.lo + (k : Int) * R.step) =
        (fun k : Nat => firstAt b R + (k : Int) * slopeAt b R) by
      funext k
      change bvalAt b (progVal R k) = firstAt b R + (k : Int) * slopeAt b R
      rw [bvalAt_progVal b hb R hRpos hdvd hlo k]
      rw [bvalAt_lo_eq_first b hb R hlo]
      unfold slopeAt
      dsimp [positions]]
    exact sum_affine R.n (firstAt b R) (slopeAt b R)

private theorem sum_eq_single_int (p r : Nat) (hr : r < p) (f : Nat → Int) :
    ((List.range p).map (fun x => if x == r then f x else 0)).sum = f r := by
  have hconst : ∀ x, (if x == r then f x else 0) = if x == r then f r else 0 := by
    intro x
    by_cases h : x == r
    · have h' : x = r := beq_iff_eq.mp h
      simp [h, h']
    · simp [h]
  rw [show (List.range p).map (fun x => if x == r then f x else 0) =
      (List.range p).map (fun x => if x == r then f r else 0) by
    apply List.map_congr_left
    intro x hx
    exact hconst x]
  rw [show (List.range p).map (fun x => if x == r then f r else 0) =
      (List.range p).map (fun x => if x == r then f r + 0 else 0) by
    apply List.map_congr_left
    intro x hx
    by_cases h : x == r <;> simp [h]]
  rw [sum_update p r hr (fun _ : Nat => 0) (f r)]
  rw [sum_map_zero]
  simp

private theorem sum_map_mul_left_int {α : Type} (l : List α) (c : Int) (f : α → Int) :
    ((l.map fun x => c * f x).sum) = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.mul_add]

private theorem bvalAt_of_nat_match (b : Lane) (hb : 0 < b.stride) (pos i : Nat)
    (h : pos = b.start + i * b.stride) :
    bvalAt b (pos : Int) = b.base + b.step * (i : Int) := by
  unfold bvalAt
  have hcast : (pos : Int) = (b.start : Int) + (i : Int) * (b.stride : Int) := by
    exact_mod_cast h
  have hdiv : ((pos : Int) - (b.start : Int)) / (b.stride : Int) = (i : Int) := by
    apply Int.ediv_eq_of_eq_mul_right (by omega : (b.stride : Int) ≠ 0)
    grind
  rw [hdiv]

private theorem inner_sum_match (b : Lane) (hb : 0 < b.stride) (z : Int) (pos i : Nat)
    (hi : i < b.n) (hmatch : pos = b.start + i * b.stride) :
    ((List.range b.n).map (fun j => if pos == b.start + j * b.stride then
      z * (b.base + b.step * (j : Int)) else 0)).sum = z * bvalAt b (pos : Int) := by
  have hfun : ∀ j : Nat, (if pos == b.start + j * b.stride then
      z * (b.base + b.step * (j : Int)) else 0) =
      if j == i then z * bvalAt b (pos : Int) else 0 := by
    intro j
    by_cases hji : j == i
    · have hjeq : j = i := beq_iff_eq.mp hji
      subst j
      have hpj : pos == b.start + i * b.stride := by rw [hmatch]; simp
      rw [if_pos hpj, if_pos hji]
      congr 1
      exact (bvalAt_of_nat_match b hb pos i hmatch).symm
    · have hnone : ¬ pos == b.start + j * b.stride := by
        intro hpj
        have hpj' : pos = b.start + j * b.stride := beq_iff_eq.mp hpj
        have heq : b.start + i * b.stride = b.start + j * b.stride := by omega
        have hmul : i * b.stride = j * b.stride := by omega
        have hij : i = j := Nat.eq_of_mul_eq_mul_right hb (by simpa [Nat.mul_comm] using hmul)
        exact hji (by rw [hij]; simp)
      simp [hnone, hji]
  rw [show (List.range b.n).map (fun j => if pos == b.start + j * b.stride then
      z * (b.base + b.step * (j : Int)) else 0) =
      (List.range b.n).map (fun j => if j == i then z * bvalAt b (pos : Int) else 0) by
    apply List.map_congr_left
    intro j hj
    exact hfun j]
  rw [sum_eq_single_int b.n i hi (fun _ => z * bvalAt b (pos : Int))]

private theorem inner_sum_nomatch (b : Lane) (hb : 0 < b.stride) (z : Int) (pos : Nat)
    (hnone : ∀ i, i < b.n → pos ≠ b.start + i * b.stride) :
    ((List.range b.n).map (fun j => if pos == b.start + j * b.stride then
      z * (b.base + b.step * (j : Int)) else 0)).sum = 0 := by
  rw [show (List.range b.n).map (fun j => if pos == b.start + j * b.stride then
      z * (b.base + b.step * (j : Int)) else 0) =
      (List.range b.n).map (fun _ : Nat => 0) by
    apply List.map_congr_left
    intro j hj
    have hjlt : j < b.n := List.mem_range.mp hj
    have hne := hnone j hjlt
    have hnebeq : ¬ pos == b.start + j * b.stride := by
      intro he
      exact hne (beq_iff_eq.mp he)
    simp [hnebeq]]
  rw [sum_map_zero]

private theorem aligned_lane_aux (r p nA : Nat) (hp : 0 < p) (z : Int) (b : Lane)
    (hb : 0 < b.stride) :
    ((List.range nA).map (fun q =>
      ((List.range b.n).map (fun i => if r + q * p == b.start + i * b.stride then
        z * (b.base + b.step * (i : Int)) else 0)).sum)).sum =
    aligned (⟨r, p, nA, z, 0⟩ : Lane) b := by
  let A : Progression := ⟨r, p, nA⟩
  let B : Progression := positions b
  have hA : 0 < A.step := by dsimp [A]; exact hp
  have hRpos : 0 < (intersection A B).step := intersection_step_pos A B hA
  have hmemR : ∀ k < (intersection A B).n, contains (intersection A B) (progVal (intersection A B) k) = true := by
    intro k hk
    rw [contains_iff_mem]
    exact ⟨k, hk, rfl⟩
  have hmemB : ∀ k < (intersection A B).n, contains B (progVal (intersection A B) k) = true := by
    intro k hk
    exact ((intersection_contains A B (progVal (intersection A B) k)).mp (hmemR k hk)).2
  have hq : ∀ q < nA,
      ((List.range b.n).map (fun i => if r + q * p == b.start + i * b.stride then
        z * (b.base + b.step * (i : Int)) else 0)).sum =
      if contains B ((r + q * p : Nat) : Int) then z * bvalAt b ((r + q * p : Nat) : Int) else 0 := by
    intro q hq
    have hcastq : ((r + q * p : Nat) : Int) = (r : Int) + (q : Int) * (p : Int) := by omega
    by_cases hc : contains B ((r + q * p : Nat) : Int) = true
    · obtain ⟨i, hi, hiv⟩ := (contains_iff_mem B ((r + q * p : Nat) : Int)).mp hc
      dsimp [B, positions, progVal] at hiv
      have hivNat : r + q * p = b.start + i * b.stride := by omega
      rw [inner_sum_match b hb z (r + q * p) i hi hivNat]
      simp [hcastq] at hc ⊢
      simp [hc]
    · have hnone : ∀ i, i < b.n → r + q * p ≠ b.start + i * b.stride := by
        intro i hi he
        apply hc
        rw [contains_iff_mem]
        exact ⟨i, hi, by dsimp [B, positions, progVal]; omega⟩
      rw [inner_sum_nomatch b hb z (r + q * p) hnone]
      simp [hcastq] at hc ⊢
      simp [hc]
  rw [show (List.range nA).map (fun q : Nat => ((List.range b.n).map (fun i => if r + q * p == b.start + i * b.stride then
      z * (b.base + b.step * (i : Int)) else 0)).sum) =
      (List.range nA).map (fun q : Nat => if contains B ((r + q * p : Nat) : Int) then
        z * bvalAt b ((r + q * p : Nat) : Int) else 0) by
    apply List.map_congr_left
    intro q hqmem
    exact hq q (List.mem_range.mp hqmem)]
  rw [show (List.range nA).map (fun q : Nat => if contains B ((r + q * p : Nat) : Int) then
      z * bvalAt b ((r + q * p : Nat) : Int) else 0) =
      (List.range nA).map (fun q : Nat => z * (if contains B ((r + q * p : Nat) : Int) then
        bvalAt b ((r + q * p : Nat) : Int) else 0)) by
    apply List.map_congr_left
    intro q hqmem
    have hcastq : ((r + q * p : Nat) : Int) = (r : Int) + (q : Int) * (p : Int) := by omega
    by_cases hc : contains B ((r + q * p : Nat) : Int)
    · simp [hcastq] at hc ⊢
      simp [hc]
    · simp [hcastq] at hc ⊢
      simp [hc]]
  rw [sum_map_mul_left_int]
  rw [← sum_map_ite_zero (List.range nA)
      (fun q : Nat => contains B ((r + q * p : Nat) : Int))
      (fun q : Nat => bvalAt b ((r + q * p : Nat) : Int))]
  have hprogA : progVals A = (List.range nA).map (fun q => ((r + q * p : Nat) : Int)) := by
    unfold progVals A
    rfl
  rw [show ((List.range nA).filter (fun q : Nat => contains B ((r + q * p : Nat) : Int))).map
        (fun q : Nat => bvalAt b ((r + q * p : Nat) : Int)) =
      ((progVals A).filter (contains B)).map (bvalAt b) by
    rw [hprogA, List.filter_map, List.map_map]
    rfl]
  rw [← sum_progVals_filter A B hA (bvalAt b)]
  rw [aligned_sum_aux b hb (intersection A B) hRpos hmemB]
  rw [aligned_const (a := (⟨r, p, nA, z, 0⟩ : Lane)) (b := b) (by rfl)]
  rfl

theorem sound_AlignedProduct : AlignedProduct.Goal := by
  intro c b hstride
  let p := 2 * width c
  have hp : 0 < p := by dsimp [p, width]; omega
  let make : Nat → Int × Int := fun r => (twVal c r, 0)
  let g : Nat → Int → Int := fun i v =>
    ((List.range b.n).map (fun j => if i == b.start + j * b.stride then
      v * (b.base + b.step * (j : Int)) else 0)).sum
  rw [rows_tw]
  rw [List.map_map]
  change ((List.range c.n).map (fun i => g i (twVal c i))).sum = _
  have hstrip := striped_sum p hp c.n make g
  rw [show (fun i : Nat => g i (stripedValue p make i)) = (fun i : Nat => g i (twVal c i)) by
    funext i; rw [striped_tw]] at hstrip
  rw [hstrip]
  dsimp [make, g]
  simp only [Int.zero_mul, Int.add_zero]
  rw [lanes_tw]
  rw [show 2 * width c = p by rfl]
  rw [List.map_map]
  change _ = ((List.range (min p c.n)).map (fun r =>
    aligned (lane c.n r p (twVal c r) 0) b)).sum
  let f : Nat → Int := fun r =>
    ((List.range ((c.n - r + p - 1) / p)).map (fun q =>
      ((List.range b.n).map (fun i => if r + q * p == b.start + i * b.stride then
        twVal c r * (b.base + b.step * (i : Int)) else 0)).sum)).sum
  have htail : ∀ r ∈ List.range p, ¬ r < min p c.n → f r = 0 := by
    intro r hr hnot
    have hrp : r < p := List.mem_range.mp hr
    have hnle : c.n ≤ r := by
      by_cases hpn : p ≤ c.n
      · have hmin : min p c.n = p := Nat.min_eq_left hpn
        omega
      · have hmin : min p c.n = c.n := Nat.min_eq_right (Nat.le_of_not_ge hpn)
        omega
    have hq : (c.n - r + p - 1) / p = 0 := by
      have heq : c.n - r + p - 1 = p - 1 := by omega
      rw [heq, Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))]
    dsimp [f]
    rw [hq]
    simp
  have hf : (List.range p).map f = (List.range p).map (fun r => if r < min p c.n then f r else 0) := by
    apply List.map_congr_left
    intro r hr
    by_cases hlt : r < min p c.n
    · simp [hlt]
    · simp [hlt, htail r hr hlt]
  have htrunc : ((List.range p).map f).sum = ((List.range (min p c.n)).map f).sum := by
    rw [hf]
    rw [show (fun r : Nat => if r < min p c.n then f r else 0) =
        (fun r : Nat => if decide (r < min p c.n) then f r else 0) by
      funext r
      by_cases h : r < min p c.n <;> simp [h]]
    rw [← sum_map_ite_zero]
    rw [filter_range_lt p (min p c.n) (Nat.min_le_left _ _)]
  change ((List.range p).map f).sum = _
  rw [htrunc]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  dsimp [f]
  exact aligned_lane_aux r p ((c.n - r + p - 1) / p) hp (twVal c r) b hstride

end TinylambOpt.ResearchProofs.Atlas.EncodedPages.TriangleWave.Remaining

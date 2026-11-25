import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.Round2.Task50

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RestartedDelta

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
      simp [hs'] at hhi hlo ⊢
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

private theorem contains_if_single (lo : Int) (c : Bool) (v : Int) :
    contains ⟨lo, 1, if c then 1 else 0⟩ v = true ↔ c = true ∧ v = lo := by
  by_cases hc : c = true
  · subst c
    simp
    rw [contains_iff_mem]
    unfold progMem progVal
    constructor
    · rintro ⟨k, hk, hval⟩
      have hk' : k < 1 := by simpa using hk
      have hk0 : k = 0 := by omega
      subst k
      simpa using hval
    · intro hv
      subst v
      refine ⟨0, by simp, ?_⟩
      simp
  · have hcf : c = false := by cases c <;> simp_all
    simp [hcf, contains]

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
    exact Int.le_add_of_nonneg_right (Int.mul_nonneg (by omega) (by omega))
  · rw [hval]
    exact Int.add_le_add_left hmul' a.lo

private theorem contains_dvd (a : Progression) (v : Int) (h : contains a v = true) :
    (a.step : Int) ∣ v - a.lo := by
  obtain ⟨k, _, hval⟩ := (contains_iff_mem a v).mp h
  unfold progVal at hval
  refine ⟨(k : Int), ?_⟩
  rw [hval]
  calc
    a.lo + (k : Int) * a.step - a.lo = (k : Int) * a.step := by omega
    _ = a.step * (k : Int) := Int.mul_comm _ _

private theorem common_gap_dvd (a b : Progression) (v : Int)
    (ha : contains a v = true) (hb : contains b v = true) :
    (a.step.gcd b.step : Int) ∣ b.lo - a.lo := by
  obtain ⟨i, _, hi⟩ := (contains_iff_mem a v).mp ha
  obtain ⟨j, _, hj⟩ := (contains_iff_mem b v).mp hb
  unfold progVal at hi hj
  have hgdr : (a.step.gcd b.step : Int) ∣ (b.step : Int) := by
    exact_mod_cast Nat.gcd_dvd_right a.step b.step
  have hgdl : (a.step.gcd b.step : Int) ∣ (a.step : Int) := by
    exact_mod_cast Nat.gcd_dvd_left a.step b.step
  have h1 : (a.step.gcd b.step : Int) ∣ (b.step : Int) * (j : Int) := by
    simpa [Int.mul_comm] using
      (Int.dvd_mul_of_dvd_right hgdr : (a.step.gcd b.step : Int) ∣ (j : Int) * (b.step : Int))
  have h2 : (a.step.gcd b.step : Int) ∣ (a.step : Int) * (i : Int) := by
    simpa [Int.mul_comm] using
      (Int.dvd_mul_of_dvd_right hgdl : (a.step.gcd b.step : Int) ∣ (i : Int) * (a.step : Int))
  have heq : (a.step : Int) * (i : Int) - (b.step : Int) * (j : Int) = b.lo - a.lo := by
    have hA : (i : Int) * (a.step : Int) = v - a.lo := by omega
    have hB : (j : Int) * (b.step : Int) = v - b.lo := by omega
    calc
      (a.step : Int) * (i : Int) - (b.step : Int) * (j : Int) =
          (i : Int) * (a.step : Int) - (j : Int) * (b.step : Int) := by
            rw [Int.mul_comm (a.step : Int) (i : Int), Int.mul_comm (b.step : Int) (j : Int)]
      _ = (v - a.lo) - (v - b.lo) := by rw [hA, hB]
      _ = b.lo - a.lo := by omega
  rw [← heq]
  exact Int.dvd_sub h2 h1

private theorem lcm_dvd_of_dvd (s t : Nat) (hs : 0 < s) (ht : 0 < t) (delta : Int)
    (hds : (s : Int) ∣ delta) (hdt : (t : Int) ∣ delta) :
    (((s / s.gcd t) * t : Nat) : Int) ∣ delta := by
  let g := s.gcd t
  let sa := s / g
  let tb := t / g
  have hg : 0 < g := Nat.gcd_pos_of_pos_left t hs
  have hsEq : (s : Int) = (g : Int) * (sa : Int) := by
    exact_mod_cast (by simpa [sa, g, Nat.mul_comm] using (Nat.div_mul_cancel (Nat.gcd_dvd_left s t)).symm)
  have htEq : (t : Int) = (g : Int) * (tb : Int) := by
    exact_mod_cast (by simpa [tb, g, Nat.mul_comm] using (Nat.div_mul_cancel (Nat.gcd_dvd_right s t)).symm)
  have hcop : sa.gcd tb = 1 := by simpa [sa, tb, g] using reduced_coprime s t hs ht
  obtain ⟨k1, hk1⟩ := hds
  obtain ⟨k2, hk2⟩ := hdt
  rcases hpair : bezout sa tb with ⟨u, v⟩
  have hbez := bezout_spec sa tb
  rw [hpair] at hbez
  change (sa.gcd tb : Int) = (sa : Int) * u + (tb : Int) * v at hbez
  rw [hcop] at hbez
  simp only [Int.ofNat_one] at hbez
  refine ⟨u*k2 + v*k1, ?_⟩
  change delta = ((sa * t : Nat) : Int) * (u*k2 + v*k1)
  rw [Int.natCast_mul, htEq]
  grind

private theorem span_contains_iff_nat (first : Int) (period : Nat) (high v : Int)
    (hp : 0 < period) :
    contains ⟨first, period, if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ v = true ↔
      first ≤ v ∧ v ≤ high ∧ (period : Int) ∣ v - first := by
  have hpI : (0 : Int) < period := by exact_mod_cast hp
  have h := span_contains_iff first (period : Int) high v hpI
  simpa [Int.toNat_of_nonneg (by omega : 0 ≤ (period : Int))] using h

private theorem intersection_contains (a b : Progression) (v : Int) :
    contains (intersection a b) v = true ↔ contains a v = true ∧ contains b v = true := by
  by_cases han : a.n = 0
  · have haeq : a = ⟨a.lo, a.step, 0⟩ := by
      cases a with | mk lo st n => simp_all
    rw [haeq]
    simp [intersection, contains]
  · by_cases hbn : b.n = 0
    · have hbeq : b = ⟨b.lo, b.step, 0⟩ := by
        cases b with | mk lo st n => simp_all
      rw [hbeq]
      simp [intersection, contains]
    · have hanp : 0 < a.n := by omega
      have hbnp : 0 < b.n := by omega
      by_cases has : a.step = 0
      · have haeq : a = ⟨a.lo, 0, a.n⟩ := by
          cases a with | mk lo st n => simp_all
        rw [haeq] at han hanp ⊢
        simp [intersection, contains_if_single, contains_const_iff, han, hbn, has]
        by_cases hv : v = a.lo
        · subst v
          by_cases hc : contains b a.lo = true <;> simp [hc, hanp]
        · simp [hv]
      · by_cases hbs : b.step = 0
        · have hbeq : b = ⟨b.lo, 0, b.n⟩ := by
            cases b with | mk lo st n => simp_all
          rw [hbeq] at hbn hbnp ⊢
          simp [intersection, contains_if_single, contains_const_iff, han, hbn, has, hbs]
          by_cases hv : v = b.lo
          · subst v
            by_cases hc : contains a b.lo = true <;> simp [hc, hbnp, and_comm, and_left_comm]
          · simp [hv]
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
          have hg : 0 < g := by
            dsimp [g]
            exact Nat.gcd_pos_of_pos_left b.step hasp
          have hdiv : 0 < a.step / g := by
            apply Nat.div_pos
            · exact Nat.le_of_dvd hasp (by dsimp [g]; exact Nat.gcd_dvd_left a.step b.step)
            · exact hg
          have hp : (0 : Int) < period := by
            have hpnat : 0 < period := by
              dsimp [period]
              exact Nat.mul_pos hdiv hbsp
            exact_mod_cast hpnat
          have hgapdef : gap = b.lo - a.lo := rfl
          have hanbeq : (a.n == 0) = false := by simp [han]
          have hbnbeq : (b.n == 0) = false := by simp [hbn]
          have hasbeq : (a.step == 0) = false := by simp [has]
          have hbsbeq : (b.step == 0) = false := by simp [hbs]
          have hzbeq : (a.n == 0 || b.n == 0) = false := by simp [hanbeq, hbnbeq]
          by_cases hgapmod : gap % (g : Int) = 0
          · have hgne : ¬ ((gap % (g : Int) != 0) = true) := by simp [hgapmod]
            have hgap : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgapmod
            have hroot := root_dvd a.step b.step hasp hbsp a.lo b.lo gap hgapdef hgap
            have hfirst : first = low + (root - low) % (period : Int) := rfl
            have hfroot := first_root_dvd low root first period hp hfirst
            have hremnon : 0 ≤ (root - low) % (period : Int) := Int.emod_nonneg _ (by omega)
            have hflow : low ≤ first := by dsimp [first]; omega
            have hpa : (a.step : Int) ∣ (period : Int) := by
              have hnat : a.step ∣ (a.step / g) * b.step := by
                have hga : a.step / g * g = a.step := Nat.div_mul_cancel (by dsimp [g]; exact Nat.gcd_dvd_left a.step b.step)
                have hgb : b.step / g * g = b.step := Nat.div_mul_cancel (by dsimp [g]; exact Nat.gcd_dvd_right a.step b.step)
                refine ⟨b.step / g, ?_⟩
                grind
              dsimp [period]
              exact_mod_cast hnat
            have hpb : (b.step : Int) ∣ (period : Int) := by
              have hnat : b.step ∣ (a.step / g) * b.step := by
                refine ⟨a.step / g, ?_⟩
                grind
              dsimp [period]
              exact_mod_cast hnat
            have hinteq : intersection a b =
                ⟨first, period, if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ := by
              unfold intersection
              simp [hzbeq, hasbeq, hbsbeq, hgapmod, first, period, low, high, unit, root, g, gap]
            rw [hinteq]
            rw [span_contains_iff_nat first period high v (by dsimp [period]; exact Nat.mul_pos hdiv hbsp)]
            constructor
            · rintro ⟨hfirstx, hxhigh, hxfirst⟩
              have hxroot : (period : Int) ∣ v - root := by
                have heq : v - root = (v - first) + (first - root) := by omega
                rw [heq]
                exact Int.dvd_add hxfirst hfroot
              have hxrootA : (a.step : Int) ∣ v - root := Int.dvd_trans hpa hxroot
              have hxrootB : (b.step : Int) ∣ v - root := Int.dvd_trans hpb hxroot
              have hxalo : (a.step : Int) ∣ v - a.lo := by
                have hsum : (v - root) + (root - a.lo) = v - a.lo := by omega
                rw [← hsum]
                exact Int.dvd_add hxrootA hroot.1
              have hxblo : (b.step : Int) ∣ v - b.lo := by
                have hsum : (v - root) + (root - b.lo) = v - b.lo := by omega
                rw [← hsum]
                exact Int.dvd_add hxrootB hroot.2
              have hxlow : low ≤ v := by omega
              have hab := contains_bounds a v
              have hbb := contains_bounds b v
              constructor
              · exact mem_of_bounds_dvd a v hanp hasp (by omega) (by omega) hxalo
              · exact mem_of_bounds_dvd b v hbnp hbsp (by omega) (by omega) hxblo
            · rintro ⟨ha, hb⟩
              have hab := contains_bounds a v ha
              have hbb := contains_bounds b v hb
              have hxalo0 := contains_dvd a v ha
              have hxblo0 := contains_dvd b v hb
              have hva : (a.step : Int) ∣ v - root := by
                have hsum : (v - a.lo) - (root - a.lo) = v - root := by omega
                rw [← hsum]
                exact Int.dvd_sub hxalo0 hroot.1
              have hvb : (b.step : Int) ∣ v - root := by
                have hsum : (v - b.lo) - (root - b.lo) = v - root := by omega
                rw [← hsum]
                exact Int.dvd_sub hxblo0 hroot.2
              have hxroot : (period : Int) ∣ v - root := by
                have h := lcm_dvd_of_dvd a.step b.step hasp hbsp (v - root) hva hvb
                simpa [period, g] using h
              have hxfirst : (period : Int) ∣ v - first := by
                have heq : v - first = (v - root) - (first - root) := by omega
                rw [heq]
                exact Int.dvd_sub hxroot hfroot
              have hlowv : low ≤ v := by
                dsimp [low]
                omega
              have hfirstv : first ≤ v :=
                first_le_of_congruence low root first v (period : Int) hp rfl hlowv hxroot
              exact ⟨hfirstv, by omega, hxfirst⟩
          · have hgapne : gap % (g : Int) ≠ 0 := by simpa using hgapmod
            have hgne : ((gap % (g : Int) != 0) = true) := by simp [hgapne]
            have hinteq : intersection a b = ⟨0, 1, 0⟩ := by
              unfold intersection
              simp [hzbeq, hasbeq, hbsbeq, hgapne, hgne, gap, g]
            rw [hinteq]
            simp [contains]
            intro hanp' hlo hhi hmod hbnp' hblo hbhi
            have ha : contains a v = true := by
              simp [contains, hanp', hlo, hhi, hmod]
            constructor
            · intro hbstep
              have hlast : lastVal b = b.lo := by simp [lastVal, hbstep]
              have hbmod' : (b.step == 0 || (v - b.lo) % (b.step : Int) == 0) = true := by
                simp [hbstep]
              have hb : contains b v = true := by
                simp [contains, hbnp', hblo, hbhi, hbmod']
              have hg := common_gap_dvd a b v ha hb
              exact hgapne (Int.emod_eq_zero_of_dvd hg)
            · intro hbmod0
              have hbmod' : (b.step == 0 || (v - b.lo) % (b.step : Int) == 0) = true := by
                simp [hbmod0]
              have hb : contains b v = true := by
                simp [contains, hbnp', hblo, hbhi, hbmod']
              have hg := common_gap_dvd a b v ha hb
              exact hgapne (Int.emod_eq_zero_of_dvd hg)


-- =====================================================================
-- Counting common values of two progressions
-- =====================================================================

private def progVals (p : Progression) : List Int :=
  (List.range p.n).map (fun i : Nat => p.lo + (i : Int) * p.step)

private theorem contains_iff_progVals (p : Progression) (v : Int) :
    contains p v = true ↔ v ∈ progVals p := by
  rw [contains_iff_mem]
  unfold progMem progVal progVals
  rw [List.mem_map]
  constructor
  · rintro ⟨k, hk, hval⟩
    refine ⟨k, ?_, hval.symm⟩
    simpa using hk
  · rintro ⟨k, hk, hval⟩
    refine ⟨k, ?_, hval.symm⟩
    simpa using hk

private theorem count_eq_one_of_nodup_mem (l : List Int) (hnd : l.Nodup) {a : Int}
    (ha : a ∈ l) : l.count a = 1 := by
  induction l with
  | nil => simp at ha
  | cons x xs ih =>
      rw [List.count_cons]
      simp only [List.nodup_cons] at hnd
      rcases hnd with ⟨hxnot, hxsnd⟩
      simp only [List.mem_cons] at ha
      by_cases hxa : x = a
      · subst x
        have hc : xs.count a = 0 := List.count_eq_zero_of_not_mem hxnot
        simp [hc]
      · have hax : a ∈ xs := by rcases ha with rfl | h <;> simp_all
        have hc := ih hxsnd hax
        simp [hxa, hc]

private theorem perm_of_nodup_mem (l₁ l₂ : List Int) (h₁ : l₁.Nodup) (h₂ : l₂.Nodup)
    (hmem : ∀ a : Int, a ∈ l₁ ↔ a ∈ l₂) : l₁.Perm l₂ := by
  apply (List.perm_iff_count).mpr
  intro a
  by_cases ha : a ∈ l₁
  · have ha2 : a ∈ l₂ := (hmem a).mp ha
    rw [count_eq_one_of_nodup_mem l₁ h₁ ha, count_eq_one_of_nodup_mem l₂ h₂ ha2]
  · have ha2 : ¬ a ∈ l₂ := fun h => ha ((hmem a).mpr h)
    rw [List.count_eq_zero_of_not_mem ha, List.count_eq_zero_of_not_mem ha2]

private theorem nodup_map_of_injective {α β : Type} (f : α → β) (l : List α)
    (hf : ∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) (hnd : l.Nodup) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.nodup_cons] at hnd ⊢
      rcases hnd with ⟨hxnot, hxsnd⟩
      constructor
      · intro hxmem
        simp only [List.mem_map] at hxmem
        rcases hxmem with ⟨y, hy, hfy⟩
        have hxy : x = y := hf x (by simp) y (by simp [hy]) hfy.symm
        subst y
        exact hxnot hy
      · exact ih (fun a ha b hb hab => hf a (by simp [ha]) b (by simp [hb]) hab) hxsnd

private theorem nodup_filter (l : List Int) (hnd : l.Nodup) (f : Int → Bool) : (l.filter f).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.nodup_cons] at hnd ⊢
      rcases hnd with ⟨ha, has⟩
      by_cases hfa : f a = true
      · simp [hfa]
        exact ⟨ha, ih has⟩
      · simp [hfa]
        exact ih has

private theorem progVals_nodup (p : Progression) (hp : 0 < p.step) : (progVals p).Nodup := by
  unfold progVals
  apply nodup_map_of_injective
  · intro a ha b hb hab
    have hstep : (p.step : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hp)
    have hmul : (a : Int) * (p.step : Int) = (b : Int) * (p.step : Int) := by omega
    have habI : (a : Int) = (b : Int) := (Int.mul_eq_mul_right_iff hstep).mp hmul
    exact_mod_cast habI
  · exact List.nodup_range

private theorem intersection_step_pos (p q : Progression) : 0 < (intersection p q).step := by
  unfold intersection
  dsimp only
  split
  · simp
  next h1 =>
    split
    · simp
    next h2 =>
      split
      · simp
      next h3 =>
        split
        · simp
        next h4 =>
          have hps : 0 < p.step := Nat.pos_of_ne_zero (by simpa using h2)
          have hqs : 0 < q.step := Nat.pos_of_ne_zero (by simpa using h3)
          have hg : 0 < p.step.gcd q.step := Nat.gcd_pos_of_pos_left q.step hps
          have hdiv : 0 < p.step / p.step.gcd q.step :=
            Nat.div_pos (Nat.le_of_dvd hps (Nat.gcd_dvd_left p.step q.step)) hg
          exact Nat.mul_pos hdiv hqs

private theorem countP_contains_prog (p q : Progression) :
    (progVals p).countP (fun v => contains q v) =
      (intersection p q).n * (if p.step == 0 then p.n else 1) := by
  by_cases hpn : p.n = 0
  · simp [progVals, hpn, intersection, contains]
  by_cases hqn : q.n = 0
  · simp [progVals, hqn, intersection, contains]
  by_cases hps0 : p.step = 0
  · have hvals : progVals p = (List.range p.n).map (fun _ : Nat => p.lo) := by
      unfold progVals
      apply List.map_congr_left
      intro i hi
      simp [hps0]
    have hcnt : ((List.range p.n).map (fun _ : Nat => p.lo)).countP (fun v => contains q v) =
        if contains q p.lo then p.n else 0 := by
      induction p.n with
      | zero => simp
      | succ n ih =>
          rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append,
            List.countP_append]
          by_cases hc : contains q p.lo <;> simp [hc, ih]
    have hinteq : intersection p q = ⟨p.lo, 1, if contains q p.lo then 1 else 0⟩ := by
      unfold intersection
      simp [hpn, hqn, hps0]
    rw [hvals, hcnt, hinteq]
    by_cases hc : contains q p.lo <;> simp [hc, hps0, Nat.mul_comm]
  · have hps : 0 < p.step := Nat.pos_of_ne_zero hps0
    let r := intersection p q
    have hrstep : 0 < r.step := by dsimp [r]; exact intersection_step_pos p q
    have hvalsnodup : (progVals p).Nodup := progVals_nodup p hps
    have hrnodup : (progVals r).Nodup := progVals_nodup r hrstep
    have hfilternodup : ((progVals p).filter (fun v => contains q v)).Nodup :=
      nodup_filter (progVals p) hvalsnodup (fun v => contains q v)
    have hperm : List.Perm ((progVals p).filter (fun v => contains q v)) (progVals r) :=
      perm_of_nodup_mem _ _ hfilternodup hrnodup (by
        intro v
        rw [List.mem_filter]
        rw [← contains_iff_progVals r v]
        dsimp [r]
        rw [intersection_contains]
        rw [contains_iff_progVals p v, contains_iff_progVals q v])
    change (progVals p).countP (fun v => contains q v) = r.n * (if p.step == 0 then p.n else 1)
    rw [List.countP_eq_length_filter, hperm.length_eq]
    simp [progVals, hps0]



-- =====================================================================
-- Lane value multisets are the progressions used by joinCount
-- =====================================================================

private theorem values_mem_iff_lane (a : Lane) (v : Int) :
    contains (values a) v = true ↔ ∃ j, j < a.n ∧ v = a.base + (j : Int) * a.step := by
  unfold values
  by_cases hneg : a.step < 0
  · have habs : (a.step.natAbs : Int) = -a.step := by
      rw [← Int.natAbs_neg]
      exact Int.natAbs_of_nonneg (by omega)
    rw [contains_iff_mem]
    unfold progMem progVal
    simp only [hneg, if_true, habs]
    constructor
    · rintro ⟨k, hk, hv⟩
      refine ⟨a.n - 1 - k, by omega, ?_⟩
      rw [hv]
      have hcast : ((a.n - 1 - k : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (k : Int) := by omega
      rw [hcast, Int.sub_mul, Int.mul_neg]
      omega
    · rintro ⟨j, hj, hv⟩
      refine ⟨a.n - 1 - j, by omega, ?_⟩
      rw [hv]
      have hcast : ((a.n - 1 - j : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (j : Int) := by omega
      rw [hcast, Int.sub_mul, Int.mul_neg]
      grind
  · have hnonneg : 0 ≤ a.step := by omega
    have habs : (a.step.natAbs : Int) = a.step := Int.natAbs_of_nonneg hnonneg
    rw [contains_iff_mem]
    unfold progMem progVal
    simp [hneg, habs]

private def laneVals (a : Lane) : List Int :=
  (List.range a.n).map (fun j : Nat => a.base + (j : Int) * a.step)

private theorem countP_map_eq {α β : Type} (f : α → β) (p : β → Bool) (l : List α) :
    (l.map f).countP p = l.countP (fun x => p (f x)) := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.countP_cons]
      by_cases h : p (f x) <;> simp [h, ih]

private theorem countP_eq_sum_ite {α : Type} (l : List α) (p : α → Bool) :
    l.countP p = (l.map (fun x => if p x then 1 else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.countP_cons, List.map_cons, List.sum_cons]
      by_cases h : p x <;> simp [h, ih] <;> omega

private theorem sum_map_mul_const {α : Type} (l : List α) (f : α → Nat) (c : Nat) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.mul_add]

private theorem countP_map_const (n : Nat) (c : Int) (p : Int → Bool) :
    ((List.range n).map (fun _ : Nat => c)).countP p = if p c then n else 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.countP_append]
      by_cases h : p c <;> simp [h, ih]

private theorem progVals_values_lane_perm (a : Lane) :
    List.Perm (progVals (values a)) (laneVals a) := by
  unfold laneVals
  by_cases hstep : a.step = 0
  · have hval : progVals (values a) = (List.range a.n).map (fun _ : Nat => a.base) := by
      unfold progVals values
      simp [hstep]
    have hval2 : (List.range a.n).map (fun j : Nat => a.base + (j : Int) * a.step) =
        (List.range a.n).map (fun _ : Nat => a.base) := by
      apply List.map_congr_left
      intro j hj
      simp [hstep]
    rw [hval, hval2]
  · have hspos : 0 < (values a).step := by
      unfold values
      exact Nat.pos_of_ne_zero (by simpa using hstep)
    have hnod1 : (progVals (values a)).Nodup := progVals_nodup (values a) hspos
    have hnod2 : ((List.range a.n).map (fun j : Nat => a.base + (j : Int) * a.step)).Nodup := by
      apply nodup_map_of_injective
      · intro x hx y hy hxy
        have hne : (a.step : Int) ≠ 0 := by exact_mod_cast hstep
        have hmul : (x : Int) * a.step = (y : Int) * a.step := by omega
        have hxyI : (x : Int) = (y : Int) := (Int.mul_eq_mul_right_iff hne).mp hmul
        exact_mod_cast hxyI
      · exact List.nodup_range
    apply perm_of_nodup_mem _ _ hnod1 hnod2
    intro v
    rw [← contains_iff_progVals (values a) v]
    rw [values_mem_iff_lane]
    rw [List.mem_map]
    constructor
    · rintro ⟨j, hj, hv⟩
      exact ⟨j, by simpa using hj, hv.symm⟩
    · rintro ⟨j, hj, hv⟩
      exact ⟨j, by simpa using hj, hv.symm⟩

private theorem lane_count_contains (b : Lane) (v : Int) :
    (laneVals b).countP (fun w => w == v) =
      (if b.step == 0 then b.n else 1) * (if contains (values b) v then 1 else 0) := by
  by_cases hstep : b.step = 0
  · unfold laneVals
    have hmap : (List.range b.n).map (fun j : Nat => b.base + (j : Int) * b.step) =
        (List.range b.n).map (fun _ : Nat => b.base) := by
      apply List.map_congr_left
      intro j hj
      simp [hstep]
    rw [hmap, countP_map_const]
    have hc : contains (values b) v = true ↔ 0 < b.n ∧ v = b.base := by
      simp [values, hstep, contains_const_iff]
    by_cases hbv : b.base == v
    · have hbv' : b.base = v := beq_iff_eq.mp hbv
      by_cases hn : 0 < b.n
      · have hc' : contains (values b) v = true := hc.mpr ⟨hn, hbv'.symm⟩
        simp [hbv, hstep, hc']
      · have hc' : ¬ contains (values b) v = true := by
          intro hh; exact hn (hc.mp hh).1
        simp [hbv, hstep, hc', Nat.eq_zero_of_not_pos hn]
    · have hne : b.base ≠ v := by intro hh; exact hbv (by rw [hh]; simp)
      have hc' : ¬ contains (values b) v = true := by
        intro hh; exact hne (hc.mp hh).2.symm
      simp [hbv, hne, hstep, hc']
  · have hne : (b.step : Int) ≠ 0 := by exact_mod_cast hstep
    have hinj : ∀ i ∈ List.range b.n, ∀ j ∈ List.range b.n,
        b.base + (i : Int) * b.step = b.base + (j : Int) * b.step → i = j := by
      intro i hi j hj heq
      have hmul : (i : Int) * b.step = (j : Int) * b.step := by omega
      have hij : (i : Int) = (j : Int) := (Int.mul_eq_mul_right_iff hne).mp hmul
      exact_mod_cast hij
    have hnod : (laneVals b).Nodup := by
      unfold laneVals
      exact nodup_map_of_injective _ _ hinj List.nodup_range
    by_cases hv : contains (values b) v = true
    · obtain ⟨j, hj, hvj⟩ := (values_mem_iff_lane b v).mp hv
      have hmem : v ∈ laneVals b := by
        unfold laneVals
        rw [List.mem_map]
        exact ⟨j, by simpa using hj, hvj.symm⟩
      have hcnt : (laneVals b).countP (fun w => w == v) = 1 := by
        change (laneVals b).count v = 1
        exact count_eq_one_of_nodup_mem (laneVals b) hnod hmem
      rw [hcnt]
      simp [hstep, hv]
    · have hnot : ¬ v ∈ laneVals b := by
        intro hm
        have hm' : ∃ j, j < b.n ∧ v = b.base + (j : Int) * b.step := by
          unfold laneVals at hm
          rw [List.mem_map] at hm
          rcases hm with ⟨j, hj, hvj⟩
          exact ⟨j, by simpa using hj, hvj.symm⟩
        exact hv ((values_mem_iff_lane b v).mpr hm')
      have hcnt : (laneVals b).countP (fun w => w == v) = 0 := by
        change (laneVals b).count v = 0
        exact List.count_eq_zero_of_not_mem hnot
      rw [hcnt]
      simp [hstep, hv]

private theorem joinCount_lane (a b : Lane) :
    ((List.range a.n).map (fun j : Nat => (List.range b.n).countP
      (fun i : Nat => a.base + (j : Int) * a.step == b.base + b.step * (i : Int)))).sum = joinCount a b := by
  have hcountb : ∀ j : Nat, (List.range b.n).countP
      (fun i : Nat => a.base + (j : Int) * a.step == b.base + b.step * (i : Int)) =
      (laneVals b).countP (fun w => w == a.base + (j : Int) * a.step) := by
    intro j
    unfold laneVals
    rw [countP_map_eq]
    apply congrArg (fun (p : Nat → Bool) => (List.range b.n).countP p)
    funext i
    by_cases heq : a.base + (j : Int) * a.step = b.base + b.step * (i : Int)
    · have h1 : (a.base + (j : Int) * a.step == b.base + b.step * (i : Int)) = true := by
        rw [beq_iff_eq]
        exact heq
      have h2 : (b.base + (i : Int) * b.step == a.base + (j : Int) * a.step) = true := by
        rw [beq_iff_eq]
        rw [Int.mul_comm (i : Int) b.step]
        exact heq.symm
      rw [h1, h2]
    · have h1 : (a.base + (j : Int) * a.step == b.base + b.step * (i : Int)) = false :=
        beq_eq_false_iff_ne.mpr heq
      have h2 : (b.base + (i : Int) * b.step == a.base + (j : Int) * a.step) = false :=
        beq_eq_false_iff_ne.mpr (by
          intro hh'
          apply heq
          rw [Int.mul_comm b.step (i : Int)]
          exact hh'.symm)
      rw [h1, h2]
  simp only [hcountb]
  have hrewrite : ∀ j : Nat,
      (laneVals b).countP (fun w => w == a.base + (j : Int) * a.step) =
        (if b.step == 0 then b.n else 1) *
          (if contains (values b) (a.base + (j : Int) * a.step) then 1 else 0) := by
    intro j
    exact lane_count_contains b (a.base + (j : Int) * a.step)
  simp only [hrewrite]
  let c : Nat := if b.step == 0 then b.n else 1
  change ((List.range a.n).map (fun j : Nat => c *
      (if contains (values b) (a.base + (j : Int) * a.step) then 1 else 0))).sum = joinCount a b
  rw [sum_map_mul_const]
  have hind : ((List.range a.n).map (fun j : Nat =>
        if contains (values b) (a.base + (j : Int) * a.step) then 1 else 0)).sum =
      (laneVals a).countP (fun w => contains (values b) w) := by
    unfold laneVals
    rw [countP_map_eq]
    rw [countP_eq_sum_ite]
  rw [hind]
  have hperm := progVals_values_lane_perm a
  have hcnt : (laneVals a).countP (fun w => contains (values b) w) =
      (progVals (values a)).countP (fun w => contains (values b) w) := by
    exact (List.Perm.countP_eq (fun w => contains (values b) w) hperm.symm)
  rw [hcnt]
  rw [countP_contains_prog]
  unfold joinCount
  have hstepa : ((values a).step == 0) = (a.step == 0) := by
    unfold values
    by_cases h : a.step == 0
    · have h' : a.step = 0 := beq_iff_eq.mp h
      simp [h, h']
    · have h' : a.step ≠ 0 := by intro hh; exact h (by rw [hh]; simp)
      have hnat : a.step.natAbs ≠ 0 := by
        intro hn
        exact h' (Int.natAbs_eq_zero.mp hn)
      simp [h, h', hnat]
  rw [hstepa]
  unfold c
  by_cases ha0 : a.step = 0 <;> by_cases hb0 : b.step = 0 <;>
    simp [ha0, hb0, values, Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]


-- =====================================================================
-- SegmentedSlope layout: rows are the concatenation of its two lanes
-- =====================================================================

private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun i : Nat => (a.start + i * a.stride, a.base + (i : Int) * a.step))

private theorem range_filterMap_all {α : Type} (n : Nat) (f : Nat → Option α) (g : Nat → α)
    (h : ∀ i, i < n → f i = some (g i)) :
    (List.range n).filterMap f = (List.range n).map g := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filterMap_append]
      simp [h n (by omega), ih (fun i hi => h i (by omega))]

-- =====================================================================
-- RestartedDelta layout: rows are the concatenation of its two lanes
-- =====================================================================

private theorem restarted_rows_eq (c : Config) :
    rows TinylambOpt.ResearchGoals.Atlas.EncodedPages.RestartedDelta.layout c =
      lanePoints (span c 0 c.split c.base c.step) ++
      lanePoints (span c c.split c.n c.bias c.step) := by
  unfold rows
  have hFM : (List.range c.n).filterMap
        (fun i => (TinylambOpt.ResearchGoals.Atlas.EncodedPages.RestartedDelta.layout.read c i).map (fun v => (i, v))) =
      (List.range c.n).map (fun i : Nat =>
        (i, if i < c.split then c.base + c.step * (i : Int)
            else c.bias + c.step * ((i - c.split : Nat) : Int))) := by
    apply range_filterMap_all
    intro i hi
    by_cases hs : i < c.split <;>
      simp [TinylambOpt.ResearchGoals.Atlas.EncodedPages.RestartedDelta.layout, hs]
  rw [hFM]
  let s := min c.n c.split
  have hn : c.n = s + (c.n - s) := by dsimp [s]; omega
  rw [hn, List.range_add]
  simp only [List.map_append]
  congr 1
  · unfold lanePoints span lane
    simp [s, Nat.mul_comm]
    intro a ha
    have ha' : a < c.split := by omega
    simp [ha']
    rw [Int.mul_comm]
  · by_cases hsplit : c.split ≤ c.n
    · unfold lanePoints span lane
      simp only
      have hs : s = c.split := by
        show min c.n c.split = c.split
        exact Nat.min_eq_right hsplit
      have hlen : c.n - s = c.n - c.split := by rw [hs]
      rw [hlen]
      have hsum : c.split + (c.n - c.split) = c.n := by omega
      have hlen2 : min c.n (s + (c.n - c.split)) = c.n := by
        rw [hs, hsum]
        simp
      rw [hlen2]
      have hdiv : (c.n - c.split + 1 - 1) / 1 = c.n - c.split := by omega
      rw [hdiv]
      rw [List.map_map]
      apply List.map_congr_left
      intro j hj
      have hjlt : j < c.n - c.split := List.mem_range.mp hj
      rw [hs]
      have hnot : ¬ c.split + j < c.split := by omega
      have hsub : c.split + j - c.split = j := by omega
      simp [hnot, hsub, Nat.mul_comm]
      rw [Int.mul_comm]
    · unfold lanePoints span lane
      have hs : s = c.n := by
        show min c.n c.split = c.n
        exact Nat.min_eq_left (by omega)
      have hz : c.n - c.split = 0 := by omega
      simp [hs, hz, hsplit]

private theorem lane_residue_join (a b : Lane) :
    ((lanePoints a).map (fun r => (List.range b.n).countP
      (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum = joinCount a b := by
  have hmap : (lanePoints a).map (fun r => (List.range b.n).countP
      (fun i : Nat => r.2 == b.base + b.step * (i : Int))) =
      (laneVals a).map (fun v => (List.range b.n).countP
        (fun i : Nat => v == b.base + b.step * (i : Int))) := by
    unfold lanePoints laneVals
    simp [List.map_map, Function.comp_def]
  rw [hmap]
  unfold laneVals
  rw [List.map_map]
  exact joinCount_lane a b

theorem sound_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.RestartedDelta.ResidueJoin.Goal := by
  intro c b
  rw [restarted_rows_eq]
  simp only [List.map_append, List.sum_append]
  rw [lane_residue_join, lane_residue_join]
  simp [TinylambOpt.ResearchGoals.Atlas.EncodedPages.RestartedDelta.layout, span]

end TinylambOpt.ResearchProofs.Round2.Task50

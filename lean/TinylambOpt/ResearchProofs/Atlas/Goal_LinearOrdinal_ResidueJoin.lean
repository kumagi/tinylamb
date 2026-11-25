import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_LinearOrdinal_ResidueJoin

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-! ## Progression membership and intersections (adapted from EncodedPages4) -/

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
  have hk' : k ≤ a.n - 1 := Nat.le_pred_of_lt hk
  have hkint : (k : Int) ≤ (a.n - 1 : Nat) := by exact_mod_cast hk'
  have hmul := Int.mul_le_mul_of_nonneg_left hkint (by omega : 0 ≤ (a.step : Int))
  have hmul' : (k : Int) * a.step ≤ ((a.n - 1 : Nat) : Int) * a.step := by
    simpa [Int.mul_comm] using hmul
  unfold lastVal
  rw [hval]
  constructor
  · exact Int.le_add_of_nonneg_right
      (Int.mul_nonneg (by exact_mod_cast (Nat.zero_le k)) (by omega : 0 ≤ (a.step : Int)))
  · exact Int.add_le_add_left hmul' a.lo

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
  have h1 : (a.step.gcd b.step : Int) ∣ (b.step : Int) * (j : Int) := by
    simpa only using (Int.dvd_mul_of_dvd_left
      (by exact_mod_cast (Nat.gcd_dvd_right a.step b.step) :
        (a.step.gcd b.step : Int) ∣ (b.step : Int)) :
      (a.step.gcd b.step : Int) ∣ (b.step : Int) * (j : Int))
  have h2 : (a.step.gcd b.step : Int) ∣ (a.step : Int) * (i : Int) := by
    simpa only using (Int.dvd_mul_of_dvd_left
      (by exact_mod_cast (Nat.gcd_dvd_left a.step b.step) :
        (a.step.gcd b.step : Int) ∣ (a.step : Int)) :
      (a.step.gcd b.step : Int) ∣ (a.step : Int) * (i : Int))
  have hi' : v = a.lo + (a.step : Int) * (i : Int) := by simpa [Int.mul_comm] using hi
  have hj' : v = b.lo + (b.step : Int) * (j : Int) := by simpa [Int.mul_comm] using hj
  clear hi hj
  have hiv : (a.step : Int) * (i : Int) = v - a.lo := by
    rw [hi']
    omega
  have hjb : (b.step : Int) * (j : Int) = v - b.lo := by
    rw [hj']
    omega
  clear hi' hj'
  have heq : (a.step : Int) * (i : Int) - (b.step : Int) * (j : Int) = b.lo - a.lo := by
    rw [hiv, hjb]
    clear ha hb h1 h2
    omega
  rw [← heq]
  exact Int.dvd_sub h2 h1

private theorem contains_of_step_zero (a : Progression) (v : Int) (has : a.step = 0) :
    contains a v = true ↔ 0 < a.n ∧ v = a.lo := by
  obtain ⟨lo, st, n⟩ := a
  simp at has
  subst st
  simpa using contains_const_iff lo n v

private theorem contains_unit_one (c : Int) (v : Int) :
    contains ⟨c, 1, 1⟩ v = true ↔ v = c := by
  rw [contains_iff_mem]
  constructor
  · rintro ⟨k, hk, hv⟩
    have hk1 : k < 1 := by simpa using hk
    have hk0 : k = 0 := Nat.eq_zero_of_le_zero (Nat.le_of_lt_succ hk1)
    subst k
    simpa [progVal] using hv
  · intro hv
    refine ⟨0, by simp, ?_⟩
    simp [progVal, hv]

private theorem intersection_subset (a b : Progression) (v : Int) :
    contains (intersection a b) v = true → contains a v = true ∧ contains b v = true := by
  by_cases han : a.n = 0
  · simp [intersection, contains, han]
  · by_cases hbn : b.n = 0
    · simp [intersection, contains, hbn]
    · have hanp : 0 < a.n := by omega
      have hbnp : 0 < b.n := by omega
      by_cases has : a.step = 0
      · intro h
        have hia : intersection a b = ⟨a.lo, 1, if contains b a.lo then 1 else 0⟩ := by
          simp [intersection, han, hbn, has]
        rw [hia] at h
        by_cases hcb : contains b a.lo
        · rw [if_pos hcb, contains_unit_one] at h
          constructor
          · rw [contains_of_step_zero a v has]
            exact ⟨hanp, h⟩
          · rw [h]
            exact hcb
        · rw [if_neg hcb] at h
          simp [contains] at h
      · by_cases hbs : b.step = 0
        · intro h
          have hib : intersection a b = ⟨b.lo, 1, if contains a b.lo then 1 else 0⟩ := by
            simp [intersection, han, hbn, has, hbs]
          rw [hib] at h
          by_cases hca : contains a b.lo
          · rw [if_pos hca, contains_unit_one] at h
            constructor
            · rw [h]
              exact hca
            · rw [contains_of_step_zero b v hbs]
              exact ⟨hbnp, h⟩
          · rw [if_neg hca] at h
            simp [contains] at h
        · intro h
          have hasp : 0 < a.step := Nat.pos_of_ne_zero has
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
          have hdivpos : 0 < a.step / g := Nat.div_gcd_pos_of_pos_left b.step hasp
          have hpNat : 0 < period := by
            dsimp [period]
            exact Nat.mul_pos hdivpos hbsp
          have hp : (0 : Int) < period := by exact_mod_cast hpNat
          have hgapdef : gap = b.lo - a.lo := rfl
          by_cases hgapmod : gap % (g : Int) = 0
          · have hgap : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgapmod
            have hroot := root_dvd a.step b.step hasp hbsp a.lo b.lo gap hgapdef hgap
            have hfirst : first = low + (root - low) % (period : Int) := rfl
            have hfroot := first_root_dvd low root first period hp hfirst
            have hremnon : 0 ≤ (root - low) % (period : Int) := Int.emod_nonneg _ (by omega)
            have hflow : low ≤ first := by dsimp [first]; omega
            have hasg : (a.step : Int) = (g : Int) * ((a.step / g : Nat) : Int) := by
              exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_left a.step b.step)).symm
            have hbsg : (b.step : Int) = (g : Int) * ((b.step / g : Nat) : Int) := by
              exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_right a.step b.step)).symm
            have hpa : (a.step : Int) ∣ (period : Int) := by
              refine ⟨((b.step / g : Nat) : Int), ?_⟩
              calc
                (period : Int) = ((a.step / g : Nat) : Int) * (b.step : Int) := by simp [period]
                _ = ((a.step / g : Nat) : Int) * ((g : Int) * ((b.step / g : Nat) : Int)) := by rw [hbsg]
                _ = ((g : Int) * ((a.step / g : Nat) : Int)) * ((b.step / g : Nat) : Int) := by
                  rw [← Int.mul_assoc, Int.mul_comm ((a.step / g : Nat) : Int) (g : Int)]
                _ = (a.step : Int) * ((b.step / g : Nat) : Int) := by rw [← hasg]
            have hpb : (b.step : Int) ∣ (period : Int) := by
              refine ⟨((a.step / g : Nat) : Int), ?_⟩
              calc
                (period : Int) = ((a.step / g : Nat) : Int) * (b.step : Int) := by simp [period]
                _ = (b.step : Int) * ((a.step / g : Nat) : Int) := by rw [Int.mul_comm]
            have hgapmod' : (b.lo - a.lo) % (a.step.gcd b.step : Int) = 0 := by
              simpa [gap, g] using hgapmod
            have hi0 : intersection a b =
                ⟨first, period, if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ := by
              simp [intersection, han, hbn, has, hbs, hgapmod',
                g, gap, first, period, high, low, root, unit]
            rw [hi0] at h
            rw [show contains ⟨first, period,
                if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ v = true ↔
                first ≤ v ∧ v ≤ high ∧ (period : Int) ∣ v - first by
              simpa using (span_contains_iff first (period : Int) high v hp)] at h
            rcases h with ⟨hfirstx, hxhigh, hxfirst⟩
            have hxroot' : (period : Int) ∣ (v - first) + (first - root) :=
              Int.dvd_add hxfirst hfroot
            have hxroot : (period : Int) ∣ v - root := by
              rw [← show (v - first) + (first - root) = v - root by omega]
              exact hxroot'
            have hxrootA : (a.step : Int) ∣ v - root := Int.dvd_trans hpa hxroot
            have hxrootB : (b.step : Int) ∣ v - root := Int.dvd_trans hpb hxroot
            have hsuma : (a.step : Int) ∣ (v - root) + (root - a.lo) :=
              Int.dvd_add hxrootA hroot.1
            have hsumb : (b.step : Int) ∣ (v - root) + (root - b.lo) :=
              Int.dvd_add hxrootB hroot.2
            have hxalo : (a.step : Int) ∣ v - a.lo := by
              rw [← show (v - root) + (root - a.lo) = v - a.lo by omega]
              exact hsuma
            have hxblo : (b.step : Int) ∣ v - b.lo := by
              rw [← show (v - root) + (root - b.lo) = v - b.lo by omega]
              exact hsumb
            constructor
            · exact mem_of_bounds_dvd a v hanp hasp (by omega) (by omega) hxalo
            · exact mem_of_bounds_dvd b v hbnp hbsp (by omega) (by omega) hxblo
          · have hi0 : intersection a b = ⟨0, 1, 0⟩ := by
              have hgapmod' : (b.lo - a.lo) % (a.step.gcd b.step : Int) ≠ 0 := by
                simpa [gap, g] using hgapmod
              simp [intersection, han, hbn, has, hbs, hgapmod']
            rw [hi0] at h
            simp [contains] at h


/-! ## Counting row values and common values of progressions -/

private theorem natAbs_neg_eq (z : Int) (hz : z < 0) : (z.natAbs : Int) = -z := by
  rcases (Int.natAbs_eq_iff.mp rfl : z = (z.natAbs : Int) ∨ z = -(z.natAbs : Int)) with h | h
  · omega
  · omega

private theorem lane_values_contains (a : Lane) (v : Int) :
    contains (values a) v = true ↔ ∃ i, i < a.n ∧ v = a.base + a.step * (i : Int) := by
  constructor
  · intro h
    obtain ⟨k, hk, hval⟩ := (contains_iff_mem (values a) v).mp h
    unfold progVal at hval
    by_cases hs : a.step < 0
    · have habs : (a.step.natAbs : Int) = -a.step := natAbs_neg_eq a.step hs
      have hval' : v = a.base + ((a.n - 1 : Nat) : Int) * a.step + (k : Int) * (-a.step) := by
        simpa [values, hs, habs] using hval
      have hkle : k ≤ a.n - 1 := Nat.le_pred_of_lt hk
      have hpos : 0 < a.n := Nat.lt_of_le_of_lt (Nat.zero_le k) hk
      have hidx : a.n - 1 - k < a.n :=
        Nat.lt_of_le_of_lt (Nat.sub_le _ _) (Nat.sub_lt hpos (by decide))
      refine ⟨a.n - 1 - k, hidx, ?_⟩
      have hcast : ((a.n - 1 - k : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (k : Int) :=
        Int.ofNat_sub hkle
      rw [hcast]
      rw [Int.mul_sub]
      simp [Int.mul_comm, Int.mul_neg, Int.neg_mul] at hval'
      omega
    · have habs : (a.step.natAbs : Int) = a.step := Int.ofNat_natAbs_of_nonneg (by omega)
      have hval' : v = a.base + (k : Int) * a.step := by
        simpa [values, hs, habs] using hval
      exact ⟨k, hk, by simpa [Int.mul_comm] using hval'⟩
  · rintro ⟨i, hi, hv⟩
    rw [contains_iff_mem]
    by_cases hs : a.step < 0
    · have habs : (a.step.natAbs : Int) = -a.step := natAbs_neg_eq a.step hs
      have hle : i ≤ a.n - 1 := Nat.le_pred_of_lt hi
      have hpos : 0 < a.n := Nat.lt_of_le_of_lt (Nat.zero_le i) hi
      have hidx : a.n - 1 - i < a.n :=
        Nat.lt_of_le_of_lt (Nat.sub_le _ _) (Nat.sub_lt hpos (by decide))
      refine ⟨a.n - 1 - i, hidx, ?_⟩
      unfold progVal
      have hcast : ((a.n - 1 - i : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (i : Int) :=
        Int.ofNat_sub hle
      have htarget : v = a.base + ((a.n - 1 : Nat) : Int) * a.step +
          ((a.n - 1 - i : Nat) : Int) * (-a.step) := by
        rw [hcast]
        rw [Int.sub_mul]
        simp [Int.mul_comm, Int.mul_neg, Int.neg_mul] at hv ⊢
        omega
      simpa [values, hs, habs] using htarget
    · have habs : (a.step.natAbs : Int) = a.step := Int.ofNat_natAbs_of_nonneg (by omega)
      refine ⟨i, hi, ?_⟩
      unfold progVal
      simpa [values, hs, habs, Int.mul_comm] using hv

private theorem linear_inj (base step : Int) (hs : step ≠ 0) {i j : Nat}
    (h : base + step * (i : Int) = base + step * (j : Int)) : i = j := by
  have hmul : step * (i : Int) = step * (j : Int) := by omega
  have hij : (i : Int) = (j : Int) := Int.eq_of_mul_eq_mul_left hs hmul
  exact Int.ofNat_inj.mp hij

private theorem lane_value_count_const (base v : Int) (n : Nat) :
    (List.range n).countP (fun i : Nat => base + 0 * (i : Int) == v) = if v = base then n else 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.countP_append, ih]
      by_cases h : v = base
      · subst v
        simp
      · have h' : ¬ base = v := fun hb => h hb.symm
        simp [h, h']

private theorem lane_value_count_inj (base step : Int) (n : Nat) (v : Int) (hs : step ≠ 0) :
    (List.range n).countP (fun i : Nat => base + step * (i : Int) == v) =
      if ∃ i, i < n ∧ v = base + step * (i : Int) then 1 else 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.countP_append, ih]
      by_cases hlast : base + step * (n : Int) == v
      · have hlastEq : base + step * (n : Int) = v := by simpa using hlast
        have hlast' : v = base + step * (n : Int) := hlastEq.symm
        have huniq : ∀ i, i < n → v ≠ base + step * (i : Int) := by
          intro i hi hvi
          have heq : base + step * (i : Int) = base + step * (n : Int) := by omega
          have hij := linear_inj base step hs heq
          omega
        have hex : ∃ i, i < n + 1 ∧ v = base + step * (i : Int) := ⟨n, by omega, hlast'⟩
        have hnoexists : ¬ ∃ i, i < n ∧ v = base + step * (i : Int) := by
          rintro ⟨i, hi, hvi⟩
          exact huniq i hi hvi
        simp [hlast, hex, hnoexists]
      · have hlast' : v ≠ base + step * (n : Int) := by
          intro h
          exact hlast (by simpa [h])
        have hex : (∃ i, i < n + 1 ∧ v = base + step * (i : Int)) ↔
            (∃ i, i < n ∧ v = base + step * (i : Int)) := by
          constructor
          · rintro ⟨i, hi, hvi⟩
            by_cases hin : i = n
            · subst i
              exact False.elim (hlast' hvi)
            · exact ⟨i, by omega, hvi⟩
          · rintro ⟨i, hi, hvi⟩
            exact ⟨i, by omega, hvi⟩
        simp [hlast, hex]

private theorem range_reverse_eq_map (n : Nat) :
    (List.range n).reverse = (List.range n).map (fun j => n - 1 - j) := by
  induction n with
  | zero => simp
  | succ n ih =>
      have hmap : (List.range (n + 1)).map (fun j => n + 1 - 1 - j) =
          n :: (List.range n).map (fun j => n - 1 - j) := by
        rw [List.range_succ_eq_map]
        simp only [List.map_cons, List.map_map]
        congr 1
        apply List.map_congr_left
        intro j hj
        have hjlt : j < n := List.mem_range.mp hj
        have heq : n + 1 - 1 - Nat.succ j = n - 1 - j := by omega
        simpa [Function.comp_apply] using heq
      rw [hmap]
      rw [List.range_succ, List.reverse_append, List.reverse_singleton]
      rw [← ih]
      rfl

private theorem countP_reverse (n : Nat) (P : Nat → Bool) :
    (List.range n).countP P = (List.range n).countP (fun j => P (n - 1 - j)) := by
  rw [← List.countP_reverse (p := P) (l := List.range n)]
  rw [range_reverse_eq_map n]
  rw [List.countP_map]
  rfl

/-! ## Counting common values of two progressions -/

private def idxOf (p : Progression) (v : Int) : Nat := ((v - p.lo) / (p.step : Int)).toNat

private theorem contains_progVal (p : Progression) (k : Nat) (hk : k < p.n) :
    contains p (progVal p k) = true := by
  rw [contains_iff_mem]
  exact ⟨k, hk, rfl⟩

private theorem progVal_inj_pos (p : Progression) (hp : 0 < p.step) {i j : Nat}
    (h : progVal p i = progVal p j) : i = j := by
  unfold progVal at h
  have h' : p.lo + (p.step : Int) * (i : Int) = p.lo + (p.step : Int) * (j : Int) := by
    simpa [Int.mul_comm] using h
  have hmul : p.step * (i : Int) = p.step * (j : Int) := by omega
  have hij : (i : Int) = (j : Int) := Int.eq_of_mul_eq_mul_left (by omega) hmul
  exact Int.ofNat_inj.mp hij

private theorem idxOf_progVal (p : Progression) (hp : 0 < p.step) (k : Nat) :
    idxOf p (progVal p k) = k := by
  unfold idxOf progVal
  have hmul : p.lo + (k : Int) * p.step - p.lo = (p.step : Int) * (k : Int) := by
    rw [Int.mul_comm (k : Int) (p.step : Int)]
    omega
  rw [hmul]
  have hdiv : (p.step : Int) * (k : Int) / (p.step : Int) = (k : Int) := by
    exact Int.mul_ediv_cancel_left (k : Int) (by omega)
  rw [hdiv, Int.toNat_natCast k]

private theorem idxOf_mem (p : Progression) (hp : 0 < p.step) {v : Int}
    (h : contains p v = true) : idxOf p v < p.n ∧ progVal p (idxOf p v) = v := by
  obtain ⟨k, hk, hval⟩ := (contains_iff_mem p v).mp h
  unfold progVal at hval
  have hmul : v - p.lo = (p.step : Int) * (k : Int) := by
    rw [hval, Int.mul_comm (k : Int) (p.step : Int)]
    omega
  have hdiv : (v - p.lo) / (p.step : Int) = (k : Int) :=
    Int.ediv_eq_of_eq_mul_right (by omega) hmul
  unfold idxOf
  rw [hdiv]
  constructor
  · rw [Int.toNat_natCast k]
    exact hk
  · unfold progVal
    rw [Int.toNat_natCast k]
    rw [← hval]

private theorem countP_or_disjoint {α : Type} [DecidableEq α] (l : List α)
    (p q : α → Bool) (hd : ∀ x ∈ l, ¬ (p x = true ∧ q x = true)) :
    l.countP (fun x => p x || q x) = l.countP p + l.countP q := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hd' : ∀ y ∈ xs, ¬ (p y = true ∧ q y = true) := fun y hy => hd y (by simp [hy])
      have ih' := ih hd'
      have hdis : ¬ (p x = true ∧ q x = true) := hd x (by simp)
      simp only [List.countP_cons]
      by_cases hp : p x = true <;> by_cases hq : q x = true
      · exact False.elim (hdis ⟨hp, hq⟩)
      · simp [hp, hq, ih']
        omega
      · simp [hp, hq, ih']
        omega
      · simp [hp, hq, ih']

private theorem countP_eq_single (a n : Nat) :
    (List.range n).countP (fun k => k == a) = if a < n then 1 else 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.countP_append, ih]
      by_cases han : a = n
      · subst a
        simp
      · have hltgt : a < n ∨ n < a := by omega
        rcases hltgt with hlt | hgt
        · have hna : ¬ n = a := fun h => han h.symm
          have hlt' : a < n + 1 := by omega
          have hnbeq : (n == a) = false := by simp [hna]
          simp [hlt, hlt', hnbeq]
        · have hna : ¬ n = a := fun h => han h.symm
          have hnot : ¬ a < n + 1 := by omega
          have hge : n ≤ a := by omega
          have hnbeq : (n == a) = false := by simp [hna]
          simp [hnot, hge, hnbeq]

private theorem nodup_map_of_injective {α β : Type} (l : List α) (f : α → β)
    (hl : l.Nodup) (hinj : ∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.nodup_cons]
      have hlx : x ∉ xs := (List.nodup_cons.mp hl).1
      have hinj' : ∀ a ∈ xs, ∀ b ∈ xs, f a = f b → a = b := by
        intro a ha b hb heq
        exact hinj a (by simp [ha]) b (by simp [hb]) heq
      constructor
      · intro hmem
        obtain ⟨y, hy, hfy⟩ := List.mem_map.mp hmem
        have hxy : x = y := hinj x (by simp) y (by simp [hy]) hfy.symm
        subst y
        exact hlx hy
      · exact ih hl.tail hinj'

private theorem count_range_mem_of_nodup (n : Nat) (l : List Nat) (hl : l.Nodup)
    (hall : ∀ x ∈ l, x < n) :
    (List.range n).countP (fun k => decide (k ∈ l)) = l.length := by
  induction l with
  | nil => simp
  | cons a l ih =>
      have halt : a < n := hall a (by simp)
      have hall' : ∀ x ∈ l, x < n := fun x hx => hall x (by simp [hx])
      have hl' : l.Nodup := (List.nodup_cons.mp hl).2
      have hanot : a ∉ l := (List.nodup_cons.mp hl).1
      rw [show (fun k : Nat => decide (k ∈ a :: l)) =
             (fun k : Nat => (k == a) || decide (k ∈ l)) by
          funext k
          by_cases hk : k = a <;> by_cases hkl : k ∈ l <;> simp [hk, hkl]]
      rw [countP_or_disjoint]
      · rw [countP_eq_single, ih hl' hall']
        simp [halt, Nat.add_comm]
      · intro k hk hboth
        have hka : k = a := by simpa using hboth.1
        subst k
        exact hanot (by simpa using hboth.2)

private theorem count_mem_progression (p q : Progression) (hp : 0 < p.step) (hq : 0 < q.step)
    (hsub : ∀ v, contains q v = true → contains p v = true) :
    (List.range p.n).countP (fun k => contains q (progVal p k)) = q.n := by
  let f : Nat → Nat := fun j => idxOf p (progVal q j)
  have hf_mem : ∀ j, j < q.n → f j < p.n := by
    intro j hj
    have hqj : contains q (progVal q j) = true := contains_progVal q j hj
    exact (idxOf_mem p hp (hsub _ hqj)).1
  have hf_val : ∀ j, j < q.n → progVal p (f j) = progVal q j := by
    intro j hj
    have hqj : contains q (progVal q j) = true := contains_progVal q j hj
    exact (idxOf_mem p hp (hsub _ hqj)).2
  have hf_inj : ∀ j1, j1 < q.n → ∀ j2, j2 < q.n → f j1 = f j2 → j1 = j2 := by
    intro j1 hj1 j2 hj2 heq
    have v1 := hf_val j1 hj1
    have v2 := hf_val j2 hj2
    have hv : progVal q j1 = progVal q j2 := by rw [← v1, heq, v2]
    exact progVal_inj_pos q hq hv
  have hnodup : ((List.range q.n).map f).Nodup :=
    nodup_map_of_injective (List.range q.n) f List.nodup_range (by
      intro a ha b hb heq
      exact hf_inj a (List.mem_range.mp ha) b (List.mem_range.mp hb) heq)
  have hall : ∀ x ∈ (List.range q.n).map f, x < p.n := by
    intro x hx
    obtain ⟨j, hj, rfl⟩ := List.mem_map.mp hx
    exact hf_mem j (List.mem_range.mp hj)
  have hpred : ∀ k, k ∈ List.range p.n →
      (contains q (progVal p k) = true ↔ k ∈ (List.range q.n).map f) := by
    intro k hk
    have hkp : k < p.n := List.mem_range.mp hk
    constructor
    · intro h
      let j := idxOf q (progVal p k)
      have hjinfo := idxOf_mem q hq h
      have hjlt : j < q.n := hjinfo.1
      have hjval : progVal q j = progVal p k := hjinfo.2
      refine List.mem_map.mpr ⟨j, List.mem_range.mpr hjlt, ?_⟩
      change idxOf p (progVal q j) = k
      rw [hjval]
      exact idxOf_progVal p hp k
    · intro hmem
      rcases List.mem_map.mp hmem with ⟨j, hj, hfj⟩
      have hjlt : j < q.n := List.mem_range.mp hj
      have hval := hf_val j hjlt
      rw [← hfj, hval]
      exact contains_progVal q j hjlt
  have h1 : (List.range p.n).countP (fun k => contains q (progVal p k)) =
      (List.range p.n).countP (fun k => decide (k ∈ (List.range q.n).map f)) := by
    apply List.countP_congr
    intro k hk
    have hp' := hpred k hk
    by_cases hc : contains q (progVal p k) = true
    · have hm := hp'.mp hc
      simp [hc, hm]
    · have hm : ¬ k ∈ (List.range q.n).map f := fun hm => hc (hp'.mpr hm)
      simp [hc, hm]
  rw [h1]
  rw [count_range_mem_of_nodup p.n ((List.range q.n).map f) hnodup hall]
  simp

/-! ## The goal -/

private theorem rows_linear (c : Config) :
    rows TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal.layout c =
      (List.range c.n).map (fun i : Nat => (i, c.base + c.step * (i : Int))) := by
  unfold rows
  simp [TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal.layout]

private theorem sum_map_const_nat (n k : Nat) : ((List.range n).map (fun _ => k)).sum = n * k := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      simp [Nat.succ_mul, Nat.add_comm]

private def laneCount (base step : Int) (n : Nat) (v : Int) : Nat :=
  (List.range n).countP (fun i : Nat => base + step * (i : Int) == v)

private theorem lane_value_count (base step : Int) (n : Nat) (v : Int) :
    laneCount base step n v =
      if step = 0 then (if v = base then n else 0)
      else (if ∃ i, i < n ∧ v = base + step * (i : Int) then 1 else 0) := by
  by_cases hs : step = 0
  · subst step
    change (List.range n).countP (fun i : Nat => base + 0 * (i : Int) == v) = if v = base then n else 0
    simpa [Int.mul_zero, Int.add_zero] using (lane_value_count_const base v n)
  · simpa [laneCount, hs] using (lane_value_count_inj base step n v hs)

private theorem period_dvd (as bs : Nat) (has : 0 < as) (hbs : 0 < bs) {d : Int}
    (ha : (as : Int) ∣ d) (hb : (bs : Int) ∣ d) :
    (((as / as.gcd bs) * bs : Nat) : Int) ∣ d := by
  let g := as.gcd bs
  let A := as / g
  let B := bs / g
  have hg : 0 < g := Nat.gcd_pos_of_pos_left bs has
  have hA : 0 < A := Nat.div_gcd_pos_of_pos_left bs has
  have hB : 0 < B := Nat.div_gcd_pos_of_pos_right as hbs
  have hcop : A.gcd B = 1 := by simpa [A, B, g] using reduced_coprime as bs has hbs
  have hasg : (as : Int) = (g : Int) * (A : Int) := by
    exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_left as bs)).symm
  have hbsg : (bs : Int) = (g : Int) * (B : Int) := by
    exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_right as bs)).symm
  obtain ⟨k, hk⟩ := ha
  have hb' : (g : Int) * (B : Int) ∣ (g : Int) * ((A : Int) * k) := by
    rw [← hbsg]
    rw [← Int.mul_assoc]
    rw [← hasg, ← hk]
    exact hb
  have hBdvdA : (B : Int) ∣ (A : Int) * k :=
    Int.dvd_of_mul_dvd_mul_left (by omega) hb'
  have hspec := bezout_spec A B
  rw [hcop] at hspec
  have hBdvdAkx : (B : Int) ∣ (A : Int) * k * (bezout A B).1 :=
    Int.dvd_mul_of_dvd_left hBdvdA
  have hBdvdBky : (B : Int) ∣ (B : Int) * k * (bezout A B).2 := by
    refine ⟨k * (bezout A B).2, ?_⟩
    simp [Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]
  have hkdiv : (B : Int) ∣ k := by
    have heq : k = ((A : Int) * (bezout A B).1 + (B : Int) * (bezout A B).2) * k := by
      rw [← hspec]
      simp
    rw [heq]
    rw [Int.add_mul]
    simpa [Int.mul_assoc, Int.mul_comm, Int.mul_left_comm] using
      (Int.dvd_add hBdvdAkx hBdvdBky)
  obtain ⟨m, hm⟩ := hkdiv
  refine ⟨m, ?_⟩
  have hbsNat : bs = g * B := by
    dsimp [g, B]
    exact (Nat.mul_div_cancel' (Nat.gcd_dvd_right as bs)).symm
  have hperNat : (as / as.gcd bs) * bs = g * A * B := by
    calc
      (as / as.gcd bs) * bs = A * bs := by simp [A, g]
      _ = A * (g * B) := by rw [hbsNat]
      _ = g * A * B := by simp [Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
  have hper : (((as / as.gcd bs) * bs : Nat) : Int) = (g : Int) * (A : Int) * (B : Int) := by
    exact_mod_cast hperNat
  rw [hper, hk, hm, hasg]
  simp [Int.mul_assoc, Int.mul_left_comm, Int.mul_comm]

private theorem countP_eq_sum_const {α : Type} (l : List α) (f : α → Bool) (k : Nat) :
    l.countP f * k = (l.map (fun x => if f x then k else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : f x
      · simp [h]
        rw [Nat.add_mul]
        simp [ih, Nat.add_comm]
      · simp [h, ih]

private theorem countP_eq_sum_bool {α : Type} (l : List α) (f : α → Bool) :
    l.countP f = (l.map (fun x => if f x then 1 else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : f x <;> simp [h, ih] <;> omega

private theorem intersection_step_pos (a b : Progression) : 0 < (intersection a b).step := by
  unfold intersection
  dsimp only
  by_cases h1 : a.n == 0 || b.n == 0
  · simp [h1]
  · by_cases h2 : a.step == 0
    · simp [h1, h2]
    · by_cases h3 : b.step == 0
      · simp [h1, h2, h3]
      · by_cases h4 : (b.lo - a.lo) % (a.step.gcd b.step : Int) != 0
        · simp [h1, h2, h3, h4]
        · simp [h1, h2, h3, h4]
          have ha0 : a.step ≠ 0 := by simpa using h2
          have hb0 : b.step ≠ 0 := by simpa using h3
          have hapos : 0 < a.step := Nat.pos_of_ne_zero ha0
          have hbpos : 0 < b.step := Nat.pos_of_ne_zero hb0
          have hgpos : 0 < a.step.gcd b.step := Nat.gcd_pos_of_pos_left b.step hapos
          have hdivpos : 0 < a.step / a.step.gcd b.step := Nat.div_gcd_pos_of_pos_left b.step hapos
          exact Nat.mul_pos hdivpos hbpos

private theorem inter_reverse_pos (p q : Progression) (hp : 0 < p.step) (hq : 0 < q.step)
    (hgap : (p.step.gcd q.step : Int) ∣ q.lo - p.lo)
    (v : Int) (hpv : contains p v = true) (hqv : contains q v = true) :
    contains (intersection p q) v = true := by
  let g := p.step.gcd q.step
  let gap := q.lo - p.lo
  let period := (p.step / g) * q.step
  let unit := (bezout (p.step / g) (q.step / g)).1
  let root := p.lo + (p.step : Int) * ((gap / (g : Int) * unit) % ((q.step / g : Nat) : Int))
  let low := max p.lo q.lo
  let first := low + (root - low) % (period : Int)
  let high := min (lastVal p) (lastVal q)
  have hpn : 0 < p.n := by
    obtain ⟨k, hk, _⟩ := (contains_iff_mem p v).mp hpv
    exact Nat.lt_of_le_of_lt (Nat.zero_le k) hk
  have hqn : 0 < q.n := by
    obtain ⟨k, hk, _⟩ := (contains_iff_mem q v).mp hqv
    exact Nat.lt_of_le_of_lt (Nat.zero_le k) hk
  have has : p.step ≠ 0 := Nat.pos_iff_ne_zero.mp hp
  have hbs : q.step ≠ 0 := Nat.pos_iff_ne_zero.mp hq
  have hg : 0 < g := Nat.gcd_pos_of_pos_left q.step hp
  have hdiv : 0 < p.step / g := Nat.div_pos (Nat.le_of_dvd hp (Nat.gcd_dvd_left p.step q.step)) hg
  have hppos : (0 : Int) < period := by exact_mod_cast (Nat.mul_pos hdiv hq)
  have hgapdef : gap = q.lo - p.lo := rfl
  have hgapd : (g : Int) ∣ gap := hgap
  have hroot := root_dvd p.step q.step hp hq p.lo q.lo gap hgapdef hgapd
  have hfirst : first = low + (root - low) % (period : Int) := rfl
  have hfroot := first_root_dvd low root first period hppos hfirst
  have hremnon : 0 ≤ (root - low) % (period : Int) := Int.emod_nonneg _ (by omega)
  have hflow : low ≤ first := by dsimp [first]; omega
  have hga : g * (p.step / g) = p.step := Nat.mul_div_cancel' (Nat.gcd_dvd_left p.step q.step)
  have hgb : q.step / g * g = q.step := Nat.div_mul_cancel (Nat.gcd_dvd_right p.step q.step)
  have hper_eq : period = p.step * (q.step / g) := by
    dsimp [period]
    calc
      p.step / g * q.step = p.step / g * (q.step / g * g) := by rw [hgb]
      _ = (p.step / g * (q.step / g)) * g := by ac_rfl
      _ = (g * (p.step / g)) * (q.step / g) := by ac_rfl
      _ = p.step * (q.step / g) := by rw [hga]
  have hper_eq2 : period = q.step * (p.step / g) := by dsimp [period]; rw [Nat.mul_comm]
  have hpa : (p.step : Int) ∣ (period : Int) := by refine ⟨((q.step / g : Nat) : Int), ?_⟩; exact_mod_cast hper_eq
  have hpb : (q.step : Int) ∣ (period : Int) := by refine ⟨((p.step / g : Nat) : Int), ?_⟩; exact_mod_cast hper_eq2
  have hgapmod : gap % (g : Int) = 0 := Int.emod_eq_zero_of_dvd hgapd
  have hi0 : intersection p q = ⟨first, period, if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ := by
    have han : p.n ≠ 0 := Nat.ne_of_gt hpn
    have hbn : q.n ≠ 0 := Nat.ne_of_gt hqn
    simp [intersection, han, hbn, has, hbs, hgapmod, g, gap, first, period, high, low, root, unit]
  have hab := contains_bounds p v hpv
  have hbb := contains_bounds q v hqv
  have hxblo := contains_dvd q v hqv
  have hastep_root : (p.step : Int) ∣ v - root := by
    have heq : v - root = (v - p.lo) - (root - p.lo) := by omega
    rw [heq]
    exact Int.dvd_sub (contains_dvd p v hpv) hroot.1
  have hbstep_root : (q.step : Int) ∣ v - root := by
    have heq : v - root = (v - q.lo) - (root - q.lo) := by omega
    rw [heq]
    exact Int.dvd_sub hxblo hroot.2
  have hxroot : (period : Int) ∣ v - root := by
    simpa [period, g] using (period_dvd p.step q.step hp hq hastep_root hbstep_root)
  have hxfirst : (period : Int) ∣ v - first := by
    have heq : v - first = (v - root) - (first - root) := by omega
    rw [heq]
    exact Int.dvd_sub hxroot hfroot
  have hlowv : low ≤ v := by dsimp [low]; exact Int.max_le.mpr ⟨hab.1, hbb.1⟩
  have hvhigh : v ≤ high := by dsimp [high]; exact Int.le_min.mpr ⟨hab.2, hbb.2⟩
  rw [hi0]
  have hstepcast : (period : Int).toNat = period := Int.toNat_natCast period
  have hprogstep : (⟨first, period, if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ : Progression) =
      ⟨first, (period : Int).toNat, if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ := by
    simp [hstepcast]
  rw [hprogstep]
  rw [show contains ⟨first, (period : Int).toNat, if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ v = true ↔
      first ≤ v ∧ v ≤ high ∧ (period : Int) ∣ v - first by
    simpa using (span_contains_iff first (period : Int) high v hppos)]
  exact ⟨first_le_of_congruence low root first v period hppos hfirst hlowv hxroot, hvhigh, hxfirst⟩

private theorem intersection_superset (a b : Progression) (v : Int)
    (ha : contains a v = true) (hb : contains b v = true) :
    contains (intersection a b) v = true := by
  by_cases han : a.n = 0
  · obtain ⟨i, hi, _⟩ := (contains_iff_mem a v).mp ha
    omega
  by_cases hbn : b.n = 0
  · obtain ⟨i, hi, _⟩ := (contains_iff_mem b v).mp hb
    omega
  have hanp : 0 < a.n := by omega
  have hbnp : 0 < b.n := by omega
  by_cases has : a.step = 0
  · have hia : intersection a b = ⟨a.lo, 1, if contains b a.lo then 1 else 0⟩ := by
      simp [intersection, han, hbn, has]
    rw [hia]
    have hvlo : v = a.lo := ((contains_of_step_zero a v has).mp ha).2
    have hcb : contains b a.lo = true := by rw [← hvlo]; exact hb
    rw [if_pos hcb, contains_unit_one]
    exact hvlo
  · by_cases hbs : b.step = 0
    · have hib : intersection a b = ⟨b.lo, 1, if contains a b.lo then 1 else 0⟩ := by
        simp [intersection, han, hbn, has, hbs]
      rw [hib]
      have hvlo : v = b.lo := ((contains_of_step_zero b v hbs).mp hb).2
      have hca : contains a b.lo = true := by rw [← hvlo]; exact ha
      rw [if_pos hca, contains_unit_one]
      exact hvlo
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
      have hgap : (g : Int) ∣ gap := by
        have hg := common_gap_dvd a b v ha hb
        simpa [gap, g] using hg
      have hgapdef : gap = b.lo - a.lo := rfl
      have hroot := root_dvd a.step b.step hasp hbsp a.lo b.lo gap hgapdef hgap
      have hdivpos : 0 < a.step / g := Nat.div_gcd_pos_of_pos_left b.step hasp
      have hpNat : 0 < period := by dsimp [period]; exact Nat.mul_pos hdivpos hbsp
      have hp : (0 : Int) < period := by exact_mod_cast hpNat
      have hfirst : first = low + (root - low) % (period : Int) := rfl
      have hfroot := first_root_dvd low root first period hp hfirst
      have hadvd := contains_dvd a v ha
      have hbdvd := contains_dvd b v hb
      have haRoot : (a.step : Int) ∣ v - root := by
        have hd : (a.step : Int) ∣ (v - a.lo) - (root - a.lo) := Int.dvd_sub hadvd hroot.1
        rwa [show (v - a.lo) - (root - a.lo) = v - root by omega] at hd
      have hbRoot : (b.step : Int) ∣ v - root := by
        have hd : (b.step : Int) ∣ (v - b.lo) - (root - b.lo) := Int.dvd_sub hbdvd hroot.2
        rwa [show (v - b.lo) - (root - b.lo) = v - root by omega] at hd
      have hvroot : (period : Int) ∣ v - root := by
        simpa [period, g] using (period_dvd a.step b.step hasp hbsp haRoot hbRoot)
      have hvfirst : (period : Int) ∣ v - first := by
        have hd : (period : Int) ∣ (v - root) - (first - root) := Int.dvd_sub hvroot hfroot
        rwa [show (v - root) - (first - root) = v - first by omega] at hd
      have hvlow : low ≤ v := by
        have hab := contains_bounds a v ha
        have hbb := contains_bounds b v hb
        omega
      have hvfirstle : first ≤ v :=
        first_le_of_congruence low root first v (period : Int) hp rfl hvlow hvroot
      have hvhigh : v ≤ high := by
        have hab := contains_bounds a v ha
        have hbb := contains_bounds b v hb
        omega
      by_cases hgapmod : gap % (g : Int) = 0
      · have hi0 : intersection a b =
            ⟨first, period, if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ := by
          have hgm : (b.lo - a.lo) % (a.step.gcd b.step : Int) = 0 := by simpa [gap, g] using hgapmod
          simp [intersection, han, hbn, has, hbs, hgm, g, gap, first, period, high, low, root, unit]
        rw [hi0]
        rw [show contains ⟨first, period,
            if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ v = true ↔
            first ≤ v ∧ v ≤ high ∧ (period : Int) ∣ v - first by
          simpa using (span_contains_iff first (period : Int) high v hp)]
        exact ⟨hvfirstle, hvhigh, hvfirst⟩
      · exact False.elim (hgapmod (Int.emod_eq_zero_of_dvd hgap))

private theorem progVal_values_of_nonneg (a : Lane) (k : Nat) (hs : ¬ a.step < 0) :
    progVal (values a) k = a.base + a.step * (k : Int) := by
  unfold progVal
  rw [values, if_neg hs, Int.ofNat_natAbs_of_nonneg (by omega)]
  simp [Int.mul_comm]

private theorem progVal_values_of_neg (a : Lane) (k : Nat) (hk : k < a.n) (hs : a.step < 0) :
    progVal (values a) k = a.base + a.step * ((a.n - 1 - k : Nat) : Int) := by
  unfold progVal
  rw [values, if_pos hs, natAbs_neg_eq a.step hs]
  have hle : k ≤ a.n - 1 := Nat.le_pred_of_lt hk
  have hcast : ((a.n - 1 - k : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (k : Int) :=
    Int.ofNat_sub hle
  rw [hcast, Int.mul_sub]
  simp [Int.mul_comm, Int.mul_neg]
  omega

private theorem rowVal_rel (a : Lane) (k : Nat) (hk : k < a.n) :
    a.base + a.step * (k : Int) =
      if a.step < 0 then progVal (values a) (a.n - 1 - k) else progVal (values a) k := by
  by_cases hs : a.step < 0
  · rw [if_pos hs]
    have hsub : a.n - 1 - (a.n - 1 - k) = k := by omega
    rw [progVal_values_of_neg a (a.n - 1 - k) (by omega) hs, hsub]
  · rw [if_neg hs]
    rw [progVal_values_of_nonneg a k hs]

theorem sound_LinearOrdinal_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal.ResidueJoin.Goal := by
  intro c b
  let a : Lane := lane c.n 0 1 c.base c.step
  have ha_n : a.n = c.n := by simp [a, lane]
  rw [rows_linear c]
  simp only [List.map_map, Function.comp_def]
  have hcnt : ∀ j : Nat,
      (List.range b.n).countP (fun i : Nat => c.base + c.step * (j : Int) == b.base + b.step * (i : Int)) =
        laneCount b.base b.step b.n (c.base + c.step * (j : Int)) := by
    intro j
    unfold laneCount
    apply List.countP_congr
    intro i hi
    by_cases h : c.base + c.step * (j : Int) == b.base + b.step * (i : Int)
    · have heqInt : c.base + c.step * (j : Int) = b.base + b.step * (i : Int) := by simpa using h
      have h' : b.base + b.step * (i : Int) == c.base + c.step * (j : Int) := by simp [heqInt]
      simp [h, h']
    · have hneInt : c.base + c.step * (j : Int) ≠ b.base + b.step * (i : Int) := by simpa using h
      have h' : ¬ b.base + b.step * (i : Int) == c.base + c.step * (j : Int) := by
        intro hh
        have hh' : b.base + b.step * (i : Int) = c.base + c.step * (j : Int) := by simpa using hh
        exact hneInt hh'.symm
      simp [h, h']
  rw [show (List.range c.n).map (fun j : Nat =>
        (List.range b.n).countP (fun i : Nat => c.base + c.step * (j : Int) == b.base + b.step * (i : Int))) =
      (List.range c.n).map (fun j : Nat => laneCount b.base b.step b.n (c.base + c.step * (j : Int))) by
    apply List.map_congr_left
    intro j hj
    exact hcnt j]
  have hlanes : TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal.layout.lanes c = [a] := by
    simp [TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal.layout, a]
  rw [hlanes]
  simp only [List.map_singleton, List.sum_singleton]
  by_cases hcn : c.n = 0
  · have hr : List.range c.n = [] := by simp [hcn]
    rw [hr]
    simp [joinCount, values, intersection, a, lane, hcn, ha_n]
  by_cases hbn : b.n = 0
  · have hr : List.range b.n = [] := by simp [hbn]
    simp [laneCount, hr, joinCount, values, intersection, a, lane, hbn]
    rw [sum_map_const_nat]
    simp
  by_cases hcs : c.step = 0
  · have hconst : ∀ j : Nat, laneCount b.base b.step b.n (c.base + c.step * (j : Int)) =
        laneCount b.base b.step b.n c.base := by
      intro j
      simp [hcs]
    rw [show (List.range c.n).map (fun j : Nat =>
        laneCount b.base b.step b.n (c.base + c.step * (j : Int))) =
        (List.range c.n).map (fun _ : Nat => laneCount b.base b.step b.n c.base) by
      apply List.map_congr_left
      intro j hj
      exact hconst j]
    rw [sum_map_const_nat]
    have haint : intersection (values a) (values b) =
        ⟨c.base, 1, if contains (values b) c.base then 1 else 0⟩ := by
      simp [values, a, lane, intersection, hcs, hcn, hbn]
      rfl
    rw [joinCount, haint]
    by_cases hbs : b.step = 0
    · have hconts : contains (values b) c.base = true ↔ c.base = b.base := by
        rw [lane_values_contains]
        constructor
        · rintro ⟨i, hi, hval⟩
          simpa [hbs] using hval
        · intro hv
          refine ⟨0, by omega, ?_⟩
          simpa [hbs] using hv
      by_cases hv : c.base = b.base
      · have hc : contains (values b) c.base = true := hconts.mpr hv
        have hc' : contains (values b) b.base = true := by simpa [hv] using hc
        simp [lane_value_count, hbs, hv, hc, hc', a, lane, hcs, ha_n]
      · have hc : ¬ contains (values b) c.base = true := fun h => hv (hconts.mp h)
        simp [lane_value_count, hbs, hv, hc, a, hcs, ha_n]
    · have hcntval : laneCount b.base b.step b.n c.base =
          if contains (values b) c.base then 1 else 0 := by
        rw [lane_value_count]
        simp [hbs]
        by_cases hex : ∃ i, i < b.n ∧ c.base = b.base + b.step * (i : Int)
        · have hc : contains (values b) c.base = true := (lane_values_contains b c.base).mpr hex
          simp [hex, hc]
        · have hc : ¬ contains (values b) c.base = true := fun h => hex ((lane_values_contains b c.base).mp h)
          simp [hex, hc]
      rw [hcntval]
      simp [a, lane, hcs, hbs, ha_n, Nat.mul_comm]
  by_cases hbs : b.step = 0
  · have hcntval : ∀ j : Nat, laneCount b.base b.step b.n (c.base + c.step * (j : Int)) =
        if c.base + c.step * (j : Int) == b.base then b.n else 0 := by
      intro j
      simp [lane_value_count, hbs]
    rw [show (List.range c.n).map (fun j : Nat =>
        laneCount b.base b.step b.n (c.base + c.step * (j : Int))) =
        (List.range c.n).map (fun j : Nat =>
          if c.base + c.step * (j : Int) == b.base then b.n else 0) by
      apply List.map_congr_left
      intro j hj
      exact hcntval j]
    rw [show ((List.range c.n).map (fun j : Nat =>
        if c.base + c.step * (j : Int) == b.base then b.n else 0)).sum =
        b.n * (List.range c.n).countP (fun j : Nat => c.base + c.step * (j : Int) == b.base) by
      rw [← countP_eq_sum_const (List.range c.n)
        (fun j : Nat => c.base + c.step * (j : Int) == b.base) b.n]
      rw [Nat.mul_comm]]
    have hcountBool : (List.range c.n).countP (fun j : Nat => c.base + c.step * (j : Int) == b.base) =
        if contains (values a) b.base then 1 else 0 := by
      rw [lane_value_count_inj c.base c.step c.n b.base hcs]
      by_cases hex : ∃ i, i < c.n ∧ b.base = c.base + c.step * (i : Int)
      · have hc : contains (values a) b.base = true := by
          rw [lane_values_contains]
          refine ⟨hex.choose, ?_, ?_⟩
          · simpa [a, lane] using hex.choose_spec.1
          · change b.base = c.base + c.step * (hex.choose : Int)
            exact hex.choose_spec.2
        simp [hex, hc]
      · have hc : ¬ contains (values a) b.base = true := by
          intro h
          apply hex
          have h' := (lane_values_contains a b.base).mp h
          refine ⟨h'.choose, ?_, ?_⟩
          · simpa [a, lane] using h'.choose_spec.1
          · change b.base = c.base + c.step * (h'.choose : Int)
            exact h'.choose_spec.2
        simp [hex, hc]
    rw [hcountBool]
    have haint : intersection (values a) (values b) =
        ⟨b.base, 1, if contains (values a) b.base then 1 else 0⟩ := by
      simp [values, a, lane, intersection, hcs, hbs, hcn, hbn]
    rw [joinCount, haint]
    simp [a, lane, hcs, hbs, ha_n, Nat.mul_comm]
  · let p : Progression := values a
    let q : Progression := values b
    let r : Progression := intersection p q
    have hp : 0 < p.step := by
      dsimp [p, values, a]
      apply Nat.pos_of_ne_zero
      intro h
      apply hcs
      exact Int.natAbs_eq_zero.mp h
    have hr : 0 < r.step := by dsimp [r]; exact intersection_step_pos p q
    have hsubPR : ∀ v : Int, contains r v = true → contains p v = true := by
      intro v hv
      exact (intersection_subset p q v hv).1
    have hpn : p.n = c.n := by simp [p, values, a, lane, ha_n]
    have hcountR : (List.range c.n).countP (fun k => contains r (progVal p k)) = r.n := by
      rw [← hpn]
      exact count_mem_progression p r hp hr hsubPR
    have hcntj : ∀ j : Nat, laneCount b.base b.step b.n (c.base + c.step * (j : Int)) =
        if contains q (c.base + c.step * (j : Int)) then 1 else 0 := by
      intro j
      rw [lane_value_count]
      simp [hbs]
      by_cases hex : ∃ i, i < b.n ∧ c.base + c.step * (j : Int) = b.base + b.step * (i : Int)
      · have hc : contains q (c.base + c.step * (j : Int)) = true := (lane_values_contains b _).mpr hex
        simp [hex, hc, q]
      · have hc : ¬ contains q (c.base + c.step * (j : Int)) = true :=
          fun h => hex ((lane_values_contains b _).mp h)
        simp [hex, hc, q]
    rw [show (List.range c.n).map (fun j : Nat =>
        laneCount b.base b.step b.n (c.base + c.step * (j : Int))) =
        (List.range c.n).map (fun j : Nat =>
          if contains q (c.base + c.step * (j : Int)) then 1 else 0) by
      apply List.map_congr_left
      intro j hj
      exact hcntj j]
    rw [← countP_eq_sum_bool]
    have hiff : ∀ j ∈ List.range c.n,
        (contains q (c.base + c.step * (j : Int)) = true ↔
         contains r (c.base + c.step * (j : Int)) = true) := by
      intro j hj
      have hjlt : j < c.n := List.mem_range.mp hj
      have hpj : contains p (c.base + c.step * (j : Int)) = true := by
        rw [lane_values_contains a]
        exact ⟨j, by simpa [ha_n] using hjlt, rfl⟩
      constructor
      · intro hqj
        exact intersection_superset p q (c.base + c.step * (j : Int)) hpj hqj
      · intro hrj
        exact (intersection_subset p q (c.base + c.step * (j : Int)) hrj).2
    rw [show (List.range c.n).countP (fun j : Nat => contains q (c.base + c.step * (j : Int))) =
        (List.range c.n).countP (fun j : Nat => contains r (c.base + c.step * (j : Int))) by
      apply List.countP_congr
      intro j hj
      by_cases hqj : contains q (c.base + c.step * (j : Int)) = true
      · have hrj := (hiff j hj).mp hqj
        simp [hqj, hrj]
      · have hrj : ¬ contains r (c.base + c.step * (j : Int)) = true :=
          fun h => hqj ((hiff j hj).mpr h)
        simp [hqj, hrj]]
    have hpredR : ∀ j ∈ List.range c.n,
        contains r (c.base + c.step * (j : Int)) = true ↔
          contains r (progVal p (if c.step < 0 then c.n - 1 - j else j)) = true := by
      intro j hj
      have hjlt : j < c.n := List.mem_range.mp hj
      have hv := rowVal_rel a j (by simpa [ha_n] using hjlt)
      by_cases hsa : a.step < 0
      · rw [if_pos hsa] at hv
        have hsc : c.step < 0 := by change a.step < 0; exact hsa
        rw [if_pos hsc]
        change c.base + c.step * (j : Int) = progVal (values a) (a.n - 1 - j) at hv
        rw [hv]
        simp [p, ha_n]
      · rw [if_neg hsa] at hv
        have hsc : ¬ c.step < 0 := by change ¬ a.step < 0; exact hsa
        rw [if_neg hsc]
        change c.base + c.step * (j : Int) = progVal (values a) j at hv
        rw [hv]
    have hcountRowR : (List.range c.n).countP (fun j : Nat => contains r (c.base + c.step * (j : Int))) =
        (List.range c.n).countP (fun k : Nat => contains r (progVal p k)) := by
      rw [List.countP_congr hpredR]
      by_cases hs : c.step < 0
      · simp only [if_pos hs]
        exact (countP_reverse c.n (fun k => contains r (progVal p k))).symm
      · simp only [if_neg hs]
    rw [hcountRowR, hcountR]
    have hjoin : joinCount a b = r.n := by
      dsimp [r, p, q]
      rw [joinCount]
      have ha0 : a.step ≠ 0 := by simpa [a, lane] using hcs
      simpa [ha0, hbs]
    rw [hjoin]

end TinylambOpt.ResearchProofs.Atlas.Goal_LinearOrdinal_ResidueJoin

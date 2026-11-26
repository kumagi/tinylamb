import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `SparseSample.ResidueJoin` — `∀ c (b : Lane), ((rows layout c).map (fun r =>
  (List.range b.n).countP (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum =
  ((layout.lanes c).map (joinCount · b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.SparseSampleResidueJoin

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-! ## Arithmetic progressions and their intersection (CRT core) -/

/-! # Arithmetic progressions and their intersection -/

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
    have hn : 0 < n := by
      by_cases hn : 0 < n
      · exact hn
      · have hn0 : n = 0 := by omega
        simp [contains, hn0] at h
    have hlo : lo ≤ v := by
      by_cases h' : lo ≤ v
      · exact h'
      · simp [contains, h'] at h
    have hhi : v ≤ lo := by
      have hlast : lastVal ⟨lo, 0, n⟩ = lo := by simp [lastVal]
      by_cases h' : v ≤ lastVal ⟨lo, 0, n⟩
      · simpa [hlast] using h'
      · simp [contains, h'] at h
    have : v = lo := by omega
    exact ⟨hn, this⟩
  · rintro ⟨hn, rfl⟩
    have : lastVal ⟨v, 0, n⟩ = v := by simp [lastVal]
    simp [contains, hn, this]

private theorem contains_bounds (a : Progression) (v : Int) (h : contains a v = true) :
    a.lo ≤ v ∧ v ≤ lastVal a := by
  obtain ⟨k, hk, hval⟩ := (contains_iff_mem a v).mp h
  unfold progVal at hval
  have hkint : (k : Int) ≤ ((a.n - 1 : Nat) : Int) := by exact_mod_cast (by omega)
  have hmul : (k : Int) * (a.step : Int) ≤ ((a.n - 1 : Nat) : Int) * (a.step : Int) :=
    Int.mul_le_mul_of_nonneg_right hkint (Int.natCast_nonneg a.step)
  rw [hval]
  constructor
  · exact Int.le_add_of_nonneg_right
      (Int.mul_nonneg (Int.natCast_nonneg k) (Int.natCast_nonneg a.step))
  · unfold lastVal
    exact Int.add_le_add_left hmul a.lo

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
  have hd1 : (a.step.gcd b.step : Int) ∣ (b.step : Int) :=
    Int.ofNat_dvd.mpr (Nat.gcd_dvd_right a.step b.step)
  have hd2 : (a.step.gcd b.step : Int) ∣ (a.step : Int) :=
    Int.ofNat_dvd.mpr (Nat.gcd_dvd_left a.step b.step)
  obtain ⟨c1, hc1⟩ := hd1
  obtain ⟨c2, hc2⟩ := hd2
  have h1 : (a.step.gcd b.step : Int) ∣ (b.step : Int) * (j : Int) := by
    refine ⟨c1 * (j : Int), ?_⟩
    rw [hc1]
    grind
  have h2 : (a.step.gcd b.step : Int) ∣ (a.step : Int) * (i : Int) := by
    refine ⟨c2 * (i : Int), ?_⟩
    rw [hc2]
    grind
  have hi' : (a.step : Int) * (i : Int) = v - a.lo := by
    calc
      (a.step : Int) * (i : Int) = (i : Int) * (a.step : Int) := by ac_rfl
      _ = v - a.lo := by omega
  have hj' : (b.step : Int) * (j : Int) = v - b.lo := by
    calc
      (b.step : Int) * (j : Int) = (j : Int) * (b.step : Int) := by ac_rfl
      _ = v - b.lo := by omega
  have heq : (b.step : Int) * (j : Int) - (a.step : Int) * (i : Int) = a.lo - b.lo := by
    calc
      (b.step : Int) * (j : Int) - (a.step : Int) * (i : Int) = (v - b.lo) - (v - a.lo) := by rw [hj', hi']
      _ = a.lo - b.lo := by omega
  have hdiv : (a.step.gcd b.step : Int) ∣ a.lo - b.lo := by
    rw [← heq]
    exact Int.dvd_sub h1 h2
  obtain ⟨c, hc⟩ := hdiv
  refine ⟨-c, ?_⟩
  grind

private theorem period_dvd_delta (s t : Nat) (hs : 0 < s) (ht : 0 < t) (delta : Int)
    (hds : (s : Int) ∣ delta) (hdt : (t : Int) ∣ delta) :
    (((s / Nat.gcd s t) * t : Nat) : Int) ∣ delta := by
  let g := Nat.gcd s t
  let sa := s / g
  let tb := t / g
  have hg : 0 < g := Nat.gcd_pos_of_pos_left t hs
  have hcop : Nat.gcd sa tb = 1 := by
    simpa [sa, tb, g] using reduced_coprime s t hs ht
  obtain ⟨k1, hk1⟩ := hds
  obtain ⟨k2, hk2⟩ := hdt
  rcases hpair : bezout sa tb with ⟨u, v⟩
  have hbez := bezout_spec sa tb
  rw [hpair] at hbez
  simp only [Prod.snd] at hbez
  rw [hcop] at hbez
  have hg0 : (g : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hg)
  have hst : (s : Int) * k1 = (t : Int) * k2 := by rw [← hk1, ← hk2]
  have htNat : t = g * tb := by
    change t = Nat.gcd s t * (t / Nat.gcd s t)
    exact (Nat.mul_div_cancel' (Nat.gcd_dvd_right s t)).symm
  have hperNat : (s / Nat.gcd s t) * t = g * sa * tb := by
    change sa * t = g * sa * tb
    rw [htNat]
    grind
  have hper : (((s / Nat.gcd s t) * t : Nat) : Int) = (g : Int) * (sa : Int) * (tb : Int) := by
    exact_mod_cast hperNat
  have hsa : (s : Int) = (g : Int) * (sa : Int) := by
    exact_mod_cast (by simpa [sa, g] using (Nat.mul_div_cancel' (Nat.gcd_dvd_left s t)).symm)
  have htEq : (t : Int) = (g : Int) * (tb : Int) := by exact_mod_cast htNat
  rw [hsa, htEq] at hst
  have hEq : (sa : Int) * k1 = (tb : Int) * k2 := by
    apply Int.eq_of_mul_eq_mul_left hg0
    calc
      (g : Int) * ((sa : Int) * k1) = ((g : Int) * (sa : Int)) * k1 := by grind
      _ = ((g : Int) * (tb : Int)) * k2 := hst
      _ = (g : Int) * ((tb : Int) * k2) := by grind
  refine ⟨u * k2 + v * k1, ?_⟩
  rw [hk1, hsa, hper]
  grind


private theorem contains_prog_self (a : Progression) (k : Nat) (hk : k < a.n) :
    contains a (progVal a k) = true := by
  apply (contains_iff_mem a (progVal a k)).mpr
  exact ⟨k, hk, rfl⟩

private theorem intersection_contains (a b : Progression) (v : Int) :
    contains (intersection a b) v = true ↔ contains a v = true ∧ contains b v = true := by
  by_cases han : a.n = 0
  · have hint : intersection a b = ⟨0, 1, 0⟩ := by simp [intersection, han]
    rw [hint]
    simp [han, contains]
  by_cases hbn : b.n = 0
  · have hint : intersection a b = ⟨0, 1, 0⟩ := by simp [intersection, han, hbn]
    rw [hint]
    simp [hbn, contains]
  have hanp : 0 < a.n := by omega
  have hbnp : 0 < b.n := by omega
  by_cases ha0 : a.step = 0
  · by_cases hbcont : contains b a.lo = true
    · have hint : intersection a b = ⟨a.lo, 1, 1⟩ := by
        simp [intersection, han, hbn, ha0, hbcont]
      rw [hint]
      constructor
      · intro h
        have hvv : v = a.lo := by
          have hbnd : a.lo ≤ v ∧ v ≤ a.lo := by simpa [contains, lastVal] using h
          omega
        rw [hvv]
        refine ⟨?_, hbcont⟩
        apply (contains_iff_mem a a.lo).mpr
        refine ⟨0, hanp, ?_⟩
        simp [progVal, ha0]
      · rintro ⟨ha, _⟩
        obtain ⟨k, hk, hval⟩ := (contains_iff_mem a v).mp ha
        have hvv : v = a.lo := by simpa [progVal, ha0] using hval
        have hbnd : a.lo ≤ v ∧ v ≤ a.lo := by omega
        rw [hvv]
        simp [contains, lastVal]
    · have hint : intersection a b = ⟨a.lo, 1, 0⟩ := by
        simp [intersection, han, hbn, ha0, hbcont]
      rw [hint]
      constructor
      · intro h
        exact False.elim (by simpa [contains] using h)
      · rintro ⟨ha, hb⟩
        obtain ⟨k, hk, hval⟩ := (contains_iff_mem a v).mp ha
        have hvv : v = a.lo := by simpa [progVal, ha0] using hval
        exfalso
        exact hbcont (hvv ▸ hb)
  · by_cases hb0 : b.step = 0
    · by_cases hacont : contains a b.lo = true
      · have hint : intersection a b = ⟨b.lo, 1, 1⟩ := by
          simp [intersection, han, hbn, ha0, hb0, hacont]
        rw [hint]
        constructor
        · intro h
          have hvv : v = b.lo := by
            simp [contains, lastVal] at h
            omega
          subst v
          refine ⟨hacont, ?_⟩
          apply (contains_iff_mem b b.lo).mpr
          refine ⟨0, hbnp, ?_⟩
          simp [progVal, hb0]
        · rintro ⟨_, hb⟩
          obtain ⟨k, hk, hval⟩ := (contains_iff_mem b v).mp hb
          have hvv : v = b.lo := by simpa [progVal, hb0] using hval
          have hbounds : b.lo ≤ v ∧ v ≤ b.lo := by omega
          rw [hvv]
          simp [contains, lastVal]
      · have hint : intersection a b = ⟨b.lo, 1, 0⟩ := by
          simp [intersection, han, hbn, ha0, hb0, hacont]
        rw [hint]
        constructor
        · intro h
          exact False.elim (by simpa [contains] using h)
        · rintro ⟨ha, hbv⟩
          obtain ⟨k, hk, hval⟩ := (contains_iff_mem b v).mp hbv
          have hvv : v = b.lo := by simpa [progVal, hb0] using hval
          exfalso
          exact hacont (hvv ▸ ha)
    · have hasp : 0 < a.step := Nat.pos_of_ne_zero ha0
      have hbsp : 0 < b.step := Nat.pos_of_ne_zero hb0
      let g := a.step.gcd b.step
      let gap := b.lo - a.lo
      let period := (a.step / g) * b.step
      let unit := (bezout (a.step / g) (b.step / g)).1
      let root := a.lo + (a.step : Int) * ((gap / (g : Int) * unit) % ((b.step / g : Nat) : Int))
      let low := max a.lo b.lo
      let first := low + (root - low) % (period : Int)
      let high := min (lastVal a) (lastVal b)
      have hg : 0 < g := Nat.gcd_pos_of_pos_left b.step hasp
      have hasg : 0 < a.step / g := Nat.div_pos (Nat.le_of_dvd hasp (Nat.gcd_dvd_left a.step b.step)) hg
      have hpNat : 0 < period := by dsimp [period]; exact Nat.mul_pos hasg hbsp
      have hp : (0 : Int) < period := by exact_mod_cast hpNat
      have hfirst : first = low + (root - low) % (period : Int) := rfl
      have hfroot : (period : Int) ∣ first - root := by
        simpa using (first_root_dvd low root first (period : Int) hp hfirst)
      have hsaNat : a.step = g * (a.step / g) := (Nat.mul_div_cancel' (Nat.gcd_dvd_left a.step b.step)).symm
      have hgtNat : g ∣ b.step := Nat.gcd_dvd_right a.step b.step
      obtain ⟨cg, hcg⟩ := hgtNat
      have hpaNat : a.step ∣ period := by
        refine ⟨cg, ?_⟩
        dsimp [period]
        calc
          a.step / g * b.step = (a.step / g) * (g * cg) := by rw [hcg]
          _ = (g * (a.step / g)) * cg := by ac_rfl
          _ = a.step * cg := by rw [← hsaNat]
      have hpbNat : b.step ∣ period := by
        dsimp [period]
        refine ⟨a.step / g, ?_⟩
        ac_rfl
      have hpa : (a.step : Int) ∣ (period : Int) := Int.ofNat_dvd.mpr hpaNat
      have hpb : (b.step : Int) ∣ (period : Int) := Int.ofNat_dvd.mpr hpbNat
      by_cases hgapmod : gap % (g : Int) = 0
      · have hgapdvd : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgapmod
        have hroot := root_dvd a.step b.step hasp hbsp a.lo b.lo gap rfl hgapdvd
        have hint : intersection a b = ⟨first, period,
            if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ := by
          simp [intersection, han, hbn, ha0, hb0, hgapmod, first, low, high, period, unit, root, g, gap]
        rw [hint]
        have htoNat : (period : Int).toNat = period := by omega
        have hsci : contains ⟨first, period,
            if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ v = true ↔
            first ≤ v ∧ v ≤ high ∧ (period : Int) ∣ v - first := by
          simpa [htoNat] using (span_contains_iff first (period : Int) high v hp)
        rw [hsci]
        constructor
        · rintro ⟨hfirstle, hvhigh, hvfirst⟩
          have hvroot : (period : Int) ∣ v - root := by
            have heq : v - root = (v - first) + (first - root) := by omega
            rw [heq]
            exact Int.dvd_add hvfirst hfroot
          have hvarA : (a.step : Int) ∣ v - root := Int.dvd_trans hpa hvroot
          have hvarB : (b.step : Int) ∣ v - root := Int.dvd_trans hpb hvroot
          have hvalo : (a.step : Int) ∣ v - a.lo := by
            have hd := Int.dvd_add hvarA hroot.1
            have heq : v - a.lo = (v - root) + (root - a.lo) := by omega
            rw [heq]
            exact hd
          have hvbl : (b.step : Int) ∣ v - b.lo := by
            have hd := Int.dvd_add hvarB hroot.2
            have heq : v - b.lo = (v - root) + (root - b.lo) := by omega
            rw [heq]
            exact hd
          have hlow_first : low ≤ first := by
            dsimp [first]
            have hrem : 0 ≤ (root - low) % (period : Int) := Int.emod_nonneg _ (by omega)
            omega
          have hlow_v : low ≤ v := by omega
          have hhigh_a : high ≤ lastVal a := by dsimp [high]; omega
          have hhigh_b : high ≤ lastVal b := by dsimp [high]; omega
          constructor
          · exact mem_of_bounds_dvd a v hanp hasp (by dsimp [low] at hlow_v; omega)
              (by omega) hvalo
          · exact mem_of_bounds_dvd b v hbnp hbsp (by dsimp [low] at hlow_v; omega)
              (by omega) hvbl
        · rintro ⟨ha, hb⟩
          have hab := contains_bounds a v ha
          have hbb := contains_bounds b v hb
          have hvbl := contains_dvd b v hb
          have hvalo : (a.step : Int) ∣ v - a.lo := contains_dvd a v ha
          have hvarA : (a.step : Int) ∣ v - root := by
            have hd := Int.dvd_sub hvalo hroot.1
            have heq : v - root = (v - a.lo) - (root - a.lo) := by omega
            rw [heq]
            exact hd
          have hvarB : (b.step : Int) ∣ v - root := by
            have hd := Int.dvd_sub hvbl hroot.2
            have heq : v - root = (v - b.lo) - (root - b.lo) := by omega
            rw [heq]
            exact hd
          have hvroot : (period : Int) ∣ v - root :=
            by simpa [period] using
              (period_dvd_delta a.step b.step hasp hbsp (v - root) hvarA hvarB)
          have hvfirst : (period : Int) ∣ v - first := by
            have heq : v - first = (v - root) - (first - root) := by omega
            rw [heq]
            exact Int.dvd_sub hvroot hfroot
          have hvlow : low ≤ v := by dsimp [low]; omega
          have hfirst_le_v : first ≤ v :=
            by simpa using (first_le_of_congruence low root first v (period : Int) hp hfirst hvlow hvroot)
          exact ⟨hfirst_le_v, by omega, hvfirst⟩
      · have hint : intersection a b = ⟨0, 1, 0⟩ := by
          simp [intersection, han, hbn, ha0, hb0, hgapmod, gap, g]
        rw [hint]
        constructor
        · intro h
          simp [contains] at h
        · rintro ⟨ha, hb⟩
          have hgdiv := common_gap_dvd a b v ha hb
          exfalso
          exact hgapmod (Int.emod_eq_zero_of_dvd hgdiv)


/-! ## Counting common values of progressions -/

private theorem decide_eq_of_iff {a b : Prop} [Decidable a] [Decidable b] (h : a ↔ b) :
    decide a = decide b := by
  by_cases ha : a <;> by_cases hb : b <;> simp_all

private theorem filter_congr_bool {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, p x = q x) : l.filter p = l.filter q := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filter_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem progVal_inj (P : Progression) (hs : 0 < P.step) {i j : Nat}
    (hi : i < P.n) (hj : j < P.n) (h : progVal P i = progVal P j) : i = j := by
  unfold progVal at h
  have hs0 : (P.step : Int) ≠ 0 := by omega
  have hmul : (i : Int) * (P.step : Int) = (j : Int) * (P.step : Int) := by omega
  have hij : (i : Int) = (j : Int) := Int.eq_of_mul_eq_mul_right hs0 hmul
  exact_mod_cast hij

private theorem countP_prog_pos (Q : Progression) (hs : 0 < Q.step) (v : Int) :
    (List.range Q.n).countP (fun j => progVal Q j == v) =
      if contains Q v = true then 1 else 0 := by
  rw [List.countP_eq_length_filter]
  by_cases hcv : contains Q v = true
  · obtain ⟨j, hj, hval⟩ := (contains_iff_mem Q v).mp hcv
    have hpred : (List.range Q.n).filter (fun i => progVal Q i == v) =
        (List.range Q.n).filter (fun i => i == j) := by
      apply filter_congr_bool
      intro i hi
      have hilt : i < Q.n := List.mem_range.mp hi
      by_cases hij : i = j
      · subst i
        simp [hval]
      · have hne : progVal Q i ≠ v := by
          intro hv
          have heq : progVal Q i = progVal Q j := by rw [hv, hval]
          exact hij (progVal_inj Q hs hilt hj heq)
        simp [beq_eq_false_iff_ne.mpr hne, beq_eq_false_iff_ne.mpr hij]
    rw [hpred, List.filter_beq]
    have hjcnt : List.count j (List.range Q.n) = 1 :=
      (List.nodup_range (n := Q.n)).count_of_mem (List.mem_range.mpr hj)
    rw [hjcnt]
    simp [hcv]
  · have hnil : (List.range Q.n).filter (fun i => progVal Q i == v) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro i hi hv
      have hv' : v = progVal Q i := (of_decide_eq_true hv).symm
      exact hcv ((contains_iff_mem Q v).mpr ⟨i, List.mem_range.mp hi, hv'⟩)
    rw [hnil]
    simp [hcv]

private theorem countP_comm {α β : Type} [BEq α] [LawfulBEq α]
    (l : List β) (a : α) (f : β → α) :
    l.countP (fun j => a == f j) = l.countP (fun j => f j == a) := by
  apply List.countP_congr
  intro j hj
  constructor <;> intro h
  · exact beq_iff_eq.mpr (beq_iff_eq.mp h).symm
  · exact beq_iff_eq.mpr (beq_iff_eq.mp h).symm

private theorem countP_prog_zero (Q : Progression) (hs : Q.step = 0) (v : Int) :
    (List.range Q.n).countP (fun j => progVal Q j == v) =
      if contains Q v = true then Q.n else 0 := by
  by_cases hcv : contains Q v = true
  · have hlo : Q.lo = v := by
      obtain ⟨k, hk, hval⟩ := (contains_iff_mem Q v).mp hcv
      unfold progVal at hval
      rw [hs] at hval
      simpa using hval.symm
    rw [List.countP_eq_length_filter]
    have hf : (List.range Q.n).filter (fun j => progVal Q j == v) = List.range Q.n := by
      apply List.filter_eq_self.mpr
      intro j hj
      simp [progVal, hs, hlo]
    rw [hf, List.length_range]
    simp [hcv]
  · rw [List.countP_eq_length_filter]
    have hf : (List.range Q.n).filter (fun j => progVal Q j == v) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro j hj hv
      have hv' : v = Q.lo := by
        have ht : progVal Q j = v := of_decide_eq_true hv
        unfold progVal at ht
        rw [hs] at ht
        simpa using ht.symm
      apply hcv
      apply (contains_iff_mem Q v).mpr
      refine ⟨0, ?_, ?_⟩
      · have hjlt : j < Q.n := List.mem_range.mp hj
        omega
      · unfold progVal
        rw [hs, hv']
        simp
    rw [hf]
    simp [hcv]

private theorem countP_contains_sub (P r : Progression) (hP : 0 < P.step) (hr : 0 < r.step)
    (hsub : ∀ v, contains r v = true → contains P v = true) :
    (List.range P.n).countP (fun (i : Nat) => contains r (progVal P i)) = r.n := by
  let idx : Nat → Nat := fun k => ((progVal r k - P.lo) / (P.step : Int)).toNat
  have hidx : ∀ k, k < r.n → idx k < P.n ∧ progVal r k = progVal P (idx k) := by
    intro k hk
    have hcr : contains r (progVal r k) = true := contains_prog_self r k hk
    have hcp : contains P (progVal r k) = true := hsub _ hcr
    obtain ⟨i, hi, hival⟩ := (contains_iff_mem P (progVal r k)).mp hcp
    have hquot : (progVal r k - P.lo) / (P.step : Int) = (i : Int) := by
      unfold progVal at hival
      change (r.lo + (k : Int) * (r.step : Int) - P.lo) / (P.step : Int) = (i : Int)
      rw [hival]
      have hsub' : P.lo + (i : Int) * (P.step : Int) - P.lo = (i : Int) * (P.step : Int) := by omega
      rw [hsub']
      exact Int.mul_ediv_cancel (i : Int) (by omega : (P.step : Int) ≠ 0)
    constructor
    · dsimp [idx]
      rw [hquot]
      exact_mod_cast hi
    · dsimp [idx]
      rw [hquot]
      have hit : (i : Int).toNat = i := by omega
      rw [hit]
      exact hival
  let idx' : Nat → Nat := fun k => if k < r.n then idx k else P.n + r.n + k
  have hidx'_on : ∀ k, k < r.n → idx' k = idx k := by intro k hk; simp [idx', hk]
  have hidx'_inj : ∀ a b, idx' a = idx' b → a = b := by
    intro a b hab
    by_cases ha : a < r.n
    · by_cases hb : b < r.n
      · have hva := (hidx a ha).2
        have hvb := (hidx b hb).2
        rw [hidx'_on a ha, hidx'_on b hb] at hab
        have hPidx : progVal P (idx a) = progVal P (idx b) := congrArg (fun k => progVal P k) hab
        have hij := progVal_inj P hP (hidx a ha).1 (hidx b hb).1 hPidx
        have hreq : progVal r a = progVal r b := by
          calc
            progVal r a = progVal P (idx a) := hva
            _ = progVal P (idx b) := hPidx
            _ = progVal r b := hvb.symm
        exact progVal_inj r hr ha hb hreq
      · rw [hidx'_on a ha] at hab
        simp [idx', hb] at hab
        have halt : idx a < P.n := (hidx a ha).1
        omega
    · by_cases hb : b < r.n
      · rw [hidx'_on b hb] at hab
        simp [idx', ha] at hab
        have halt : idx b < P.n := (hidx b hb).1
        omega
      · simp [idx', ha, hb] at hab
        omega
  let L := (List.range P.n).filter (fun (i : Nat) => contains r (progVal P i))
  let M := (List.range r.n).map idx'
  have hperm : L.Perm M := by
    apply (List.perm_ext_iff_of_nodup
      (List.Pairwise.filter _ List.nodup_range)
      (List.Pairwise.map idx' (fun a b hne h => hne (hidx'_inj a b h)) List.nodup_range)).mpr
    intro i
    constructor
    · intro hi
      have hiP : i < P.n := List.mem_range.mp (List.mem_filter.mp hi).1
      have hcr : contains r (progVal P i) = true := (List.mem_filter.mp hi).2
      obtain ⟨k, hk, hval⟩ := (contains_iff_mem r (progVal P i)).mp hcr
      have hmemM : i ∈ M := by
        apply List.mem_map.mpr
        refine ⟨k, List.mem_range.mpr hk, ?_⟩
        rw [hidx'_on k hk]
        have hik := hidx k hk
        have heqv : progVal P (idx k) = progVal P i := by rw [← hik.2, ← hval]
        exact progVal_inj P hP hik.1 hiP heqv
      exact hmemM
    · intro hi
      obtain ⟨k, hk, hval⟩ := List.mem_map.mp hi
      have hkk := List.mem_range.mp hk
      have hik := hidx k hkk
      rw [hidx'_on k hkk] at hval
      have hiP : i < P.n := by rw [← hval]; exact hik.1
      apply List.mem_filter.mpr
      constructor
      · exact List.mem_range.mpr hiP
      · rw [← hval, ← hik.2]
        exact contains_prog_self r k hkk
  rw [List.countP_eq_length_filter]
  rw [hperm.length_eq]
  simp [M]

private theorem filter_indices_perm (P r : Progression) (hP : 0 < P.step) (hr : 0 < r.step)
    (hsub : ∀ v, contains r v = true → contains P v = true) :
    ((List.range P.n).filter (fun (i : Nat) => contains r (progVal P i))).Perm
      ((List.range r.n).map (fun k => ((progVal r k - P.lo) / (P.step : Int)).toNat)) := by
  let idx : Nat → Nat := fun k => ((progVal r k - P.lo) / (P.step : Int)).toNat
  have hidx : ∀ k, k < r.n → idx k < P.n ∧ progVal r k = progVal P (idx k) := by
    intro k hk
    have hcr : contains r (progVal r k) = true := contains_prog_self r k hk
    have hcp : contains P (progVal r k) = true := hsub _ hcr
    obtain ⟨i, hi, hival⟩ := (contains_iff_mem P (progVal r k)).mp hcp
    have hquot : (progVal r k - P.lo) / (P.step : Int) = (i : Int) := by
      unfold progVal at hival
      change (r.lo + (k : Int) * (r.step : Int) - P.lo) / (P.step : Int) = (i : Int)
      rw [hival]
      have hsub' : P.lo + (i : Int) * (P.step : Int) - P.lo = (i : Int) * (P.step : Int) := by omega
      rw [hsub']
      exact Int.mul_ediv_cancel (i : Int) (by omega : (P.step : Int) ≠ 0)
    constructor
    · dsimp [idx]
      rw [hquot]
      exact_mod_cast hi
    · dsimp [idx]
      rw [hquot]
      have hit : (i : Int).toNat = i := by omega
      rw [hit]
      exact hival
  let idx' : Nat → Nat := fun k => if k < r.n then idx k else P.n + r.n + k
  have hidx'_on : ∀ k, k < r.n → idx' k = idx k := by intro k hk; simp [idx', hk]
  have hidx'_inj : ∀ a b, idx' a = idx' b → a = b := by
    intro a b hab
    by_cases ha : a < r.n
    · by_cases hb : b < r.n
      · have hva := (hidx a ha).2
        have hvb := (hidx b hb).2
        rw [hidx'_on a ha, hidx'_on b hb] at hab
        have hPidx : progVal P (idx a) = progVal P (idx b) := congrArg (fun k => progVal P k) hab
        have hij := progVal_inj P hP (hidx a ha).1 (hidx b hb).1 hPidx
        have hreq : progVal r a = progVal r b := by
          calc
            progVal r a = progVal P (idx a) := hva
            _ = progVal P (idx b) := hPidx
            _ = progVal r b := hvb.symm
        exact progVal_inj r hr ha hb hreq
      · rw [hidx'_on a ha] at hab
        simp [idx', hb] at hab
        have halt : idx a < P.n := (hidx a ha).1
        omega
    · by_cases hb : b < r.n
      · rw [hidx'_on b hb] at hab
        simp [idx', ha] at hab
        have halt : idx b < P.n := (hidx b hb).1
        omega
      · simp [idx', ha, hb] at hab
        omega
  let L := (List.range P.n).filter (fun (i : Nat) => contains r (progVal P i))
  let M := (List.range r.n).map idx'
  have hperm : L.Perm M := by
    apply (List.perm_ext_iff_of_nodup
      (List.Pairwise.filter _ List.nodup_range)
      (List.Pairwise.map idx' (fun a b hne h => hne (hidx'_inj a b h)) List.nodup_range)).mpr
    intro i
    constructor
    · intro hi
      have hiP : i < P.n := List.mem_range.mp (List.mem_filter.mp hi).1
      have hcr : contains r (progVal P i) = true := (List.mem_filter.mp hi).2
      obtain ⟨k, hk, hval⟩ := (contains_iff_mem r (progVal P i)).mp hcr
      have hmemM : i ∈ M := by
        apply List.mem_map.mpr
        refine ⟨k, List.mem_range.mpr hk, ?_⟩
        rw [hidx'_on k hk]
        have hik := hidx k hk
        have heqv : progVal P (idx k) = progVal P i := by rw [← hik.2, ← hval]
        exact progVal_inj P hP hik.1 hiP heqv
      exact hmemM
    · intro hi
      obtain ⟨k, hk, hval⟩ := List.mem_map.mp hi
      have hkk := List.mem_range.mp hk
      have hik := hidx k hkk
      rw [hidx'_on k hkk] at hval
      have hiP : i < P.n := by rw [← hval]; exact hik.1
      apply List.mem_filter.mpr
      constructor
      · exact List.mem_range.mpr hiP
      · rw [← hval, ← hik.2]
        exact contains_prog_self r k hkk
  change ((List.range P.n).filter (fun (i : Nat) => contains r (progVal P i))).Perm
    ((List.range r.n).map (fun k => ((progVal r k - P.lo) / (P.step : Int)).toNat))
  have hM : M = (List.range r.n).map (fun k => ((progVal r k - P.lo) / (P.step : Int)).toNat) := by
    dsimp [M, idx']
    apply List.map_congr_left
    intro k hk
    have hklt := List.mem_range.mp hk
    simp [hklt, idx]
  rwa [hM] at hperm

private def countPairs (P Q : Progression) : Nat :=
  ((List.range P.n).map (fun i => (List.range Q.n).countP (fun j => progVal P i == progVal Q j))).sum

private theorem sum_map_zero_nat (m : Nat) : ((List.range m).map (fun _ => (0 : Nat))).sum = 0 := by
  induction m with
  | zero => simp
  | succ m ih => rw [List.range_succ, List.map_append, List.sum_append, ih]; simp

private theorem sum_map_const_nat (m v : Nat) :
    ((List.range m).map (fun _ => v)).sum = m * v := by
  induction m with
  | zero => simp
  | succ m ih => rw [List.range_succ, List.map_append, List.sum_append, ih]; simp [Nat.succ_mul]

private theorem sum_ite_eq_countP {α : Type} (l : List α) (p : α → Bool) (n : Nat) :
    ((l.map (fun x => if p x then n else 0)).sum) = n * l.countP p := by
  rw [List.countP_eq_length_filter]
  induction l with
  | nil => simp
  | cons x xs ih => by_cases hx : p x <;> simp [hx, ih, Nat.succ_mul, Nat.mul_succ] <;> omega

private theorem countPairs_eq (P Q : Progression) :
    countPairs P Q = (intersection P Q).n *
      (if P.step == 0 then P.n else 1) * (if Q.step == 0 then Q.n else 1) := by
  by_cases hPn : P.n = 0
  · have hrange : List.range P.n = [] := by simp [hPn]
    simp [countPairs, hrange, intersection, hPn]
  by_cases hQn : Q.n = 0
  · have hrange : List.range Q.n = [] := by simp [hQn]
    have hzero : ((List.range P.n).map (fun _ => (0 : Nat))).sum = 0 := sum_map_zero_nat P.n
    simp [countPairs, hrange, intersection, hQn, hzero]
  have hPnp : 0 < P.n := by omega
  have hQnp : 0 < Q.n := by omega
  by_cases hP0 : P.step = 0
  · have hcQ : (List.range Q.n).countP (fun j => P.lo == progVal Q j) =
        if contains Q P.lo = true then (if Q.step == 0 then Q.n else 1) else 0 := by
      rw [countP_comm (List.range Q.n) P.lo (fun j => progVal Q j)]
      by_cases hQ0 : Q.step = 0
      · rw [countP_prog_zero Q hQ0]
        by_cases hc : contains Q P.lo = true <;> simp [hc, hQ0]
      · have hQpos : 0 < Q.step := Nat.pos_of_ne_zero hQ0
        rw [countP_prog_pos Q hQpos]
        by_cases hc : contains Q P.lo = true <;> simp [hc, hQ0]
    have hsum : countPairs P Q = P.n * ((List.range Q.n).countP (fun j => P.lo == progVal Q j)) := by
      unfold countPairs
      rw [show (List.range P.n).map (fun i => (List.range Q.n).countP (fun j => progVal P i == progVal Q j)) =
          (List.range P.n).map (fun _ => (List.range Q.n).countP (fun j => P.lo == progVal Q j)) by
        apply List.map_congr_left
        intro i hi
        have hvalP : progVal P i = P.lo := by simp [progVal, hP0]
        rw [hvalP]]
      rw [sum_map_const_nat]
    have hint : intersection P Q = ⟨P.lo, 1, if contains Q P.lo = true then 1 else 0⟩ := by
      simp [intersection, hPn, hQn, hP0]
    rw [hsum, hcQ, hint]
    by_cases hc : contains Q P.lo = true <;> simp [hc, hP0, Nat.mul_assoc]
  · by_cases hQ0 : Q.step = 0
    · have hcP : (List.range P.n).countP (fun i => progVal P i == Q.lo) =
          if contains P Q.lo = true then 1 else 0 := by
        have hPpos : 0 < P.step := Nat.pos_of_ne_zero hP0
        exact countP_prog_pos P hPpos Q.lo
      have hint : intersection P Q = ⟨Q.lo, 1, if contains P Q.lo = true then 1 else 0⟩ := by
        simp [intersection, hPn, hQn, hP0, hQ0]
      unfold countPairs
      rw [show (List.range P.n).map (fun i => (List.range Q.n).countP (fun j => progVal P i == progVal Q j)) =
          (List.range P.n).map (fun i => if progVal P i == Q.lo then Q.n else 0) by
        apply List.map_congr_left
        intro i hi
        rw [countP_comm (List.range Q.n) (progVal P i) (fun j => progVal Q j)]
        rw [countP_prog_zero Q hQ0]
        have heqv : (contains Q (progVal P i) = true) ↔ (progVal P i == Q.lo) = true := by
          constructor
          · intro hc
            obtain ⟨k, hk, hv'⟩ := (contains_iff_mem Q (progVal P i)).mp hc
            have hqval : progVal Q k = Q.lo := by simp [progVal, hQ0]
            apply beq_iff_eq.mpr
            rw [hv', hqval]
          · intro hb
            have hv' : progVal P i = Q.lo := beq_iff_eq.mp hb
            apply (contains_iff_mem Q (progVal P i)).mpr
            refine ⟨0, ?_, ?_⟩
            · omega
            · change progVal P i = progVal Q 0
              rw [hv']
              simp [progVal, hQ0]
        by_cases hc : contains Q (progVal P i) = true
        · have hb : (progVal P i == Q.lo) = true := heqv.mp hc
          simp [hc, hb]
        · have hb : (progVal P i == Q.lo) ≠ true := fun h => hc (heqv.mpr h)
          simp [hc, hb]]
      rw [sum_ite_eq_countP, hcP, hint]
      by_cases hc : contains P Q.lo = true <;> simp [hc, hP0, hQ0, Nat.mul_comm, Nat.mul_left_comm, Nat.mul_assoc]
    · have hPpos : 0 < P.step := Nat.pos_of_ne_zero hP0
      have hQpos : 0 < Q.step := Nat.pos_of_ne_zero hQ0
      let r := intersection P Q
      have hsubP : ∀ v, contains r v = true → contains P v = true := by
        intro v hv
        exact ((intersection_contains P Q v).mp hv).1
      have hrstep : 0 < r.step := by
        dsimp [r, intersection]
        by_cases hg : (Q.lo - P.lo) % ((P.step.gcd Q.step : Nat) : Int) = 0
        · simp [hPn, hQn, hP0, hQ0, hg]
          have hgpos : 0 < P.step.gcd Q.step := Nat.gcd_pos_of_pos_left Q.step hPpos
          have hdiv : 0 < P.step / P.step.gcd Q.step :=
            Nat.div_pos (Nat.le_of_dvd hPpos (Nat.gcd_dvd_left P.step Q.step)) hgpos
          exact Nat.mul_pos hdiv hQpos
        · simp [hPn, hQn, hP0, hQ0, hg]
      unfold countPairs
      rw [show (List.range P.n).map (fun i => (List.range Q.n).countP (fun j => progVal P i == progVal Q j)) =
          (List.range P.n).map (fun i => if contains Q (progVal P i) then 1 else 0) by
        apply List.map_congr_left
        intro i hi
        rw [countP_comm (List.range Q.n) (progVal P i) (fun j => progVal Q j)]
        rw [countP_prog_pos Q hQpos]]
      rw [show (List.range P.n).map (fun i => if contains Q (progVal P i) then 1 else 0) =
          (List.range P.n).map (fun i => if contains r (progVal P i) then 1 else 0) by
        apply List.map_congr_left
        intro i hi
        by_cases hcr : contains r (progVal P i) = true
        · have hq : contains Q (progVal P i) = true := ((intersection_contains P Q (progVal P i)).mp hcr).2
          simp [hcr, hq]
        · have hiP : i < P.n := List.mem_range.mp hi
          have hq : contains Q (progVal P i) ≠ true := by
            intro hq
            exact hcr ((intersection_contains P Q (progVal P i)).mpr
              ⟨contains_prog_self P i hiP, hq⟩)
          simp [hcr, hq]]
      rw [sum_ite_eq_countP, countP_contains_sub P r hPpos hrstep hsubP]
      simp [r, hP0, hQ0]


/-! ## From lane points to arithmetic progressions -/

private theorem laneVal_inj (a : Lane) (hs : a.step ≠ 0) {i j : Nat}
    (h : a.base + a.step * (i : Int) = a.base + a.step * (j : Int)) : i = j := by
  have hmul : a.step * (i : Int) = a.step * (j : Int) := by omega
  have hij : (i : Int) = (j : Int) := Int.eq_of_mul_eq_mul_left (by omega) hmul
  exact_mod_cast hij

private theorem laneValues_perm (a : Lane) :
    ((List.range a.n).map (fun (i : Nat) => a.base + a.step * (i : Int))).Perm
      ((List.range a.n).map (fun (k : Nat) => progVal (values a) k)) := by
  by_cases hs0 : a.step = 0
  · have h : ((List.range a.n).map (fun (i : Nat) => a.base + a.step * (i : Int))) =
        ((List.range a.n).map (fun (k : Nat) => progVal (values a) k)) := by
      apply List.map_congr_left
      intro k hk
      simp [values, progVal, hs0]
    rw [h]
  by_cases hneg : a.step < 0
  · have hcastAbs : (a.step.natAbs : Int) = -a.step := by
      rw [← Int.natAbs_neg a.step]
      exact Int.natAbs_of_nonneg (by omega : 0 ≤ -a.step)
    apply (List.perm_ext_iff_of_nodup
      (List.Pairwise.map _ (fun i j hne h => hne (laneVal_inj a hs0 h)) List.nodup_range)
      (List.Pairwise.map _ (fun k l hne h => hne (by
        have hs : ((values a).step : Int) ≠ 0 := by
          dsimp [values]
          rw [hcastAbs]
          omega
        unfold progVal at h
        have hmul : (k : Int) * (values a).step = (l : Int) * (values a).step := by omega
        have hij : (k : Int) = (l : Int) := Int.eq_of_mul_eq_mul_right hs hmul
        exact_mod_cast hij)) List.nodup_range)).mpr
    intro v
    constructor
    · intro hv
      obtain ⟨i, hi, hval⟩ := List.mem_map.mp hv
      have halt : i ≤ a.n - 1 := by have := List.mem_range.mp hi; omega
      have hanpos : 0 < a.n := by have := List.mem_range.mp hi; omega
      have hlt : a.n - 1 - i < a.n :=
        Nat.lt_of_le_of_lt (Nat.sub_le _ _) (Nat.sub_lt hanpos (by decide : 0 < 1))
      apply List.mem_map.mpr
      refine ⟨a.n - 1 - i, List.mem_range.mpr hlt, ?_⟩
      rw [← hval]
      have hcastsub : ((a.n - 1 - i : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (i : Int) :=
        Int.natCast_sub halt
      simp [values, progVal, hneg, hcastAbs, hcastsub]
      grind
    · intro hv
      obtain ⟨k, hk, hval⟩ := List.mem_map.mp hv
      have hklt : k < a.n := List.mem_range.mp hk
      have hkle : k ≤ a.n - 1 := by omega
      have hanpos : 0 < a.n := by have := List.mem_range.mp hk; omega
      have hlt : a.n - 1 - k < a.n :=
        Nat.lt_of_le_of_lt (Nat.sub_le _ _) (Nat.sub_lt hanpos (by decide : 0 < 1))
      apply List.mem_map.mpr
      refine ⟨a.n - 1 - k, List.mem_range.mpr hlt, ?_⟩
      rw [← hval]
      have hcastsub : ((a.n - 1 - k : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (k : Int) :=
        Int.natCast_sub hkle
      simp [values, progVal, hneg, hcastAbs]
      grind
  · have hnonneg : 0 ≤ a.step := by omega
    have hcastAbs : (a.step.natAbs : Int) = a.step :=
      Int.natAbs_of_nonneg hnonneg
    have hEq : ((List.range a.n).map (fun (i : Nat) => a.base + a.step * (i : Int))) =
        ((List.range a.n).map (fun (k : Nat) => progVal (values a) k)) := by
      apply List.map_congr_left
      intro k hk
      simp [values, progVal, hneg, hcastAbs, Int.mul_comm]
    rw [hEq]

private theorem countP_beq_eq_count (l : List Int) (a : Int) :
    l.countP (fun x => x == a) = l.count a := by
  rw [List.countP_eq_length_filter, List.count_eq_length_filter]

private theorem countP_eq_of_perm {l₁ l₂ : List Int} (p : l₁.Perm l₂) (a : Int) :
    l₁.countP (fun x => x == a) = l₂.countP (fun x => x == a) := by
  rw [countP_beq_eq_count, countP_beq_eq_count, p.count a]

private theorem list_sum_perm_nat {l₁ l₂ : List Nat} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem list_sum_perm {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem laneResidue (a b : Lane) :
    ((List.range a.n).map (fun (i : Nat) => (List.range b.n).countP
      (fun (j : Nat) => a.base + a.step * (i : Int) == b.base + b.step * (j : Int)))).sum =
    joinCount a b := by
  let fA : Nat → Int := fun i => a.base + a.step * (i : Int)
  let fB : Nat → Int := fun j => b.base + b.step * (j : Int)
  let gA : Nat → Int := fun k => progVal (values a) k
  let gB : Nat → Int := fun k => progVal (values b) k
  have hpa : (List.map fA (List.range a.n)).Perm (List.map gA (List.range a.n)) := by
    dsimp [fA, gA]
    exact laneValues_perm a
  have hpb : (List.map fB (List.range b.n)).Perm (List.map gB (List.range b.n)) := by
    dsimp [fB, gB]
    exact laneValues_perm b
  calc
    ((List.range a.n).map (fun (i : Nat) => (List.range b.n).countP
      (fun (j : Nat) => a.base + a.step * (i : Int) == b.base + b.step * (j : Int)))).sum =
        ((List.map fA (List.range a.n)).map
          (fun v => (List.map fB (List.range b.n)).countP (fun x => x == v))).sum := by
          rw [List.map_map]
          apply congrArg List.sum
          apply List.map_congr_left
          intro i hi
          dsimp [Function.comp_def, fA, fB]
          rw [List.countP_map]
          change (List.range b.n).countP (fun (j : Nat) =>
              a.base + a.step * (i : Int) == b.base + b.step * (j : Int)) =
            (List.range b.n).countP (fun (j : Nat) =>
              b.base + b.step * (j : Int) == a.base + a.step * (i : Int))
          exact countP_comm (List.range b.n) (a.base + a.step * (i : Int))
            (fun (j : Nat) => b.base + b.step * (j : Int))
    _ = ((List.map gA (List.range a.n)).map
          (fun v => (List.map gB (List.range b.n)).countP (fun x => x == v))).sum := by
          have hperm : ((List.map fA (List.range a.n)).map
              (fun v => (List.map fB (List.range b.n)).countP (fun x => x == v))).Perm
              ((List.map gA (List.range a.n)).map
              (fun v => (List.map gB (List.range b.n)).countP (fun x => x == v))) := by
            have h1 : ((List.map fA (List.range a.n)).map
              (fun v => (List.map fB (List.range b.n)).countP (fun x => x == v))).Perm
              ((List.map gA (List.range a.n)).map
              (fun v => (List.map fB (List.range b.n)).countP (fun x => x == v))) := hpa.map _
            have heq : ((List.map gA (List.range a.n)).map
                (fun v => (List.map fB (List.range b.n)).countP (fun x => x == v))) =
                ((List.map gA (List.range a.n)).map
                (fun v => (List.map gB (List.range b.n)).countP (fun x => x == v))) := by
              apply List.map_congr_left
              intro v hv
              exact countP_eq_of_perm hpb v
            exact h1.trans (by rw [heq])
          exact list_sum_perm_nat hperm
    _ = countPairs (values a) (values b) := by
          unfold countPairs
          dsimp [values, gA, gB]
          rw [List.map_map]
          apply congrArg List.sum
          apply List.map_congr_left
          intro i hi
          dsimp [Function.comp_def]
          rw [List.countP_map]
          exact (countP_comm (List.range b.n) (gA i) gB).symm
    _ = joinCount a b := by
          rw [countPairs_eq]
          simp [joinCount, values]


/-! ## Decoded rows as a permutation of lane points -/

private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun (i : Nat) => (a.start + i * a.stride, a.base + a.step * (i : Int)))

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

private theorem mem_rows_some (lay : Layout) (c : Config) (f : Nat → Int)
    (h : ∀ i, i < c.n → lay.read c i = some (f i)) {r : Nat × Int} :
    r ∈ rows lay c ↔ ∃ i, i < c.n ∧ r = (i, f i) := by
  unfold rows
  rw [List.mem_filterMap]
  constructor
  · rintro ⟨i, hi, hr⟩
    refine ⟨i, List.mem_range.mp hi, ?_⟩
    simpa [h i (List.mem_range.mp hi)] using hr.symm
  · rintro ⟨i, hi, rfl⟩
    refine ⟨i, List.mem_range.mpr hi, ?_⟩
    simp [h i hi]

private theorem mem_lanePoints {a : Lane} {r : Nat × Int} :
    r ∈ lanePoints a ↔ ∃ j, j < a.n ∧ r = (a.start + j * a.stride, a.base + a.step * (j : Int)) := by
  unfold lanePoints
  rw [List.mem_map]
  constructor
  · rintro ⟨j, hj, rfl⟩
    exact ⟨j, List.mem_range.mp hj, rfl⟩
  · rintro ⟨j, hj, rfl⟩
    exact ⟨j, List.mem_range.mpr hj, rfl⟩

private theorem mem_flatMap_lanes {ls : List Lane} {r : Nat × Int} :
    r ∈ ls.flatMap lanePoints ↔ ∃ a ∈ ls, r ∈ lanePoints a := by
  induction ls with
  | nil => simp
  | cons a as ih => simp [ih]

private theorem nodup_lanePoints (a : Lane) (hs : 0 < a.stride) : (lanePoints a).Nodup := by
  unfold lanePoints
  exact List.Pairwise.map
    (fun (i : Nat) => (a.start + i * a.stride, a.base + a.step * (i : Int)))
    (by
      intro i j hne heq
      apply hne
      have hfst : a.start + i * a.stride = a.start + j * a.stride := congrArg Prod.fst heq
      have hmul : i * a.stride = j * a.stride := by omega
      exact Nat.eq_of_mul_eq_mul_right hs hmul)
    List.nodup_range

private theorem rows_perm_of_mem {lay : Layout} (c : Config)
    (hmem : ∀ r : Nat × Int, r ∈ rows lay c ↔ r ∈ (lay.lanes c).flatMap lanePoints)
    (hnd1 : (rows lay c).Nodup) (hnd2 : ((lay.lanes c).flatMap lanePoints).Nodup) :
    (rows lay c).Perm ((lay.lanes c).flatMap lanePoints) :=
  (List.perm_ext_iff_of_nodup hnd1 hnd2).mpr hmem

private theorem flatMap_sum_nat {α β : Type} (ls : List α) (g : α → List β) (f : β → Nat) :
    ((ls.flatMap g).map f).sum = (ls.map (fun a => ((g a).map f).sum)).sum := by
  induction ls with
  | nil => simp
  | cons a as ih => simp [ih]

private theorem residue_of_perm (lay : Layout) (c : Config) (b : Lane)
    (hp : (rows lay c).Perm ((lay.lanes c).flatMap lanePoints)) :
    ((rows lay c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))).sum =
    ((lay.lanes c).map (joinCount · b)).sum := by
  let f : Nat × Int → Nat := fun r => (List.range b.n).countP
    (fun (i : Nat) => r.2 == b.base + b.step * (i : Int))
  have hsum : ((rows lay c).map f).sum = (((lay.lanes c).flatMap lanePoints).map f).sum :=
    list_sum_perm_nat (hp.map f)
  change ((rows lay c).map f).sum = ((lay.lanes c).map (joinCount · b)).sum
  rw [hsum, flatMap_sum_nat]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  dsimp [f, lanePoints]
  rw [List.map_map]
  simpa [Function.comp_def] using laneResidue a b


namespace SparseSample

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample

private theorem rows_perm (c : Config) :
    (rows layout c).Perm ((layout.lanes c).flatMap lanePoints) := by
  have hw : 0 < width c := by dsimp [width]; omega
  have hslt : c.split % width c < width c := Nat.mod_lt _ hw
  have hrows : ∀ r : Nat × Int, r ∈ rows layout c ↔
      ∃ i, i < c.n ∧ i % width c = c.split % width c ∧
        r = (i, c.base + c.step * ((i / width c : Nat) : Int)) := by
    intro r
    unfold rows
    rw [List.mem_filterMap]
    constructor
    · rintro ⟨i, hi, hr⟩
      have hil : i < c.n := List.mem_range.mp hi
      by_cases hcond : i % width c == c.split % width c
      · have hbeq : i % width c = c.split % width c := beq_iff_eq.mp hcond
        have hval : layout.read c i = some (c.base + c.step * ((i / width c : Nat) : Int)) := by
          simp [layout, hcond, hbeq]
        rw [hval] at hr
        simp at hr
        exact ⟨i, hil, hbeq, hr.symm⟩
      · have hne : i % width c ≠ c.split % width c := by
          intro heq
          exact hcond (beq_iff_eq.mpr heq)
        have hnone : layout.read c i = none := by simp [layout, hne]
        simp [hnone] at hr
    · rintro ⟨i, hil, himod, rfl⟩
      refine ⟨i, List.mem_range.mpr hil, ?_⟩
      have hbeq : i % width c == c.split % width c := beq_iff_eq.mpr himod
      simp [layout, hbeq, himod]
  have hnd1 : (rows layout c).Nodup := by
    unfold rows
    refine List.Pairwise.filterMap (fun i => (layout.read c i).map (fun v => (i, v))) ?_ (List.nodup_range)
    intro i j hne b hb b' hb' heq
    apply hne
    have hb1 : b.1 = i := by
      cases hro : layout.read c i with
      | none => simp [hro] at hb
      | some x =>
          simp [hro] at hb
          rw [← hb]
    have hb2 : b'.1 = j := by
      cases hro : layout.read c j with
      | none => simp [hro] at hb'
      | some x =>
          simp [hro] at hb'
          rw [← hb']
    rw [← hb1, ← hb2]
    exact congrArg Prod.fst heq
  have hlanes : layout.lanes c = [lane c.n (c.split % width c) (width c) c.base c.step] := by rfl
  have hnd2 : ((layout.lanes c).flatMap lanePoints).Nodup := by
    rw [hlanes]
    simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil]
    exact nodup_lanePoints _ hw
  apply rows_perm_of_mem c _ hnd1 hnd2
  intro r
  rw [hrows, mem_flatMap_lanes, hlanes]
  constructor
  · rintro ⟨i, hi, himod, rfl⟩
    refine ⟨lane c.n (c.split % width c) (width c) c.base c.step, by simp, ?_⟩
    rw [mem_lanePoints]
    have hdecomp : i = c.split % width c + (i / width c) * width c := by
      calc
        i = width c * (i / width c) + i % width c := (Nat.div_add_mod i (width c)).symm
        _ = c.split % width c + (i / width c) * width c := by rw [himod, Nat.mul_comm, Nat.add_comm]
    have hlt : i / width c < (c.n - c.split % width c + width c - 1) / width c :=
      (pos_lt_iff (c.split % width c) (width c) (i / width c) c.n hw).mp (by omega)
    refine ⟨i / width c, ?_, ?_⟩
    · simpa [lane, Nat.add_comm] using hlt
    · rw [hdecomp]
      have hdivi : (c.split % width c + (i / width c) * width c) / width c = i / width c := by
        rw [Nat.add_comm, Nat.mul_comm (i / width c) (width c),
          Nat.mul_add_div hw (i / width c) (c.split % width c), Nat.div_eq_of_lt hslt, Nat.add_zero]
      simp [lane, hdivi]
  · rintro ⟨a, ha, hmem⟩
    have ha' : a = lane c.n (c.split % width c) (width c) c.base c.step := by simpa using ha
    subst a
    rw [mem_lanePoints] at hmem
    obtain ⟨j, hj, hjr⟩ := hmem
    have hjlt : j < (c.n - c.split % width c + width c - 1) / width c := by
      simpa [lane] using hj
    have hilt : c.split % width c + j * width c < c.n :=
      (pos_lt_iff (c.split % width c) (width c) j c.n hw).mpr hjlt
    refine ⟨c.split % width c + j * width c, hilt, ?_, ?_⟩
    · rw [Nat.add_mul_mod_self_right]
      exact Nat.mod_eq_of_lt hslt
    · rw [hjr]
      have hdiv : (j * width c + c.split % width c) / width c = j := by
        rw [Nat.mul_comm j (width c), Nat.mul_add_div hw j (c.split % width c),
          Nat.div_eq_of_lt hslt, Nat.add_zero]
      have hdiv' : (c.split % width c + j * width c) / width c = j := by
        rw [Nat.add_comm, hdiv]
      simp [lane, hdiv']

theorem sound_SparseSample_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.ResidueJoin.Goal := by
  intro c b
  exact residue_of_perm layout c b (rows_perm c)

end SparseSample


/-! ## Sums of arithmetic progressions -/

private theorem choose2_succ (k : Nat) :
    TinylambOpt.ResearchGoals.PairAggregates.choose2 (k + 1) =
      TinylambOpt.ResearchGoals.PairAggregates.choose2 k + k := by
  unfold TinylambOpt.ResearchGoals.PairAggregates.choose2
  by_cases hk : k = 0
  · simp [hk]
  · have hkm1 : k + 1 - 1 = k := by omega
    rw [hkm1]
    have hmul1 : (k + 1) * k = k * k + k := by rw [Nat.add_mul, Nat.one_mul]
    have hmul2 : k * (k - 1) = k * k - k := by
      rw [Nat.mul_sub_left_distrib, Nat.mul_one]
    have hle : k ≤ k * k := by
      simpa using Nat.mul_le_mul_left k (show 1 ≤ k by omega)
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
    partialSum a k = ((List.range k).map (fun (i : Nat) => a.base + (i : Int) * a.step)).sum := by
  induction k with
  | zero => simp [partialSum, tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ k ih =>
      rw [partialSum_succ, ih]
      rw [List.range_succ, List.map_append, List.sum_append]
      simp

private theorem sum_squares_eq (k : Nat) :
    ((List.range k).map (fun (i : Nat) => (i : Int) * (i : Int))).sum = (squares k : Int) := by
  induction k with
  | zero => simp [squares]
  | succ k ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih, squares_succ]
      simp [Int.natCast_add, Int.natCast_mul]

private theorem sum_arith_seq (m : Nat) (x d : Int) :
    ((List.range m).map (fun k : Nat => x + (k : Int) * d)).sum = (m : Int) * x + tri m * d := by
  induction m with
  | zero => simp [tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ m ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih, tri_succ]
      simp [Int.natCast_add]
      grind

private theorem sum_filter_add_sum_filter_not {α : Type} (l : List α) (p : α → Bool) (f : α → Int) :
    ((l.filter p).map f).sum + ((l.filter (fun x => ! p x)).map f).sum = (l.map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih] <;> omega


end TinylambOpt.ResearchProofs.Atlas.EncodedPages.SparseSampleResidueJoin

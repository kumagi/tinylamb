import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `WrappingSequence.AlignedProduct` — `∀ c (b : Lane), 0 < b.stride → ((rows layout
  c).map (fun r => ((List.range b.n).map (fun i => if r.1 == b.start+i*b.stride then
  r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum = ((layout.lanes c).map (aligned ·
  b)).sum`
- `WrappingSequence.DeleteProgression` — `∀ c mask, (((rows layout c).filter (fun r =>
  !contains mask r.1)).map Prod.snd).sum = ((layout.lanes c).map (fun a => partialSum
  a a.n - deletedSum a mask)).sum`
- `WrappingSequence.ResidueJoin` — `∀ c (b : Lane), ((rows layout c).map (fun r =>
  (List.range b.n).countP (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum =
  ((layout.lanes c).map (joinCount · b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.WrappingSequenceProgressions

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.WrappingSequence

-- =====================================================================
-- Finite-progression foundations (adapted from the verified sibling
-- proof in ResearchProofs/Atlas/EncodedPages4.lean).
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


-- Striped-page decomposition helpers (adapted from Atlas_EncodedPages_G2).
private theorem list_sum_append_int (l₁ l₂ : List Int) :
    (l₁ ++ l₂).sum = l₁.sum + l₂.sum := by
  induction l₁ with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem list_map_append {α β : Type} (f : α → β) (l₁ l₂ : List α) :
    (l₁ ++ l₂).map f = l₁.map f ++ l₂.map f := by
  induction l₁ <;> simp [*]

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


-- =====================================================================
-- WrappingSequence goals
-- =====================================================================

namespace GoalProofs

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

private theorem count_affine_ne (v base step : Int) (hsne : step ≠ 0) :
    ∀ n : Nat, (List.range n).countP (fun i : Nat => v == base + step * (i : Int)) =
      if (∃ i, i < n ∧ base + step * (i : Int) = v) then 1 else 0 := by
  intro n
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.countP_append]
      simp only [List.countP_singleton]
      by_cases hlast : v == base + step * (n : Int)
      · have hlast' : base + step * (n : Int) = v := (beq_iff_eq.mp hlast).symm
        have hnone : ∀ i, i < n → ¬ base + step * (i : Int) = v := by
          intro i hi hiv
          have heq : step * (i : Int) = step * (n : Int) := by grind
          have : (i : Int) = (n : Int) := (Int.mul_eq_mul_left_iff hsne).mp heq
          omega
        have hcnt : (List.range n).countP (fun i : Nat => v == base + step * (i : Int)) = 0 := by
          apply List.countP_eq_zero.mpr
          intro i hi ht
          have hveq : v = base + step * (i : Int) := beq_iff_eq.mp ht
          exact hnone i (List.mem_range.mp hi) hveq.symm
        rw [hcnt]
        simp [hlast']
        exact ⟨n, by omega, hlast'⟩
      · have hlast' : base + step * (n : Int) ≠ v := by
          intro he
          apply hlast
          rw [← he]
          simp
        have hexn : (∃ i, i < n ∧ base + step * (i : Int) = v) ↔
            (∃ i, i < n + 1 ∧ base + step * (i : Int) = v) := by
          constructor
          · rintro ⟨i, hi, hiv⟩
            exact ⟨i, by omega, hiv⟩
          · rintro ⟨i, hi, hiv⟩
            by_cases hin : i < n
            · exact ⟨i, hin, hiv⟩
            · have hieq : i = n := by omega
              subst i
              exact False.elim (hlast' hiv)
        rw [ih]
        by_cases hex : ∃ i, i < n ∧ base + step * (i : Int) = v
        · have hex' : ∃ i, i < n + 1 ∧ base + step * (i : Int) = v := hexn.mp hex
          simp [hex, hex', hlast]
        · have hex' : ¬ ∃ i, i < n + 1 ∧ base + step * (i : Int) = v := by
            intro h
            exact hex (hexn.mpr h)
          simp [hex, hex', hlast]

private theorem contains_values_iff (v : Int) (b : Lane) :
    contains (values b) v = true ↔ ∃ i, i < b.n ∧ b.base + b.step * (i : Int) = v := by
  rw [contains_iff_mem]
  unfold progMem progVal
  by_cases hn : b.n = 0
  · simp [values, hn]
  · have hposn : 0 < b.n := by omega
    by_cases hs : b.step = 0
    · have hlo : (values b).lo = b.base := by unfold values; simp [hs]
      have hst : ((values b).step : Int) = 0 := by unfold values; simp [hs]
      rw [hlo, hst]
      constructor
      · rintro ⟨k, hk, hv⟩
        refine ⟨0, hposn, ?_⟩
        simp [hs] at hv ⊢
        omega
      · rintro ⟨i, hi, hv⟩
        refine ⟨0, hposn, ?_⟩
        simp [hs] at hv ⊢
        omega
    · by_cases hspos : 0 < b.step
      · have hlo : (values b).lo = b.base := by
          unfold values
          simp [show ¬ b.step < 0 by omega]
        have hst : ((values b).step : Int) = b.step := by
          unfold values
          exact Int.natAbs_of_nonneg (by omega)
        rw [hlo, hst]
        constructor
        · rintro ⟨k, hk, hkv⟩
          refine ⟨k, hk, ?_⟩
          grind
        · rintro ⟨i, hi, hiv⟩
          refine ⟨i, hi, ?_⟩
          grind
      · have hsneg : b.step < 0 := by omega
        have hlo : (values b).lo = b.base + ((b.n - 1 : Nat) : Int) * b.step := by
          unfold values
          simp [hsneg]
        have hst : ((values b).step : Int) = -b.step := by
          unfold values
          rw [← Int.natAbs_neg b.step]
          exact Int.natAbs_of_nonneg (by omega)
        have hnn : (values b).n = b.n := rfl
        rw [hlo, hst, hnn]
        constructor
        · rintro ⟨k, hk, hkv⟩
          refine ⟨b.n - 1 - k, ?_, ?_⟩
          · omega
          · have hkcast : ((b.n - 1 - k : Nat) : Int) = ((b.n - 1 : Nat) : Int) - (k : Int) := by omega
            rw [hkcast]
            grind
        · rintro ⟨i, hi, hiv⟩
          refine ⟨b.n - 1 - i, ?_, ?_⟩
          · omega
          · have hicast : ((b.n - 1 - i : Nat) : Int) = ((b.n - 1 : Nat) : Int) - (i : Int) := by omega
            rw [hicast]
            grind

private theorem joinCount_const (a : Lane) (ha : a.step = 0) (b : Lane) :
    joinCount a b = a.n * ((List.range b.n).countP
      (fun i : Nat => a.base == b.base + b.step * (i : Int))) := by
  unfold joinCount
  have hvals : values a = ⟨a.base, 0, a.n⟩ := by unfold values; simp [ha]
  rw [hvals]
  have hnb : (values b).n = b.n := rfl
  by_cases han : a.n = 0
  · simp [intersection, han]
  · by_cases hbn : b.n = 0
    · simp [intersection, han, hnb, hbn, ha]
    · simp [intersection, han, hnb, hbn, ha]
      by_cases hstep0 : b.step = 0
      · have hcnt : (List.range b.n).countP (fun i : Nat => a.base == b.base) =
            if a.base == b.base then b.n else 0 := by
          induction b.n with
          | zero => simp
          | succ n ih =>
              rw [List.range_succ, List.countP_append]
              simp only [List.countP_singleton]
              by_cases h : a.base == b.base <;> simp [h, ih]
        have hcnt' : (List.range b.n).countP (fun i : Nat => a.base == b.base + b.step * (i : Int)) =
            if a.base == b.base then b.n else 0 := by simpa [hstep0] using hcnt
        rw [hcnt']
        have hc : contains (values b) a.base = true ↔ a.base == b.base := by
          rw [contains_values_iff]
          constructor
          · rintro ⟨i, _, hi⟩
            simpa [hstep0] using hi.symm
          · intro h
            have heq : a.base = b.base := beq_iff_eq.mp h
            refine ⟨0, by omega, ?_⟩
            simpa [hstep0, heq]
        by_cases h : contains (values b) a.base = true
        · have hb : (a.base == b.base) = true := hc.mp h
          simp [h, hb, hstep0]
        · have hb : (a.base == b.base) = false := by
            cases hh : a.base == b.base <;> simp_all
          simp [h, hb]
      · have hcnt := count_affine_ne a.base b.base b.step hstep0 b.n
        rw [hcnt]
        have heqex : (∃ i, i < b.n ∧ b.base + b.step * (i : Int) = a.base) ↔
            contains (values b) a.base = true := (contains_values_iff a.base b).symm
        by_cases hex : ∃ i, i < b.n ∧ b.base + b.step * (i : Int) = a.base
        · have hc : contains (values b) a.base = true := heqex.mp hex
          simp [hex, hc, hstep0]
        · have hc : contains (values b) a.base = false := by
            cases hh : contains (values b) a.base <;> simp_all
          simp [hex, hc, hstep0]

end GoalProofs

namespace GoalProofs

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.WrappingSequence

private def wmake (c : Config) : Nat → Int × Int := fun r => (c.base + c.step * (r : Int), 0)

private theorem wrows (c : Config) :
    rows layout c = (List.range c.n).map (fun i : Nat => (i, c.base + c.step * (i % width c : Nat))) := by
  unfold rows
  simp [layout]

private theorem sum_range_trunc_int (f : Nat → Int) (k p : Nat) (hk : k ≤ p) :
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
    apply sum_map_zero
  rw [hzero]
  simp only [Int.add_zero]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  have hrlt : r < k := List.mem_range.mp hr
  simp [hrlt]

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

private theorem wstriped (c : Config) (g : Nat → Int → Int) :
    ((rows layout c).map (fun r => g r.1 r.2)).sum =
      ((List.range (width c)).map (fun r =>
        ((List.range ((c.n - r + width c - 1) / width c)).map (fun q : Nat =>
          g (r + q * width c) (c.base + c.step * (r : Int)))).sum)).sum := by
  let p := width c
  have hp : 0 < p := by dsimp [p, width]; omega
  rw [wrows]
  simp only [List.map_map]
  change ((List.range c.n).map (fun i : Nat => g i (c.base + c.step * (i % p : Nat)))).sum = _
  have hsv : ∀ i : Nat, g i (stripedValue p (wmake c) i) = g i (c.base + c.step * (i % p : Nat)) := by
    intro i
    simp [stripedValue, wmake, p]
  rw [← show ((List.range c.n).map (fun i : Nat => g i (stripedValue p (wmake c) i))).sum =
      ((List.range c.n).map (fun i : Nat => g i (c.base + c.step * (i % p : Nat)))).sum by
        apply congrArg List.sum
        apply List.map_congr_left
        intro i hi
        exact hsv i]
  rw [striped_sum p hp c.n (wmake c) g]
  simp [wmake, p]

private theorem wstriped_nat (c : Config) (g : Int → Nat) :
    ((rows layout c).map (fun r => g r.2)).sum =
      ((List.range (width c)).map (fun r =>
        ((List.range ((c.n - r + width c - 1) / width c)).map (fun _ : Nat =>
          g (c.base + c.step * (r : Int)))).sum)).sum := by
  let p := width c
  have hp : 0 < p := by dsimp [p, width]; omega
  rw [wrows]
  simp only [List.map_map]
  change ((List.range c.n).map (fun i : Nat => g (c.base + c.step * (i % p : Nat)))).sum = _
  have hsv : ∀ i : Nat, g (stripedValue p (wmake c) i) = g (c.base + c.step * (i % p : Nat)) := by
    intro i
    simp [stripedValue, wmake, p]
  rw [← show ((List.range c.n).map (fun i : Nat => g (stripedValue p (wmake c) i))).sum =
      ((List.range c.n).map (fun i : Nat => g (c.base + c.step * (i % p : Nat)))).sum by
        apply congrArg List.sum
        apply List.map_congr_left
        intro i hi
        exact hsv i]
  rw [striped_sum_nat p hp c.n (wmake c) g]
  simp [wmake, p]

private theorem wlanes_sum (c : Config) (F : Lane → Int)
    (hz : ∀ r, r < width c → min (width c) c.n ≤ r →
      F (lane c.n r (width c) (c.base + c.step * (r : Int)) 0) = 0) :
    ((layout.lanes c).map F).sum =
      ((List.range (width c)).map (fun r : Nat =>
        F (lane c.n r (width c) (c.base + c.step * (r : Int)) 0))).sum := by
  let K := min (width c) c.n
  have hk : K ≤ width c := Nat.min_le_left _ _
  have hl : layout.lanes c = (List.range K).map (fun r : Nat =>
      lane c.n r (width c) (c.base + c.step * (r : Int)) 0) := by
    dsimp [K]
    simp [layout, striped]
  rw [hl]
  simp only [List.map_map, Function.comp_apply]
  change ((List.range K).map (fun r : Nat => F (lane c.n r (width c) (c.base + c.step * (r : Int)) 0))).sum =
    ((List.range (width c)).map (fun r : Nat => F (lane c.n r (width c) (c.base + c.step * (r : Int)) 0))).sum
  symm
  rw [← sum_range_trunc_int (fun r : Nat => F (lane c.n r (width c) (c.base + c.step * (r : Int)) 0)) K (width c) hk]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  have hrw : r < width c := List.mem_range.mp hr
  by_cases hrk : r < K
  · simp [hrk]
  · simp [hrk]
    exact hz r hrw (by dsimp [K] at hrk ⊢; omega)

private theorem lane_n_eq (c : Config) (r : Nat) :
    (lane c.n r (width c) (c.base + c.step * (r : Int)) 0).n =
      (c.n - r + width c - 1) / width c := by
  unfold lane
  rfl

private theorem wlanes_sum_nat (c : Config) (F : Lane → Nat)
    (hz : ∀ r, r < width c → min (width c) c.n ≤ r →
      F (lane c.n r (width c) (c.base + c.step * (r : Int)) 0) = 0) :
    ((layout.lanes c).map F).sum =
      ((List.range (width c)).map (fun r : Nat =>
        F (lane c.n r (width c) (c.base + c.step * (r : Int)) 0))).sum := by
  let K := min (width c) c.n
  have hk : K ≤ width c := Nat.min_le_left _ _
  have hl : layout.lanes c = (List.range K).map (fun r : Nat =>
      lane c.n r (width c) (c.base + c.step * (r : Int)) 0) := by
    dsimp [K]
    simp [layout, striped]
  rw [hl]
  simp only [List.map_map, Function.comp_apply]
  change ((List.range K).map (fun r : Nat => F (lane c.n r (width c) (c.base + c.step * (r : Int)) 0))).sum =
    ((List.range (width c)).map (fun r : Nat => F (lane c.n r (width c) (c.base + c.step * (r : Int)) 0))).sum
  symm
  rw [← sum_range_trunc (fun r : Nat => F (lane c.n r (width c) (c.base + c.step * (r : Int)) 0)) K (width c) hk]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  have hrw : r < width c := List.mem_range.mp hr
  by_cases hrk : r < K
  · simp [hrk]
  · simp [hrk]
    exact hz r hrw (by dsimp [K] at hrk ⊢; omega)

private theorem wlanes_zero_nat (c : Config) (F : Lane → Nat)
    (hzero : ∀ a, a.n = 0 → F a = 0) :
    ∀ r, r < width c → min (width c) c.n ≤ r →
      F (lane c.n r (width c) (c.base + c.step * (r : Int)) 0) = 0 := by
  intro r hr hle
  apply hzero
  rw [lane_n_eq]
  apply Nat.div_eq_of_lt
  by_cases hcn : c.n < width c
  · have hmin : min (width c) c.n = c.n := Nat.min_eq_right (by omega)
    omega
  · have hmin : min (width c) c.n = width c := Nat.min_eq_left (by omega)
    omega

private theorem sum_filter_map_snd_eq (l : List (Nat × Int)) (mask : Progression) :
    ((l.filter (fun r => !contains mask r.1)).map Prod.snd).sum =
      (l.map (fun r => if !contains mask r.1 then r.2 else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hc : contains mask x.1 = true
      · simp [hc, ih]
      · have hcf : contains mask x.1 = false := by cases h' : contains mask x.1 <;> simp_all
        simp [hcf, ih]

private theorem filter_map_eq_map_filter {α β : Type} (f : α → β) (l : List α) (p : β → Bool) :
    (l.map f).filter p = (l.filter (fun x => p (f x))).map f := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p (f x) <;> simp [h, ih]

private theorem partialSum_of_n_zero (a : Lane) (ha : a.n = 0) : partialSum a a.n = 0 := by
  rw [ha]
  simp [partialSum, tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]

private theorem deletedSum_of_n_zero (a : Lane) (mask : Progression) (ha : a.n = 0) :
    deletedSum a mask = 0 := by
  dsimp [deletedSum]
  have hpn : (positions a).n = 0 := by dsimp [positions]; exact ha
  rw [show intersection (positions a) mask = ⟨0, 1, 0⟩ by
    unfold intersection
    rw [if_pos (by simp [hpn])]
    rfl]
  change (0 : Int) * firstAt a (⟨0, 1, 0⟩ : Progression) +
    tri 0 * slopeAt a (⟨0, 1, 0⟩ : Progression) = 0
  simp [tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]

private theorem lane_step_zero (c : Config) (r : Nat) :
    (lane c.n r (width c) (c.base + c.step * (r : Int)) 0).step = 0 := rfl

private theorem wlanes_zero (c : Config) (F : Lane → Int)
    (hzero : ∀ a, a.n = 0 → F a = 0) :
    ∀ r, r < width c → min (width c) c.n ≤ r →
      F (lane c.n r (width c) (c.base + c.step * (r : Int)) 0) = 0 := by
  intro r hr hle
  apply hzero
  rw [lane_n_eq]
  apply Nat.div_eq_of_lt
  by_cases hcn : c.n < width c
  · have hmin : min (width c) c.n = c.n := Nat.min_eq_right (by omega)
    omega
  · have hmin : min (width c) c.n = width c := Nat.min_eq_left (by omega)
    omega

end GoalProofs

namespace GoalProofs

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.WrappingSequence

private theorem partialSum_const_aux (base step : Int) (hs : step = 0) :
    ∀ n : Nat, partialSum ⟨0, 0, n, base, step⟩ n = (n : Int) * base := by
  intro n
  induction n with
  | zero => simp [partialSum, tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ n ih =>
      rw [partialSum_succ]
      change partialSum ⟨0, 0, n, base, step⟩ n + (base + (n : Int) * step) = ((n + 1 : Nat) : Int) * base
      rw [ih]
      simp [hs, Int.natCast_add]
      grind

private theorem sum_ite_contains (v : Int) (mask : Progression) (p r : Nat) :
    ∀ K : Nat,
      ((List.range K).map (fun q : Nat => if contains mask (r + q * p) then v else 0)).sum =
        v * ((List.range K).countP (fun q : Nat => contains mask (r + q * p)) : Int) := by
  intro K
  induction K with
  | zero => simp
  | succ K ih =>
      rw [List.range_succ, List.map_append, List.sum_append, List.countP_append]
      simp only [List.map_cons, List.sum_cons, List.map_nil, List.sum_nil, List.countP_cons]
      rw [ih]
      by_cases h : contains mask (r + K * p) <;> simp [h] <;> grind

private theorem sum_not_contains (v : Int) (mask : Progression) (p r : Nat) :
    ∀ K : Nat,
      ((List.range K).map (fun q : Nat => if !contains mask (r + q * p) then v else 0)).sum =
        v * ((K : Int) - ((List.range K).countP (fun q : Nat => contains mask (r + q * p)) : Int)) := by
  intro K
  induction K with
  | zero => simp
  | succ K ih =>
      rw [List.range_succ, List.map_append, List.sum_append, List.countP_append]
      simp only [List.map_cons, List.sum_cons, List.map_nil, List.sum_nil, List.countP_cons]
      rw [ih]
      have hcast : ((K + 1 : Nat) : Int) = (K : Int) + 1 := by omega
      rw [hcast]
      by_cases h : contains mask (r + K * p) <;> simp [h] <;> grind

theorem sound_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.WrappingSequence.ResidueJoin.Goal := by
  intro c b
  rw [wstriped_nat c (fun v => (List.range b.n).countP
    (fun k : Nat => v == b.base + b.step * (k : Int)))]
  rw [wlanes_sum_nat c (fun a => joinCount a b)
    (wlanes_zero_nat c (fun a => joinCount a b) (by
      intro a ha
      have hvn : (values a).n = 0 := by dsimp [values]; exact ha
      have hinter : intersection (values a) (values b) = ⟨0, 1, 0⟩ := by
        unfold intersection
        rw [if_pos (by simp [hvn])]
        rfl
      unfold joinCount
      rw [hinter]
      simp))]

  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  have hrp : r < width c := List.mem_range.mp hr
  let K := (c.n - r + width c - 1) / width c
  let cnt := (List.range b.n).countP (fun k : Nat =>
    c.base + c.step * (r : Int) == b.base + b.step * (k : Int))
  rw [show ((List.range K).map (fun q : Nat => (List.range b.n).countP
      (fun k : Nat => c.base + c.step * (r : Int) == b.base + b.step * (k : Int)))).sum =
      K * cnt by simpa [cnt] using (sum_range_const_nat K cnt)]
  rw [joinCount_const (lane c.n r (width c) (c.base + c.step * (r : Int)) 0)
    (lane_step_zero c r) b]
  rw [lane_n_eq]
  simp [K, cnt, lane]

theorem sound_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.WrappingSequence.DeleteProgression.Goal := by
  intro c mask
  rw [sum_filter_map_snd_eq (rows layout c) mask]
  rw [wstriped c (fun i v => if !contains mask i then v else 0)]
  rw [wlanes_sum c (fun a => partialSum a a.n - deletedSum a mask)
    (wlanes_zero c (fun a => partialSum a a.n - deletedSum a mask) (by
      intro a ha
      rw [partialSum_of_n_zero a ha, deletedSum_of_n_zero a mask ha]
      simp))]

  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  have hrp : r < width c := List.mem_range.mp hr
  let K := (c.n - r + width c - 1) / width c
  let v := c.base + c.step * (r : Int)
  let a : Lane := lane c.n r (width c) v 0
  have ha_n : a.n = K := by dsimp [a, K]; exact lane_n_eq c r
  have hp_pos : 0 < (positions a).step := by dsimp [a, positions, lane]; dsimp [width]; omega
  have hposvals : progVals (positions a) = ((List.range K).map (fun q : Nat => (r + q * width c : Nat))).map (fun x : Nat => (x : Int)) := by
    dsimp [progVals, positions, a, K]
    simp [lane]
  have hcnt : ((progVals (positions a)).filter (contains mask)).length =
      (List.range K).countP (fun q : Nat => contains mask (r + q * width c)) := by
    rw [hposvals]
    rw [filter_map_eq_map_filter (fun x : Nat => (x : Int))
      ((List.range K).map (fun q : Nat => r + q * width c)) (contains mask)]
    simp only [List.length_map]
    rw [filter_map_eq_map_filter (fun q : Nat => r + q * width c) (List.range K)
      (fun x : Nat => contains mask (x : Int))]
    simp only [List.length_map]
    rw [List.countP_eq_length_filter]
    apply congrArg List.length
    apply List.filter_congr
    intro q hq
    simp
  have hoverlap : (intersection (positions a) mask).n =
      (List.range K).countP (fun q : Nat => contains mask (r + q * width c)) := by
    have hperm := perm_progVals_intersection (positions a) mask hp_pos
    have hlen : (intersection (positions a) mask).n =
        ((progVals (positions a)).filter (contains mask)).length := by
      simpa [progVals] using hperm.length_eq
    rw [hlen, hcnt]
  have hpsum : partialSum a a.n = (K : Int) * v := by
    rw [ha_n]
    dsimp [a]
    exact partialSum_const_aux (c.base + c.step * (r : Int)) 0 rfl K
  have hdsum : deletedSum a mask = ((intersection (positions a) mask).n : Int) * v := by
    unfold deletedSum
    dsimp only
    have hfa : firstAt a (intersection (positions a) mask) = v := by
      unfold firstAt
      change v + (((intersection (positions a) mask).lo - (r : Int)) / (width c : Int)) * 0 = v
      simp
    have hsa : slopeAt a (intersection (positions a) mask) = 0 := by
      unfold slopeAt
      change (((intersection (positions a) mask).step / width c : Nat) : Int) * 0 = 0
      simp
    rw [hfa, hsa]
    grind
  change ((List.range K).map (fun q : Nat => if !contains mask (r + q * width c) then v else 0)).sum = _
  rw [show ((List.range K).map (fun q : Nat => if !contains mask (r + q * width c) then v else 0)).sum =
      v * ((K : Int) - ((List.range K).countP (fun q : Nat => contains mask (r + q * width c)) : Int)) by
        simpa [Int.natCast_add, Int.natCast_mul, v, K] using
          (sum_not_contains v mask (width c) r K)]
  rw [hpsum, hdsum, hoverlap]
  simp [a, v]
  grind

end GoalProofs


-- =====================================================================
-- Round 2: AlignedProduct for WrappingSequence.
-- The layout is striped with period width c and each lane is constant,
-- so aligned a b collapses to a linear moment sum over the intersection
-- of the lane positions with the positions of b.
-- =====================================================================

namespace GoalProofs

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.WrappingSequence

private def bValueAt (b : Lane) (v : Int) : Int :=
  b.base + ((v - (b.start : Int)) / (b.stride : Int)) * b.step

private theorem sum_ite_eq_filter {α : Type} (l : List α) (p : α → Bool) (f : α → Int) :
    ((l.map (fun i => if p i then f i else 0)).sum) = ((l.filter p).map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x = true <;> simp [h, ih]

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

private theorem sum_range_mul_const_int (f : Nat → Int) (n : Nat) (c : Int) :
    ((List.range n).map (fun i => c * f i)).sum = c * ((List.range n).map f).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append,
        List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      rw [ih]
      grind

private theorem sum_affine (n : Nat) (y dy : Int) :
    ((List.range n).map (fun k : Nat => y + (k : Int) * dy)).sum =
      (n : Int) * y + tri n * dy := by
  induction n with
  | zero => simp [tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      rw [ih, tri_succ]
      simp [Int.natCast_add]
      grind

private theorem aligned_of_n_zero (a : Lane) (ha : a.n = 0) (b : Lane) : aligned a b = 0 := by
  unfold aligned
  have hint : intersection (positions a) (positions b) = ⟨0, 1, 0⟩ := by
    dsimp [positions]
    unfold intersection
    rw [if_pos (by simp [ha])]
    rfl
  rw [hint]
  simp [tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]

/-- For a fixed physical ordinal, exactly one b-index can match (b.stride>0),
    so the inner sum collapses to the single b value. -/
private theorem inner_affine_nat (b : Lane) (hb : 0 < b.stride) (P : Nat) (v : Int) :
    ((List.range b.n).map (fun j : Nat =>
      if P == b.start + j * b.stride then v * (b.base + b.step * (j : Int)) else 0)).sum =
    if contains (positions b) (P : Int) then v * bValueAt b (P : Int) else 0 := by
  by_cases hc : contains (positions b) (P : Int) = true
  · rw [if_pos hc]
    have hmem := (contains_iff_mem (positions b) (P : Int)).mp hc
    rcases hmem with ⟨j, hj, hjval⟩
    unfold progVal at hjval
    dsimp [positions] at hjval
    have hbneI : (b.stride : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hb)
    have hdivj : ((P : Int) - (b.start : Int)) / (b.stride : Int) = (j : Int) := by
      rw [hjval]
      have hsub : (b.start : Int) + (j : Int) * (b.stride : Int) - (b.start : Int) =
          (j : Int) * (b.stride : Int) := by omega
      rw [hsub]
      rw [Int.mul_comm (j : Int) (b.stride : Int)]
      exact Int.mul_ediv_cancel_left (j : Int) hbneI
    have hp : ∀ i : Nat, i ∈ List.range b.n →
        (P == b.start + i * b.stride) = (i == j) := by
      intro i hi
      have hil : i < b.n := List.mem_range.mp hi
      by_cases heq : i = j
      · subst i
        have hnat : P = b.start + j * b.stride := by exact_mod_cast hjval
        simp [hnat]
      · have hb1 : (P == b.start + i * b.stride) = false := by
          rw [beq_eq_false_iff_ne]
          intro h
          have hIntEq : (P : Int) = (b.start : Int) + (i : Int) * (b.stride : Int) := by omega
          have hEq2 : (b.start : Int) + (j : Int) * (b.stride : Int) =
              (b.start : Int) + (i : Int) * (b.stride : Int) := by
            rw [← hjval, hIntEq]
          have hmul : (j : Int) * (b.stride : Int) = (i : Int) * (b.stride : Int) := by omega
          have hji : (j : Int) = (i : Int) := Int.eq_of_mul_eq_mul_right hbneI hmul
          exact heq (by exact_mod_cast hji.symm)
        rw [hb1]
        simp [heq]
    rw [sum_ite_eq_filter]
    rw [show (List.range b.n).filter (fun i : Nat => P == b.start + i * b.stride) =
        (List.range b.n).filter (fun i : Nat => i == j) by
      apply List.filter_congr
      intro i hi
      exact hp i hi]
    rw [range_filter_eq_const b.n j (by simpa [positions] using hj)]
    simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
    dsimp [bValueAt]
    rw [hdivj]
    grind
  · rw [if_neg hc]
    rw [sum_ite_eq_filter]
    have hnil : (List.range b.n).filter (fun i : Nat => P == b.start + i * b.stride) = [] := by
      apply List.eq_nil_iff_forall_not_mem.mpr
      intro t ht
      have htlt : t < b.n := List.mem_range.mp (List.mem_filter.mp ht).1
      have heq := (List.mem_filter.mp ht).2
      have hnateq : P = b.start + t * b.stride := Nat.eq_of_beq_eq_true heq
      have hIntEq : (P : Int) = (b.start : Int) + (t : Int) * (b.stride : Int) := by omega
      apply hc
      apply (contains_iff_mem (positions b) (P : Int)).mpr
      refine ⟨t, htlt, ?_⟩
      dsimp [positions]
      unfold progVal
      exact hIntEq
    rw [hnil]
    simp

/-- The value of a constant lane on the intersection progression is a.base,
    and the aligned product expands to the first two moments of the b lane. -/
private theorem aligned_const_pvals_sum (a b : Lane) (ha0 : a.step = 0) (ha_pos : 0 < a.stride)
    (hb : 0 < b.stride) :
    aligned a b = ((progVals (intersection (positions a) (positions b))).map
      (fun P : Int => a.base * bValueAt b P)).sum := by
  let pa : Progression := positions a
  let pb : Progression := positions b
  let R : Progression := intersection pa pb
  change aligned a b = ((progVals R).map (fun P : Int => a.base * bValueAt b P)).sum
  by_cases hRn : R.n = 0
  · rw [show (progVals R) = [] by simp [progVals, hRn]]
    simp [aligned, R, pa, pb, hRn, tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  · have hpa_n : pa.n ≠ 0 := by
      intro h
      have hRzero : R = ⟨0, 1, 0⟩ := by
        dsimp [R]
        unfold intersection
        rw [if_pos (by simp [h])]
        rfl
      exact hRn (by rw [hRzero])
    have hpb_n : pb.n ≠ 0 := by
      intro h
      have hRzero : R = ⟨0, 1, 0⟩ := by
        dsimp [R]
        unfold intersection
        rw [if_pos (by simp [h])]
        rfl
      exact hRn (by rw [hRzero])
    have hpa_s : pa.step ≠ 0 := by
      dsimp [pa, positions]
      omega
    have hpb_s : pb.step ≠ 0 := by
      dsimp [pb, positions]
      omega
    have hgap0 : (pb.lo - pa.lo) % (Nat.gcd pa.step pb.step : Int) = 0 := by
      by_cases hg : (pb.lo - pa.lo) % (Nat.gcd pa.step pb.step : Int) = 0
      · exact hg
      · have hif : ((pb.lo - pa.lo) % (pa.step.gcd pb.step : Int) != 0) = true := by
          cases hh : ((pb.lo - pa.lo) % (pa.step.gcd pb.step : Int) != 0) <;> simp_all
        have hRzero : R = ⟨0, 1, 0⟩ := by
          dsimp [R]
          unfold intersection
          rw [if_neg (by simp [hpa_n, hpb_n]), if_neg (by simp [hpa_s]),
            if_neg (by simp [hpb_s]), if_pos hif]
          rfl
        exact False.elim (hRn (by rw [hRzero]))
    have hrstep : R.step = (pa.step / Nat.gcd pa.step pb.step) * pb.step := by
      dsimp [R]
      unfold intersection
      rw [if_neg (by simp [hpa_n, hpb_n]), if_neg (by simp [hpa_s]),
        if_neg (by simp [hpb_s]), if_neg (by simp [hgap0])]
      rfl
    have hrstep' : R.step = (a.stride / Nat.gcd a.stride b.stride) * b.stride := by
      simpa [pa, pb, positions] using hrstep
    have hbs0 : b.stride ≠ 0 := by omega
    have hbs0I : (b.stride : Int) ≠ 0 := by exact_mod_cast hbs0
    have hRnpos : 0 < R.n := Nat.pos_of_ne_zero hRn
    have hrlo_mem : contains R R.lo = true := by
      apply (contains_iff_mem R R.lo).mpr
      refine ⟨0, hRnpos, ?_⟩
      simp [progVal]
    have hpos_mem : contains pb R.lo = true :=
      (intersection_contains pa pb R.lo).mp hrlo_mem |>.2
    rcases (contains_iff_mem pb R.lo).mp hpos_mem with ⟨j0, hj0, hj0val⟩
    unfold progVal at hj0val
    dsimp [pb, positions] at hj0val
    have hdivlo : (R.lo - (b.start : Int)) / (b.stride : Int) = (j0 : Int) := by
      rw [hj0val]
      have hsub : (b.start : Int) + (j0 : Int) * (b.stride : Int) - (b.start : Int) =
          (j0 : Int) * (b.stride : Int) := by omega
      rw [hsub]
      rw [Int.mul_comm (j0 : Int) (b.stride : Int)]
      exact Int.mul_ediv_cancel_left (j0 : Int) hbs0I
    have hstepInt : (R.step : Int) = (b.stride : Int) *
        (a.stride / Nat.gcd a.stride b.stride : Nat) := by
      rw [hrstep']
      simp [Int.natCast_mul, Nat.mul_comm]
    have hdivk : ∀ k : Nat,
        (R.lo + (k : Int) * (R.step : Int) - (b.start : Int)) / (b.stride : Int) =
          (j0 : Int) + (k : Int) * (a.stride / Nat.gcd a.stride b.stride : Nat) := by
      intro k
      have hnum : R.lo + (k : Int) * (R.step : Int) - (b.start : Int) =
          ((j0 : Int) + (k : Int) * (a.stride / Nat.gcd a.stride b.stride : Nat)) *
            (b.stride : Int) := by
        have hstepmul : (k : Int) * (R.step : Int) =
            ((k : Int) * (a.stride / Nat.gcd a.stride b.stride : Nat)) * (b.stride : Int) := by
          rw [hstepInt]
          grind
        have hlosub : R.lo - (b.start : Int) = (j0 : Int) * (b.stride : Int) := by
          rw [hj0val]
          omega
        have hrearr : R.lo + (k : Int) * (R.step : Int) - (b.start : Int) =
            (R.lo - (b.start : Int)) + (k : Int) * (R.step : Int) := by omega
        rw [hrearr, hlosub, hstepmul]
        grind
      rw [hnum]
      rw [Int.mul_comm ((j0 : Int) + (k : Int) * (a.stride / Nat.gcd a.stride b.stride : Nat)) (b.stride : Int)]
      exact Int.mul_ediv_cancel_left _ hbs0I
    let x : Int := firstAt a R
    let dx : Int := slopeAt a R
    let y : Int := firstAt b R
    let dy : Int := slopeAt b R
    have hx : x = a.base := by
      dsimp [x, firstAt]
      rw [ha0]
      simp
    have hdx : dx = 0 := by
      dsimp [dx, slopeAt]
      rw [ha0]
      simp
    have hy : y = b.base + (j0 : Int) * b.step := by
      dsimp [y, firstAt]
      rw [hdivlo]
    let q : Nat := a.stride / Nat.gcd a.stride b.stride
    have hdy : dy = (q : Int) * b.step := by
      dsimp [dy, slopeAt]
      change (((R.step / b.stride : Nat) : Int) * b.step) = (q : Int) * b.step
      rw [hrstep']
      dsimp [q]
      have hcancel :
          (((a.stride : Int) / (Nat.gcd a.stride b.stride : Int)) * (b.stride : Int)) /
              (b.stride : Int) =
            (a.stride : Int) / (Nat.gcd a.stride b.stride : Int) := by
        exact Int.ediv_eq_of_eq_mul_left hbs0I rfl
      rw [hcancel]
    change aligned a b = ((progVals R).map (fun P : Int => a.base * bValueAt b P)).sum
    rw [show (progVals R) = (List.range R.n).map
        (fun k : Nat => R.lo + (k : Int) * (R.step : Int)) by
      rfl]
    rw [List.map_map]
    change aligned a b = ((List.range R.n).map (fun k : Nat =>
      a.base * bValueAt b (R.lo + (k : Int) * (R.step : Int)))).sum
    rw [show ((List.range R.n).map (fun k : Nat =>
        a.base * bValueAt b (R.lo + (k : Int) * (R.step : Int)))) =
        ((List.range R.n).map (fun k : Nat => a.base * (y + (k : Int) * dy))) by
      apply List.map_congr_left
      intro k hk
      dsimp [bValueAt, y, dy]
      rw [hdivk k]
      grind]
    rw [show ((List.range R.n).map (fun k : Nat => a.base * (y + (k : Int) * dy))).sum =
        a.base * (((List.range R.n).map (fun k : Nat => y + (k : Int) * dy)).sum) by
      exact sum_range_mul_const_int _ _ _]
    rw [sum_affine R.n y dy]
    unfold aligned
    dsimp [x, dx, y, dy] at hx hdx hy hdy ⊢
    rw [hx, hdx, hy, hdy]
    simp
    grind

/-- One striped residue class: the nested sum over the lane positions
    and the b positions equals the aligned product of that lane. -/
private theorem lane_aligned_const_sum (c : Config) (b : Lane) (hb : 0 < b.stride)
    (r : Nat) (hr : r < width c) :
    let p := width c
    let K := (c.n - r + p - 1) / p
    let v := c.base + c.step * (r : Int)
    let a : Lane := lane c.n r p v 0
    ((List.range K).map (fun q : Nat => ((List.range b.n).map (fun j : Nat =>
      if r + q * p == b.start + j * b.stride then
        v * (b.base + b.step * (j : Int)) else 0)).sum)).sum = aligned a b := by
  dsimp only
  let p := width c
  let K := (c.n - r + p - 1) / p
  let v := c.base + c.step * (r : Int)
  let a : Lane := lane c.n r p v 0
  let posA : Progression := positions a
  let posB : Progression := positions b
  let R : Progression := intersection posA posB
  have hp : 0 < p := by dsimp [p, width]; omega
  have ha_n : a.n = K := by dsimp [a, K]; exact lane_n_eq c r
  have ha_pos : 0 < a.stride := by dsimp [a, p]; exact hp
  have hposApos : 0 < posA.step := by dsimp [posA, positions]; exact ha_pos
  have hposBpos : 0 < posB.step := by dsimp [posB, positions]; exact hb
  let F : Int → Int := fun P : Int => v * bValueAt b P
  change ((List.range K).map (fun q : Nat => ((List.range b.n).map (fun j : Nat =>
      if r + q * p == b.start + j * b.stride then
        v * (b.base + b.step * (j : Int)) else 0)).sum)).sum = aligned a b
  rw [show ((List.range K).map (fun q : Nat => ((List.range b.n).map (fun j : Nat =>
      if r + q * p == b.start + j * b.stride then
        v * (b.base + b.step * (j : Int)) else 0)).sum)) =
      ((List.range K).map (fun q : Nat =>
        if contains posB ((r + q * p : Nat) : Int) then F ((r + q * p : Nat) : Int) else 0)) by
    apply List.map_congr_left
    intro q hq
    dsimp [F]
    exact inner_affine_nat b hb (r + q * p) v]
  rw [show ((List.range K).map (fun q : Nat =>
        if contains posB ((r + q * p : Nat) : Int) then F ((r + q * p : Nat) : Int) else 0)).sum =
      (((progVals posA).filter (contains posB)).map F).sum by
    rw [sum_ite_eq_filter]
    have hprog : progVals posA =
        (List.range a.n).map (fun q : Nat => ((a.start + q * a.stride : Nat) : Int)) := by
      unfold progVals
      dsimp [progVal, posA, positions]
    rw [hprog, filter_map_eq_map_filter]
    rw [List.map_map]
    dsimp [a, K, lane]
    simp [Function.comp_def]]
  rw [← sum_progVals_filter posA posB hposApos F]
  change ((progVals R).map F).sum = aligned a b
  have hal := aligned_const_pvals_sum a b (by dsimp [a]; rfl) ha_pos hb
  rw [hal]
  dsimp [F, R, posA, posB, a, lane]

theorem sound_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.WrappingSequence.AlignedProduct.Goal := by
  intro c b hb
  rw [wstriped c (fun i v => ((List.range b.n).map (fun j : Nat =>
    if i == b.start + j * b.stride then v * (b.base + b.step * (j : Int)) else 0)).sum)]
  rw [wlanes_sum c (fun a => aligned a b) (by
    intro r hr hle
    apply aligned_of_n_zero
    rw [lane_n_eq]
    apply Nat.div_eq_of_lt
    omega)]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  exact lane_aligned_const_sum c b hb r (List.mem_range.mp hr)

end GoalProofs

end TinylambOpt.ResearchProofs.Atlas.EncodedPages.WrappingSequenceProgressions

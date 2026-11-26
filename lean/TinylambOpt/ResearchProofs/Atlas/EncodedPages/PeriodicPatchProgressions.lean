import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `PeriodicPatch.AlignedProduct` — `∀ c (b : Lane), 0 < b.stride → ((rows layout c).map
  (fun r => ((List.range b.n).map (fun i => if r.1 == b.start+i*b.stride then
  r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum = ((layout.lanes c).map (aligned ·
  b)).sum`
- `PeriodicPatch.DeleteProgression` — `∀ c mask, (((rows layout c).filter (fun r =>
  !contains mask r.1)).map Prod.snd).sum = ((layout.lanes c).map (fun a => partialSum
  a a.n - deletedSum a mask)).sum`
- `PeriodicPatch.ResidueJoin` — `∀ c (b : Lane), ((rows layout c).map (fun r =>
  (List.range b.n).countP (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum =
  ((layout.lanes c).map (joinCount · b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.PeriodicPatchProgressions

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.PeriodicPatch

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
-- PeriodicPatch layout: rows are partitioned by their residue modulo width
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

private theorem sum_map_zero_list (l : List Nat) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l <;> simp [*]

private theorem striped_sum_partition (p : Nat) (hp : 0 < p) (F : Nat → Int) :
    ∀ xs : List Nat, (xs.map F).sum =
      ((List.range p).map (fun r => ((xs.filter (fun i => i % p == r)).map F).sum)).sum := by
  intro xs
  induction xs with
  | nil =>
      rw [List.map_nil, List.sum_nil]
      exact (sum_map_zero_list (List.range p)).symm
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

private theorem div_eq_one_of_bounds (t p : Nat) (hp : 0 < p)
    (hge : p ≤ t) (hlt : t < 2 * p) : t / p = 1 := by
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
      rw [mod_step_count n p r hp hr, mod_step_count (n + 1) p r hp hr]
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

private theorem flatMap_map_sum {α : Type} (ls : List Lane) (f : Lane → List α) (F : α → Int) :
    ((ls.flatMap f).map F).sum = (ls.map (fun a => ((f a).map F).sum)).sum := by
  induction ls with
  | nil => simp
  | cons a as ih => simp [ih, List.map_append, List.sum_append]

private theorem periodicPatch_read_mod (c : Config) (r q : Nat) (hr : r < width c) :
    c.base + c.step * ((r + q * width c : Nat) : Int) +
        (if (r + q * width c) % width c == 0 then c.bias else 0) =
      (c.base + c.step * (r : Int) + (if r == 0 then c.bias else 0)) +
        (q : Int) * (c.step * (width c : Int)) := by
  have hp : 0 < width c := by dsimp [width]; omega
  have hmod : (r + q * width c) % width c = r := by
    rw [Nat.mul_comm q (width c), Nat.add_mul_mod_self_left]
    exact Nat.mod_eq_of_lt hr
  have hbeq : ((r + q * width c) % width c == 0) = (r == 0) := by rw [hmod]
  rw [hbeq]
  push_cast
  grind

private theorem periodicPatch_lane_rows (c : Config) (r : Nat) (hr : r < min (width c) c.n)
    (F : Nat × Int → Int) :
    ((lanePoints (lane c.n r (width c)
        (c.base + c.step * (r : Int) + (if r == 0 then c.bias else 0))
        (c.step * (width c : Int)))).map F).sum =
      (((List.range c.n).filter (fun i => i % width c == r)).map
        (fun i : Nat => F (i, c.base + c.step * (i : Int) +
          if i % width c == 0 then c.bias else 0))).sum := by
  have hp : 0 < width c := by dsimp [width]; omega
  have hrp : r < width c := by omega
  unfold lanePoints
  dsimp [lane]
  rw [range_filter_mod_eq c.n (width c) r hp hrp]
  rw [List.map_map, List.map_map]
  simp [Function.comp_def]
  apply congrArg List.sum
  apply List.map_congr_left
  intro q hq
  have hval :
      (c.base + c.step * (r : Int) + (if r = 0 then c.bias else 0)) +
          (q : Int) * (c.step * (width c : Int)) =
      c.base + c.step * ((r : Int) + (q : Int) * (width c : Int)) +
          (if r % width c = 0 then c.bias else 0) := by
    have hreq : r % width c = r := Nat.mod_eq_of_lt hrp
    rw [hreq]
    grind
  rw [hval]

private theorem striped_sum_partition_min (n p : Nat) (hp : 0 < p) (F : Nat → Int) :
    ((List.range n).map F).sum =
      ((List.range (min n p)).map (fun r =>
        (((List.range n).filter (fun i => i % p == r)).map F).sum)).sum := by
  rw [striped_sum_partition p hp F (List.range n)]
  by_cases hpn : p ≤ n
  · have hmin : min n p = p := Nat.min_eq_right hpn
    simp [hmin]
  · have hnp : n < p := by omega
    have hmin : min n p = n := Nat.min_eq_left (by omega)
    rw [hmin]
    have hp_eq : n + (p - n) = p := by omega
    have hsplit : List.range p = List.range n ++ (List.range (p - n)).map (fun i : Nat => n + i) := by
      rw [← hp_eq, List.range_add]
      have hsub : n + (p - n) - n = p - n := by omega
      rw [hsub]
    rw [hsplit]
    simp only [List.map_append, List.sum_append]
    have hzero : (((List.range (p - n)).map (fun i : Nat => n + i)).map
        (fun r => (((List.range n).filter (fun j => j % p == r)).map F).sum)).sum = 0 := by
      rw [List.map_map]
      simp [Function.comp_def]
      have hmaps : ((List.range (p - n)).map
          (fun i : Nat => (((List.range n).filter (fun j => j % p == n + i)).map F).sum)) =
          ((List.range (p - n)).map (fun _ : Nat => (0 : Int))) := by
        apply List.map_congr_left
        intro i hi
        have hnil : (List.range n).filter (fun j => j % p == n + i) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro j hj
          have hj' : j < n := List.mem_range.mp hj
          have hjp : j < p := by omega
          have hjmod : j % p = j := Nat.mod_eq_of_lt hjp
          rw [beq_iff_eq, hjmod]
          omega
        simp [hnil]
      rw [hmaps]
      exact sum_map_zero_list (List.range (p - n))
    rw [hzero]
    simp

private theorem periodicPatch_sum (c : Config) (F : Nat × Int → Int) :
    ((rows PeriodicPatch.layout c).map F).sum =
      (((PeriodicPatch.layout.lanes c).flatMap lanePoints).map F).sum := by
  let p := width c
  have hp : 0 < p := by dsimp [p, width]; omega
  have hrows : rows PeriodicPatch.layout c =
      (List.range c.n).map (fun i : Nat => (i, c.base + c.step * (i : Int) +
        if i % p == 0 then c.bias else 0)) := by
    unfold rows
    apply range_filterMap_all
    intro i hi
    simp [PeriodicPatch.layout, p]
  rw [hrows]
  rw [List.map_map]
  simp only [Function.comp_def]
  rw [striped_sum_partition_min c.n p hp (fun i : Nat =>
    F (i, c.base + c.step * (i : Int) + if i % p == 0 then c.bias else 0))]
  rw [flatMap_map_sum]
  rw [show min c.n p = min p c.n by exact Nat.min_comm _ _]
  unfold PeriodicPatch.layout striped
  dsimp only
  dsimp [p]
  rw [List.map_map]
  simp only [Function.comp_def]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  have hamin : a < min (width c) c.n := List.mem_range.mp ha
  exact (periodicPatch_lane_rows c a hamin F).symm

private theorem lanePoints_snd (a : Lane) : (lanePoints a).map Prod.snd = laneVals a := by
  unfold lanePoints laneVals
  simp [List.map_map]

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

private theorem sum_range_linear (n : Nat) (x d : Int) :
    ((List.range n).map (fun k : Nat => x + (k : Int) * d)).sum = (n : Int) * x + tri n * d := by
  induction n with
  | zero => simp [tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append,
        List.sum_append, ih, tri_succ]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      simp [Int.natCast_add]
      grind

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

private theorem perm_sum_eq {l₁ l₂ : List Int} (h : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction h with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private def lanePositions (a : Lane) : List Int :=
  (List.range a.n).map (fun i : Nat => a.start + i * a.stride)

private theorem positions_mem_iff_lane (a : Lane) (p : Int) :
    contains (positions a) p = true ↔ ∃ i, i < a.n ∧ p = (a.start + i * a.stride : Nat) := by
  rw [contains_iff_mem]
  unfold progMem progVal positions
  constructor
  · rintro ⟨i, hi, hv⟩
    exact ⟨i, hi, hv⟩
  · rintro ⟨i, hi, hv⟩
    exact ⟨i, hi, hv⟩

private theorem contains_positions_stride_one (a : Lane) (hs : a.stride = 1) (p : Int) :
    contains (positions a) p = true ↔
      0 < a.n ∧ (a.start : Int) ≤ p ∧ p ≤ (a.start : Int) + ((a.n - 1 : Nat) : Int) := by
  rw [contains_iff_mem]
  unfold progMem progVal positions
  simp only [hs, Nat.mul_one]
  constructor
  · rintro ⟨i, hi, hv⟩
    constructor <;> omega
  · rintro ⟨hn, hlo, hhi⟩
    refine ⟨(p - (a.start : Int)).toNat, ?_, ?_⟩
    · have hnonneg : 0 ≤ p - (a.start : Int) := by omega
      have hcast : ((p - (a.start : Int)).toNat : Int) = p - (a.start : Int) :=
        Int.toNat_of_nonneg hnonneg
      have hle : (p - (a.start : Int)).toNat ≤ a.n - 1 := by
        apply Int.toNat_le.mpr
        omega
      omega
    · have hnonneg : 0 ≤ p - (a.start : Int) := by omega
      have hcast : ((p - (a.start : Int)).toNat : Int) = p - (a.start : Int) :=
        Int.toNat_of_nonneg hnonneg
      rw [hcast]
      omega

private def laneValAt (a : Lane) (p : Int) : Int :=
  a.base + ((p - (a.start : Int)) / (a.stride : Int)) * a.step

private theorem firstAt_eq_laneValAt (a : Lane) (r : Progression) :
    firstAt a r = laneValAt a r.lo := by
  unfold firstAt laneValAt
  rfl

private theorem lcm_step_dvd (s t : Nat) (hs : 0 < s) (ht : 0 < t) :
    s ∣ (s / s.gcd t) * t ∧ t ∣ (s / s.gcd t) * t := by
  let g := s.gcd t
  have hgpos : 0 < g := Nat.gcd_pos_of_pos_left t hs
  have hsdvd : g ∣ s := Nat.gcd_dvd_left s t
  have htdvd : g ∣ t := Nat.gcd_dvd_right s t
  have hseq : s = g * (s / g) := by simpa [g] using (Nat.mul_div_cancel' hsdvd).symm
  have hteq : t = g * (t / g) := by simpa [g] using (Nat.mul_div_cancel' htdvd).symm
  constructor
  · refine ⟨t / g, ?_⟩
    calc
      (s / g) * t = (s / g) * (g * (t / g)) := by exact congrArg (fun x => (s / g) * x) hteq
      _ = (g * (s / g)) * (t / g) := by ac_rfl
      _ = s * (t / g) := by rw [← hseq]
  · refine ⟨s / g, ?_⟩
    rw [Nat.mul_comm]

private theorem inter_step_dvd (p q : Progression) (hp : 0 < p.step) (hq : 0 < q.step)
    (hn : (intersection p q).n ≠ 0) :
    p.step ∣ (intersection p q).step ∧ q.step ∣ (intersection p q).step := by
  have hnpos : 0 < (intersection p q).n := by omega
  have hlo_mem : (intersection p q).lo ∈ progVals (intersection p q) := by
    unfold progVals
    rw [List.mem_map]
    exact ⟨0, by simpa using hnpos, by simp⟩
  have hloc : contains (intersection p q) (intersection p q).lo = true :=
    (contains_iff_progVals (intersection p q) (intersection p q).lo).mpr hlo_mem
  have hcommon := (intersection_contains p q (intersection p q).lo).mp hloc
  have hgapdvd := common_gap_dvd p q (intersection p q).lo hcommon.1 hcommon.2
  have hgapzero : ((q.lo - p.lo) % (p.step.gcd q.step : Int)) = 0 :=
    Int.emod_eq_zero_of_dvd hgapdvd
  have hpn : p.n ≠ 0 := by
    intro h
    apply hn
    unfold intersection
    simp [h]
  have hqn : q.n ≠ 0 := by
    intro h
    apply hn
    unfold intersection
    simp [h]
  have hpne : p.step ≠ 0 := by omega
  have hqne : q.step ≠ 0 := by omega
  have hbeq : ((q.lo - p.lo) % (p.step.gcd q.step : Int) != 0) = false := by
    simp [hgapzero]
  have hstepeq : (intersection p q).step = (p.step / p.step.gcd q.step) * q.step := by
    unfold intersection
    simp [hpn, hqn, hpne, hqne, hbeq]
  rw [hstepeq]
  exact lcm_step_dvd p.step q.step hp hq

private theorem laneValAt_progress (a : Lane) (hspos : 0 < a.stride) (r : Progression)
    (hdlo : (a.stride : Int) ∣ r.lo - (a.start : Int))
    (hdiv : a.stride ∣ r.step) (k : Nat) :
    laneValAt a (r.lo + (k : Int) * (r.step : Int)) = firstAt a r + (k : Int) * slopeAt a r := by
  unfold laneValAt firstAt slopeAt
  obtain ⟨q, hq⟩ := hdlo
  obtain ⟨m, hm⟩ := hdiv
  have hane : (a.stride : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hspos)
  have hq' : r.lo - (a.start : Int) = q * (a.stride : Int) := by
    rw [hq, Int.mul_comm]
  have hm' : (r.step : Int) = (m : Int) * (a.stride : Int) := by
    rw [hm]
    push_cast
    ac_rfl
  have hdivlo : (r.lo - (a.start : Int)) / (a.stride : Int) = q := by
    rw [hq']
    exact Int.ediv_eq_of_eq_mul_left hane rfl
  have hnum : r.lo + (k : Int) * (r.step : Int) - (a.start : Int) =
      (q + (k : Int) * (m : Int)) * (a.stride : Int) := by
    rw [hm']
    calc
      r.lo + (k : Int) * ((m : Int) * (a.stride : Int)) - (a.start : Int) =
          (r.lo - (a.start : Int)) + ((k : Int) * (m : Int)) * (a.stride : Int) := by grind
      _ = q * (a.stride : Int) + ((k : Int) * (m : Int)) * (a.stride : Int) := by rw [hq']
      _ = (q + (k : Int) * (m : Int)) * (a.stride : Int) := by rw [Int.add_mul]
  have hdivnum : (r.lo + (k : Int) * (r.step : Int) - (a.start : Int)) / (a.stride : Int) =
      q + (k : Int) * (m : Int) := by
    rw [hnum]
    exact Int.ediv_eq_of_eq_mul_left hane rfl
  have hslope : r.step / a.stride = m := by
    rw [hm, Nat.mul_comm a.stride m]
    exact Nat.mul_div_left m hspos
  rw [hdivnum, hdivlo, hslope]
  grind

private theorem lanePositions_snd_value (a : Lane) (hspos : 0 < a.stride) (i : Nat) :
    laneValAt a (a.start + i * a.stride : Nat) = a.base + (i : Int) * a.step := by
  unfold laneValAt
  have hsub : (((a.start + i * a.stride : Nat) : Int) - (a.start : Int)) =
      (i : Int) * (a.stride : Int) := by
    simp only [Int.natCast_add, Int.natCast_mul]
    omega
  rw [hsub]
  have hane : (a.stride : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hspos)
  rw [Int.ediv_eq_of_eq_mul_left hane rfl]

private theorem mem_lanePositions_iff (a : Lane) (p : Int) :
    p ∈ lanePositions a ↔ contains (positions a) p = true := by
  unfold lanePositions
  rw [List.mem_map]
  rw [contains_iff_mem]
  unfold progMem progVal positions
  constructor
  · rintro ⟨i, hi, hv⟩
    exact ⟨i, by simpa using hi, hv.symm⟩
  · rintro ⟨i, hi, hv⟩
    exact ⟨i, by simpa using hi, hv.symm⟩

private theorem deletedSum_eq_sum (a : Lane) (ha : 0 < a.stride) (m : Progression) :
    deletedSum a m =
      (((lanePositions a).filter (fun p => contains m p)).map (fun p => laneValAt a p)).sum := by
  let o := intersection (positions a) m
  have hostep : 0 < o.step := by dsimp [o]; exact intersection_step_pos (positions a) m
  have hnodO : (progVals o).Nodup := progVals_nodup o hostep
  have hposnod : (lanePositions a).Nodup := by
    unfold lanePositions
    apply nodup_map_of_injective
    · intro x hx y hy hxy
      have hmul : x * a.stride = y * a.stride := by omega
      exact Nat.eq_of_mul_eq_mul_left ha (by simpa [Nat.mul_comm] using hmul)
    · exact List.nodup_range
  have hfilternod : ((lanePositions a).filter (fun p => contains m p)).Nodup :=
    nodup_filter (lanePositions a) hposnod (fun p => contains m p)
  have hperm : List.Perm (progVals o) ((lanePositions a).filter (fun p => contains m p)) := by
    apply perm_of_nodup_mem _ _ hnodO hfilternod
    intro p
    rw [← contains_iff_progVals o p]
    rw [List.mem_filter]
    rw [mem_lanePositions_iff]
    dsimp [o]
    rw [intersection_contains]
  have hsumperm : ((progVals o).map (fun p => laneValAt a p)).sum =
      (((lanePositions a).filter (fun p => contains m p)).map (fun p => laneValAt a p)).sum := by
    exact perm_sum_eq (List.Perm.map (fun p => laneValAt a p) hperm)
  by_cases hn0 : o.n = 0
  · have hlen0 : ((lanePositions a).filter (fun p => contains m p)).length = 0 := by
      rw [← hperm.length_eq]
      simp [progVals, hn0]
    have hnil : (lanePositions a).filter (fun p => contains m p) = [] :=
      List.length_eq_zero_iff.mp hlen0
    simp [hnil, deletedSum, o, hn0, tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  · have hnpos : 0 < o.n := by omega
    have hlo_mem : o.lo ∈ progVals o := by
      unfold progVals
      rw [List.mem_map]
      exact ⟨0, by simpa using hnpos, by simp⟩
    have hlo_contains : contains o o.lo = true := (contains_iff_progVals o o.lo).mpr hlo_mem
    have hlo_pos : contains (positions a) o.lo = true := by
      dsimp [o] at hlo_contains
      exact (intersection_contains (positions a) m o.lo).mp hlo_contains |>.1
    have hlo_le : (a.start : Int) ≤ o.lo := by
      have hb := (contains_bounds (positions a) o.lo hlo_pos).1
      simpa [positions] using hb
    have hdlo : (a.stride : Int) ∣ o.lo - (a.start : Int) := by
      have hd := contains_dvd (positions a) o.lo hlo_pos
      simpa [positions] using hd
    have hmap : (progVals o).map (fun p => laneValAt a p) =
        (List.range o.n).map (fun k : Nat => firstAt a o + (k : Int) * slopeAt a o) := by
      unfold progVals
      rw [List.map_map]
      simp only [Function.comp_def]
      apply List.map_congr_left
      intro k hk
      by_cases hk0 : k = 0
      · subst k
        simp [firstAt_eq_laneValAt]
      · have hm0 : m.step ≠ 0 := by
          intro hmz
          have hnon : o.n ≤ 1 := by
            rcases m with ⟨mlo, mst, mn⟩
            simp at hmz
            subst mst
            by_cases hle : o.n ≤ 1
            · exact hle
            · have hn2 : 2 ≤ o.n := by omega
              have hmem0 : o.lo ∈ progVals o := by
                unfold progVals
                rw [List.mem_map]
                exact ⟨0, by simpa using (show 0 < o.n by omega), by simp⟩
              have hmem1 : o.lo + (o.step : Int) ∈ progVals o := by
                unfold progVals
                rw [List.mem_map]
                exact ⟨1, by simpa using (Nat.lt_of_succ_le hn2), by simp⟩
              have hc0 : contains o o.lo = true := (contains_iff_progVals o o.lo).mpr hmem0
              have hc1 : contains o (o.lo + (o.step : Int)) = true :=
                (contains_iff_progVals o (o.lo + (o.step : Int))).mpr hmem1
              have hm0c : contains ⟨mlo, 0, mn⟩ o.lo = true := by
                have hh := (intersection_contains (positions a) ⟨mlo, 0, mn⟩ o.lo).mp
                  (by simpa [o] using hc0)
                exact hh.2
              have hm1c : contains ⟨mlo, 0, mn⟩ (o.lo + (o.step : Int)) = true := by
                have hh := (intersection_contains (positions a) ⟨mlo, 0, mn⟩
                  (o.lo + (o.step : Int))).mp (by simpa [o] using hc1)
                exact hh.2
              have hv0 := (contains_const_iff mlo mn o.lo).mp hm0c
              have hv1 := (contains_const_iff mlo mn (o.lo + (o.step : Int))).mp hm1c
              have hopos : (0 : Int) < o.step := by exact_mod_cast hostep
              omega
          have hklt : k < o.n := List.mem_range.mp hk
          omega
        have hmpos : 0 < m.step := Nat.pos_of_ne_zero hm0
        have hd := (inter_step_dvd (positions a) m (by simpa [positions] using ha) hmpos
          (by dsimp [o] at hn0; exact hn0)).1
        have hstepdiv : a.stride ∣ o.step := by simpa [o, positions] using hd
        exact laneValAt_progress a ha o hdlo hstepdiv k
    rw [← hsumperm, hmap, sum_range_linear]
    rfl

private theorem lane_delete_sum (a : Lane) (ha : 0 < a.stride) (m : Progression) :
    (((lanePositions a).filter (fun p => !contains m p)).map (fun p => laneValAt a p)).sum =
      partialSum a a.n - deletedSum a m := by
  have htot : ((lanePositions a).map (fun p => laneValAt a p)).sum = partialSum a a.n := by
    unfold lanePositions
    rw [List.map_map]
    change ((List.range a.n).map (fun i : Nat => laneValAt a (a.start + i * a.stride : Nat))).sum = partialSum a a.n
    have hval : (List.range a.n).map (fun i : Nat => laneValAt a (a.start + i * a.stride : Nat)) =
        (List.range a.n).map (fun i : Nat => a.base + (i : Int) * a.step) := by
      apply List.map_congr_left
      intro i hi
      exact lanePositions_snd_value a ha i
    rw [hval]
    rw [partialSum_eq_sum]
  have hpart : ((lanePositions a).map (fun p => laneValAt a p)).sum =
      (((lanePositions a).filter (fun p => contains m p)).map (fun p => laneValAt a p)).sum +
      (((lanePositions a).filter (fun p => !contains m p)).map (fun p => laneValAt a p)).sum := by
    induction (lanePositions a) with
    | nil => simp
    | cons x xs ih =>
        by_cases h : contains m x <;> simp [h, ih] <;> omega
  rw [htot] at hpart
  rw [deletedSum_eq_sum a ha m]
  omega

private theorem lanePoints_filter_sum (a : Lane) (ha : 0 < a.stride) (m : Progression) :
    (((lanePoints a).filter (fun r => !contains m (r.1 : Int))).map Prod.snd).sum =
      (((lanePositions a).filter (fun p => !contains m p)).map (fun p => laneValAt a p)).sum := by
  unfold lanePoints lanePositions
  induction (List.range a.n) with
  | nil => simp
  | cons i is ih =>
      simp only [List.map_cons, List.filter_cons, List.sum_cons]
      have hval : laneValAt a (a.start + i * a.stride : Nat) = a.base + (i : Int) * a.step :=
        lanePositions_snd_value a ha i
      have hval' : laneValAt a ((a.start : Int) + (i : Int) * (a.stride : Int)) =
          a.base + (i : Int) * a.step := by
        simpa [Int.natCast_add, Int.natCast_mul] using hval
      by_cases h : contains m ((a.start : Int) + (i : Int) * (a.stride : Int)) <;>
        simp [h, ih, hval', Int.natCast_add, Int.natCast_mul]

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

private theorem sum_squares_eq (k : Nat) :
    ((List.range k).map (fun i : Nat => (i : Int) * (i : Int))).sum = (squares k : Int) := by
  induction k with
  | zero => simp [squares]
  | succ k ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih, squares_succ]
      simp [Int.natCast_add, Int.natCast_mul]

private theorem sum_range_quadratic (n : Nat) (x y dx dy : Int) :
    ((List.range n).map (fun k : Nat => (x + (k : Int) * dx) * (y + (k : Int) * dy))).sum =
      (n : Int) * x * y + tri n * (x * dy + y * dx) + squares n * dx * dy := by
  induction n with
  | zero => simp [tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append,
        List.sum_append, ih, tri_succ, squares_succ]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      simp [Int.natCast_add, Int.natCast_mul, Int.add_mul, Int.mul_add]
      grind

private theorem sum_map_zero_int (n : Nat) :
    ((List.range n).map (fun _ => (0 : Int))).sum = 0 := by
  induction n with
  | zero => simp
  | succ n ih => simp [List.range_succ, ih]

private theorem sum_eq_single_idx (n j : Nat) (hj : j < n) (f : Nat → Int)
    (hz : ∀ i, i < n → i ≠ j → f i = 0) :
    ((List.range n).map f).sum = f j := by
  induction n generalizing j f with
  | zero => omega
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
      by_cases hjn : j = n
      · subst j
        have hmap : (List.range n).map f = (List.range n).map (fun _ => (0 : Int)) := by
          apply List.map_congr_left
          intro i hi
          exact hz i (by
            have hi' := List.mem_range.mp hi
            omega) (by
            have hi' := List.mem_range.mp hi
            omega)
        rw [hmap, sum_map_zero_int]
        simp
      · have hjnlt : j < n := by omega
        have hfn : f n = 0 := hz n (by omega) (by omega)
        have ih' := ih j hjnlt f (fun i hi hij => hz i (by omega) hij)
        simp [hfn, ih']

private theorem lane_inner_sum (b : Lane) (hbs : 0 < b.stride) (p : Nat) (v : Int) :
    ((List.range b.n).map (fun i : Nat =>
      if p == b.start + i * b.stride then v * (b.base + b.step * (i : Int)) else 0)).sum =
      if contains (positions b) (p : Int) then v * laneValAt b (p : Int) else 0 := by
  by_cases hc : contains (positions b) (p : Int) = true
  · obtain ⟨j, hj, hpj⟩ := (positions_mem_iff_lane b (p : Int)).mp hc
    have hp_eq : p = b.start + j * b.stride := by exact_mod_cast hpj
    have hval : laneValAt b (p : Int) = b.base + (j : Int) * b.step := by
      rw [hpj]
      unfold laneValAt
      have hsub : ((b.start + j * b.stride : Nat) : Int) - (b.start : Int) =
          (j : Int) * (b.stride : Int) := by
        simp [Int.natCast_add, Int.natCast_mul]
        omega
      rw [hsub]
      have hbne : (b.stride : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hbs)
      rw [Int.ediv_eq_of_eq_mul_left hbne rfl]
    have huniq : ∀ i, i < b.n → (p == b.start + i * b.stride) = true → i = j := by
      intro i hi hbeq
      have heq : p = b.start + i * b.stride := Nat.eq_of_beq_eq_true hbeq
      rw [hp_eq] at heq
      have hmul : i * b.stride = j * b.stride := by omega
      exact Nat.eq_of_mul_eq_mul_right hbs hmul
    have hsum : ((List.range b.n).map (fun i : Nat =>
        if p == b.start + i * b.stride then v * (b.base + b.step * (i : Int)) else 0)).sum =
        v * (b.base + b.step * (j : Int)) := by
      have hbeq : (p == b.start + j * b.stride) = true := by
        rw [beq_iff_eq]
        exact hp_eq
      calc
        ((List.range b.n).map (fun i : Nat =>
            if p == b.start + i * b.stride then v * (b.base + b.step * (i : Int)) else 0)).sum =
            (if p == b.start + j * b.stride then v * (b.base + b.step * (j : Int)) else 0) := by
              apply sum_eq_single_idx b.n j hj (fun i : Nat =>
                if p == b.start + i * b.stride then v * (b.base + b.step * (i : Int)) else 0)
              intro i hi hij
              have hne : p ≠ b.start + i * b.stride := by
                intro hh
                exact hij (huniq i hi (by rw [hh]; simp))
              simp [beq_eq_false_iff_ne.mpr hne]
        _ = v * (b.base + b.step * (j : Int)) := by simp [hbeq]
    rw [hsum, hval, hc]
    simp [Int.mul_comm]
  · have hnot : ∀ i, i < b.n → (p == b.start + i * b.stride) = false := by
      intro i hi
      have hne : p ≠ b.start + i * b.stride := by
        intro hh
        have hmem : contains (positions b) (p : Int) = true := by
          rw [positions_mem_iff_lane]
          exact ⟨i, hi, by exact_mod_cast hh⟩
        exact hc hmem
      exact beq_eq_false_iff_ne.mpr hne
    have hmap : (List.range b.n).map (fun i : Nat =>
        if p == b.start + i * b.stride then v * (b.base + b.step * (i : Int)) else 0) =
        (List.range b.n).map (fun _ => (0 : Int)) := by
      apply List.map_congr_left
      intro i hi
      simp [hnot i (List.mem_range.mp hi)]
    rw [hmap, sum_map_zero_int]
    simp [hc]

private theorem sum_filter_eq_sum_ite {α : Type} (xs : List α) (p : α → Bool) (g : α → Int) :
    ((xs.filter p).map g).sum = (xs.map (fun x => if p x then g x else 0)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem lanePoints_inner_sum (a b : Lane) (ha : 0 < a.stride) (hbs : 0 < b.stride) :
    ((lanePoints a).map (fun r => ((List.range b.n).map (fun i : Nat =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)).sum =
    (((lanePositions a).filter (fun p => contains (positions b) p)).map
      (fun p => laneValAt a p * laneValAt b p)).sum := by
  unfold lanePoints lanePositions
  rw [sum_filter_eq_sum_ite]
  rw [List.map_map, List.map_map]
  apply congrArg List.sum
  apply List.map_congr_left
  intro i hi
  change ((List.range b.n).map (fun j : Nat =>
      if a.start + i * a.stride == b.start + j * b.stride then
        (a.base + (i : Int) * a.step) * (b.base + b.step * (j : Int)) else 0)).sum =
    if contains (positions b) ((a.start + i * a.stride : Nat) : Int) then
      laneValAt a ((a.start + i * a.stride : Nat) : Int) *
        laneValAt b ((a.start + i * a.stride : Nat) : Int) else 0
  have hi' : i < a.n := List.mem_range.mp hi
  have hpoint := lane_inner_sum b hbs (a.start + i * a.stride) (a.base + (i : Int) * a.step)
  rw [hpoint]
  have hval : laneValAt a ((a.start + i * a.stride : Nat) : Int) = a.base + (i : Int) * a.step :=
    lanePositions_snd_value a ha i
  rw [hval]

private theorem aligned_eq_sum (a b : Lane) (ha : 0 < a.stride) (hbs : 0 < b.stride) :
    aligned a b =
      (((lanePositions a).filter (fun p => contains (positions b) p)).map
        (fun p => laneValAt a p * laneValAt b p)).sum := by
  let o := intersection (positions a) (positions b)
  have hostep : 0 < o.step := by dsimp [o]; exact intersection_step_pos (positions a) (positions b)
  have hnodO : (progVals o).Nodup := progVals_nodup o hostep
  have hposnod : (lanePositions a).Nodup := by
    unfold lanePositions
    apply nodup_map_of_injective
    · intro x hx y hy hxy
      have hmul : x * a.stride = y * a.stride := by omega
      exact Nat.eq_of_mul_eq_mul_left ha (by simpa [Nat.mul_comm] using hmul)
    · exact List.nodup_range
  have hfilternod : ((lanePositions a).filter (fun p => contains (positions b) p)).Nodup :=
    nodup_filter (lanePositions a) hposnod (fun p => contains (positions b) p)
  have hperm : List.Perm (progVals o)
      ((lanePositions a).filter (fun p => contains (positions b) p)) := by
    apply perm_of_nodup_mem _ _ hnodO hfilternod
    intro p
    rw [← contains_iff_progVals o p]
    rw [List.mem_filter]
    rw [mem_lanePositions_iff]
    dsimp [o]
    rw [intersection_contains]
  have hsumperm : ((progVals o).map (fun p => laneValAt a p * laneValAt b p)).sum =
      (((lanePositions a).filter (fun p => contains (positions b) p)).map
        (fun p => laneValAt a p * laneValAt b p)).sum := by
    exact perm_sum_eq (List.Perm.map (fun p => laneValAt a p * laneValAt b p) hperm)
  by_cases hn0 : o.n = 0
  · have hlen0 : ((lanePositions a).filter (fun p => contains (positions b) p)).length = 0 := by
      rw [← hperm.length_eq]
      simp [progVals, hn0]
    have hnil : (lanePositions a).filter (fun p => contains (positions b) p) = [] :=
      List.length_eq_zero_iff.mp hlen0
    simp [hnil, aligned, o, hn0, tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  · have hnpos : 0 < o.n := by omega
    have hlo_mem : o.lo ∈ progVals o := by
      unfold progVals
      rw [List.mem_map]
      exact ⟨0, by simpa using hnpos, by simp⟩
    have hlo_contains : contains o o.lo = true := (contains_iff_progVals o o.lo).mpr hlo_mem
    have hlo_common : contains (positions a) o.lo = true ∧ contains (positions b) o.lo = true := by
      dsimp [o] at hlo_contains
      exact (intersection_contains (positions a) (positions b) o.lo).mp hlo_contains
    have hlo_a : (a.start : Int) ≤ o.lo := by
      have hb := (contains_bounds (positions a) o.lo hlo_common.1).1
      simpa [positions] using hb
    have hdlo_a : (a.stride : Int) ∣ o.lo - (a.start : Int) := by
      have hv := contains_dvd (positions a) o.lo hlo_common.1
      simpa [positions] using hv
    have hdlo_b : (b.stride : Int) ∣ o.lo - (b.start : Int) := by
      have hv := contains_dvd (positions b) o.lo hlo_common.2
      simpa [positions] using hv
    have hd := inter_step_dvd (positions a) (positions b)
      (by simpa [positions] using ha) (by simpa [positions] using hbs)
      (by simpa [o] using hn0)
    have hdiv_a : a.stride ∣ o.step := by simpa [o, positions] using hd.1
    have hdiv_b : b.stride ∣ o.step := by simpa [o, positions] using hd.2
    have hmap : (progVals o).map (fun p => laneValAt a p * laneValAt b p) =
        (List.range o.n).map (fun k : Nat =>
          (firstAt a o + (k : Int) * slopeAt a o) * (firstAt b o + (k : Int) * slopeAt b o)) := by
      unfold progVals
      rw [List.map_map]
      simp only [Function.comp_def]
      apply List.map_congr_left
      intro k hk
      change laneValAt a (o.lo + (k : Int) * (o.step : Int)) *
          laneValAt b (o.lo + (k : Int) * (o.step : Int)) =
        (firstAt a o + (k : Int) * slopeAt a o) *
          (firstAt b o + (k : Int) * slopeAt b o)
      rw [laneValAt_progress a ha o hdlo_a hdiv_a k,
          laneValAt_progress b hbs o hdlo_b hdiv_b k]
    rw [← hsumperm, hmap, sum_range_quadratic]
    unfold aligned
    dsimp [o]

private theorem lane_aligned (a b : Lane) (ha : 0 < a.stride) (hbs : 0 < b.stride) :
    ((lanePoints a).map (fun r => ((List.range b.n).map (fun i : Nat =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)).sum =
      aligned a b := by
  rw [lanePoints_inner_sum a b ha hbs]
  exact (aligned_eq_sum a b ha hbs).symm

private theorem sum_natCast_eq {α : Type} (l : List α) (g : α → Nat) :
    ((l.map (fun x => (g x : Int))).sum) = (((l.map g).sum : Nat) : Int) := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.natCast_add]

private theorem lane_residue_join_int (a b : Lane) :
    ((lanePoints a).map (fun r => ((List.range b.n).countP
      (fun i : Nat => r.2 == b.base + b.step * (i : Int)) : Int))).sum =
      (joinCount a b : Int) := by
  rw [sum_natCast_eq]
  exact congrArg Int.ofNat (lane_residue_join a b)

private theorem periodicPatch_lane_stride_pos (c : Config) {a : Lane}
    (ha : a ∈ PeriodicPatch.layout.lanes c) : 0 < a.stride := by
  unfold PeriodicPatch.layout striped at ha
  rcases List.mem_map.mp ha with ⟨r, hr, rfl⟩
  dsimp [lane, width]
  omega

theorem sound_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.PeriodicPatch.ResidueJoin.Goal := by
  intro c b
  let F : Nat × Int → Int := fun r => ((List.range b.n).countP
    (fun i : Nat => r.2 == b.base + b.step * (i : Int)) : Int)
  have hInt : ((rows PeriodicPatch.layout c).map F).sum =
      ((PeriodicPatch.layout.lanes c).map (fun a => (joinCount a b : Int))).sum := by
    rw [periodicPatch_sum c F]
    rw [flatMap_map_sum]
    apply congrArg List.sum
    apply List.map_congr_left
    intro a ha
    exact lane_residue_join_int a b
  have hleft : ((rows PeriodicPatch.layout c).map F).sum =
      (((rows PeriodicPatch.layout c).map (fun r => (List.range b.n).countP
        (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum : Int) := by
    dsimp [F]
    exact sum_natCast_eq _ _
  have hright : ((PeriodicPatch.layout.lanes c).map (fun a => (joinCount a b : Int))).sum =
      (((PeriodicPatch.layout.lanes c).map (fun a => joinCount a b)).sum : Int) := by
    exact sum_natCast_eq _ _
  have hcast : (((rows PeriodicPatch.layout c).map (fun r => (List.range b.n).countP
        (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum : Int) =
      (((PeriodicPatch.layout.lanes c).map (fun a => joinCount a b)).sum : Int) := by
    rw [← hleft, ← hright]
    exact hInt
  exact_mod_cast hcast


private theorem lanePoints_keep_sum (a : Lane) (ha : 0 < a.stride) (m : Progression) :
    ((lanePoints a).map (fun r => if !contains m (r.1 : Int) then r.2 else 0)).sum =
      partialSum a a.n - deletedSum a m := by
  rw [← sum_filter_eq_sum_ite]
  rw [lanePoints_filter_sum a ha m]
  exact lane_delete_sum a ha m

theorem sound_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.PeriodicPatch.DeleteProgression.Goal := by
  intro c mask
  let F : Nat × Int → Int := fun r => if !contains mask (r.1 : Int) then r.2 else 0
  have hfilter : (((rows PeriodicPatch.layout c).filter
        (fun r => !contains mask (r.1 : Int))).map Prod.snd).sum =
      ((rows PeriodicPatch.layout c).map F).sum := by
    dsimp [F]
    exact sum_filter_eq_sum_ite (rows PeriodicPatch.layout c)
      (fun r => !contains mask (r.1 : Int)) Prod.snd
  rw [hfilter, periodicPatch_sum c F]
  rw [flatMap_map_sum]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  dsimp [F]
  exact lanePoints_keep_sum a (periodicPatch_lane_stride_pos c ha) mask


theorem sound_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.PeriodicPatch.AlignedProduct.Goal := by
  intro c b hbs
  let F : Nat × Int → Int := fun r => ((List.range b.n).map (fun i : Nat =>
    if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum
  rw [periodicPatch_sum c F]
  rw [flatMap_map_sum]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  exact lane_aligned a b (periodicPatch_lane_stride_pos c ha) hbs

end TinylambOpt.ResearchProofs.Atlas.EncodedPages.PeriodicPatchProgressions

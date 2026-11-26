import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `AlternatingSign.ResidueJoin` — `∀ c (b : Lane), ((rows layout c).map (fun r =>
  (List.range b.n).countP (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum =
  ((layout.lanes c).map (joinCount · b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.AlternatingSignResidueJoin

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.AlternatingSign

/-! ## Progression algebra, CRT intersection and counting common values
(locally copied from the verified sibling proof files `PeriodicPatch_Remaining`
and `SegmentedSlope_Remaining`). -/

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

private theorem contains_span_iff (first step high v : Int) (hs : 0 < step) :
    contains ⟨first, step.toNat, if first ≤ high then ((high - first) / step).toNat + 1 else 0⟩ v = true ↔
      first ≤ v ∧ v ≤ high ∧ step ∣ v - first := by
  change (let n := if first ≤ high then ((high - first) / step).toNat + 1 else 0;
    contains ⟨first, step.toNat, n⟩ v = true ↔ first ≤ v ∧ v ≤ high ∧ step ∣ v - first)
  exact span_contains_iff first step high v hs

private theorem contains_span_nat_iff (first : Int) (step : Nat) (high v : Int)
    (hs : 0 < (step : Int)) :
    contains ⟨first, step, if first ≤ high then ((high - first) / (step : Int)).toNat + 1 else 0⟩ v = true ↔
      first ≤ v ∧ v ≤ high ∧ (step : Int) ∣ v - first := by
  have h := contains_span_iff first (step : Int) high v hs
  have hcast' : (((step : Int).toNat : Nat) : Int) = (step : Int) := by
    exact Int.toNat_of_nonneg (by exact_mod_cast (Nat.zero_le step))
  have hcast : (step : Int).toNat = step := by exact_mod_cast hcast' 
  simpa [hcast] using h

private theorem lcm_dvd_of_dvd (a b : Nat) (ha : 0 < a) (hb : 0 < b) (x : Int)
    (hxa : (a : Int) ∣ x) (hxb : (b : Int) ∣ x) :
    (((a / a.gcd b) * b : Nat) : Int) ∣ x := by
  let g := a.gcd b
  let A := a / g
  let B := b / g
  have hg : 0 < g := Nat.gcd_pos_of_pos_left b ha
  have ha_eq_nat : a = g * A := by
    simpa [g, A] using (Nat.mul_div_cancel' (Nat.gcd_dvd_left a b)).symm
  have hb_eq_nat : b = g * B := by
    simpa [g, B] using (Nat.mul_div_cancel' (Nat.gcd_dvd_right a b)).symm
  have ha_eq : (a : Int) = (g : Int) * (A : Int) := by exact_mod_cast ha_eq_nat
  have hb_eq : (b : Int) = (g : Int) * (B : Int) := by exact_mod_cast hb_eq_nat
  obtain ⟨p, hp⟩ := hxa
  have hx_form : x = (g : Int) * ((A : Int) * p) := by
    rw [hp, ha_eq]
    ac_rfl
  have hdiv0 : (g : Int) * (B : Int) ∣ (g : Int) * ((A : Int) * p) := by
    rw [← hx_form, ← hb_eq]
    exact hxb
  have hB_dvd : (B : Int) ∣ (A : Int) * p :=
    Int.dvd_of_mul_dvd_mul_left (by exact_mod_cast (show g ≠ 0 by omega)) hdiv0
  obtain ⟨t, ht⟩ := hB_dvd
  have hcop : A.gcd B = 1 := by
    simpa [A, B, g] using reduced_coprime a b ha hb
  have hbez := bezout_spec A B
  rw [hcop] at hbez
  simp only [Int.ofNat_one] at hbez
  have hB_dvd_p : (B : Int) ∣ p := by
    refine ⟨(bezout A B).1 * t + (bezout A B).2 * p, ?_⟩
    calc
      p = ((A : Int) * (bezout A B).1 + (B : Int) * (bezout A B).2) * p := by rw [← hbez]; simp
      _ = (bezout A B).1 * ((A : Int) * p) + (B : Int) * ((bezout A B).2 * p) := by
        rw [Int.add_mul]
        ac_rfl
      _ = (bezout A B).1 * ((B : Int) * t) + (B : Int) * ((bezout A B).2 * p) := by rw [ht]
      _ = (B : Int) * ((bezout A B).1 * t) + (B : Int) * ((bezout A B).2 * p) := by ac_rfl
      _ = (B : Int) * ((bezout A B).1 * t + (bezout A B).2 * p) := by rw [Int.mul_add]
  obtain ⟨q, hq⟩ := hB_dvd_p
  refine ⟨q, ?_⟩
  rw [hx_form, hq]
  have hper : ((a / a.gcd b) * b : Nat) = g * (A * B) := by
    dsimp [A, B, g]
    have hb' : b = a.gcd b * (b / a.gcd b) := by
      exact (Nat.mul_div_cancel' (Nat.gcd_dvd_right a b)).symm
    calc
      (a / a.gcd b) * b = (a / a.gcd b) * (a.gcd b * (b / a.gcd b)) :=
        congrArg (fun t : Nat => (a / a.gcd b) * t) hb' 
      _ = a.gcd b * ((a / a.gcd b) * (b / a.gcd b)) := by ac_rfl
  rw [hper]
  push_cast
  ac_rfl

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

private theorem contains_one_iff (lo : Int) (v : Int) :
    contains ⟨lo, 1, 1⟩ v = true ↔ v = lo := by
  simp [contains, lastVal]
  omega

private theorem contains_bounds (a : Progression) (v : Int) (h : contains a v = true) :
    a.lo ≤ v ∧ v ≤ lastVal a := by
  have hm := (contains_iff_mem a v).mp h
  obtain ⟨k, hk, hval⟩ := hm
  unfold progVal at hval
  have hkint : (k : Int) ≤ ((a.n - 1 : Nat) : Int) := by exact_mod_cast (by omega)
  have hnon : 0 ≤ (a.step : Int) := by exact_mod_cast (Nat.zero_le a.step)
  have hmul : (k : Int) * (a.step : Int) ≤ ((a.n - 1 : Nat) : Int) * (a.step : Int) :=
    Int.mul_le_mul_of_nonneg_right hkint hnon
  unfold lastVal
  constructor
  · rw [hval]
    have hknon : (0 : Int) ≤ k := by exact_mod_cast (Nat.zero_le k)
    have hmnon : (0 : Int) ≤ (k : Int) * (a.step : Int) := Int.mul_nonneg hknon hnon
    omega
  · rw [hval]
    exact Int.add_le_add_left hmul a.lo

private theorem contains_dvd (a : Progression) (v : Int) (h : contains a v = true) :
    (a.step : Int) ∣ v - a.lo := by
  obtain ⟨k, _, hval⟩ := (contains_iff_mem a v).mp h
  unfold progVal at hval
  refine ⟨(k : Int), ?_⟩
  rw [Int.mul_comm]
  omega

private theorem common_gap_dvd (a b : Progression) (v : Int)
    (ha : contains a v = true) (hb : contains b v = true) :
    (a.step.gcd b.step : Int) ∣ b.lo - a.lo := by
  obtain ⟨i, _, hi⟩ := (contains_iff_mem a v).mp ha
  obtain ⟨j, _, hj⟩ := (contains_iff_mem b v).mp hb
  unfold progVal at hi hj
  rw [Int.mul_comm (i : Int) (a.step : Int)] at hi
  rw [Int.mul_comm (j : Int) (b.step : Int)] at hj
  obtain ⟨u, hu⟩ := Int.gcd_dvd_right a.step b.step
  obtain ⟨w, hw⟩ := Int.gcd_dvd_left a.step b.step
  have h1 : (a.step.gcd b.step : Int) ∣ (b.step : Int) * (j : Int) := by
    refine ⟨u * (j : Int), ?_⟩
    rw [hu]
    ac_rfl
  have h2 : (a.step.gcd b.step : Int) ∣ (a.step : Int) * (i : Int) := by
    refine ⟨w * (i : Int), ?_⟩
    rw [hw]
    ac_rfl
  have heq : (b.step : Int) * (j : Int) - (a.step : Int) * (i : Int) = a.lo - b.lo := by omega
  have hdiv_neg : (a.step.gcd b.step : Int) ∣ a.lo - b.lo := by
    rw [← heq]
    exact Int.dvd_sub h1 h2
  obtain ⟨k, hk⟩ := hdiv_neg
  refine ⟨-k, ?_⟩
  rw [Int.mul_neg]
  omega

private theorem intersection_contains (a b : Progression) (v : Int) :
    contains (intersection a b) v = true ↔ contains a v = true ∧ contains b v = true := by
  by_cases han : a.n = 0
  · rcases a with ⟨alo, ast, an⟩
    simp at han
    subst an
    simp [intersection, contains]
  · by_cases hbn : b.n = 0
    · rcases b with ⟨blo, bst, bn⟩
      simp at hbn
      subst bn
      simp [intersection, contains]
    · have hanp : 0 < a.n := by omega
      have hbnp : 0 < b.n := by omega
      by_cases has : a.step = 0
      · rcases a with ⟨alo, ast, an⟩
        simp at has
        subst ast
        simp at han hanp
        have hieq : intersection ⟨alo, 0, an⟩ b =
            ⟨alo, 1, if contains b alo = true then 1 else 0⟩ := by
          simp [intersection, han, hbn]
        rw [hieq]
        constructor
        · intro h
          have hm := (contains_iff_mem ⟨alo, 1, if contains b alo = true then 1 else 0⟩ v).mp h
          rcases hm with ⟨k, hk, hv⟩
          unfold progVal at hv
          by_cases hc : contains b alo = true
          · have hk0 : k = 0 := by simp [hc] at hk; omega
            subst k
            simp at hv
            constructor
            · exact (contains_iff_mem ⟨alo, 0, an⟩ v).mpr ⟨0, hanp, by simp [progVal, hv]⟩
            · rw [hv]
              exact hc
          · simp [hc] at hk
        · rintro ⟨ha, hb⟩
          have hma := (contains_iff_mem ⟨alo, 0, an⟩ v).mp ha
          rcases hma with ⟨k, hk, hv⟩
          unfold progVal at hv
          simp at hv
          have hc : contains b alo = true := by rw [← hv]; exact hb
          simp [hc]
          exact (contains_iff_mem ⟨alo, 1, 1⟩ v).mpr ⟨0, by simp [hc], by simp [progVal, hv]⟩
      · by_cases hbs : b.step = 0
        · rcases b with ⟨blo, bst, bn⟩
          simp at hbs
          subst bst
          simp at hbn hbnp
          have hieq : intersection a ⟨blo, 0, bn⟩ =
              ⟨blo, 1, if contains a blo = true then 1 else 0⟩ := by
            simp [intersection, han, hbn, has]
          rw [hieq]
          constructor
          · intro h
            have hm := (contains_iff_mem ⟨blo, 1, if contains a blo = true then 1 else 0⟩ v).mp h
            rcases hm with ⟨k, hk, hv⟩
            unfold progVal at hv
            by_cases hc : contains a blo = true
            · have hk0 : k = 0 := by simp [hc] at hk; omega
              subst k
              simp at hv
              constructor
              · rw [hv]
                exact hc
              · exact (contains_iff_mem ⟨blo, 0, bn⟩ v).mpr ⟨0, hbnp, by simp [progVal, hv]⟩
            · simp [hc] at hk
          · rintro ⟨ha, hb⟩
            have hmb := (contains_iff_mem ⟨blo, 0, bn⟩ v).mp hb
            rcases hmb with ⟨k, hk, hv⟩
            unfold progVal at hv
            simp at hv
            have hc : contains a blo = true := by rw [← hv]; exact ha
            simp [hc]
            exact (contains_iff_mem ⟨blo, 1, 1⟩ v).mpr ⟨0, by simp [hc], by simp [progVal, hv]⟩
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
          have hperpos : 0 < period := by
            dsimp [period]
            apply Nat.mul_pos
            · simpa [g] using (Nat.div_gcd_pos_of_pos_left b.step hasp)
            · exact hbsp
          have hp : (0 : Int) < period := by exact_mod_cast hperpos
          have hstep_a : a.step = g * (a.step / g) := by
            simpa [g] using (Nat.mul_div_cancel' (Nat.gcd_dvd_left a.step b.step)).symm
          have hstep_b : b.step = g * (b.step / g) := by
            simpa [g] using (Nat.mul_div_cancel' (Nat.gcd_dvd_right a.step b.step)).symm
          have hper_a : period = a.step * (b.step / g) := by
            calc
              period = (a.step / g) * b.step := rfl
              _ = (a.step / g) * (g * (b.step / g)) :=
                congrArg (fun t : Nat => (a.step / g) * t) hstep_b
              _ = (g * (a.step / g)) * (b.step / g) := by ac_rfl
              _ = a.step * (b.step / g) :=
                congrArg (fun t : Nat => t * (b.step / g)) hstep_a.symm
          have hper_b : period = b.step * (a.step / g) := by
            calc
              period = (a.step / g) * b.step := rfl
              _ = (a.step / g) * (g * (b.step / g)) :=
                congrArg (fun t : Nat => (a.step / g) * t) hstep_b
              _ = (g * (b.step / g)) * (a.step / g) := by ac_rfl
              _ = b.step * (a.step / g) :=
                congrArg (fun t : Nat => t * (a.step / g)) hstep_b.symm
          have hgapdef : gap = b.lo - a.lo := rfl
          by_cases hgapmod : gap % (g : Int) = 0
          · have hgap : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgapmod
            have hroot := root_dvd a.step b.step hasp hbsp a.lo b.lo gap hgapdef hgap
            have hfirst : first = low + (root - low) % (period : Int) := rfl
            have hfroot := first_root_dvd low root first period hp hfirst
            have hremnon : 0 ≤ (root - low) % (period : Int) := Int.emod_nonneg _ (by omega)
            have hflow : low ≤ first := by dsimp [first]; omega
            have hpa : (a.step : Int) ∣ (period : Int) := by
              refine ⟨((b.step / g : Nat) : Int), ?_⟩
              exact_mod_cast hper_a
            have hpb : (b.step : Int) ∣ (period : Int) := by
              refine ⟨((a.step / g : Nat) : Int), ?_⟩
              exact_mod_cast hper_b
            have hgapmod' : (b.lo - a.lo) % (g : Int) = 0 := by simpa [gap] using hgapmod
            have hinter : intersection a b =
                ⟨first, period, if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ := by
              simp [intersection, han, hbn, has, hbs, hgapmod', g, gap, first, period, root, unit, low, high]
            rw [hinter]
            rw [contains_span_nat_iff first period high v hp]
            constructor
            · rintro ⟨hfirstx, hxhigh, hxfirst⟩
              have hxroot : (period : Int) ∣ v - root := by
                have heq : v - root = (v - first) + (first - root) := by omega
                rw [heq]
                exact Int.dvd_add hxfirst hfroot
              have hxalo0 : (a.step : Int) ∣ v - root := Int.dvd_trans hpa hxroot
              have hxalo : (a.step : Int) ∣ v - a.lo := by
                have ht := Int.dvd_add hxalo0 hroot.1
                have heq : (v - root) + (root - a.lo) = v - a.lo := by omega
                rw [heq] at ht
                exact ht
              have hxblo0 : (b.step : Int) ∣ v - root := Int.dvd_trans hpb hxroot
              have hxblo : (b.step : Int) ∣ v - b.lo := by
                have ht := Int.dvd_add hxblo0 hroot.2
                have heq : (v - root) + (root - b.lo) = v - b.lo := by omega
                rw [heq] at ht
                exact ht
              have hxlow : low ≤ v := by omega
              have hab := contains_bounds a v
              have hbb := contains_bounds b v
              constructor
              · exact mem_of_bounds_dvd a v hanp hasp (by omega) (by omega) hxalo
              · exact mem_of_bounds_dvd b v hbnp hbsp (by omega) (by omega) hxblo
            · rintro ⟨ha, hb⟩
              have hab := contains_bounds a v ha
              have hbb := contains_bounds b v hb
              have hxa0 := contains_dvd a v ha
              have hxb0 := contains_dvd b v hb
              have hxroota : (a.step : Int) ∣ v - root := by
                have ht := Int.dvd_sub hxa0 hroot.1
                have heq : (v - a.lo) - (root - a.lo) = v - root := by omega
                rw [heq] at ht
                exact ht
              have hxrootb : (b.step : Int) ∣ v - root := by
                have ht := Int.dvd_sub hxb0 hroot.2
                have heq : (v - b.lo) - (root - b.lo) = v - root := by omega
                rw [heq] at ht
                exact ht
              have hxroot : (period : Int) ∣ v - root := by
                simpa [period, g] using
                  (lcm_dvd_of_dvd a.step b.step hasp hbsp (v - root) hxroota hxrootb)
              have hxfirst : (period : Int) ∣ v - first := by
                have ht := Int.dvd_sub hxroot hfroot
                have heq : (v - root) - (first - root) = v - first := by omega
                rw [heq] at ht
                exact ht
              have hlow : low ≤ v := by
                have ha_lo := (contains_bounds a v ha).1
                have hb_lo := (contains_bounds b v hb).1
                dsimp [low]
                omega
              have hfirstx : first ≤ v :=
                first_le_of_congruence low root first v (period : Int) hp (by rfl) hlow hxroot
              exact ⟨hfirstx, by omega, hxfirst⟩
          · have hgapne : gap % (g : Int) ≠ 0 := by simpa using hgapmod
            have hgapmod' : (b.lo - a.lo) % (g : Int) ≠ 0 := by simpa [gap] using hgapmod
            have hinter : intersection a b = ⟨0, 1, 0⟩ := by
              simp [intersection, han, hbn, has, hbs, hgapmod', g, gap]
            rw [hinter]
            constructor
            · intro h
              exfalso
              simpa [contains] using h
            · intro hpair
              exfalso
              have hg := common_gap_dvd a b v hpair.1 hpair.2
              exact hgapne (Int.emod_eq_zero_of_dvd hg)



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
      hvalsnodup.filter (fun v => contains q v)
    have hperm : List.Perm ((progVals p).filter (fun v => contains q v)) (progVals r) :=
      perm_of_nodup_mem _ _ hfilternodup hrnodup (by
        intro v
        rw [List.mem_filter]
        rw [← contains_iff_progVals p v]
        rw [← contains_iff_progVals r v]
        simpa [r] using (intersection_contains p q v).symm)
    change (progVals p).countP (fun v => contains q v) = r.n * (if p.step == 0 then p.n else 1)
    rw [List.countP_eq_length_filter, hperm.length_eq]
    simp [progVals, hps0]

/-! ## Lane values versus progression values -/

private theorem countP_eq_sum_nat {α : Type} (l : List α) (p : α → Bool) :
    l.countP p = (l.map (fun x => if p x then 1 else 0)).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih] <;> omega

private theorem sum_nat_mul_indicator (l : List Int) (c : Nat) (p : Int → Bool) :
    (l.map (fun v => c * (if p v then 1 else 0))).sum = c * l.countP p := by
  rw [countP_eq_sum_nat]
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x <;> simp [h, ih, Nat.mul_add]

private def laneVals (a : Lane) : List Int :=
  (List.range a.n).map (fun q : Nat => a.base + (q : Int) * a.step)

private theorem reverse_range_perm (n : Nat) :
    ((List.range n).map (fun q : Nat => n - 1 - q)).Perm (List.range n) := by
  have hnod : ((List.range n).map (fun q : Nat => n - 1 - q)).Nodup := by
    apply nodup_map_of_injective
    · intro a ha b hb hab
      have ha' : a < n := List.mem_range.mp ha
      have hb' : b < n := List.mem_range.mp hb
      omega
    · exact List.nodup_range
  exact (List.perm_ext_iff_of_nodup hnod List.nodup_range).mpr (by
    intro x
    simp only [List.mem_map, List.mem_range]
    constructor
    · rintro ⟨q, hq, rfl⟩
      omega
    · intro hx
      refine ⟨n - 1 - x, by omega, ?_⟩
      have hxle : x ≤ n - 1 := by omega
      exact Nat.sub_sub_self hxle)

private theorem laneVals_perm_progVals (a : Lane) :
    (laneVals a).Perm (progVals (values a)) := by
  unfold laneVals progVals
  by_cases hzero : a.step = 0
  · have hv : values a = ⟨a.base, 0, a.n⟩ := by simp [values, hzero]
    rw [hv]
    have heq : (List.range a.n).map (fun q : Nat => a.base + (q : Int) * a.step) =
        (List.range a.n).map (fun i : Nat => a.base + (i : Int) * (0 : Int)) := by
      apply List.map_congr_left
      intro q hq
      simp [hzero]
    rw [heq]
    simp
  · by_cases hpos : 0 < a.step
    · have hv : values a = ⟨a.base, a.step.natAbs, a.n⟩ := by
        unfold values
        simp [hpos, (show ¬ a.step < 0 by omega)]
      have hnat : ((a.step.natAbs : Nat) : Int) = a.step := Int.ofNat_natAbs_of_nonneg (by omega)
      rw [hv, hnat]
    · have hneg : a.step < 0 := by omega
      have hv : values a = ⟨a.base + ((a.n - 1 : Nat) : Int) * a.step, a.step.natAbs, a.n⟩ := by
        unfold values
        simp [hneg]
      have hnat : ((a.step.natAbs : Nat) : Int) = -a.step := by
        rw [← Int.natAbs_neg a.step]
        exact Int.ofNat_natAbs_of_nonneg (by omega : (0 : Int) ≤ -a.step)
      rw [hv, hnat]
      let f : Nat → Int := fun q => a.base + ((a.n - 1 - q : Nat) : Int) * a.step
      have hleft : (List.range a.n).map (fun q : Nat => a.base + (q : Int) * a.step) =
          ((List.range a.n).map (fun q : Nat => a.n - 1 - q)).map f := by
        rw [List.map_map]
        apply List.map_congr_left
        intro q hq
        dsimp [f]
        have hq' : q < a.n := List.mem_range.mp hq
        have hqle : q ≤ a.n - 1 := by omega
        have hsub : a.n - 1 - (a.n - 1 - q) = q := Nat.sub_sub_self hqle
        rw [hsub]
      have hright : (List.range a.n).map f =
          (List.range a.n).map (fun i : Nat => a.base + ((a.n - 1 : Nat) : Int) * a.step + (i : Int) * (-a.step)) := by
        apply List.map_congr_left
        intro i hi
        dsimp [f]
        have hi' : i < a.n := List.mem_range.mp hi
        have hile : i ≤ a.n - 1 := by omega
        rw [Int.ofNat_sub hile]
        grind
      change (List.map (fun q : Nat => a.base + (q : Int) * a.step) (List.range a.n)).Perm
        (List.map (fun i : Nat => a.base + ((a.n - 1 : Nat) : Int) * a.step + (i : Int) * (-a.step)) (List.range a.n))
      rw [hleft]
      rw [← hright]
      exact (reverse_range_perm a.n).map f

private theorem countP_contains_lane (a : Lane) (q : Progression) :
    (laneVals a).countP (fun v => contains q v) =
      (intersection (values a) q).n * (if a.step == 0 then a.n else 1) := by
  rw [(laneVals_perm_progVals a).countP_eq (fun v => contains q v)]
  have hvstep : ((values a).step == 0) = (a.step == 0) := by
    unfold values
    by_cases h0 : a.step = 0
    · simp [h0]
    · have hn : a.step.natAbs ≠ 0 := by
        intro hn0
        exact h0 (Int.natAbs_eq_zero.mp hn0)
      rw [beq_eq_false_iff_ne.mpr hn, beq_eq_false_iff_ne.mpr h0]
  have hvnum : (values a).n = a.n := rfl
  simpa [hvstep, hvnum] using countP_contains_prog (values a) q

private theorem countP_eq_laneValue (b : Lane) (hbs : b.step ≠ 0) (v : Int) :
    (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)) =
      if contains (values b) v then 1 else 0 := by
  by_cases hc : contains (values b) v = true
  · have hmem : v ∈ progVals (values b) := (contains_iff_progVals (values b) v).mp hc
    have hperm := laneVals_perm_progVals b
    have hmem' : v ∈ laneVals b := hperm.mem_iff.mpr hmem
    unfold laneVals at hmem'
    rcases List.mem_map.mp hmem' with ⟨i, hi, hival⟩
    have hnod : (laneVals b).Nodup := by
      rw [(laneVals_perm_progVals b).nodup_iff]
      apply progVals_nodup (values b)
      dsimp [values]
      exact Int.natAbs_pos.mpr hbs
    have hfperm : ((List.range b.n).filter (fun j : Nat => v == b.base + b.step * (j : Int))).Perm [i] := by
      have hfnod : ((List.range b.n).filter (fun j : Nat => v == b.base + b.step * (j : Int))).Nodup :=
        List.nodup_range.filter _
      exact (List.perm_ext_iff_of_nodup hfnod (by simp)).mpr (by
        intro j
        simp only [List.mem_filter, List.mem_singleton]
        constructor
        · rintro ⟨hj, hbeq⟩
          have hjv : v = b.base + b.step * (j : Int) := (beq_iff_eq.mp hbeq)
          have hiv : v = b.base + b.step * (i : Int) := by simpa [Int.mul_comm] using hival.symm
          have hi_eq : v = b.base + (i : Int) * b.step := by simpa [Int.mul_comm] using hival.symm
          have hj_eq : v = b.base + b.step * (j : Int) := beq_iff_eq.mp hbeq
          have hcancel : (i : Int) * b.step = b.step * (j : Int) := by
            calc
              (i : Int) * b.step = b.base + (i : Int) * b.step - b.base := by omega
              _ = v - b.base := by rw [hi_eq]
              _ = b.base + b.step * (j : Int) - b.base := by rw [hj_eq]
              _ = b.step * (j : Int) := by omega
          have hmul : (j : Int) * b.step = (i : Int) * b.step := by simpa [Int.mul_comm] using hcancel.symm
          have hstep : b.step ≠ 0 := hbs
          exact_mod_cast (Int.mul_eq_mul_right_iff hstep).mp hmul
        · intro hji
          subst j
          exact ⟨hi, by simpa [Int.mul_comm] using hival.symm⟩)
    rw [List.countP_eq_length_filter, hfperm.length_eq]
    simp [hc]
  · have hzero : (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)) = 0 := by
      rw [List.countP_eq_zero]
      intro i hi
      intro hbeq
      have hv' : v = b.base + b.step * (i : Int) := beq_iff_eq.mp hbeq
      apply hc
      apply (contains_iff_progVals (values b) v).mpr
      rw [← (laneVals_perm_progVals b).mem_iff]
      unfold laneVals
      exact List.mem_map.mpr ⟨i, hi, by simpa [Int.mul_comm] using hv'.symm⟩
    simp [hzero, hc]

private theorem countP_eq_laneValue_const (b : Lane) (hbz : b.step = 0) (v : Int) :
    (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)) =
      if contains (values b) v then b.n else 0 := by
  by_cases hv : v == b.base
  · have hv' : v = b.base := beq_iff_eq.mp hv
    subst v
    have hc : contains (values b) b.base = true ↔ 0 < b.n := by
      rw [contains_iff_progVals]
      unfold progVals values
      simp [hbz]
      constructor
      · rintro ⟨k, hk⟩
        omega
      · intro hk
        exact ⟨0, hk⟩
    by_cases hn : 0 < b.n
    · have hc' : contains (values b) b.base = true := hc.mpr hn
      simp [hbz, hv, hc']
    · have hc' : contains (values b) b.base ≠ true := by simpa [hc] using hn
      have hbzero : b.n = 0 := by omega
      simp [hbz, hv, hc', hbzero]
  · have hc' : contains (values b) v ≠ true := by
      intro hh
      have hmem := (contains_iff_progVals (values b) v).mp hh
      have hmem' : v ∈ laneVals b := (laneVals_perm_progVals b).mem_iff.mpr hmem
      unfold laneVals at hmem'
      rcases List.mem_map.mp hmem' with ⟨i, hi, hival⟩
      have : v = b.base := by simpa [hbz] using hival.symm
      exact hv (by rw [this]; simp)
    simp [hbz, hv, hc']

private theorem lane_join_count (a b : Lane) :
    ((laneVals a).map (fun v =>
      (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)))).sum =
      joinCount a b := by
  have hcnt := countP_contains_lane a (values b)
  unfold joinCount
  by_cases hbz : b.step = 0
  · have hrep : ∀ v : Int,
        (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)) =
          b.n * (if contains (values b) v then 1 else 0) := by
      intro v
      rw [countP_eq_laneValue_const b hbz v]
      by_cases hc : contains (values b) v <;> simp [hc]
    rw [show ((laneVals a).map (fun v =>
      (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)))).sum =
      ((laneVals a).map (fun v => b.n * (if contains (values b) v then 1 else 0))).sum by
      apply congrArg List.sum
      apply List.map_congr_left
      intro v hv
      exact hrep v]
    rw [sum_nat_mul_indicator]
    rw [hcnt]
    simp [hbz]
    ac_rfl
  · have hrep : ∀ v : Int,
        (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)) =
          if contains (values b) v then 1 else 0 := countP_eq_laneValue b hbz
    rw [show ((laneVals a).map (fun v =>
      (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)))).sum =
      (laneVals a).countP (fun v => contains (values b) v) by
      rw [countP_eq_sum_nat]
      apply congrArg List.sum
      apply List.map_congr_left
      intro v hv
      rw [hrep v]]
    rw [hcnt]
    simp [hbz]

/-! ## The alternating-sign page as two lanes -/

private def altTerm (c : Config) (i : Nat) : Int :=
  (if i % 2 == 0 then (1 : Int) else -1) * (c.base + c.step * (i : Int))

private theorem rows_alt (c : Config) :
    rows layout c = (List.range c.n).map (fun i => (i, altTerm c i)) := by
  unfold rows
  simp [layout, altTerm]

private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun q : Nat => (a.start + q * a.stride, a.base + (q : Int) * a.step))

private theorem mem_lanePoints (a : Lane) (x : Nat × Int) :
    x ∈ lanePoints a ↔ ∃ q, q < a.n ∧ x = (a.start + q * a.stride, a.base + (q : Int) * a.step) := by
  unfold lanePoints
  rw [List.mem_map]
  constructor
  · rintro ⟨q, hq, hval⟩
    exact ⟨q, List.mem_range.mp hq, hval.symm⟩
  · rintro ⟨q, hq, hval⟩
    exact ⟨q, List.mem_range.mpr hq, hval.symm⟩

private theorem lanePoints_nodup (a : Lane) (hs : 0 < a.stride) : (lanePoints a).Nodup := by
  unfold lanePoints
  apply nodup_map_of_injective
  · intro i hi j hj h
    have hfst := congrArg Prod.fst h
    have hmul : i * a.stride = j * a.stride := Nat.add_left_cancel hfst
    exact Nat.eq_of_mul_eq_mul_right hs hmul
  · exact List.nodup_range

private theorem altTerm_even (c : Config) (i : Nat) (hi : i % 2 = 0) :
    altTerm c i = c.base + c.step * (i : Int) := by
  have hb : i % 2 == 0 := by simpa [hi]
  simp [altTerm, hb]

private theorem altTerm_odd (c : Config) (i : Nat) (hi : i % 2 = 1) :
    altTerm c i = -(c.base + c.step * (i : Int)) := by
  have hb : ¬ i % 2 == 0 := by
    rw [beq_iff_eq]
    omega
  simp [altTerm, hb]

private theorem even_or_odd (i : Nat) : i % 2 = 0 ∨ i % 2 = 1 := by
  have h := Nat.mod_lt i (by omega : 0 < 2)
  omega

private theorem alt_rows_perm (c : Config) :
    (rows layout c).Perm ((layout.lanes c).flatMap lanePoints) := by
  let a0 := lane c.n 0 2 c.base (2 * c.step)
  let a1 := lane c.n 1 2 (-(c.base + c.step)) (-2 * c.step)
  have hlanes : layout.lanes c = [a0, a1] := by rfl
  have ha0n : a0.n = (c.n + 1) / 2 := by dsimp [a0, lane]
  have ha1n : a1.n = c.n / 2 := by
    dsimp [a1, lane]
    by_cases h : c.n = 0
    · simp [h]
    · have hcn : c.n - 1 + 1 = c.n := by omega
      rw [hcn]
  rw [rows_alt c, hlanes]
  simp [List.flatMap_cons]
  have hleft : ((List.range c.n).map (fun i => (i, altTerm c i))).Nodup := by
    apply nodup_map_of_injective
    · intro i hi j hj h
      exact congrArg Prod.fst h
    · exact List.nodup_range
  have hdisj : ∀ x : Nat × Int, x ∈ lanePoints a0 → ∀ y : Nat × Int, y ∈ lanePoints a1 → x ≠ y := by
    intro x hx0 y hx1 heqxy
    rcases (mem_lanePoints a0 x).mp hx0 with ⟨q, hq, hqx⟩
    rcases (mem_lanePoints a1 y).mp hx1 with ⟨r, hr, hry⟩
    have heq : (a0.start + q * a0.stride, a0.base + (q : Int) * a0.step) =
        (a1.start + r * a1.stride, a1.base + (r : Int) * a1.step) := hqx.symm.trans (heqxy.trans hry)
    have hfst := congrArg Prod.fst heq
    have hq' : q * 2 < c.n := by
      rw [ha0n] at hq
      have hh := (Nat.lt_div_iff_mul_lt (by omega : 0 < 2)).mp hq
      omega
    have hr' : r * 2 < c.n - 1 := by
      rw [ha1n] at hr
      exact (Nat.lt_div_iff_mul_lt (by omega : 0 < 2)).mp hr
    dsimp [a0, lane, a1, lane] at hfst
    omega
  have hright : (lanePoints a0 ++ lanePoints a1).Nodup := by
    have h0 := lanePoints_nodup a0 (by dsimp [a0, lane]; omega)
    have h1 := lanePoints_nodup a1 (by dsimp [a1, lane]; omega)
    exact List.nodup_append.mpr ⟨h0, h1, hdisj⟩
  apply (List.perm_ext_iff_of_nodup hleft hright).mpr
  intro x
  constructor
  · intro hx
    rcases List.mem_map.mp hx with ⟨i, hi, rfl⟩
    have hin : i < c.n := List.mem_range.mp hi
    rcases even_or_odd i with he | ho
    · apply List.mem_append.mpr
      left
      apply (mem_lanePoints a0 _).mpr
      refine ⟨i / 2, ?_, ?_⟩
      · rw [ha0n]
        apply (Nat.lt_div_iff_mul_lt (by omega : 0 < 2)).mpr
        have hd := Nat.div_add_mod i 2
        omega
      · apply Prod.ext
        · dsimp [a0, lane]
          have hd := Nat.div_add_mod i 2
          omega
        · rw [altTerm_even c i he]
          dsimp [a0, lane]
          have hd := Nat.div_add_mod i 2
          have hcast : ((i / 2 : Nat) : Int) * 2 = (i : Int) := by exact_mod_cast (show (i / 2) * 2 = i by omega)
          grind
    · apply List.mem_append.mpr
      right
      apply (mem_lanePoints a1 _).mpr
      refine ⟨i / 2, ?_, ?_⟩
      · rw [ha1n]
        apply (Nat.lt_div_iff_mul_lt (by omega : 0 < 2)).mpr
        have hd := Nat.div_add_mod i 2
        omega
      · apply Prod.ext
        · dsimp [a1, lane]
          have hd := Nat.div_add_mod i 2
          omega
        · rw [altTerm_odd c i ho]
          dsimp [a1, lane]
          have hd := Nat.div_add_mod i 2
          have hcast : ((i / 2 : Nat) : Int) * 2 + 1 = (i : Int) := by exact_mod_cast (show (i / 2) * 2 + 1 = i by omega)
          grind
  · intro hx
    rcases List.mem_append.mp hx with hx | hx
    · rcases (mem_lanePoints a0 _).mp hx with ⟨q, hq, hqx⟩
      apply List.mem_map.mpr
      refine ⟨2 * q, ?_, ?_⟩
      · rw [ha0n] at hq
        have hqle : q + 1 ≤ (c.n + 1) / 2 := Nat.succ_le_of_lt hq
        have hmul : 2 * ((c.n + 1) / 2) ≤ c.n + 1 := by
          have := Nat.div_mul_le_self (c.n + 1) 2
          omega
        have hle : 2 * (q + 1) ≤ c.n + 1 := Nat.le_trans (Nat.mul_le_mul_left 2 hqle) hmul
        have hle' : 2 * q + 1 ≤ c.n := by omega
        exact List.mem_range.mpr (Nat.lt_of_succ_le hle') 
      · rw [hqx]
        apply Prod.ext
        · dsimp [a0, lane]
          omega
        · rw [altTerm_even]
          · dsimp [a0, lane]
            grind
          · omega
    · rcases (mem_lanePoints a1 _).mp hx with ⟨q, hq, hqx⟩
      apply List.mem_map.mpr
      refine ⟨2 * q + 1, ?_, ?_⟩
      · rw [ha1n] at hq
        have hqle : q + 1 ≤ c.n / 2 := Nat.succ_le_of_lt hq
        have hmul : 2 * (c.n / 2) ≤ c.n := by
          have := Nat.div_mul_le_self c.n 2
          omega
        have hle : 2 * (q + 1) ≤ c.n := Nat.le_trans (Nat.mul_le_mul_left 2 hqle) hmul
        have hle' : 2 * q + 2 ≤ c.n := by omega
        exact List.mem_range.mpr (Nat.lt_of_succ_le hle') 
      · rw [hqx]
        apply Prod.ext
        · dsimp [a1, lane]
          omega
        · rw [altTerm_odd]
          · dsimp [a1, lane]
            grind
          · omega

private theorem lanePoints_count_sum (a b : Lane) :
    ((lanePoints a).map (fun r : Nat × Int =>
      (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum =
      joinCount a b := by
  unfold lanePoints
  rw [List.map_map]
  change ((List.range a.n).map ((fun v : Int =>
    (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int))) ∘
      (fun q : Nat => a.base + (q : Int) * a.step))).sum = joinCount a b
  have h := lane_join_count a b
  unfold laneVals at h
  simpa [Function.comp_apply, Int.mul_comm] using h

private theorem flatMap_lanePoints_count_sum (ls : List Lane) (b : Lane) :
    ((ls.flatMap lanePoints).map (fun r : Nat × Int =>
      (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum =
      (ls.map (fun a => joinCount a b)).sum := by
  induction ls with
  | nil => simp
  | cons a as ih => simp [List.map_append, List.sum_append, ih, lanePoints_count_sum]

private theorem perm_sum_nat {l₁ l₂ : List Nat} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

theorem sound_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.AlternatingSign.ResidueJoin.Goal := by
  intro c b
  have hp := (alt_rows_perm c).map (fun r : Nat × Int =>
    (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))
  rw [perm_sum_nat hp]
  exact flatMap_lanePoints_count_sum (layout.lanes c) b

end TinylambOpt.ResearchProofs.Atlas.EncodedPages.AlternatingSignResidueJoin

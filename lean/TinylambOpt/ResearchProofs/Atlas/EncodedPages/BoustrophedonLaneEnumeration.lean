import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

The modeled inputs and results come from `TinylambOpt.ResearchGoals.Atlas.EncodedPages`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.BoustrophedonLaneEnumeration

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.Boustrophedon

private theorem bezout_spec (a b : Nat) :
    (a : Int) * (bezout a b).1 + (b : Int) * (bezout a b).2 = (Nat.gcd a b : Int) := by
  induction b using Nat.strongRecOn generalizing a with
  | ind b ih =>
      by_cases hb : b = 0
      · subst b
        rw [bezout, dite_eq_left rfl]
        simp
      · rw [bezout, dite_eq_right hb]
        have hmod : a % b < b := Nat.mod_lt _ (Nat.pos_of_ne_zero hb)
        have ih' := ih (a % b) hmod b
        rcases hbez : bezout b (a % b) with ⟨x, y⟩
        rw [hbez] at ih'
        dsimp at ih'
        dsimp
        have hnmod : (a : Int) = (b : Int) * (a / b : Nat) + (a % b : Int) := by
          have h := Nat.div_add_mod a b
          omega
        have hgcd : Nat.gcd b (a % b) = Nat.gcd a b := by
          rw [Nat.gcd_comm b (a % b)]
          rw [← Nat.gcd_rec b a]
          rw [Nat.gcd_comm b a]
        rw [← hgcd]
        rw [← ih']
        rw [hnmod]
        grind

private theorem crt_period (a b : Nat) (ha : 0 < a) (hb : 0 < b) :
    let g := a.gcd b
    let A := a / g
    let B := b / g
    A * b = a * B ∧ A * b = g * A * B ∧ g * A = a ∧ g * B = b ∧ 0 < g ∧ 0 < B := by
  dsimp
  have hgpos : 0 < a.gcd b := Nat.gcd_pos_of_pos_left b ha
  have hga : a.gcd b ∣ a := Nat.gcd_dvd_left a b
  have hgb : a.gcd b ∣ b := Nat.gcd_dvd_right a b
  have hA : a.gcd b * (a / a.gcd b) = a := by
    rw [Nat.mul_comm]
    exact Nat.div_mul_cancel hga
  have hB : a.gcd b * (b / a.gcd b) = b := by
    rw [Nat.mul_comm]
    exact Nat.div_mul_cancel hgb
  have hBpos : 0 < b / a.gcd b := Nat.div_pos (Nat.le_of_dvd hb hgb) hgpos
  constructor
  · grind
  constructor
  · grind
  constructor
  · exact hA
  constructor
  · exact hB
  constructor
  · exact hgpos
  · exact hBpos

private theorem dvd_lcm_of_both (g A B : Nat) (u y : Int)
    (hbez : (A : Int) * u + (B : Int) * y = 1) {x : Int}
    (hA : (g : Int) * (A : Int) ∣ x) (hB : (g : Int) * (B : Int) ∣ x) :
    (g : Int) * (A : Int) * (B : Int) ∣ x := by
  rcases hA with ⟨p, hp⟩
  rcases hB with ⟨r, hr⟩
  refine ⟨u * r + y * p, ?_⟩
  grind

private theorem crt_root_mod (a b : Progression) (ha : 0 < a.step) (hb : 0 < b.step)
    (hgap : (b.lo - a.lo) % (a.step.gcd b.step : Int) = 0) :
    let g := a.step.gcd b.step
    let A := a.step / g
    let B := b.step / g
    let unit := (bezout A B).1
    let t : Int := ((b.lo - a.lo) / (g : Int) * unit) % (B : Int)
    let root := a.lo + (a.step : Int) * t
    (b.step : Int) ∣ root - b.lo := by
  dsimp
  let g := a.step.gcd b.step
  let A := a.step / g
  let B := b.step / g
  let u : Int := (bezout A B).1
  let y : Int := (bezout A B).2
  let q : Int := (b.lo - a.lo) / (g : Int)
  let t : Int := (q * u) % (B : Int)
  have hper := crt_period a.step b.step ha hb
  dsimp only at hper
  rcases hper with ⟨hper1, hper2, hgA, hgB, hgpos, hBpos⟩
  have hgcdAB : A.gcd B = 1 := by
    dsimp [A, B]
    exact Nat.coprime_iff_gcd_eq_one.mp (Nat.coprime_div_gcd_div_gcd hgpos)
  have hbez : (A : Int) * u + (B : Int) * y = 1 := by
    dsimp [u, y]
    have hh := bezout_spec A B
    rw [hgcdAB] at hh
    simpa using hh
  have hgapdec : b.lo - a.lo = q * (g : Int) := by
    have hd := Int.ediv_mul_add_emod (b.lo - a.lo) (g : Int)
    have hg0 : (b.lo - a.lo) % (g : Int) = 0 := by simpa [g] using hgap
    dsimp [q]
    omega
  have htdec : q * u = (q * u) / (B : Int) * (B : Int) + t := by
    have hd := Int.ediv_mul_add_emod (q * u) (B : Int)
    dsimp [t]
    omega
  have hdiv : ∃ m : Int, (A : Int) * t - q = (B : Int) * m := by
    refine ⟨-(q * y) - (A : Int) * ((q * u) / (B : Int)), ?_⟩
    grind
  rcases hdiv with ⟨m, hm⟩
  change (b.step : Int) ∣ a.lo + (a.step : Int) * t - b.lo
  refine ⟨m, ?_⟩
  have hgAI : (a.step : Int) = (g : Int) * (A : Int) := by
    exact_mod_cast hgA.symm
  have hgBI : (b.step : Int) = (g : Int) * (B : Int) := by
    exact_mod_cast hgB.symm
  have h1 : a.lo + (a.step : Int) * t - b.lo = (g : Int) * (A : Int) * t - q * (g : Int) := by
    rw [hgAI]
    omega
  have h2 : (g : Int) * (A : Int) * t - q * (g : Int) = (g : Int) * ((A : Int) * t - q) := by
    grind
  have h3 : (g : Int) * ((A : Int) * t - q) = (g : Int) * ((B : Int) * m) := by
    rw [hm]
  have h4 : (g : Int) * ((B : Int) * m) = (b.step : Int) * m := by
    rw [hgBI]
    grind
  omega

private theorem lastVal_count (lo : Int) (s q : Nat) :
    lastVal ⟨lo, s, q + 1⟩ = lo + (q : Int) * (s : Int) := by
  rfl

private theorem contains_bounds {lo hi : Int} {p : Nat} (hp : 0 < p) (hlohi : lo ≤ hi) :
    contains ⟨lo, p, ((hi - lo) / (p : Int)).toNat + 1⟩ v ↔
      lo ≤ v ∧ v ≤ hi ∧ (v - lo) % (p : Int) = 0 := by
  have hpI : (p : Int) ≠ 0 := by omega
  have hpp : (0 : Int) < p := by omega
  have hdiv_nonneg : 0 ≤ (hi - lo) / (p : Int) := by
    apply Int.ediv_nonneg <;> omega
  have htoNat : (((hi - lo) / (p : Int)).toNat : Int) = (hi - lo) / (p : Int) :=
    Int.toNat_of_nonneg hdiv_nonneg
  have hqp_le : ((hi - lo) / (p : Int)) * (p : Int) ≤ hi - lo := by
    have hd := Int.ediv_mul_add_emod (hi - lo) (p : Int)
    have hr : 0 ≤ (hi - lo) % (p : Int) := Int.emod_nonneg _ hpI
    omega
  unfold contains
  simp only [Bool.and_eq_true, Bool.or_eq_true, decide_eq_true_eq, beq_iff_eq]
  have hn : 0 < ((hi - lo) / (p : Int)).toNat + 1 := by omega
  have hpz : ¬ p = 0 := by omega
  simp [hn, hpz, lastVal_count, htoNat]
  constructor
  · intro h
    rcases h with ⟨⟨hlo, hupper⟩, hmod⟩
    refine ⟨hlo, ?_, hmod⟩
    let q : Int := (v - lo) / (p : Int)
    have hqnonneg : 0 ≤ q := by dsimp [q]; apply Int.ediv_nonneg <;> omega
    have hqle : q ≤ (hi - lo) / (p : Int) := by
      dsimp [q]
      apply Int.ediv_le_ediv hpp
      omega
    have hmul : q * (p : Int) ≤ ((hi - lo) / (p : Int)) * (p : Int) :=
      Int.mul_le_mul_of_nonneg_right hqle (by omega)
    have hdec : lo + q * (p : Int) ≤ hi := by omega
    have hvq : v = lo + q * (p : Int) := by
      have hd := Int.ediv_mul_add_emod (v - lo) (p : Int)
      dsimp [q]
      omega
    rw [hvq]
    exact hdec
  · intro h
    rcases h with ⟨hlo, hhi, hmod⟩
    let q : Int := (v - lo) / (p : Int)
    have hqnonneg : 0 ≤ q := by dsimp [q]; apply Int.ediv_nonneg <;> omega
    have hvq : v = lo + q * (p : Int) := by
      have hd := Int.ediv_mul_add_emod (v - lo) (p : Int)
      dsimp [q]
      omega
    have hqle : q ≤ (hi - lo) / (p : Int) := by
      dsimp [q]
      apply Int.ediv_le_ediv hpp
      omega
    have hmul : q * (p : Int) ≤ ((hi - lo) / (p : Int)) * (p : Int) :=
      Int.mul_le_mul_of_nonneg_right hqle (by omega)
    refine ⟨⟨hlo, ?_⟩, ?_⟩
    · rw [hvq]
      omega
    · exact hmod

private theorem contains_div_of_common {p : Progression} {v : Int} (h : contains p v) :
    (p.step : Int) ∣ v - p.lo := by
  unfold contains at h
  simp only [Bool.and_eq_true, Bool.or_eq_true, decide_eq_true_eq, beq_iff_eq] at h
  rcases h with ⟨⟨⟨_, hlo⟩, hhi⟩, hzero | hmod⟩
  · have hl : lastVal p = p.lo := by
      dsimp [lastVal]
      rw [hzero]
      simp
    have hv : v = p.lo := by omega
    rw [hzero, hv]
    refine ⟨0, ?_⟩
    simp
  · exact Int.dvd_of_emod_eq_zero hmod

private theorem contains_bounds_of_common {p : Progression} {v : Int} (h : contains p v) :
    p.lo ≤ v ∧ v ≤ lastVal p := by
  unfold contains at h
  simp only [Bool.and_eq_true, Bool.or_eq_true, decide_eq_true_eq, beq_iff_eq] at h
  exact ⟨h.1.1.2, h.1.2⟩

private theorem contains_npos {p : Progression} {v : Int} (h : contains p v) : 0 < p.n := by
  unfold contains at h
  simp only [Bool.and_eq_true, Bool.or_eq_true, decide_eq_true_eq, beq_iff_eq] at h
  exact h.1.1.1

private theorem nat_dvd_int {m n : Nat} (h : m ∣ n) : (m : Int) ∣ (n : Int) := by
  exact_mod_cast h

private theorem contains_of_bounds_mod (p : Progression) (v : Int) (hn : 0 < p.n)
    (hlo : p.lo ≤ v) (hhi : v ≤ lastVal p) (hmod : (p.step : Int) ∣ v - p.lo) :
    contains p v := by
  by_cases hs : p.step = 0
  · have hv : v = p.lo := by
      have hl : lastVal p = p.lo := by
        dsimp [lastVal]
        rw [hs]
        simp
      rcases hmod with ⟨k, hk⟩
      rw [hs] at hk
      simp at hk
      omega
    have hll : p.lo ≤ lastVal p := by omega
    unfold contains
    simp [hn, hs, hlo, hhi, hv, hll]
  · have hz : (v - p.lo) % (p.step : Int) = 0 := Int.emod_eq_zero_of_dvd hmod
    unfold contains
    simp [hn, hs, hlo, hhi, hz]

private theorem intersection_eq_of_pos (a b : Progression) (ha : 0 < a.step) (hb : 0 < b.step)
    (hna : a.n ≠ 0) (hnb : b.n ≠ 0)
    (hgap0 : (b.lo - a.lo) % (a.step.gcd b.step : Int) = 0) :
    intersection a b =
      let g := a.step.gcd b.step
      let gap := b.lo - a.lo
      let period := a.step / g * b.step
      let unit := (bezout (a.step / g) (b.step / g)).1
      let root := a.lo + (a.step : Int) * (gap / (g : Int) * unit % (b.step / g : Nat))
      let low := max a.lo b.lo
      let first := low + (root - low) % (period : Int)
      let high := min (lastVal a) (lastVal b)
      ({ lo := first, step := period,
         n := if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0 } : Progression) := by
  have ha0 : a.step ≠ 0 := by omega
  have hb0 : b.step ≠ 0 := by omega
  simp [intersection, hna, hnb, ha0, hb0, hgap0]

private theorem intersection_contains (a b : Progression) (ha : 0 < a.step) (hb : 0 < b.step) :
    ∀ v, contains (intersection a b) v ↔ contains a v ∧ contains b v := by
  intro v
  let g := a.step.gcd b.step
  let A := a.step / g
  let B := b.step / g
  let period := A * b.step
  let u : Int := (bezout A B).1
  let y : Int := (bezout A B).2
  let q : Int := (b.lo - a.lo) / (g : Int)
  let t : Int := (q * u) % (B : Int)
  let root := a.lo + (a.step : Int) * t
  let low := max a.lo b.lo
  let first := low + (root - low) % (period : Int)
  let high := min (lastVal a) (lastVal b)
  have hper := crt_period a.step b.step ha hb
  dsimp only at hper
  rcases hper with ⟨hper1, hper2, hgA, hgB, hgpos, hBpos⟩
  have hgcdAB : A.gcd B = 1 := by
    dsimp [A, B]
    exact Nat.coprime_iff_gcd_eq_one.mp (Nat.coprime_div_gcd_div_gcd hgpos)
  have hbez : (A : Int) * u + (B : Int) * y = 1 := by
    dsimp [u, y]
    have hh := bezout_spec A B
    rw [hgcdAB] at hh
    simpa using hh
  have hperI : (period : Int) = (g : Int) * (A : Int) * (B : Int) := by
    exact_mod_cast hper2
  have hApos : 0 < A := Nat.div_pos (Nat.le_of_dvd ha (Nat.gcd_dvd_left a.step b.step)) hgpos
  have hperiodpos : 0 < period := Nat.mul_pos hApos hb
  have hgAI : (a.step : Int) = (g : Int) * (A : Int) := by exact_mod_cast hgA.symm
  have hgBI : (b.step : Int) = (g : Int) * (B : Int) := by exact_mod_cast hgB.symm
  have hperA : (a.step : Int) ∣ (period : Int) := by
    rw [hperI, hgAI]
    refine ⟨(B : Int), ?_⟩
    grind
  have hperB : (b.step : Int) ∣ (period : Int) := by
    rw [hperI, hgBI]
    refine ⟨(A : Int), ?_⟩
    grind
  have hrootA : (a.step : Int) ∣ root - a.lo := by
    dsimp [root]
    refine ⟨t, ?_⟩
    omega
  by_cases hn0 : a.n = 0 ∨ b.n = 0
  · have hfalse : ¬ (contains a v ∧ contains b v) := by
      intro hh
      have hpa := contains_npos hh.1
      have hpb := contains_npos hh.2
      omega
    have hl : ¬ contains (intersection a b) v := by
      intro hh
      have hp := contains_npos hh
      simp [intersection, hn0] at hp
    constructor
    · intro hh; exact False.elim (hl hh)
    · intro hh; exact False.elim (hfalse hh)
  · have hna : a.n ≠ 0 := by omega
    have hnb : b.n ≠ 0 := by omega
    by_cases hgap0 : (b.lo - a.lo) % (g : Int) = 0
    · have hrootB : (b.step : Int) ∣ root - b.lo := by
        change (b.step : Int) ∣ a.lo + (a.step : Int) * t - b.lo
        simpa [g, A, B, u, q, t] using crt_root_mod a b ha hb (by simpa [g] using hgap0)
      have hfirstroot : (period : Int) ∣ first - root := by
        refine ⟨-((root - low) / (period : Int)), ?_⟩
        dsimp [first]
        have hd := Int.ediv_mul_add_emod (root - low) (period : Int)
        grind
      have hcommonFirstA : (a.step : Int) ∣ first - a.lo := by
        rcases hperA with ⟨k, hk⟩
        rcases hfirstroot with ⟨m, hm⟩
        rcases hrootA with ⟨n, hn⟩
        refine ⟨k * m + n, ?_⟩
        grind
      have hcommonFirstB : (b.step : Int) ∣ first - b.lo := by
        rcases hperB with ⟨k, hk⟩
        rcases hfirstroot with ⟨m, hm⟩
        rcases hrootB with ⟨n, hn⟩
        refine ⟨k * m + n, ?_⟩
        grind
      have hfirstLow : low ≤ first := by
        dsimp [first]
        have hn : 0 ≤ (root - low) % (period : Int) :=
          Int.emod_nonneg _ (by omega : (period : Int) ≠ 0)
        omega
      have hfirstUp : first < low + (period : Int) := by
        dsimp [first]
        have hlt : (root - low) % (period : Int) < (period : Int) :=
          Int.emod_lt_of_pos _ (by omega : (0 : Int) < period)
        omega
      rw [intersection_eq_of_pos a b ha hb hna hnb (by simpa [g] using hgap0)]
      dsimp only
      by_cases hfh : first ≤ high
      · rw [if_pos hfh]
        rw [contains_bounds hperiodpos hfh]
        constructor
        · intro hv
          rcases hv with ⟨hfv, hvh, hmodv⟩
          have hvfirst : (period : Int) ∣ v - first := Int.dvd_of_emod_eq_zero hmodv
          have hvA0 : (a.step : Int) ∣ v - a.lo := by
            rcases hperA with ⟨k, hk⟩
            rcases hvfirst with ⟨m, hm⟩
            rcases hcommonFirstA with ⟨n, hn⟩
            refine ⟨k * m + n, ?_⟩
            grind
          have hvB0 : (b.step : Int) ∣ v - b.lo := by
            rcases hperB with ⟨k, hk⟩
            rcases hvfirst with ⟨m, hm⟩
            rcases hcommonFirstB with ⟨n, hn⟩
            refine ⟨k * m + n, ?_⟩
            grind
          constructor
          · apply contains_of_bounds_mod a v (by omega)
            · omega
            · omega
            · exact hvA0
          · apply contains_of_bounds_mod b v (by omega)
            · omega
            · omega
            · exact hvB0
        · intro hv
          rcases hv with ⟨hva, hvb⟩
          have hva0 := contains_div_of_common hva
          have hvb0 := contains_div_of_common hvb
          have hvroot : (period : Int) ∣ v - root := by
            have h1a : (a.step : Int) ∣ (v - a.lo) - (root - a.lo) := Int.dvd_sub hva0 hrootA
            have h1b : (a.step : Int) ∣ v - root := by
              rw [show (v - a.lo) - (root - a.lo) = v - root by omega] at h1a
              exact h1a
            have h1 : (g : Int) * (A : Int) ∣ v - root := by
              rw [← hgAI]
              exact h1b
            have h2b : (b.step : Int) ∣ (v - b.lo) - (root - b.lo) := Int.dvd_sub hvb0 hrootB
            have h2c : (b.step : Int) ∣ v - root := by
              rw [show (v - b.lo) - (root - b.lo) = v - root by omega] at h2b
              exact h2b
            have h2 : (g : Int) * (B : Int) ∣ v - root := by
              rw [← hgBI]
              exact h2c
            simpa [hperI] using dvd_lcm_of_both g A B u y hbez h1 h2
          have hvfirst : (period : Int) ∣ v - first := by
            rcases hvroot with ⟨m, hm⟩
            rcases hfirstroot with ⟨k, hk⟩
            refine ⟨m - k, ?_⟩
            grind
          have hlowv : low ≤ v := by
            have ha1 := (contains_bounds_of_common hva).1
            have hb1 := (contains_bounds_of_common hvb).1
            dsimp [low]
            omega
          have hvHigh : v ≤ high := by
            have ha2 := (contains_bounds_of_common hva).2
            have hb2 := (contains_bounds_of_common hvb).2
            dsimp [high]
            omega
          have hfv : first ≤ v := by
            rcases hvfirst with ⟨m, hm⟩
            by_cases hle : first ≤ v
            · exact hle
            · exfalso
              have hvEq : v = first + (period : Int) * m := by omega
              have hmneg : m < 0 := by
                by_cases hm0 : 0 ≤ m
                · have hn : 0 ≤ (period : Int) * m := Int.mul_nonneg (by omega) hm0
                  omega
                · omega
              have hpm_le : (period : Int) * m ≤ -(period : Int) := by
                have hh := Int.mul_le_mul_of_nonneg_right (show m ≤ -1 by omega) (by omega : 0 ≤ (period : Int))
                rw [Int.mul_comm] at hh
                simpa using hh
              have hvlt : v < low := by omega
              omega
          refine ⟨hfv, hvHigh, ?_⟩
          rcases hvfirst with ⟨m, hm⟩
          rw [show v - first = (period : Int) * m by omega]
          exact Int.emod_eq_zero_of_dvd ⟨m, rfl⟩
      · have hfalse : ¬ (contains a v ∧ contains b v) := by
          intro hv
          have hnva := contains_npos hv.1
          have hnvb := contains_npos hv.2
          have hlowv : low ≤ v := by
            have ha1 := (contains_bounds_of_common hv.1).1
            have hb1 := (contains_bounds_of_common hv.2).1
            dsimp [low]
            omega
          have hva0 := contains_div_of_common hv.1
          have hvb0 := contains_div_of_common hv.2
          have hvroot : (period : Int) ∣ v - root := by
            have h1a : (a.step : Int) ∣ (v - a.lo) - (root - a.lo) := Int.dvd_sub hva0 hrootA
            have h1b : (a.step : Int) ∣ v - root := by
              rw [show (v - a.lo) - (root - a.lo) = v - root by omega] at h1a
              exact h1a
            have h1 : (g : Int) * (A : Int) ∣ v - root := by
              rw [← hgAI]
              exact h1b
            have h2b : (b.step : Int) ∣ (v - b.lo) - (root - b.lo) := Int.dvd_sub hvb0 hrootB
            have h2c : (b.step : Int) ∣ v - root := by
              rw [show (v - b.lo) - (root - b.lo) = v - root by omega] at h2b
              exact h2b
            have h2 : (g : Int) * (B : Int) ∣ v - root := by
              rw [← hgBI]
              exact h2c
            simpa [hperI] using dvd_lcm_of_both g A B u y hbez h1 h2
          have hvfirst : (period : Int) ∣ v - first := by
            rcases hvroot with ⟨m, hm⟩
            rcases hfirstroot with ⟨k, hk⟩
            refine ⟨m - k, ?_⟩
            grind
          have hfv : first ≤ v := by
            rcases hvfirst with ⟨m, hm⟩
            by_cases hle : first ≤ v
            · exact hle
            · exfalso
              have hvEq : v = first + (period : Int) * m := by omega
              have hmneg : m < 0 := by
                by_cases hm0 : 0 ≤ m
                · have hn : 0 ≤ (period : Int) * m := Int.mul_nonneg (by omega) hm0
                  omega
                · omega
              have hpm_le : (period : Int) * m ≤ -(period : Int) := by
                have hh := Int.mul_le_mul_of_nonneg_right (show m ≤ -1 by omega) (by omega : 0 ≤ (period : Int))
                rw [Int.mul_comm] at hh
                simpa using hh
              have hvlt : v < low := by omega
              omega
          have hvHigh : v ≤ high := by
            have ha2 := (contains_bounds_of_common hv.1).2
            have hb2 := (contains_bounds_of_common hv.2).2
            dsimp [high]
            omega
          omega
        rw [if_neg hfh]
        have hl : ¬ contains ({ lo := first, step := period, n := 0 } : Progression) v := by
          intro hh
          have hn : 0 < (0 : Nat) := by simpa using contains_npos hh
          omega
        constructor
        · intro hh; exact False.elim (hl hh)
        · intro hh; exact False.elim (hfalse hh)
    · have hfalse : ¬ (contains a v ∧ contains b v) := by
        intro hv
        have hva0 := contains_div_of_common hv.1
        have hvb0 := contains_div_of_common hv.2
        have hga : (g : Int) ∣ a.step := by
          dsimp [g]
          exact nat_dvd_int (Nat.gcd_dvd_left a.step b.step)
        have hgb : (g : Int) ∣ b.step := by
          dsimp [g]
          exact nat_dvd_int (Nat.gcd_dvd_right a.step b.step)
        have h1 : (g : Int) ∣ v - a.lo := Int.dvd_trans hga hva0
        have h2 : (g : Int) ∣ v - b.lo := Int.dvd_trans hgb hvb0
        have hgapdiv0 : (g : Int) ∣ (v - a.lo) - (v - b.lo) := Int.dvd_sub h1 h2
        have hgapdiv : (g : Int) ∣ b.lo - a.lo := by
          rw [show (v - a.lo) - (v - b.lo) = b.lo - a.lo by omega] at hgapdiv0
          exact hgapdiv0
        rcases hgapdiv with ⟨k, hk⟩
        have hz : (b.lo - a.lo) % (g : Int) = 0 := Int.emod_eq_zero_of_dvd ⟨k, hk⟩
        exact hgap0 hz
      have ha0 : a.step ≠ 0 := by omega
      have hb0 : b.step ≠ 0 := by omega
      have hgneq : ((b.lo - a.lo) % (g : Int) != 0) = true := by
        simp [hgap0]
      have hieq : intersection a b = ({ lo := 0, step := 1, n := 0 } : Progression) := by
        simp [intersection, hna, hnb, ha0, hb0, hgneq, g]
      have hl : ¬ contains (intersection a b) v := by
        rw [hieq]
        intro hh
        have hn : 0 < (0 : Nat) := by simpa using contains_npos hh
        omega
      constructor
      · intro hh; exact False.elim (hl hh)
      · intro hh; exact False.elim (hfalse hh)


open TinylambOpt.ResearchGoals.Atlas.EncodedPages

private theorem list_sorted_eq_of_mem {xs ys : List Int}
    (hxs : xs.Pairwise (· < ·)) (hys : ys.Pairwise (· < ·))
    (hmem : ∀ v, v ∈ xs ↔ v ∈ ys) : xs = ys := by
  induction xs generalizing ys with
  | nil =>
      cases ys with
      | nil => rfl
      | cons y ys =>
          have hy : y ∈ ([] : List Int) := (hmem y).2 (by simp)
          simp at hy
  | cons x xs ih =>
      cases ys with
      | nil =>
          have hx : x ∈ ([] : List Int) := (hmem x).1 (by simp)
          simp at hx
      | cons y ys =>
          have hyinxs : y ∈ x :: xs := (hmem y).2 (by simp)
          have hxiny : x ∈ y :: ys := (hmem x).1 (by simp)
          have hylex : y ≤ x := by
            rcases List.mem_cons.mp hxiny with hxy | hxTail
            · omega
            · have hlt := List.rel_of_pairwise_cons hys hxTail
              omega
          have hxley : x ≤ y := by
            rcases List.mem_cons.mp hyinxs with hyx | hyTail
            · omega
            · have hlt := List.rel_of_pairwise_cons hxs hyTail
              omega
          have hxy : x = y := by omega
          subst y
          have hxs' : xs.Pairwise (· < ·) := (List.pairwise_cons.mp hxs).2
          have hys' : ys.Pairwise (· < ·) := (List.pairwise_cons.mp hys).2
          have hmem' : ∀ v, v ∈ xs ↔ v ∈ ys := by
            intro v
            constructor
            · intro hv
              have h : v ∈ x :: xs := by simp [hv]
              have h' := (hmem v).1 h
              rcases List.mem_cons.mp h' with hveq | hvys
              · subst v
                have hxx : x ∈ xs := by simpa using hv
                have hlt := List.rel_of_pairwise_cons hxs hxx
                omega
              · exact hvys
            · intro hv
              have h : v ∈ x :: ys := by simp [hv]
              have h' := (hmem v).2 h
              rcases List.mem_cons.mp h' with hveq | hvxs
              · subst v
                have hxx : x ∈ ys := by simpa using hv
                have hlt := List.rel_of_pairwise_cons hys hxx
                omega
              · exact hvxs
          rw [ih hxs' hys' hmem']

private theorem progVals_pairwise (p : Progression) (hp : 0 < p.step) :
    ((List.range p.n).map (fun k : Nat => p.lo + (k : Int) * (p.step : Int))).Pairwise (· < ·) := by
  induction p.n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append]
      rw [List.pairwise_append]
      refine ⟨ih, by simp, ?_⟩
      intro a ha b hb
      rcases List.mem_map.mp ha with ⟨k, hk, rfl⟩
      have hklt : k < n := List.mem_range.mp hk
      simp at hb
      subst b
      have hsp : (0 : Int) < p.step := by omega
      have hkn : k < n := hklt
      have hdiff : ((n : Nat) : Int) - (k : Int) > 0 := by omega
      have hmul : 0 < (((n : Nat) : Int) - (k : Int)) * (p.step : Int) :=
        Int.mul_pos hdiff hsp
      have hid : (n : Int) * (p.step : Int) =
          (k : Int) * (p.step : Int) + (((n : Nat) : Int) - (k : Int)) * (p.step : Int) := by
        grind
      omega

private theorem mem_progVals (p : Progression) (v : Int) :
    v ∈ (List.range p.n).map (fun k : Nat => p.lo + (k : Int) * (p.step : Int)) ↔ contains p v := by
  constructor
  · intro hv
    rcases List.mem_map.mp hv with ⟨k, hk, rfl⟩
    have hklt : k < p.n := List.mem_range.mp hk
    unfold contains
    simp only [Bool.and_eq_true, Bool.or_eq_true, decide_eq_true_eq, beq_iff_eq]
    have hn : 0 < p.n := by omega
    have hlo : p.lo ≤ p.lo + (k : Int) * (p.step : Int) := by
      by_cases hz : p.step = 0
      · simp [hz]
      · have hsp : 0 < p.step := Nat.pos_of_ne_zero hz
        have hk0 : (0 : Int) ≤ k := by omega
        have hmul : 0 ≤ (k : Int) * (p.step : Int) := Int.mul_nonneg hk0 (by omega)
        omega
    have hhi : p.lo + (k : Int) * (p.step : Int) ≤ lastVal p := by
      dsimp [lastVal]
      have hmul := Int.mul_le_mul_of_nonneg_right (by omega : (k : Int) ≤ ((p.n - 1 : Nat) : Int)) (by omega : 0 ≤ (p.step : Int))
      omega
    refine ⟨⟨⟨hn, hlo⟩, hhi⟩, ?_⟩
    by_cases hz : p.step = 0
    · left; exact hz
    · right
      rw [Int.emod_eq_zero_of_dvd]
      refine ⟨(k : Int), ?_⟩
      grind
  · intro hv
    unfold contains at hv
    simp only [Bool.and_eq_true, Bool.or_eq_true, decide_eq_true_eq, beq_iff_eq] at hv
    rcases hv with ⟨⟨⟨hn, hlo⟩, hhi⟩, hzero | hmod⟩
    · refine List.mem_map.mpr ⟨0, by simpa using hn, ?_⟩
      dsimp [lastVal] at hhi
      simp [hzero] at hhi
      simp [hzero]
      omega
    · by_cases hz : p.step = 0
      · refine List.mem_map.mpr ⟨0, by simpa using hn, ?_⟩
        dsimp [lastVal] at hhi
        simp [hz] at hhi hmod
        simp [hz]
        omega
      · let k : Int := (v - p.lo) / (p.step : Int)
        have hspI : (0 : Int) < p.step := by omega
        have hk0 : 0 ≤ k := by dsimp [k]; apply Int.ediv_nonneg <;> omega
        have hvk : v = p.lo + k * (p.step : Int) := by
          have hd := Int.ediv_mul_add_emod (v - p.lo) (p.step : Int)
          dsimp [k]
          omega
        have hklt : k.toNat < p.n := by
          have hkcast : (k.toNat : Int) = k := Int.toNat_of_nonneg hk0
          have hhi' : v ≤ p.lo + ((p.n - 1 : Nat) : Int) * (p.step : Int) := by
            simpa [lastVal] using hhi
          have hmul_le : k * (p.step : Int) ≤ ((p.n - 1 : Nat) : Int) * (p.step : Int) := by
            omega
          have hkupper : k ≤ ((p.n - 1 : Nat) : Int) := Int.le_of_mul_le_mul_right hmul_le hspI
          omega
        refine List.mem_map.mpr ⟨k.toNat, List.mem_range.mpr hklt, ?_⟩
        rw [hvk]
        congr 2
        exact Int.toNat_of_nonneg hk0

private theorem intersection_step_pos (a b : Progression) : 0 < (intersection a b).step := by
  by_cases hn0 : (a.n == 0 || b.n == 0) = true
  · simp [intersection, hn0]
  · by_cases ha0 : (a.step == 0) = true
    · simp [intersection, hn0, ha0]
    · by_cases hb0 : (b.step == 0) = true
      · simp [intersection, hn0, ha0, hb0]
      · by_cases hg : ((b.lo - a.lo) % (a.step.gcd b.step : Int) != 0) = true
        · simp [intersection, hn0, ha0, hb0, hg]
        · have haNZ : a.step ≠ 0 := by simpa using ha0
          have hbNZ : b.step ≠ 0 := by simpa using hb0
          have hgpos : 0 < a.step.gcd b.step := Nat.gcd_pos_of_pos_left b.step (Nat.pos_of_ne_zero haNZ)
          have hApos : 0 < a.step / a.step.gcd b.step :=
            Nat.div_pos (Nat.le_of_dvd (Nat.pos_of_ne_zero haNZ) (Nat.gcd_dvd_left a.step b.step)) hgpos
          have hsteppos : 0 < a.step / a.step.gcd b.step * b.step := Nat.mul_pos hApos (Nat.pos_of_ne_zero hbNZ)
          simp [intersection, hn0, ha0, hb0, hg, hsteppos]

private theorem intersection_progvals_eq (a b : Progression) (ha : 0 < a.step) (hb : 0 < b.step) :
    ((List.range a.n).map (fun k : Nat => a.lo + (k : Int) * (a.step : Int))).filter
        (fun v => contains b v) =
      (List.range (intersection a b).n).map
        (fun k : Nat => (intersection a b).lo + (k : Int) * (intersection a b).step) := by
  apply list_sorted_eq_of_mem
  · exact (progVals_pairwise a ha).filter (fun v => contains b v)
  · exact progVals_pairwise (intersection a b) (intersection_step_pos a b)
  · intro v
    simp only [List.mem_filter]
    constructor
    · intro h
      have hc : contains a v ∧ contains b v := ⟨(mem_progVals a v).1 h.1, h.2⟩
      exact (mem_progVals (intersection a b) v).2 ((intersection_contains a b ha hb v).2 hc)
    · intro h
      have hc := (intersection_contains a b ha hb v).1 ((mem_progVals (intersection a b) v).1 h)
      exact ⟨(mem_progVals a v).2 hc.1, hc.2⟩

private theorem intersection_n_eq_filter_length (a b : Progression) (ha : 0 < a.step) (hb : 0 < b.step) :
    (intersection a b).n =
      ((List.range a.n).map (fun k : Nat => a.lo + (k : Int) * (a.step : Int))).countP
        (fun v => contains b v) := by
  have heq := intersection_progvals_eq a b ha hb
  have hlen := congrArg List.length heq
  simpa [List.countP_eq_length_filter] using hlen.symm

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

private theorem list_sorted_eq_of_mem_nat {xs ys : List Nat}
    (hxs : xs.Pairwise (· < ·)) (hys : ys.Pairwise (· < ·))
    (hmem : ∀ v, v ∈ xs ↔ v ∈ ys) : xs = ys := by
  induction xs generalizing ys with
  | nil =>
      cases ys with
      | nil => rfl
      | cons y ys =>
          have hy : y ∈ ([] : List Nat) := (hmem y).2 (by simp)
          simp at hy
  | cons x xs ih =>
      cases ys with
      | nil =>
          have hx : x ∈ ([] : List Nat) := (hmem x).1 (by simp)
          simp at hx
      | cons y ys =>
          have hylex : y ≤ x := by
            have hxiny : x ∈ y :: ys := (hmem x).1 (by simp)
            rcases List.mem_cons.mp hxiny with hxy | hxTail
            · omega
            · have hlt := List.rel_of_pairwise_cons hys hxTail
              omega
          have hxley : x ≤ y := by
            have hyinxs : y ∈ x :: xs := (hmem y).2 (by simp)
            rcases List.mem_cons.mp hyinxs with hyx | hyTail
            · omega
            · have hlt := List.rel_of_pairwise_cons hxs hyTail
              omega
          have hxy : x = y := by omega
          subst y
          have hxs' : xs.Pairwise (· < ·) := (List.pairwise_cons.mp hxs).2
          have hys' : ys.Pairwise (· < ·) := (List.pairwise_cons.mp hys).2
          have hmem' : ∀ v, v ∈ xs ↔ v ∈ ys := by
            intro v
            constructor
            · intro hv
              have h : v ∈ x :: xs := by simp [hv]
              have h' := (hmem v).1 h
              rcases List.mem_cons.mp h' with hveq | hvys
              · subst v
                have hxx : x ∈ xs := by simpa using hv
                have hlt := List.rel_of_pairwise_cons hxs hxx
                omega
              · exact hvys
            · intro hv
              have h : v ∈ x :: ys := by simp [hv]
              have h' := (hmem v).2 h
              rcases List.mem_cons.mp h' with hveq | hvxs
              · subst v
                have hxx : x ∈ ys := by simpa using hv
                have hlt := List.rel_of_pairwise_cons hys hxx
                omega
              · exact hvxs
          rw [ih hxs' hys' hmem']

private theorem range_pairwise_lt (n : Nat) : (List.range n).Pairwise (· < ·) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.pairwise_append]
      refine ⟨ih, by simp, ?_⟩
      intro a ha b hb
      simp at hb
      subst b
      exact List.mem_range.mp ha

private theorem arith_positions_pairwise (m r d : Nat) (hd : 0 < d) :
    ((List.range m).map (fun j => r + j * d)).Pairwise (· < ·) := by
  induction m with
  | zero => simp
  | succ m ih =>
      rw [List.range_succ, List.map_append, List.pairwise_append]
      refine ⟨ih, by simp, ?_⟩
      intro a ha b hb
      rcases List.mem_map.mp ha with ⟨k, hk, rfl⟩
      have hklt : k < m := List.mem_range.mp hk
      simp at hb
      subst b
      have hdiff : 0 < m - k := by omega
      have hmul : 0 < (m - k) * d := Nat.mul_pos hdiff hd
      have hm : m = k + (m - k) := by omega
      rw [hm, Nat.add_mul]
      omega

private theorem lane_count_formula (n r d : Nat) (hd : 0 < d) (hr : r < d) :
    (lane n r d 0 0).n = if r < n then (n - 1 - r) / d + 1 else 0 := by
  dsimp [lane]
  by_cases hrn : r < n
  · rw [if_pos hrn]
    have hle : r ≤ n - 1 := by omega
    have h1 : n - r + d - 1 = n - 1 - r + d := by omega
    rw [h1]
    rw [show n - 1 - r + d = n - 1 - r + d * 1 by simp]
    rw [Nat.add_mul_div_left (n - 1 - r) 1 hd]
  · rw [if_neg hrn]
    have hsub : n - r = 0 := by omega
    rw [hsub]
    simp
    omega

private theorem range_arith_eq_filter_mod (n r d : Nat) (hd : 0 < d) (hr : r < d) :
    (List.range ((lane n r d 0 0).n)).map (fun j : Nat => r + j * d) =
      (List.range n).filter (fun i : Nat => i % d == r) := by
  apply list_sorted_eq_of_mem_nat
  · exact arith_positions_pairwise _ r d hd
  · exact (range_pairwise_lt n).filter _
  · intro p
    constructor
    · intro hp
      rcases List.mem_map.mp hp with ⟨j, hj, rfl⟩
      have hjlt : j < (lane n r d 0 0).n := List.mem_range.mp hj
      rw [lane_count_formula n r d hd hr] at hjlt
      by_cases hrn : r < n
      · rw [if_pos hrn] at hjlt
        simp only [List.mem_filter, List.mem_range]
        have hle : j ≤ (n - 1 - r) / d := by omega
        have hmul := Nat.mul_le_mul_right d hle
        have hdivle : ((n - 1 - r) / d) * d ≤ n - 1 - r := Nat.div_mul_le_self _ _
        have hbound : r + j * d < n := by omega
        refine ⟨hbound, ?_⟩
        rw [Nat.add_mod]
        simp [Nat.mod_eq_of_lt hr]
      · rw [if_neg hrn] at hjlt
        omega
    · intro hp
      simp only [List.mem_filter, List.mem_range] at hp
      rcases hp with ⟨hpn, hmod⟩
      have hmod' : p % d = r := by simpa using hmod
      rw [lane_count_formula n r d hd hr]
      by_cases hrn : r < n
      · rw [if_pos hrn]
        have hdec := Nat.mod_add_div p d
        rw [hmod'] at hdec
        have hpr : p = r + d * (p / d) := hdec.symm
        refine List.mem_map.mpr ⟨p / d, ?_, ?_⟩
        · have hmul_le : (p / d) * d ≤ n - 1 - r := by
            rw [Nat.mul_comm (p / d) d]
            omega
          have hle : p / d ≤ (n - 1 - r) / d := (Nat.le_div_iff_mul_le hd).2 hmul_le
          exact List.mem_range.mpr (by omega)
        · rw [Nat.mul_comm (p / d) d]
          omega
      · have hmod'' : p % d = r := by simpa using hmod
        have hmodle : p % d ≤ p := Nat.mod_le p d
        omega


private theorem count_range_nat (n p : Nat) :
    List.count p (List.range n) = if p < n then 1 else 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.count_append, ih, List.count_singleton]
      by_cases hp : p = n
      · subst p; simp
      · have hnp' : n ≠ p := by omega
        have hnep : (n == p) = false := by simpa [hnp']
        rw [hnep]
        by_cases hlt : p < n
        · have hlt1 : p < n + 1 := by omega
          simp [hlt, hlt1]
        · have hlt1 : ¬ p < n + 1 := by omega
          simp [hlt, hlt1]

private theorem sum_indicator_range (m p : Nat) :
    ((List.range m).map (fun r : Nat => if p == r then 1 else 0)).sum =
      if p < m then 1 else 0 := by
  induction m with
  | zero => simp
  | succ m ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      by_cases hpm : p < m
      · have hpm1 : p < m + 1 := by omega
        have hpnm : p ≠ m := by omega
        have hpne : (p == m) = false := by simpa [hpnm]
        simp [hpm, hpm1, hpne]
        omega
      · by_cases hpeq : p = m
        · subst p
          simp
        · have hpne : (p == m) = false := by simpa [hpeq]
          have hpm1 : ¬ p < m + 1 := by omega
          simp [hpm, hpm1, hpne]
          exact hpeq

private theorem count_filter_of_mem {α : Type} [BEq α] [LawfulBEq α]
    (l : List α) (p : α) (f : α → Bool) (hf : f p = true) :
    List.count p (l.filter f) = List.count p l := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      by_cases hx : f x = true
      · rw [List.filter_cons_of_pos hx]
        by_cases hxp : (x == p) = true
        · simp [List.count_cons, ih, hxp]
        · have hxpf : (x == p) = false := by simpa [hxp]
          simp [List.count_cons, ih, hxpf]
      · rw [List.filter_cons_of_neg hx]
        have hxp' : (x == p) = false := by
          by_cases h : (x == p) = true
          · have heq : x = p := by simpa using h
            subst x
            exact False.elim (hx hf)
          · exact Bool.eq_false_of_ne_true h
        simp [List.count_cons, ih, hxp']

private theorem range_perm_by_mod (n d : Nat) (hd : 0 < d) :
    (List.range n).Perm
      ((List.range (min d n)).flatMap
        (fun r : Nat => (List.range n).filter (fun i : Nat => i % d == r))) := by
  apply List.perm_iff_count.mpr
  intro p
  rw [List.count_flatMap]
  change (List.range n).count p =
    ((List.range (min d n)).map
      (fun r : Nat => List.count p ((List.range n).filter (fun i : Nat => i % d == r)))).sum
  rw [count_range_nat]
  by_cases hpn : p < n
  · rw [if_pos hpn]
    have hmodlt : p % d < d := Nat.mod_lt _ hd
    have hmodn : p % d < n := by
      have hle : p % d ≤ p := Nat.mod_le p d
      omega
    have hrmin : p % d < min d n := by omega
    have hfilter : ∀ r : Nat,
        List.count p ((List.range n).filter (fun i : Nat => i % d == r)) =
          if (p % d) == r then 1 else 0 := by
      intro r
      by_cases hr : (p % d) == r
      · have hrmod : p % d = r := by simpa using hr
        have hpr : (fun i : Nat => i % d == r) p = true := by simp [hrmod]
        rw [count_filter_of_mem _ p _ hpr]
        rw [count_range_nat, if_pos hpn, hr]
        simp
      · have hnot : ¬ p ∈ (List.range n).filter (fun i : Nat => i % d == r) := by
          simp only [List.mem_filter, List.mem_range]
          intro hh
          apply hr
          simpa using hh.2.symm
        rw [List.count_eq_zero.mpr hnot]
        simp [hr]
    simp only [hfilter]
    have hsum := sum_indicator_range (min d n) (p % d)
    rw [hsum, if_pos hrmin]
  · rw [if_neg hpn]
    have hnot : ∀ r : Nat,
        List.count p ((List.range n).filter (fun i : Nat => i % d == r)) = 0 := by
      intro r
      apply List.count_eq_zero.mpr
      simp only [List.mem_filter, List.mem_range]
      intro hh
      exact hpn hh.1
    have hmap : ((List.range (min d n)).map
        (fun r : Nat => List.count p ((List.range n).filter (fun i : Nat => i % d == r)))).sum =
        ((List.range (min d n)).map (fun _ : Nat => 0)).sum := by
      apply congrArg List.sum
      apply List.map_congr_left
      intro r hr
      exact hnot r
    rw [hmap]
    symm
    induction (List.range (min d n)) with
    | nil => simp
    | cons x xs ih => simp [ih]


private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun i : Nat => (a.start + i * a.stride, a.base + (i : Int) * a.step))
private def allPoints (ls : List Lane) : List (Nat × Int) := ls.flatMap lanePoints

private theorem flatMap_congr {α β : Type} (l : List α) (f g : α → List β)
    (h : ∀ a ∈ l, f a = g a) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      simp [List.flatMap_cons, h a (by simp), ih (fun b hb => h b (by simp [hb]))]

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem bous_lane_value (c : Config) (r j : Nat) (hr : r < 2 * width c) :
    c.base + c.step * (if r < width c then r else 3 * width c - 1 - r : Nat) +
        (j : Int) * (c.step * (2 * width c)) =
      c.base + c.step *
        (let i := r + j * (2 * width c)
         let k := i / width c
         let rr := i % width c
         (k * width c + (if k % 2 == 0 then rr else width c - 1 - rr) : Nat)) := by
  let w := width c
  have hw : 0 < w := by dsimp [w, width]; omega
  have hi_div_w : (r + j * (2 * w)) / w = 2 * j + r / w := by
    have h1 : r + j * (2 * w) = r + w * (2 * j) := by grind
    rw [h1, Nat.add_mul_div_left r (2 * j) hw]
    omega
  have hi_mod_w : (r + j * (2 * w)) % w = r % w := by
    have h1 : r + j * (2 * w) = r + w * (2 * j) := by grind
    rw [h1, Nat.add_mul_mod_self_left]
  by_cases hrw : r < w
  · have hrw0 : r / w = 0 := Nat.div_eq_of_lt hrw
    have hrwmod : r % w = r := Nat.mod_eq_of_lt hrw
    simp [hi_div_w, hi_mod_w, hrw0, hrwmod, hrw, w]
    grind
  · have hrwge : w ≤ r := by omega
    have hrwlt : r < 2 * w := by simpa [w] using hr
    have hrwdiv : r / w = 1 := by
      apply Nat.div_eq_of_lt_le <;> omega
    have hrwmod : r % w = r - w := by
      have hdec := Nat.mod_add_div r w
      rw [hrwdiv] at hdec
      simp at hdec
      omega
    simp [hi_div_w, hi_mod_w, hrwdiv, hrwmod, hrw, w]
    grind

private theorem bous_rows_eq (c : Config) :
    (rows layout c).Perm (allPoints (layout.lanes c)) := by
  unfold rows
  simp only [layout, Option.map_some]
  rw [filterMap_some_eq_map]
  have hperm := range_perm_by_mod c.n (2 * width c) (by dsimp [width]; omega)
  let G : Nat → Nat × Int := fun i =>
      (i, c.base + c.step *
        (let k := i / width c
         let r := i % width c
         (k * width c + (if k % 2 == 0 then r else width c - 1 - r) : Nat)))
  have hperm' := hperm.map G
  have heq : allPoints (layout.lanes c) =
      List.map G (List.flatMap
        (fun r : Nat => (List.range c.n).filter (fun i : Nat => i % (2 * width c) == r))
        (List.range (min (2 * width c) c.n))) := by
    rw [List.map_flatMap]
    unfold allPoints
    simp only [layout, striped]
    rw [List.flatMap_map]
    apply flatMap_congr
    intro r hrmem
    have hr : r < 2 * width c := by
      have := List.mem_range.mp hrmem
      omega
    have hpos := range_arith_eq_filter_mod c.n r (2 * width c) (by dsimp [width]; omega) hr
    rw [← hpos]
    simp only [List.map_map]
    apply List.map_congr_left
    intro j hj
    dsimp [lanePoints]
    congr 1
    have hbase : c.base + c.step * (if r < width c then r else 2 * width c - 1 - (r - width c)) =
        c.base + c.step * (if r < width c then r else 3 * width c - 1 - r) := by
      by_cases hrw : r < width c
      · simp [hrw]
      · have heq : 2 * width c - 1 - (r - width c) = 3 * width c - 1 - r := by omega
        simp [hrw, heq]
    rw [hbase]
    dsimp [lane]
    simpa using (bous_lane_value c r j hr)
  change (List.map G (List.range c.n)).Perm (allPoints (layout.lanes c))
  rw [heq]
  exact hperm'

end TinylambOpt.ResearchProofs.Atlas.EncodedPages.BoustrophedonLaneEnumeration

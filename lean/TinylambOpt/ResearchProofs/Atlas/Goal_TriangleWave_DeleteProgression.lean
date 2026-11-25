import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_TriangleWave_DeleteProgression

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.TriangleWave

/-! Brute-force sanity check on small configurations. -/
#eval do
  let mut bad : Array (Nat × Nat × Int × Int × Int × Nat × Nat) := #[]
  for n in List.range 13 do
    for p in List.range 6 do
      for b in [-2, 0, 3] do
        for s in [-3, -1, 0, 1, 2] do
          let c : Config := ⟨n, 0, p, b, s, 0, []⟩
          for mlo in List.range (n + 5) do
            for mstep in List.range 5 do
              for mn in List.range (n + 5) do
                let mask : Progression := ⟨mlo - 2, mstep, mn⟩
                let lhs := (((rows layout c).filter (fun r : Nat × Int => !contains mask r.1)).map Prod.snd).sum
                let rhs := ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum
                if lhs != rhs then bad := bad.push (n, p, b, s, mlo - 2, mstep, mn)
  pure bad.toList

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

/-! Count of intersection values when the mask progression is constant. -/
private theorem intersection_count_step_zero (r w aN : Nat) (hw : 0 < w) (b : Progression)
    (hb : b.step = 0) :
    (intersection ⟨r, w, aN⟩ b).n =
      ((List.range aN).map (fun q : Nat => (r : Int) + (q : Int) * (w : Int))).countP
        (fun v => contains b v) := by
  have hw0 : w ≠ 0 := by omega
  have hbcont (v : Int) : contains b v = true ↔ 0 < b.n ∧ v = b.lo := by
    unfold contains lastVal
    simp [hb]
    constructor
    · intro h
      omega
    · intro h
      omega
  by_cases haN : aN = 0
  · simp [intersection, haN]
  by_cases hbn : b.n = 0
  · have hfil : ((List.range aN).map (fun q : Nat => (r : Int) + (q : Int) * (w : Int))).filter
        (fun v => contains b v) = [] := by
      apply List.eq_nil_iff_forall_not_mem.mpr
      intro v hv
      have hv' := List.mem_filter.mp hv
      have hc := (hbcont v).1 hv'.2
      omega
    rw [List.countP_eq_length_filter, hfil]
    simp [intersection, haN, hbn]
  · have hint : intersection ⟨r, w, aN⟩ b =
        ⟨b.lo, 1, if contains ⟨r, w, aN⟩ b.lo then 1 else 0⟩ := by
      simp [intersection, haN, hbn, hb, hw0]
    rw [hint]
    change (if contains ⟨r, w, aN⟩ b.lo = true then 1 else 0) =
      ((List.range aN).map (fun q : Nat => (r : Int) + (q : Int) * (w : Int))).countP
        (fun v => contains b v)
    rw [List.countP_eq_length_filter]
    by_cases hc : contains ⟨r, w, aN⟩ b.lo = true
    · rw [if_pos hc]
      have hmemv : b.lo ∈ (List.range aN).map (fun q : Nat => (r : Int) + (q : Int) * (w : Int)) := by
        exact (mem_progVals ⟨r, w, aN⟩ b.lo).2 hc
      have hfil : ((List.range aN).map (fun q : Nat => (r : Int) + (q : Int) * (w : Int))).filter
          (fun v => contains b v) = [b.lo] := by
        apply list_sorted_eq_of_mem
        · exact (progVals_pairwise ⟨r, w, aN⟩ hw).filter (fun v => contains b v)
        · simp
        · intro v
          constructor
          · intro hv
            have hvEq : v = b.lo := ((hbcont v).1 (List.mem_filter.mp hv).2).2
            simpa [hvEq]
          · intro hv
            have hvEq : v = b.lo := by simpa using hv
            subst v
            exact List.mem_filter.mpr ⟨hmemv, (hbcont b.lo).2 ⟨by omega, rfl⟩⟩
      rw [hfil]
      simp
    · rw [if_neg hc]
      have hnotmem : b.lo ∉ (List.range aN).map
          (fun q : Nat => (r : Int) + (q : Int) * (w : Int)) := by
        intro hm
        have hc' : contains ⟨r, w, aN⟩ b.lo = true :=
          (mem_progVals ⟨r, w, aN⟩ b.lo).1 hm
        exact hc hc'
      have hfil : ((List.range aN).map (fun q : Nat => (r : Int) + (q : Int) * (w : Int))).filter
          (fun v => contains b v) = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro v hv
        have hvEq : v = b.lo := ((hbcont v).1 (List.mem_filter.mp hv).2).2
        exact hnotmem (by simpa [hvEq] using (List.mem_filter.mp hv).1)
      rw [hfil]
      simp

private theorem intersection_count (r w aN : Nat) (hw : 0 < w) (b : Progression) :
    (intersection ⟨r, w, aN⟩ b).n =
      ((List.range aN).map (fun q : Nat => (r : Int) + (q : Int) * (w : Int))).countP
        (fun v => contains b v) := by
  by_cases hb : b.step = 0
  · exact intersection_count_step_zero r w aN hw b hb
  · simpa using (intersection_n_eq_filter_length (⟨r, w, aN⟩ : Progression) b hw
      (Nat.pos_of_ne_zero hb))

private theorem countP_complement (l : List Int) (p : Int → Bool) :
    l.countP p + l.countP (fun v => !p v) = l.length := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x = true <;> simp [h, ih] <;> omega

private theorem map_filter_eq_int (xs : List Nat) (f : Nat → Int) (p : Int → Bool) :
    (xs.map f).filter p = (xs.filter (fun x => p (f x))).map f := by
  induction xs with
  | nil => simp
  | cons x xs ih => by_cases h : p (f x) = true <;> simp [h, ih]

private theorem survivor_count (r w aN : Nat) (hw : 0 < w) (b : Progression) :
    ((List.range aN).filter (fun q => !contains b ((r + q * w : Nat) : Int))).length =
      aN - (intersection ⟨r, w, aN⟩ b).n := by
  have hcast0 : ((List.range aN).filter
      (fun q : Nat => !contains b ((r : Int) + (q : Int) * (w : Int)))).length =
      ((List.range aN).map (fun q : Nat => (r : Int) + (q : Int) * (w : Int))).countP
        (fun v => !contains b v) := by
    induction (List.range aN) with
    | nil => simp
    | cons q qs ih =>
        simp only [List.filter_cons, List.map_cons, List.countP_cons]
        by_cases h : contains b ((r : Int) + (q : Int) * (w : Int)) = true
        · simp [h, ih]
        · simp [h, ih]
  have hcast : ((List.range aN).filter (fun q => !contains b ((r + q * w : Nat) : Int))).length =
      ((List.range aN).map (fun q : Nat => (r : Int) + (q : Int) * (w : Int))).countP
        (fun v => !contains b v) := by
    rw [← hcast0]
    apply congrArg List.length
    apply List.filter_congr
    intro q hq
    have hv : ((r + q * w : Nat) : Int) = (r : Int) + (q : Int) * (w : Int) := by simp
    simpa [hv]
  rw [hcast, intersection_count r w aN hw b]
  have hcomp := countP_complement
    ((List.range aN).map (fun q : Nat => (r : Int) + (q : Int) * (w : Int)))
    (fun v => contains b v)
  have hlen : ((List.range aN).map (fun q : Nat => (r : Int) + (q : Int) * (w : Int))).length = aN := by simp
  omega

private theorem sum_ite_const (xs : List Nat) (P : Nat → Bool) (v : Int) :
    (xs.map (fun q => if P q then v else 0)).sum = ((xs.filter P).length : Int) * v := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases h : P x = true
      · simp [h, ih, Int.add_mul, Int.add_comm, Int.add_left_comm, Int.add_assoc]
      · simp [h, ih]

private theorem const_lane_delete (n r w : Nat) (hw : 0 < w) (v : Int) (mask : Progression) :
    let a := lane n r w v 0
    partialSum a a.n - deletedSum a mask =
      ((a.n - (intersection (positions a) mask).n : Nat) : Int) * v := by
  dsimp
  let aN := (n - r + w - 1) / w
  let o := intersection ⟨r, w, aN⟩ mask
  have hle : o.n ≤ aN := by
    have hc := intersection_count r w aN hw mask
    have hcomp := countP_complement
      ((List.range aN).map (fun q : Nat => (r : Int) + (q : Int) * (w : Int)))
      (fun v => contains mask v)
    have hlen : ((List.range aN).map (fun q : Nat => (r : Int) + (q : Int) * (w : Int))).length = aN := by simp
    change (intersection ⟨r, w, aN⟩ mask).n ≤ aN
    rw [hc]
    omega
  have hcalc : (aN : Int) * v - (o.n : Int) * v = ((aN - o.n : Nat) : Int) * v := by
    rw [Int.ofNat_sub hle, Int.sub_mul]
  rw [show partialSum (lane n r w v 0) (lane n r w v 0).n = (aN : Int) * v by
    simp [partialSum, lane, aN]]
  rw [show deletedSum (lane n r w v 0) mask = (o.n : Int) * v by
    simp [deletedSum, firstAt, slopeAt, lane, aN, positions, o]]
  exact hcalc

private theorem map_filter_map_snd (f : Nat → Int) (p : Nat → Bool) (xs : List Nat) :
    ((xs.map (fun i => (i, f i))).filter (fun r => p r.1)).map Prod.snd = (xs.filter p).map f := by
  induction xs with
  | nil => simp
  | cons i is ih =>
      simp only [List.map_cons, List.filter_cons]
      by_cases h : p i = true <;> simp [h, ih]

private theorem sum_range_min_of_zero {n w : Nat} (S : Nat → Int)
    (hzero : ∀ r, n ≤ r → r < w → S r = 0) :
    ((List.range w).map S).sum = ((List.range (min w n)).map S).sum := by
  induction w with
  | zero => simp
  | succ w ih =>
      by_cases hnw : n ≤ w
      · have hmin1 : min (w + 1) n = n := by omega
        have hmin0 : min w n = n := by omega
        simp [List.range_succ, hmin1, hmin0, hzero w hnw (by omega),
          ih (fun r hnr hr => hzero r hnr (by omega))]
      · have hmin1 : min (w + 1) n = w + 1 := by omega
        have hmin0 : min w n = w := by omega
        simp [List.range_succ, hmin1, hmin0, ih (fun r hnr hr => hzero r hnr (by omega))]

private theorem residue_sum_eq_lane (c : Config) (mask : Progression) (p : Nat) (hp : 0 < p)
    (r : Nat) (hrp : r < p) (hrn : r < c.n) :
    (((List.range c.n).filter (fun i : Nat => i % p == r)).map
        (fun i : Nat => if !contains mask (i : Int) then
          c.base + c.step * Int.ofNat (min (i % p) (p - i % p)) else 0)).sum =
      partialSum (lane c.n r p (c.base + c.step * Int.ofNat (min r (p - r))) 0)
        (lane c.n r p (c.base + c.step * Int.ofNat (min r (p - r))) 0).n -
        deletedSum (lane c.n r p (c.base + c.step * Int.ofNat (min r (p - r))) 0) mask := by
  let aN := (c.n - r + p - 1) / p
  rw [range_filter_mod_eq c.n p r hp hrp]
  simp only [List.map_map]
  have hmap : (List.range aN).map
        ((fun i : Nat => if !contains mask (i : Int) then
          c.base + c.step * Int.ofNat (min (i % p) (p - i % p)) else 0) ∘
          (fun q : Nat => r + q * p)) =
      (List.range aN).map (fun q : Nat =>
        if !contains mask ((r + q * p : Nat) : Int) then
          c.base + c.step * Int.ofNat (min r (p - r)) else 0) := by
    apply List.map_congr_left
    intro q hq
    dsimp [Function.comp_apply]
    have hmq : (r + q * p) % p = r := by
      rw [Nat.add_mul_mod_self_right]
      exact Nat.mod_eq_of_lt hrp
    simp [hmq]
  rw [hmap]
  rw [sum_ite_const]
  rw [survivor_count r p aN hp mask]
  have hconst := const_lane_delete c.n r p hp (c.base + c.step * Int.ofNat (min r (p - r))) mask
  simpa [aN, lane, positions] using hconst.symm

theorem sound_TriangleWave_DeleteProgression : TriangleWave.DeleteProgression.Goal := by
  intro c mask
  let p := 2 * width c
  let n := c.n
  let P : Nat → Bool := fun i => !contains mask (i : Int)
  let F : Nat → Int := fun i => c.base + c.step * Int.ofNat (min (i % p) (p - i % p))
  let S : Nat → Int := fun r =>
    (((List.range n).filter (fun i => i % p == r)).map
      (fun i => if P i then F i else 0)).sum
  let T : Nat → Int := fun r =>
    partialSum (lane n r p (c.base + c.step * Int.ofNat (min r (p - r))) 0)
      (lane n r p (c.base + c.step * Int.ofNat (min r (p - r))) 0).n -
      deletedSum (lane n r p (c.base + c.step * Int.ofNat (min r (p - r))) 0) mask
  have hp : 0 < p := by simp [p, width]
  have hrows : rows layout c =
      (List.range n).map (fun i : Nat => (i, c.base + c.step * Int.ofNat (min (i % p) (p - i % p)))) := by
    dsimp [n, p]
    unfold rows
    simp [TriangleWave.layout]
  have hL0 : (((rows layout c).filter (fun r : Nat × Int => !contains mask r.1)).map Prod.snd).sum =
      (((List.range n).filter P).map F).sum := by
    rw [hrows]
    dsimp [P, F]
    exact congrArg List.sum (map_filter_map_snd (fun i : Nat => c.base + c.step * Int.ofNat (min (i % p) (p - i % p)))
      (fun i : Nat => !contains mask (i : Int)) (List.range n))
  rw [hL0]
  have htoite : (((List.range n).filter P).map F).sum =
      ((List.range n).map (fun i => if P i then F i else 0)).sum := by
    induction (List.range n) with
    | nil => simp
    | cons i is ih => by_cases h : P i = true <;> simp [h, ih]
  rw [htoite]
  have hpart := striped_sum_partition p hp (fun i => if P i then F i else 0) (List.range n)
  change ((List.range n).map (fun i => if P i then F i else 0)).sum =
      ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum
  rw [hpart]
  change ((List.range p).map S).sum =
      ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum
  have hST : ∀ r, r < p → S r = T r := by
    intro r hrw
    by_cases hrn : r < n
    · dsimp [S, T, P, F]
      simpa [p, n] using (residue_sum_eq_lane c mask p hp r (by simpa [p] using hrw)
        (by simpa [n] using hrn))
    · have hnr : n ≤ r := by omega
      have haN0 : (n - r + p - 1) / p = 0 := by
        apply Nat.div_eq_of_lt
        omega
      have hSzero : S r = 0 := by
        dsimp [S]
        rw [range_filter_mod_eq n p r hp hrw, haN0]
        simp
      have hTzero : T r = 0 := by
        dsimp [T]
        simp [partialSum, deletedSum, firstAt, slopeAt, positions, intersection, lane, haN0]
      rw [hSzero, hTzero]
  have hsumST : ((List.range p).map S).sum = ((List.range p).map T).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro r hr
    exact hST r (List.mem_range.mp hr)
  rw [hsumST]
  have hTtrim : ((List.range p).map T).sum = ((List.range (min p n)).map T).sum := by
    apply sum_range_min_of_zero
    intro r hnr hrw
    have haN0 : (n - r + p - 1) / p = 0 := by
      apply Nat.div_eq_of_lt
      omega
    dsimp [T]
    simp [partialSum, deletedSum, firstAt, slopeAt, positions, intersection, lane, haN0]
  rw [hTtrim]
  have hlanes : layout.lanes c =
      (List.range (min p n)).map (fun r => lane n r p (c.base + c.step * Int.ofNat (min r (p - r))) 0) := by
    dsimp [p, n]
    simp [TriangleWave.layout, striped, p]
  rw [hlanes, List.map_map]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  rfl

end TinylambOpt.ResearchProofs.Atlas.Goal_TriangleWave_DeleteProgression

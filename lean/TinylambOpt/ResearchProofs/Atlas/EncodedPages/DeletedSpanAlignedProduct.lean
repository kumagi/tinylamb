import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `DeletedSpan.AlignedProduct` — `∀ c (b : Lane), 0 < b.stride → ((rows layout c).map
  (fun r => ((List.range b.n).map (fun i => if r.1 == b.start+i*b.stride then
  r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum = ((layout.lanes c).map (aligned ·
  b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.DeletedSpanAlignedProduct

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.DeletedSpan

private theorem bezout_spec (a b : Nat) :
    (let p := bezout a b; (p.1 : Int) * (a : Int) + (p.2 : Int) * (b : Int) = (Nat.gcd a b : Int)) := by
  revert a
  refine Nat.strongRecOn b ?_
  intro b ih a
  by_cases hb : b = 0
  · subst b
    simp [bezout]
  · have hlt : a % b < b := Nat.mod_lt a (Nat.pos_of_ne_zero hb)
    have ih' := ih (a % b) hlt b
    have hdiv : a / b * b + a % b = a := by rw [Nat.mul_comm]; exact Nat.div_add_mod a b
    unfold bezout
    rw [dif_neg hb]
    rcases hrec : bezout b (a % b) with ⟨x, y⟩
    rw [hrec] at ih'
    simp only at ih'
    dsimp
    rw [Nat.gcd_comm a b, Nat.gcd_rec]
    rw [← Nat.gcd_comm b (a % b)]
    change (y : Int) * (a : Int) + ((x : Int) - (a / b : Int) * (y : Int)) * (b : Int) = (Nat.gcd b (a % b) : Int)
    rw [← ih']
    have hdivI : ((a / b * b + a % b : Nat) : Int) = (a : Int) := by exact_mod_cast hdiv
    grind

private theorem gcd_div_gcd_div_gcd_one {a b : Nat} (ha : 0 < a) (hb : 0 < b) :
    (a / a.gcd b).gcd (b / a.gcd b) = 1 := by
  let g := a.gcd b
  have hg : 0 < g := Nat.gcd_pos_of_pos_left b ha
  have hga : g ∣ a := Nat.gcd_dvd_left a b
  have hgb : g ∣ b := Nat.gcd_dvd_right a b
  have hmul : g * (a / g).gcd (b / g) = g := by
    rw [← Nat.gcd_mul_left]
    rw [Nat.mul_comm g (a / g), Nat.div_mul_cancel hga]
    rw [Nat.mul_comm g (b / g), Nat.div_mul_cancel hgb]
  exact Nat.eq_of_mul_eq_mul_left hg (by simpa [g] using hmul)

private theorem bezout_unit (A B : Nat) (hB : 0 < B) (hcop : A.gcd B = 1) :
    let p := bezout A B
    ((p.1 : Int) * (A : Int)) % (B : Int) = 1 % (B : Int) := by
  rcases hp : bezout A B with ⟨u, v⟩
  have hspec := bezout_spec A B
  rw [hp] at hspec
  simp only at hspec
  rw [hcop] at hspec
  simp at hspec
  dsimp
  have hbposI : (0 : Int) < (B : Int) := by exact_mod_cast hB
  have hmod : ((u : Int) * (A : Int) + (v : Int) * (B : Int)) % (B : Int) = (1 : Int) % (B : Int) := by rw [hspec]
  rw [Int.add_emod, Int.mul_emod] at hmod
  have hv0 : ((v : Int) * (B : Int)) % (B : Int) = 0 := by
    apply Int.emod_eq_zero_of_dvd
    exact ⟨v, by grind⟩
  rw [hv0] at hmod
  simp at hmod
  rw [← Int.mul_emod] at hmod
  simpa using hmod

-- ---------------------------------------------------------------------------
-- Membership in a progression, as an index existential.
-- ---------------------------------------------------------------------------

private theorem contains_iff_pset (p : Progression) (v : Int) :
    contains p v ↔ ∃ k : Nat, k < p.n ∧ v = p.lo + (k : Int) * p.step := by
  have hform : contains p v ↔ (0 < p.n ∧ p.lo ≤ v ∧ v ≤ lastVal p ∧
      (p.step = 0 ∨ (v - p.lo) % (p.step : Int) = 0)) := by
    simp [contains]
    constructor <;> intro h <;> grind
  rw [hform]
  unfold lastVal
  by_cases hs : p.step = 0
  · simp only [hs, Int.mul_zero, Int.add_zero, or_true, true_and]
    constructor
    · rintro ⟨hn, hlo, hhi⟩
      refine ⟨0, hn, ?_⟩
      omega
    · rintro ⟨k, hk, hv⟩
      exact ⟨by omega, by omega, by omega⟩
  · have hs' : 0 < p.step := Nat.pos_of_ne_zero hs
    have hsI : (0 : Int) < (p.step : Int) := by exact_mod_cast hs'
    have hsnonneg : (0 : Int) ≤ (p.step : Int) := by omega
    constructor
    · rintro ⟨hn, hlo, hhi, hmod0⟩
      have hmod : (v - p.lo) % (p.step : Int) = 0 := by
        rcases hmod0 with h | h
        · exact (hs h).elim
        · exact h
      have hdvd : (p.step : Int) ∣ v - p.lo := Int.dvd_of_emod_eq_zero hmod
      rcases hdvd with ⟨q, hq0⟩
      have hstepq : (p.step : Int) * q = v - p.lo := by grind
      have hstepq' : (v - p.lo) = (p.step : Int) * q := hstepq.symm
      have hqnonneg : 0 ≤ q := by
        have htmp0 : (0 : Int) ≤ (p.step : Int) * q := by omega
        have htmp : (p.step : Int) * 0 ≤ (p.step : Int) * q := by simpa using htmp0
        exact Int.le_of_mul_le_mul_left htmp hsI
      have hcomm : ((p.n - 1 : Nat) : Int) * (p.step : Int) = (p.step : Int) * ((p.n - 1 : Nat) : Int) := by grind
      have hhi' : v - p.lo ≤ (p.step : Int) * ((p.n - 1 : Nat) : Int) := by
        rw [← hcomm]
        omega
      have hle : (p.step : Int) * q ≤ (p.step : Int) * ((p.n - 1 : Nat) : Int) := by omega
      have hqle : q ≤ ((p.n - 1 : Nat) : Int) := Int.le_of_mul_le_mul_left hle hsI
      have hqcast : ((p.n - 1 : Nat) : Int) = (p.n : Int) - 1 := by omega
      have hqnI : q < (p.n : Int) := by omega
      refine ⟨q.toNat, ?_, ?_⟩
      · have hqt : (q.toNat : Int) < (p.n : Int) := by simpa [Int.toNat_of_nonneg hqnonneg] using hqnI
        exact_mod_cast hqt
      · rw [Int.toNat_of_nonneg hqnonneg]
        rw [show q * (p.step : Int) = (p.step : Int) * q by grind]
        omega
    · rintro ⟨k, hk, rfl⟩
      have hknI : (k : Int) < (p.n : Int) := by exact_mod_cast hk
      have hn : 0 < p.n := by omega
      have hkI0 : (0 : Int) ≤ (k : Int) := by exact_mod_cast (Nat.zero_le k)
      have hkstep : 0 ≤ (k : Int) * (p.step : Int) := Int.mul_nonneg hkI0 hsnonneg
      have hkle : k ≤ p.n - 1 := by omega
      have hkleI : (k : Int) ≤ ((p.n - 1 : Nat) : Int) := by exact_mod_cast hkle
      have hmul : (k : Int) * (p.step : Int) ≤ ((p.n - 1 : Nat) : Int) * (p.step : Int) :=
        Int.mul_le_mul_of_nonneg_right hkleI hsnonneg
      have hmod : ((p.lo + (k : Int) * (p.step : Int) - p.lo) % (p.step : Int)) = 0 := by
        apply Int.emod_eq_zero_of_dvd
        exact ⟨(k : Int), by grind⟩
      refine ⟨hn, ?_, ?_, Or.inr hmod⟩
      · omega
      · omega



-- ---------------------------------------------------------------------------
-- The intersection of two positive-step progressions: index characterization.
-- ---------------------------------------------------------------------------

private theorem int_dvd_of_dvd_nat {m n : Nat} (h : m ∣ n) : (m : Int) ∣ (n : Int) := by
  rcases h with ⟨k, rfl⟩
  exact ⟨(k : Int), by grind⟩


private theorem dvd_sub_int {m x y : Int} (hx : m ∣ x) (hy : m ∣ y) : m ∣ x - y := by
  rcases hx with ⟨a, ha⟩
  rcases hy with ⟨b, hb⟩
  exact ⟨a - b, by grind⟩

private theorem pset_intersection (a b : Progression) (ha : 0 < a.step) (hb : 0 < b.step) (v : Int) :
    (∃ k : Nat, k < (intersection a b).n ∧ v = (intersection a b).lo + (k : Int) * (intersection a b).step) ↔
      (∃ i : Nat, i < a.n ∧ v = a.lo + (i : Int) * a.step) ∧
      (∃ j : Nat, j < b.n ∧ v = b.lo + (j : Int) * b.step) := by
  let g := a.step.gcd b.step
  let A := a.step / g
  let B := b.step / g
  let P := A * b.step
  let gap := b.lo - a.lo
  let low := max a.lo b.lo
  let high := min (lastVal a) (lastVal b)
  have hg : 0 < g := by dsimp [g]; exact Nat.gcd_pos_of_pos_left b.step ha
  have hA : 0 < A := by
    dsimp [A, g]
    apply Nat.div_pos
    · exact Nat.le_of_dvd ha (Nat.gcd_dvd_left a.step b.step)
    · exact Nat.gcd_pos_of_pos_left b.step ha
  have hB : 0 < B := by
    dsimp [B, g]
    apply Nat.div_pos
    · exact Nat.le_of_dvd hb (Nat.gcd_dvd_right a.step b.step)
    · exact Nat.gcd_pos_of_pos_left b.step ha
  have hP : 0 < P := by dsimp [P]; exact Nat.mul_pos hA hb
  have hAst : g * A = a.step := by
    dsimp [g, A]
    rw [Nat.mul_comm, Nat.div_mul_cancel (Nat.gcd_dvd_left a.step b.step)]
  have hBst : g * B = b.step := by
    dsimp [g, B]
    rw [Nat.mul_comm, Nat.div_mul_cancel (Nat.gcd_dvd_right a.step b.step)]
  have hP1 : P = a.step * B := by
    dsimp [P]
    calc
      A * b.step = A * (g * B) := by rw [hBst]
      _ = (g * A) * B := by grind
      _ = a.step * B := by rw [hAst]
  have hP2 : P = A * b.step := rfl
  have hcop : A.gcd B = 1 := by
    dsimp [A, B, g]
    exact gcd_div_gcd_div_gcd_one ha hb
  have hgI : (0 : Int) < (g : Int) := by exact_mod_cast hg
  have hPI : (0 : Int) < (P : Int) := by exact_mod_cast hP
  have hastep : a.step ≠ 0 := by omega
  have hbstep : b.step ≠ 0 := by omega
  by_cases ha0 : a.n = 0
  · simp [intersection, hastep, ha0]
  by_cases hb0 : b.n = 0
  · simp [intersection, hbstep, hb0]
  by_cases hgap : gap % (g : Int) = 0
  · change (b.lo - a.lo) % (a.step.gcd b.step : Int) = 0 at hgap
    let q : Int := gap / (g : Int)
    let u : Int := (bezout A B).1
    let w : Int := (bezout A B).2
    let t : Int := (q * u) % (B : Int)
    let root : Int := a.lo + (a.step : Int) * t
    let first : Int := low + (root - low) % (P : Int)
    let cnt : Nat := if first ≤ high then ((high - first) / (P : Int)).toNat + 1 else 0
    have hqg : q * (g : Int) = gap := by
      dsimp [q]
      exact Int.ediv_mul_cancel (Int.dvd_of_emod_eq_zero hgap)
    have hbez : u * (A : Int) + w * (B : Int) = 1 := by
      have hs := bezout_spec A B
      dsimp [u, w]
      rw [hs, hcop]
      simp
    have ht : (q * u) = (B : Int) * ((q * u) / (B : Int)) + t := by
      dsimp [t]
      rw [Int.mul_ediv_add_emod]
    have hdt : (B : Int) ∣ (A : Int) * t - q := by
      refine ⟨-q * w - (A : Int) * ((q * u) / (B : Int)), ?_⟩
      rw [show t = q * u - (B : Int) * ((q * u) / (B : Int)) by omega]
      grind
    have hroot_b : (b.step : Int) ∣ root - b.lo := by
      rcases hdt with ⟨k, hk⟩
      refine ⟨k, ?_⟩
      dsimp [root, gap] at *
      have hasting : (a.step : Int) = (g : Int) * (A : Int) := by rw [← hAst]; simp
      have hbsting : (b.step : Int) = (g : Int) * (B : Int) := by rw [← hBst]; simp
      grind
    have hroot_a : (a.step : Int) ∣ root - a.lo := by
      refine ⟨t, ?_⟩
      dsimp [root]
      grind
    have hfirst_root : (P : Int) ∣ first - root := by
      refine ⟨-((root - low) / (P : Int)), ?_⟩
      dsimp [first]
      have heq := Int.mul_ediv_add_emod (root - low) (P : Int)
      grind
    have hP_a : (P : Int) = (a.step : Int) * (B : Int) := by
      have hh := congrArg (fun n : Nat => (n : Int)) hP1
      simpa using hh
    have hP_b : (P : Int) = (A : Int) * (b.step : Int) := by
      have hh := congrArg (fun n : Nat => (n : Int)) hP2
      simpa using hh
    have hfirst_a : (a.step : Int) ∣ first - a.lo := by
      rcases hfirst_root with ⟨k, hk⟩
      refine ⟨(B : Int) * k + t, ?_⟩
      rw [hP_a] at hk
      dsimp [root] at hk
      grind
    have hfirst_b : (b.step : Int) ∣ first - b.lo := by
      rcases hfirst_root with ⟨k, hk⟩
      rcases hroot_b with ⟨m, hm⟩
      refine ⟨(A : Int) * k + m, ?_⟩
      rw [hP_b] at hk
      grind
    have hfirst_low : low ≤ first := by
      dsimp [first]
      have hnonneg := Int.emod_nonneg (root - low) (by omega : (P : Int) ≠ 0)
      omega
    have hfirst_lt : first < low + (P : Int) := by
      dsimp [first]
      have hlt := Int.emod_lt_of_pos (root - low) hPI
      omega
    have hinter_eq : intersection a b = ⟨first, P, cnt⟩ := by
      simp [intersection, ha0, hb0, hastep, hbstep, hgap]
      dsimp [g, A, B, P, gap, low, high, q, u, t, root, first, cnt]
      simp
    rw [hinter_eq]
    constructor
    · rintro ⟨k, hk, hv⟩
      have hkcnt : k < cnt := hk
      have hcntpos : 0 < cnt := by omega
      have hfirsthigh : first ≤ high := by
        dsimp [cnt] at hcntpos
        split at hcntpos
        · assumption
        · omega
      have hk_le : k ≤ ((high - first) / (P : Int)).toNat := by
        dsimp [cnt] at hkcnt
        split at hkcnt
        · omega
        · omega
      have hkI : (k : Int) ≤ ((high - first) / (P : Int)).toNat := by exact_mod_cast hk_le
      have hkcast : (((high - first) / (P : Int)).toNat : Int) = (high - first) / (P : Int) := by
        apply Int.toNat_of_nonneg
        apply Int.ediv_nonneg
        · omega
        · omega
      have hmul : (k : Int) * (P : Int) ≤ high - first := by
        rw [hkcast] at hkI
        exact (Int.le_ediv_iff_mul_le hPI).mp hkI
      have hv_le : v ≤ high := by
        dsimp at hv
        omega
      have hv_ge : low ≤ v := by
        dsimp at hv
        have hkI0 : (0 : Int) ≤ (k : Int) := by exact_mod_cast (Nat.zero_le k)
        have : 0 ≤ (k : Int) * (P : Int) := Int.mul_nonneg hkI0 (by omega : (0 : Int) ≤ (P : Int))
        omega
      have hv_a : (a.step : Int) ∣ v - a.lo := by
        dsimp at hv
        rw [show v - a.lo = (first - a.lo) + (k : Int) * (P : Int) by omega]
        rw [hP_a]
        rcases hfirst_a with ⟨m, hm⟩
        exact ⟨m + (k : Int) * (B : Int), by grind⟩
      have hv_b : (b.step : Int) ∣ v - b.lo := by
        dsimp at hv
        rw [show v - b.lo = (first - b.lo) + (k : Int) * (P : Int) by omega]
        rw [hP_b]
        rcases hfirst_b with ⟨m, hm⟩
        exact ⟨m + (k : Int) * (A : Int), by grind⟩
      constructor
      · rcases hv_a with ⟨i0, hi0⟩
        have hi0nonneg : 0 ≤ i0 := by
          have hi0tmp : (a.step : Int) * 0 ≤ (a.step : Int) * i0 := by
            rw [Int.mul_zero]
            omega
          exact Int.le_of_mul_le_mul_left hi0tmp (by exact_mod_cast ha)
        have hi0le : i0 ≤ ((a.n - 1 : Nat) : Int) := by
          have hlast : lastVal a = a.lo + ((a.n - 1 : Nat) : Int) * (a.step : Int) := rfl
          have hvlast : v ≤ a.lo + ((a.n - 1 : Nat) : Int) * (a.step : Int) := by
            dsimp [high] at hv_le
            omega
          have : (a.step : Int) * i0 ≤ (a.step : Int) * ((a.n - 1 : Nat) : Int) := by
            rw [← hi0]
            rw [show (a.step : Int) * ((a.n - 1 : Nat) : Int) = ((a.n - 1 : Nat) : Int) * (a.step : Int) by grind]
            omega
          exact Int.le_of_mul_le_mul_left this (by exact_mod_cast ha)
        have hi0n : i0 < (a.n : Int) := by
          have hcast : ((a.n - 1 : Nat) : Int) = (a.n : Int) - 1 := by omega
          omega
        refine ⟨i0.toNat, ?_, ?_⟩
        · have : (i0.toNat : Int) < (a.n : Int) := by simpa [Int.toNat_of_nonneg hi0nonneg] using hi0n
          exact_mod_cast this
        · rw [Int.toNat_of_nonneg hi0nonneg]
          rw [show i0 * (a.step : Int) = (a.step : Int) * i0 by grind]
          omega
      · rcases hv_b with ⟨j0, hj0⟩
        have hj0nonneg : 0 ≤ j0 := by
          have hj0tmp : (b.step : Int) * 0 ≤ (b.step : Int) * j0 := by
            rw [Int.mul_zero]
            omega
          exact Int.le_of_mul_le_mul_left hj0tmp (by exact_mod_cast hb)
        have hj0le : j0 ≤ ((b.n - 1 : Nat) : Int) := by
          have hvlast : v ≤ b.lo + ((b.n - 1 : Nat) : Int) * (b.step : Int) := by
            dsimp [high, lastVal] at hv_le
            omega
          have : (b.step : Int) * j0 ≤ (b.step : Int) * ((b.n - 1 : Nat) : Int) := by
            rw [← hj0]
            rw [show (b.step : Int) * ((b.n - 1 : Nat) : Int) = ((b.n - 1 : Nat) : Int) * (b.step : Int) by grind]
            omega
          exact Int.le_of_mul_le_mul_left this (by exact_mod_cast hb)
        have hj0n : j0 < (b.n : Int) := by
          have hcast : ((b.n - 1 : Nat) : Int) = (b.n : Int) - 1 := by omega
          omega
        refine ⟨j0.toNat, ?_, ?_⟩
        · have : (j0.toNat : Int) < (b.n : Int) := by simpa [Int.toNat_of_nonneg hj0nonneg] using hj0n
          exact_mod_cast this
        · rw [Int.toNat_of_nonneg hj0nonneg]
          rw [show j0 * (b.step : Int) = (b.step : Int) * j0 by grind]
          omega
    · rintro ⟨⟨i, hi, hvi⟩, ⟨j, hj, hvj⟩⟩
      have hlow_a : a.lo ≤ max a.lo b.lo := Int.le_max_left _ _
      have hlow_b : b.lo ≤ max a.lo b.lo := Int.le_max_right _ _
      have hhigh_a : min (lastVal a) (lastVal b) ≤ lastVal a := Int.min_le_left _ _
      have hhigh_b : min (lastVal a) (lastVal b) ≤ lastVal b := Int.min_le_right _ _
      have hia_nonneg : 0 ≤ (i : Int) * (a.step : Int) :=
        Int.mul_nonneg (by exact_mod_cast (Nat.zero_le i)) (by omega)
      have hjb_nonneg : 0 ≤ (j : Int) * (b.step : Int) :=
        Int.mul_nonneg (by exact_mod_cast (Nat.zero_le j)) (by omega)
      have hi_le : (i : Int) ≤ ((a.n - 1 : Nat) : Int) := by omega
      have hj_le : (j : Int) ≤ ((b.n - 1 : Nat) : Int) := by omega
      have hi_mul : (i : Int) * (a.step : Int) ≤ ((a.n - 1 : Nat) : Int) * (a.step : Int) :=
        Int.mul_le_mul_of_nonneg_right hi_le (by omega)
      have hj_mul : (j : Int) * (b.step : Int) ≤ ((b.n - 1 : Nat) : Int) * (b.step : Int) :=
        Int.mul_le_mul_of_nonneg_right hj_le (by omega)
      have hvlow : low ≤ v := by dsimp [low] at hlow_a hlow_b ⊢; omega
      have hvhigh : v ≤ high := by dsimp [high] at hhigh_a hhigh_b ⊢; dsimp [lastVal] at hhigh_a hhigh_b ⊢; omega
      have hvroot_a : (a.step : Int) ∣ v - root := by
        rw [show v - root = (v - a.lo) - (root - a.lo) by omega]
        exact dvd_sub_int ⟨i, by rw [hvi]; grind⟩ hroot_a
      have hvroot_b : (b.step : Int) ∣ v - root := by
        rw [show v - root = (v - b.lo) - (root - b.lo) by omega]
        exact dvd_sub_int ⟨j, by rw [hvj]; grind⟩ hroot_b
      have hvroot : (P : Int) ∣ v - root := by
        rcases hvroot_a with ⟨ia, hia⟩
        rcases hvroot_b with ⟨ib, hib⟩
        rw [hP_a]
        have hasting' : (a.step : Int) = (g : Int) * (A : Int) := by rw [← hAst]; simp
        have hbsting' : (b.step : Int) = (g : Int) * (B : Int) := by rw [← hBst]; simp
        rw [hasting'] at hia
        rw [hbsting'] at hib
        have hcancel : (A : Int) * ia = (B : Int) * ib := by
          apply Int.eq_of_mul_eq_mul_left (a := (g : Int)) (by omega : (g : Int) ≠ 0)
          grind
        refine ⟨w * ia + u * ib, ?_⟩
        grind
      have hvfirst : (P : Int) ∣ v - first := by
        rw [show v - first = v - root - (first - root) by grind]
        exact dvd_sub_int (m := (P : Int)) hvroot hfirst_root
      rcases hvfirst with ⟨k0, hk0⟩
      have hk0nonneg : 0 ≤ k0 := by
        by_cases hnon : 0 ≤ k0
        · exact hnon
        · exfalso
          have hk0le : k0 ≤ -1 := by omega
          have hkmul : (P : Int) * k0 ≤ -(P : Int) := by
            have htmp := Int.mul_le_mul_of_nonneg_left hk0le (by omega : (0 : Int) ≤ (P : Int))
            grind
          have hvle : v ≤ first - (P : Int) := by
            rw [show v = first + (P : Int) * k0 by grind]
            omega
          omega
      have hk0le : (k0 : Int) ≤ (high - first) / (P : Int) := by
        apply (Int.le_ediv_iff_mul_le hPI).mpr
        rw [show v = first + (P : Int) * k0 by grind] at hvhigh
        have hvhigh' : first + k0 * (P : Int) ≤ high := by grind
        omega
      have hk0ltI : (k0 : Int) < ((high - first) / (P : Int) : Int) + 1 := by omega
      have hvfirst_le : first ≤ v := by
        rw [show v = first + (P : Int) * k0 by grind]
        have : 0 ≤ (P : Int) * k0 := Int.mul_nonneg (by omega) hk0nonneg
        omega
      have hcntpos : 0 < cnt := by
        have hfirsthigh : first ≤ high := by omega
        dsimp [cnt]
        rw [if_pos hfirsthigh]
        omega
      refine ⟨k0.toNat, ?_, ?_⟩
      · have hfirsthigh : first ≤ high := by omega
        have hdivnonneg : 0 ≤ (high - first) / (P : Int) := Int.ediv_nonneg (by omega) (by omega : (0 : Int) ≤ (P : Int))
        have hcast : (((high - first) / (P : Int)).toNat : Int) = (high - first) / (P : Int) :=
          Int.toNat_of_nonneg hdivnonneg
        have hk0cast : (k0.toNat : Int) = k0 := Int.toNat_of_nonneg hk0nonneg
        have hk0le_toNat : k0.toNat ≤ ((high - first) / (P : Int)).toNat := by
          have hleInt : (k0.toNat : Int) ≤ (((high - first) / (P : Int)).toNat : Int) := by
            rw [hk0cast, hcast]
            exact hk0le
          exact_mod_cast hleInt
        dsimp [cnt]
        rw [if_pos hfirsthigh]
        omega
      · dsimp
        have hk0cast : (k0.toNat : Int) = k0 := Int.toNat_of_nonneg hk0nonneg
        rw [show (k0.toNat : Int) = k0 from hk0cast]
        grind
  · have hgap_ne : gap % (g : Int) ≠ 0 := hgap
    change (b.lo - a.lo) % (a.step.gcd b.step : Int) ≠ 0 at hgap_ne
    have hinter_empty : intersection a b = ⟨0, 1, 0⟩ := by
      simp [intersection, ha0, hb0, hastep, hbstep, hgap_ne]
    rw [hinter_empty]
    constructor
    · rintro ⟨k, hk, _⟩
      change k < 0 at hk
      omega
    · rintro ⟨⟨i, hi, hvi⟩, ⟨j, hj, hvj⟩⟩
      have hdvd : (g : Int) ∣ gap := by
        refine ⟨(i : Int) * (A : Int) - (j : Int) * (B : Int), ?_⟩
        dsimp [gap]
        have hasting : (a.step : Int) = (g : Int) * (A : Int) := by rw [← hAst]; simp
        have hbsting : (b.step : Int) = (g : Int) * (B : Int) := by rw [← hBst]; simp
        have hvEq : a.lo + (i : Int) * (a.step : Int) = b.lo + (j : Int) * (b.step : Int) := by rw [← hvi, ← hvj]
        have hEq : b.lo - a.lo = (i : Int) * (a.step : Int) - (j : Int) * (b.step : Int) := by omega
        rw [hEq, hasting, hbsting]
        grind
      exact (hgap_ne (Int.emod_eq_zero_of_dvd hdvd)).elim



-- ---------------------------------------------------------------------------
-- Enumerating a progression by index.
-- ---------------------------------------------------------------------------


private theorem list_sum_perm_int {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private def pvals (p : Progression) : List Int :=
  (List.range p.n).map (fun k : Nat => p.lo + (k : Int) * p.step)

private theorem mem_pvals_pset (p : Progression) (v : Int) :
    v ∈ pvals p ↔ ∃ k : Nat, k < p.n ∧ v = p.lo + (k : Int) * p.step := by
  constructor
  · intro h
    rcases List.mem_map.mp h with ⟨k, hk, hv⟩
    exact ⟨k, List.mem_range.mp hk, hv.symm⟩
  · rintro ⟨k, hk, hv⟩
    apply List.mem_map.mpr
    exact ⟨k, List.mem_range.mpr hk, hv.symm⟩

private theorem pvals_nodup (p : Progression) (hs : 0 < p.step) : (pvals p).Nodup := by
  rw [List.nodup_iff_pairwise_ne]
  unfold pvals
  refine List.Pairwise.map (fun k : Nat => p.lo + (k : Int) * p.step) ?_ List.nodup_range
  intro k m hkm heq
  have hcast : (k : Int) * (p.step : Int) = (m : Int) * (p.step : Int) := by omega
  have hkmeq : (k : Int) = (m : Int) :=
    Int.eq_of_mul_eq_mul_right (by exact_mod_cast (Nat.ne_of_gt hs)) hcast
  have hkmeqNat : k = m := by exact_mod_cast hkmeq
  exact hkm hkmeqNat

private theorem nodup_filter {α : Type} (p : α → Bool) {l : List α} (h : l.Nodup) :
    (l.filter p).Nodup := by
  rw [List.nodup_iff_pairwise_ne] at h ⊢
  exact h.filter p

private theorem intersection_step_pos (a b : Progression) (ha : 0 < a.step) (hb : 0 < b.step) :
    0 < (intersection a b).step ∨ (intersection a b).n = 0 := by
  by_cases hzero : a.n = 0 ∨ b.n = 0
  · right
    simp [intersection, hzero]
  · left
    let g := a.step.gcd b.step
    let A := a.step / g
    have hg : 0 < g := by dsimp [g]; exact Nat.gcd_pos_of_pos_left b.step ha
    have hA : 0 < A := by
      dsimp [A, g]
      apply Nat.div_pos
      · exact Nat.le_of_dvd ha (Nat.gcd_dvd_left a.step b.step)
      · exact hg
    have hastep : a.step ≠ 0 := by omega
    have hbstep : b.step ≠ 0 := by omega
    by_cases hgap : (b.lo - a.lo) % (g : Int) = 0
    · change (b.lo - a.lo) % (a.step.gcd b.step : Int) = 0 at hgap
      have hstep : (intersection a b).step = A * b.step := by
        simp [intersection, hzero, hastep, hbstep, hgap]
        dsimp [A, g]
      rw [hstep]
      exact Nat.mul_pos hA hb
    · change (b.lo - a.lo) % (a.step.gcd b.step : Int) ≠ 0 at hgap
      have hstep : (intersection a b).step = 1 := by
        simp [intersection, hzero, hastep, hbstep, hgap]
      rw [hstep]
      omega

private theorem pvals_intersection_perm (a b : Progression) (ha : 0 < a.step) (hb : 0 < b.step) :
    (pvals (intersection a b)).Perm ((pvals a).filter (fun v => contains b v)) := by
  have hnodupI : (pvals (intersection a b)).Nodup := by
    rcases intersection_step_pos a b ha hb with hs | hn
    · exact pvals_nodup _ hs
    · simp [pvals, hn]
  have hnodupF : ((pvals a).filter (fun v => contains b v)).Nodup :=
    nodup_filter _ (pvals_nodup a ha)
  apply (List.perm_ext_iff_of_nodup hnodupI hnodupF).mpr
  intro v
  rw [mem_pvals_pset, List.mem_filter, mem_pvals_pset]
  rw [contains_iff_pset]
  exact pset_intersection a b ha hb v

private theorem pvals_inter_length (a b : Progression) (ha : 0 < a.step) (hb : 0 < b.step) :
    (intersection a b).n = ((pvals a).filter (fun v => contains b v)).length := by
  have hp := pvals_intersection_perm a b ha hb
  have hlen := List.Perm.length_eq hp
  simpa [pvals] using hlen

private theorem pvals_inter_sum (a b : Progression) (ha : 0 < a.step) (hb : 0 < b.step)
    (f : Int → Int) :
    ((pvals (intersection a b)).map f).sum = (((pvals a).filter (fun v => contains b v)).map f).sum := by
  exact list_sum_perm_int ((pvals_intersection_perm a b ha hb).map f)

private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun i : Nat => (a.start + i * a.stride, a.base + (i : Int) * a.step))


private theorem filterMap_congr' {α β : Type} {l : List α} {f g : α → Option β}
    (h : ∀ x ∈ l, f x = g x) : l.filterMap f = l.filterMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filterMap_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]



-- ---------------------------------------------------------------------------
-- Closed forms for sums along a lane.
-- ---------------------------------------------------------------------------

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
    have hle : k ≤ k * k := by
      have := Nat.mul_le_mul_left k (show 1 ≤ k by omega)
      simpa using this
    have hcan : k * k - k + k = k * k := Nat.sub_add_cancel hle
    have hEq : (k + 1) * k = k * (k - 1) + 2 * k := by rw [hmul1, hmul2]; omega
    rw [hEq]
    exact Nat.add_mul_div_left (k * (k - 1)) k (by decide : 0 < 2)

private theorem tri_succ (k : Nat) : tri (k + 1) = tri k + (k : Int) := by
  dsimp [tri]
  rw [choose2_succ]
  simp [Int.natCast_add]

private theorem range_succ_map_sum (f : Nat → Int) (n : Nat) :
    ((List.range (n+1)).map f).sum = ((List.range n).map f).sum + f n := by
  rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
  simp

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
      rw [range_succ_map_sum]
      rw [partialSum_succ, ih]

private theorem sum_arith (lo step : Int) (n : Nat) :
    ((List.range n).map (fun k : Nat => lo + (k : Int) * step)).sum =
      (n : Int) * lo + tri n * step := by
  induction n with
  | zero => simp [tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ n ih =>
      rw [range_succ_map_sum, ih, tri_succ]
      simp [Int.natCast_add]
      grind

private def laneValueAt (a : Lane) (v : Int) : Int :=
  a.base + (v - (a.start : Int)) * a.step

private theorem sum_laneValues_progression (a : Lane) (hs : a.stride = 1) (r : Progression) :
    ((pvals r).map (fun v => laneValueAt a v)).sum =
      (r.n : Int) * firstAt a r + tri r.n * slopeAt a r := by
  unfold pvals laneValueAt firstAt slopeAt
  rw [List.map_map]
  change ((List.range r.n).map (fun k : Nat =>
      a.base + ((r.lo + (k : Int) * r.step) - (a.start : Int)) * a.step)).sum = _
  have hfun : ∀ k : Nat,
      a.base + ((r.lo + (k : Int) * r.step) - (a.start : Int)) * a.step =
        (a.base + (r.lo - (a.start : Int)) * a.step) + (k : Int) * ((r.step : Int) * a.step) := by
    intro k
    grind
  rw [show ((List.range r.n).map (fun k : Nat =>
      a.base + ((r.lo + (k : Int) * r.step) - (a.start : Int)) * a.step)) =
      ((List.range r.n).map (fun k : Nat =>
        (a.base + (r.lo - (a.start : Int)) * a.step) + (k : Int) * ((r.step : Int) * a.step))) by
    apply List.map_congr_left
    intro k hk
    exact hfun k]
  rw [sum_arith]
  simp [firstAt, slopeAt, hs, Int.ediv_one]
private theorem lanePoints_positions (a : Lane) (hs : a.stride = 1) :
    (lanePoints a).map (fun r => (r.1 : Int)) = pvals (positions a) := by
  unfold lanePoints pvals positions
  rw [List.map_map]
  apply List.map_congr_left
  intro i hi
  simp [hs, Int.mul_comm]

private theorem range_filter_eq_single_sum (n t : Nat) (f : Nat → Int) :
    (((List.range n).filter (fun i => i == t)).map f).sum = if t < n then f t else 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append]
      by_cases h : n == t
      · have hnt : n = t := Nat.eq_of_beq_eq_true h
        subst t
        simp [ih]
      · have hne : n ≠ t := by intro he; exact h (by rw [he]; simp)
        have htlt : t < n ∨ n < t := by omega
        rcases htlt with ht | ht
        · have hif : t < n + 1 := by omega
          simp [h, ih, ht, hif]
        · have hif : ¬ t < n + 1 := by omega
          simp [h, ih, show ¬ t < n from by omega, hif]
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
private theorem sum_range_product_linear (n : Nat) (x y dx dy : Int) :
    ((List.range n).map (fun k : Nat => (x + (k : Int) * dx) * (y + (k : Int) * dy))).sum =
      (n : Int) * x * y + tri n * (x * dy + y * dx) + squares n * dx * dy := by
  induction n with
  | zero => simp [tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ n ih =>
      rw [range_succ_map_sum, ih, tri_succ, squares_succ]
      simp [Int.natCast_add, Int.add_mul]
      grind
private theorem filterMap_range_split {α β : Type} (n s : Nat) (f g : Nat → Option β) :
    (List.range n).filterMap (fun i => if i < s then f i else g i) =
      (List.range (min n s)).filterMap f ++
        (List.range (n - s)).filterMap (fun j => g (s + j)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filterMap_append, ih]
      by_cases h : n < s
      · have hmins : min n s = n := Nat.min_eq_left (by omega)
        have hmins' : min (n + 1) s = n + 1 := Nat.min_eq_left (by omega)
        have hz : n + 1 - s = 0 := by omega
        have hz' : n - s = 0 := by omega
        cases hfn : f n with
        | none => simp [h, hfn, hmins, hmins', hz, hz', List.range_succ, List.filterMap_append]
        | some x => simp [h, hfn, hmins, hmins', hz, hz', List.range_succ, List.filterMap_append]
      · have hmins : min n s = s := Nat.min_eq_right (by omega)
        have hmins' : min (n + 1) s = s := Nat.min_eq_right (by omega)
        have hs : n + 1 - s = (n - s) + 1 := by omega
        have hs2 : s + (n - s) = n := by omega
        cases hgn : g n with
        | none => simp [h, hgn, hmins, hmins', hs, hs2, List.range_succ, List.filterMap_append]
        | some x => simp [h, hgn, hmins, hmins', hs, hs2, List.range_succ, List.filterMap_append]

private theorem filterMap_none {α β : Type} (l : List α) :
    l.filterMap (fun _ : α => (none : Option β)) = [] := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]
private theorem rows_lanes (c : Config) :
    rows layout c =
      lanePoints (span c 0 c.split c.base c.step) ++
      lanePoints (span c (c.split + width c) c.n
        (c.base + c.step * ((c.split + width c : Nat) : Int)) c.step) := by
  unfold rows
  have hread : (List.range c.n).filterMap
      (fun i => (layout.read c i).map (fun v => (i, v))) =
      (List.range c.n).filterMap (fun i =>
        if i < c.split then some (i, c.base + c.step * (i : Int))
        else if i < c.split + width c then none
        else some (i, c.base + c.step * (i : Int))) := by
    apply filterMap_congr'
    intro i hi
    simp only [layout]
    by_cases h1 : i < c.split <;> by_cases h2 : i < c.split + width c <;> simp [h1, h2] <;> omega
  rw [hread]
  rw [filterMap_range_split (α := Nat) (β := Nat × Int) c.n c.split
      (fun i : Nat => some (i, c.base + c.step * (i : Int)))
      (fun i => if i < c.split + width c then none
        else some (i, c.base + c.step * (i : Int)))]
  rw [filterMap_some_eq_map]
  have hfilter : (List.range (c.n - c.split)).filterMap
      (fun j => if c.split + j < c.split + width c then none
        else some (c.split + j, c.base + c.step * ((c.split + j : Nat) : Int))) =
      (List.range (c.n - c.split)).filterMap
      (fun j => if j < width c then none
        else some (c.split + j, c.base + c.step * ((c.split + j : Nat) : Int))) := by
    apply filterMap_congr'
    intro j hj
    have heq : (c.split + j < c.split + width c) = (j < width c) := by
      rw [Nat.add_lt_add_iff_left]
    by_cases h : j < width c <;> simp [h, heq]
  rw [hfilter]
  rw [filterMap_range_split (α := Nat) (β := Nat × Int) (c.n - c.split) (width c)
      (fun _ : Nat => (none : Option (Nat × Int)))
      (fun j => some (c.split + j, c.base + c.step * ((c.split + j : Nat) : Int)))]
  rw [filterMap_none (α := Nat) (β := Nat × Int)]
  simp only [List.nil_append]
  have hleft : (List.range (min c.n c.split)).map
      (fun i : Nat => (i, c.base + c.step * (i : Int))) =
      lanePoints (span c 0 c.split c.base c.step) := by
    unfold lanePoints span lane
    simp only
    have hc : (min c.n c.split - 0 + 1 - 1) / 1 = min c.n c.split := by omega
    rw [hc]
    apply List.map_congr_left
    intro i hi
    simp
    rw [Int.mul_comm]
  have hcnt : c.n - (c.split + width c) = c.n - c.split - width c := by
    dsimp [width]
    rw [Nat.sub_sub]
  have hright : (List.range (c.n - c.split - width c)).map
      (fun j : Nat => (c.split + (width c + j), c.base + c.step * ((c.split + (width c + j) : Nat) : Int))) =
      lanePoints (span c (c.split + width c) c.n
        (c.base + c.step * ((c.split + width c : Nat) : Int)) c.step) := by
    unfold lanePoints span lane
    simp only
    have hc2 : (min c.n c.n - (c.split + width c) + 1 - 1) / 1 =
        c.n - (c.split + width c) := by omega
    rw [hc2, hcnt]
    apply List.map_congr_left
    intro j hj
    simp [Int.mul_add, Int.add_mul]
    constructor
    · omega
    · rw [Int.mul_comm (j : Int) c.step]
      omega
  rw [filterMap_some_eq_map (l := List.range (c.n - c.split - width c))
        (f := fun j : Nat => (c.split + (width c + j), c.base + c.step * ((c.split + (width c + j) : Nat) : Int)))]
  rw [hleft, hright]
private theorem intersection_pos_step_one (a b : Lane) (hs : a.stride = 1) (hb : 0 < b.stride) :
    (intersection (positions a) (positions b)).step =
      if a.n = 0 ∨ b.n = 0 then 1 else b.stride := by
  dsimp [positions]
  rw [hs]
  have hbne : b.stride ≠ 0 := by omega
  by_cases h : a.n = 0 ∨ b.n = 0
  · simp [intersection, h]
  · simp [intersection, h, hbne, Nat.gcd_one_right]



private def bValueAt (b : Lane) (v : Int) : Int :=
  b.base + ((v - (b.start : Int)) / (b.stride : Int)) * b.step

private theorem aligned_pvals_sum (a : Lane) (hs : a.stride = 1) (b : Lane) (hb : 0 < b.stride) :
    aligned a b =
      ((pvals (intersection (positions a) (positions b))).map
        (fun v => laneValueAt a v * bValueAt b v)).sum := by
  let r := intersection (positions a) (positions b)
  by_cases hrn : r.n = 0
  · simp [aligned, r, hrn, pvals, tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  · have hz : ¬ (a.n = 0 ∨ b.n = 0) := by
      intro h
      simp [r, intersection, positions, h] at hrn
    have hstep : r.step = b.stride := by
      rw [show r = intersection (positions a) (positions b) by rfl]
      rw [intersection_pos_step_one a b hs hb]
      simp [hz]
    have hrpos : 0 < r.n := Nat.pos_of_ne_zero hrn
    have hmem0 : r.lo ∈ pvals r := (mem_pvals_pset r r.lo).mpr ⟨0, hrpos, by simp⟩
    have hpset := (pset_intersection (positions a) (positions b)
      (by simp [positions, hs]) (by simpa [positions] using hb) r.lo).mp
      ((mem_pvals_pset r r.lo).mp hmem0)
    rcases hpset.2 with ⟨j, hj, hvlo⟩
    dsimp [positions] at hvlo
    have hdivlo : (r.lo - (b.start : Int)) / (b.stride : Int) = (j : Int) := by
      rw [hvlo]
      have hbneI : (b.stride : Int) ≠ 0 := by omega
      have hsub : b.start + (j : Int) * (b.stride : Int) - b.start = (j : Int) * (b.stride : Int) := by omega
      rw [hsub]
      exact Int.ediv_eq_of_eq_mul_left hbneI rfl
    let x : Int := firstAt a r
    let y : Int := firstAt b r
    let dx : Int := slopeAt a r
    let dy : Int := slopeAt b r
    have hx : x = laneValueAt a r.lo := by
      dsimp [x, firstAt, laneValueAt]
      simp [hs, Int.ediv_one]
    have hdy : dy = b.step := by
      dsimp [dy, slopeAt]
      have hbne : b.stride ≠ 0 := by omega
      rw [hstep]
      simp [hbne]
    have hy : y = b.base + (j : Int) * b.step := by
      dsimp [y, firstAt]
      rw [hdivlo]
    have hdx : dx = a.step * (b.stride : Int) := by
      dsimp [dx, slopeAt]
      rw [hstep]
      simp [hs, Int.ediv_one]
      grind
    have hterm : ∀ k : Nat,
        laneValueAt a (r.lo + (k : Int) * (r.step : Int)) *
          bValueAt b (r.lo + (k : Int) * (r.step : Int)) =
        (x + (k : Int) * dx) * (y + (k : Int) * dy) := by
      intro k
      rw [hx, hy, hdy, hdx]
      dsimp [laneValueAt, bValueAt]
      rw [hstep]
      have hbneI : (b.stride : Int) ≠ 0 := by omega
      have hdivk : (r.lo + (k : Int) * (b.stride : Int) - (b.start : Int)) / (b.stride : Int) =
          (j : Int) + (k : Int) := by
        rw [show r.lo + (k : Int) * (b.stride : Int) - (b.start : Int) =
            (j : Int) * (b.stride : Int) + (k : Int) * (b.stride : Int) by
          rw [hvlo]
          grind]
        rw [← Int.add_mul]
        exact Int.ediv_eq_of_eq_mul_left hbneI rfl
      rw [hdivk]
      grind
    rw [show ((pvals r).map (fun v => laneValueAt a v * bValueAt b v)) =
        ((List.range r.n).map (fun k : Nat => laneValueAt a (r.lo + (k : Int) * (r.step : Int)) *
          bValueAt b (r.lo + (k : Int) * (r.step : Int)))) by
      unfold pvals
      rw [List.map_map]
      rfl]
    rw [show ((List.range r.n).map (fun k : Nat => laneValueAt a (r.lo + (k : Int) * (r.step : Int)) *
          bValueAt b (r.lo + (k : Int) * (r.step : Int)))) =
        ((List.range r.n).map (fun k : Nat => (x + (k : Int) * dx) * (y + (k : Int) * dy))) by
      apply List.map_congr_left
      intro k hk
      exact hterm k]
    rw [sum_range_product_linear]
    unfold aligned
    change (r.n : Int) * firstAt a r * firstAt b r +
      tri r.n * (firstAt a r * slopeAt b r + firstAt b r * slopeAt a r) +
      squares r.n * slopeAt a r * slopeAt b r = _
    dsimp [x, y, dx, dy]

private theorem sum_ite_eq_filter {α : Type} (p : α → Bool) (l : List α) (f : α → Int) :
    ((l.map (fun i => if p i then f i else 0)).sum) = (((l.filter p).map f).sum) := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]


private theorem filter_intPositions_g (a : Lane) (hs : a.stride = 1) (p : Int → Bool) (g : Int → Int) :
    (((lanePoints a).map (fun r => (r.1 : Int))).filter p).map g =
      ((lanePoints a).filter (fun r => p (r.1 : Int))).map (fun r => g (r.1 : Int)) := by
  have hfilt : ((lanePoints a).map (fun r => (r.1 : Int))).filter p =
      (((lanePoints a).filter (fun r => p (r.1 : Int))).map (fun r => (r.1 : Int))) := by
    induction (lanePoints a) with
    | nil => simp
    | cons r rs ih => by_cases h : p (r.1 : Int) <;> simp [h, ih]
  rw [hfilt, List.map_map]
  apply List.map_congr_left
  intro r hr
  rfl

private theorem lane_aligned_sum (a : Lane) (hs : a.stride = 1) (b : Lane) (hb : 0 < b.stride) :
    ((lanePoints a).map (fun r => ((List.range b.n).map (fun i : Nat =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)).sum =
    aligned a b := by
  have hposb : 0 < (positions b).step := by simpa [positions] using hb
  have hposa : 0 < (positions a).step := by simp [positions, hs]
  rw [aligned_pvals_sum a hs b hb]
  rw [pvals_inter_sum (positions a) (positions b) hposa hposb]
  rw [← lanePoints_positions a hs]
  rw [filter_intPositions_g a hs (fun v => contains (positions b) v)
    (fun v => laneValueAt a v * bValueAt b v)]
  -- inner product reduction
  have hinner : ∀ r : Nat × Int, r ∈ lanePoints a →
      ((List.range b.n).map (fun i : Nat =>
        if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum =
      (if contains (positions b) (r.1 : Int) then laneValueAt a (r.1 : Int) * bValueAt b (r.1 : Int) else 0) := by
    intro r hr
    by_cases hc : contains (positions b) (r.1 : Int)
    · rw [if_pos hc]
      have hmem := (contains_iff_pset (positions b) (r.1 : Int)).mp hc
      rcases hmem with ⟨j, hj, hvj⟩
      dsimp [positions] at hvj
      have hbneI : (b.stride : Int) ≠ 0 := by omega
      have hdivj : ((r.1 : Int) - b.start) / (b.stride : Int) = (j : Int) := by
        rw [hvj]
        have hsub : b.start + (j : Int) * (b.stride : Int) - b.start = (j : Int) * (b.stride : Int) := by omega
        rw [hsub]
        exact Int.ediv_eq_of_eq_mul_left hbneI rfl
      have hp : ∀ i : Nat, i ∈ List.range b.n →
          (r.1 == b.start + i * b.stride) = (i == j) := by
        intro i hi
        have hil : i < b.n := List.mem_range.mp hi
        by_cases heq : i = j
        · subst i
          have hnat : r.1 = b.start + j * b.stride := by exact_mod_cast hvj
          simp [hnat]
        · have hb1 : (r.1 == b.start + i * b.stride) = false := by
            rw [beq_eq_false_iff_ne]
            intro h
            have : b.start + (j : Int) * b.stride = b.start + (i : Int) * b.stride := by
              rw [← hvj, h]
              rfl
            have hmul : (j : Int) * (b.stride : Int) = (i : Int) * (b.stride : Int) := by omega
            have hji : (j : Int) = (i : Int) := Int.eq_of_mul_eq_mul_right hbneI hmul
            exact heq (by exact_mod_cast hji.symm)
          rw [hb1]
          simp [heq]
      rw [sum_ite_eq_filter]
      rw [show (List.range b.n).filter (fun i : Nat => (r.1 == b.start + i * b.stride)) =
          (List.range b.n).filter (fun i : Nat => (i == j)) by
        apply List.filter_congr
        intro i hi
        exact hp i hi]
      -- sum of f over filtered singleton: use range_filter_eq_single_sum
      rw [range_filter_eq_single_sum]
      have hjb : j < b.n := by simpa [positions] using hj
      rw [if_pos hjb]
      dsimp [bValueAt]
      rw [hdivj]
      have hrval : laneValueAt a (r.1 : Int) = r.2 := by
        rcases List.mem_map.mp hr with ⟨i, hi, rfl⟩
        simp [laneValueAt, hs]
        grind
      rw [hrval]
      grind
    · rw [if_neg hc]
      rw [sum_ite_eq_filter]
      have hnil : (List.range b.n).filter (fun i : Nat => (r.1 == b.start + i * b.stride)) = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro t ht
        have htmem := (List.mem_filter.mp ht).1
        have htlt : t < b.n := List.mem_range.mp htmem
        have heq := (List.mem_filter.mp ht).2
        have hb1 : (r.1 == b.start + t * b.stride) = true := heq
        have hv : (r.1 : Int) = b.start + (t : Int) * b.stride := by
          have hnateq : r.1 = b.start + t * b.stride := Nat.eq_of_beq_eq_true hb1
          exact_mod_cast hnateq
        apply hc
        apply (contains_iff_pset (positions b) (r.1 : Int)).mpr
        refine ⟨t, htlt, ?_⟩
        simpa [positions] using hv
      rw [hnil]
      simp
  -- main equality
  rw [show ((lanePoints a).map (fun r => ((List.range b.n).map (fun i : Nat =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)) =
      ((lanePoints a).map (fun r => if contains (positions b) (r.1 : Int) then
        laneValueAt a (r.1 : Int) * bValueAt b (r.1 : Int) else 0)) by
    apply List.map_congr_left
    intro r hr
    exact hinner r hr]
  -- right side is sum over filtered pvals of positions a
  have hsum : ((lanePoints a).map (fun r : Nat × Int => if contains (positions b) (r.1 : Int) then
        laneValueAt a (r.1 : Int) * bValueAt b (r.1 : Int) else 0)).sum =
      ((((lanePoints a).filter (fun r : Nat × Int => contains (positions b) (r.1 : Int))).map
        (fun r => laneValueAt a (r.1 : Int) * bValueAt b (r.1 : Int))).sum) := by
    exact sum_ite_eq_filter (fun r : Nat × Int => contains (positions b) (r.1 : Int)) (lanePoints a)
      (fun r => laneValueAt a (r.1 : Int) * bValueAt b (r.1 : Int))
  rw [hsum]

theorem sound_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.DeletedSpan.AlignedProduct.Goal := by
  intro c b hb
  rw [rows_lanes]
  simp only [List.filter_append, List.map_append, List.sum_append]
  simp only [layout, List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
  rw [lane_aligned_sum (span c 0 c.split c.base c.step) (by simp [span, lane]) b hb]
  rw [lane_aligned_sum (span c (c.split + width c) c.n
        (c.base + c.step * ((c.split + width c : Nat) : Int)) c.step) (by simp [span, lane]) b hb]
  simp only [Int.natCast_add]
  omega

end TinylambOpt.ResearchProofs.Atlas.EncodedPages.DeletedSpanAlignedProduct

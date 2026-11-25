import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.SparseSample.Remaining

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample

/-! ## Progression membership and Chinese-remainder helpers -/

private def progVal (a : Progression) (k : Nat) : Int := a.lo + (k : Int) * a.step

private theorem contains_iff_mem (p : Progression) (v : Int) :
    contains p v = true ↔ v ∈ (List.range p.n).map (fun i : Nat => progVal p i) := by
  unfold contains progVal
  simp only [Bool.and_eq_true, decide_eq_true_eq, Bool.or_eq_true, beq_iff_eq]
  rw [List.mem_map]
  constructor
  · intro h
    rcases h with ⟨⟨⟨hn, hlo⟩, hhi⟩, hmod⟩
    by_cases hs : p.step = 0
    · refine ⟨0, List.mem_range.mpr hn, ?_⟩
      have hlast : lastVal p = p.lo := by simp [lastVal, hs]
      omega
    · have hpos : 0 < (p.step : Int) := by exact_mod_cast Nat.pos_of_ne_zero hs
      have hm : (v - p.lo) % (p.step : Int) = 0 := by
        rcases hmod with hz | hm
        · exact False.elim (hs hz)
        · exact hm
      have hdvd : (p.step : Int) ∣ v - p.lo := Int.dvd_of_emod_eq_zero hm
      have hcancel : (v - p.lo) / (p.step : Int) * (p.step : Int) = v - p.lo :=
        Int.ediv_mul_cancel hdvd
      have hnonneg : 0 ≤ (v - p.lo) / (p.step : Int) :=
        Int.ediv_nonneg (by omega) (by omega)
      have hle : (v - p.lo) / (p.step : Int) ≤ ((p.n - 1 : Nat) : Int) := by
        apply Int.ediv_le_of_le_mul hpos
        simp only [lastVal] at hhi
        omega
      have hleNat : ((v - p.lo) / (p.step : Int)).toNat ≤ p.n - 1 := by
        simpa using Int.toNat_le_toNat hle
      refine ⟨((v - p.lo) / (p.step : Int)).toNat, List.mem_range.mpr (by omega), ?_⟩
      rw [Int.toNat_of_nonneg hnonneg]
      omega
  · rintro ⟨i, hi, rfl⟩
    have hin : i < p.n := List.mem_range.mp hi
    have hic : i ≤ p.n - 1 := by omega
    have hs : 0 ≤ (p.step : Int) := Int.natCast_nonneg _
    have hi' : 0 ≤ (i : Int) := Int.natCast_nonneg _
    have hnpos : 0 < p.n := by omega
    refine ⟨⟨⟨hnpos, Int.le_add_of_nonneg_right (Int.mul_nonneg hi' hs)⟩, ?_⟩, Or.inr ?_⟩
    · have hu := Int.mul_le_mul_of_nonneg_right (show (i : Int) ≤ ((p.n - 1 : Nat) : Int) by
        exact_mod_cast hic) hs
      simpa only [lastVal, Int.natCast_sub (by omega : 1 ≤ p.n)] using
        Int.add_le_add_left hu p.lo
    · have hsub : p.lo + (i : Int) * (p.step : Int) - p.lo = (i : Int) * (p.step : Int) := by omega
      rw [hsub]
      exact Int.mul_emod_left (i : Int) (p.step : Int)

private theorem bezout_eq (a b : Nat) (hb : b ≠ 0) :
    bezout a b =
      let (x, y) := bezout b (a % b)
      (y, x - (a / b : Int) * y) := by
  rw [bezout.eq_1, dif_neg hb]

private theorem bezout_spec (a b : Nat) :
    (a : Int) * (bezout a b).1 + (b : Int) * (bezout a b).2 = (Nat.gcd a b : Int) := by
  have main : ∀ b a : Nat,
      (a : Int) * (bezout a b).1 + (b : Int) * (bezout a b).2 = (Nat.gcd a b : Int) := by
    intro b
    induction b using Nat.strongRecOn with
    | ind b ih =>
        intro a
        by_cases hb0 : b = 0
        · subst b
          simp [bezout]
        · have hb : b ≠ 0 := hb0
          rw [bezout_eq a b hb]
          rcases hpair : bezout b (a % b) with ⟨x, y⟩
          have hrec := ih (a % b) (Nat.mod_lt a (Nat.pos_of_ne_zero hb)) b
          rw [hpair] at hrec
          change (b : Int) * x + ((a % b : Nat) : Int) * y = (Nat.gcd b (a % b) : Int) at hrec
          have hgcd : Nat.gcd a b = Nat.gcd b (a % b) := by
            rw [Nat.gcd_comm a b, Nat.gcd_rec b a, Nat.gcd_comm]
          have hd : (a : Int) = (b : Int) * ((a / b : Nat) : Int) + ((a % b : Nat) : Int) := by
            exact_mod_cast (Nat.div_add_mod a b).symm
          clear ih
          rw [hgcd]
          grind
  exact main b a

private theorem crt_delta_mod (s t : Nat) (hs : 0 < s) (ht : 0 < t) (gap : Int)
    (hgap : gap % (Nat.gcd s t : Int) = 0) :
    (((s : Int) * (((gap / (Nat.gcd s t : Int)) *
        (bezout (s / Nat.gcd s t) (t / Nat.gcd s t)).1) %
      ((t / Nat.gcd s t) : Int))) - gap) % (t : Int) = 0 := by
  let g := Nat.gcd s t
  let sa := s / g
  let tb := t / g
  have hg : 0 < g := Nat.gcd_pos_of_pos_left t hs
  have hsa : 0 < sa := by
    dsimp [sa]
    exact Nat.div_pos (Nat.le_of_dvd hs (Nat.gcd_dvd_left s t)) hg
  have htb : 0 < tb := by
    dsimp [tb]
    exact Nat.div_pos (Nat.le_of_dvd ht (Nat.gcd_dvd_right s t)) hg
  have hsEq : (s : Int) = (g : Int) * (sa : Int) := by
    exact_mod_cast (by simpa [sa, g, Nat.mul_comm] using
      (Nat.div_mul_cancel (Nat.gcd_dvd_left s t)).symm)
  have htEq : (t : Int) = (g : Int) * (tb : Int) := by
    exact_mod_cast (by simpa [tb, g, Nat.mul_comm] using
      (Nat.div_mul_cancel (Nat.gcd_dvd_right s t)).symm)
  have hcop : Nat.gcd sa tb = 1 := Nat.coprime_div_gcd_div_gcd hg
  have hdvd : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgap
  obtain ⟨z, hz⟩ := hdvd
  rcases hpair : bezout sa tb with ⟨u, v⟩
  have hbez := bezout_spec sa tb
  rw [hpair] at hbez
  change (sa : Int) * u + (tb : Int) * v = (Nat.gcd sa tb : Int) at hbez
  rw [hcop] at hbez
  have hzdiv : gap / (g : Int) = z := by
    rw [hz, Int.mul_ediv_cancel_left z (by exact_mod_cast Nat.ne_of_gt hg)]
  rw [hzdiv]
  let x := (z * u) % (tb : Int)
  let q := (z * u) / (tb : Int)
  change ((s : Int) * x - gap) % (t : Int) = 0
  have hx : x + (tb : Int) * q = z * u := Int.emod_add_mul_ediv (z * u) (tb : Int)
  have hexpr : (s : Int) * x - gap = -(t : Int) * (z * v + (sa : Int) * q) := by
    rw [hz, hsEq, htEq]
    grind
  have hsubmod : ((s : Int) * x - gap) % (t : Int) = 0 := by
    rw [hexpr]
    have hcomm : -(t : Int) * (z * v + (sa : Int) * q) =
        (z * v + (sa : Int) * q) * (-(t : Int)) := Int.mul_comm _ _
    rw [hcomm]
    simpa only [Int.emod_neg] using Int.mul_emod_left (z * v + (sa : Int) * q) (-(t : Int))
  exact hsubmod

private theorem period_dvd_delta (s t : Nat) (hs : 0 < s) (ht : 0 < t) (delta : Int)
    (hds : (s : Int) ∣ delta) (hdt : (t : Int) ∣ delta) :
    (((s / Nat.gcd s t) * t : Nat) : Int) ∣ delta := by
  let g := Nat.gcd s t
  let sa := s / g
  let tb := t / g
  have hg : 0 < g := Nat.gcd_pos_of_pos_left t hs
  have hsa : 0 < sa := Nat.div_pos (Nat.le_of_dvd hs (Nat.gcd_dvd_left s t)) hg
  have htb : 0 < tb := Nat.div_pos (Nat.le_of_dvd ht (Nat.gcd_dvd_right s t)) hg
  have hsEq : (s : Int) = (g : Int) * (sa : Int) := by
    exact_mod_cast (by simpa [sa, g, Nat.mul_comm] using
      (Nat.div_mul_cancel (Nat.gcd_dvd_left s t)).symm)
  have htEq : (t : Int) = (g : Int) * (tb : Int) := by
    exact_mod_cast (by simpa [tb, g, Nat.mul_comm] using
      (Nat.div_mul_cancel (Nat.gcd_dvd_right s t)).symm)
  have hcop : Nat.gcd sa tb = 1 := Nat.coprime_div_gcd_div_gcd hg
  obtain ⟨k1, hk1⟩ := hds
  obtain ⟨k2, hk2⟩ := hdt
  rcases hpair : bezout sa tb with ⟨u, v⟩
  have hbez := bezout_spec sa tb
  rw [hpair] at hbez
  change (sa : Int) * u + (tb : Int) * v = (Nat.gcd sa tb : Int) at hbez
  rw [hcop] at hbez
  clear hpair
  refine ⟨u * k2 + v * k1, ?_⟩
  change delta = ((sa * t : Nat) : Int) * (u * k2 + v * k1)
  grind

private theorem crt_first_solution (s t : Nat) (hs : 0 < s) (ht : 0 < t)
    (alo blo : Int) (hgap : (blo - alo) % (Nat.gcd s t : Int) = 0) :
    let g := Nat.gcd s t
    let sa := s / g
    let tb := t / g
    let root := alo + (s : Int) * (((blo - alo) / (g : Int) * (bezout sa tb).1) % (tb : Int))
    let period := sa * t
    let low := max alo blo
    let first := low + (root - low) % (period : Int)
    0 < period ∧ low ≤ first ∧ first < low + (period : Int) ∧
      (first - alo) % (s : Int) = 0 ∧ (first - blo) % (t : Int) = 0 := by
  dsimp only
  let g := Nat.gcd s t
  let sa := s / g
  let tb := t / g
  let k := ((blo - alo) / (g : Int) * (bezout sa tb).1) % (tb : Int)
  let root := alo + (s : Int) * k
  let period := sa * t
  let low := max alo blo
  let first := low + (root - low) % (period : Int)
  change 0 < period ∧ low ≤ first ∧ first < low + (period : Int) ∧
    (first - alo) % (s : Int) = 0 ∧ (first - blo) % (t : Int) = 0
  have hg : 0 < g := Nat.gcd_pos_of_pos_left t hs
  have hsa : 0 < sa := Nat.div_pos (Nat.le_of_dvd hs (Nat.gcd_dvd_left s t)) hg
  have htb : 0 < tb := Nat.div_pos (Nat.le_of_dvd ht (Nat.gcd_dvd_right s t)) hg
  have hsEq : (s : Int) = (g : Int) * (sa : Int) := by
    exact_mod_cast (by simpa [sa, g, Nat.mul_comm] using
      (Nat.div_mul_cancel (Nat.gcd_dvd_left s t)).symm)
  have htEq : (t : Int) = (g : Int) * (tb : Int) := by
    exact_mod_cast (by simpa [tb, g, Nat.mul_comm] using
      (Nat.div_mul_cancel (Nat.gcd_dvd_right s t)).symm)
  have hperS : (s : Int) ∣ (period : Int) := by
    refine ⟨(tb : Int), ?_⟩
    change (period : Int) = (s : Int) * (tb : Int)
    have hnat : period = s * tb := by
      dsimp [period, g, sa, tb]
      have hdiv := Nat.div_mul_cancel (Nat.gcd_dvd_right s t)
      grind
    exact_mod_cast hnat
  have hperT : (t : Int) ∣ (period : Int) := by
    refine ⟨(sa : Int), ?_⟩
    change (period : Int) = (t : Int) * (sa : Int)
    have hnat : period = t * sa := by
      dsimp [period]
      exact Nat.mul_comm sa t
    exact_mod_cast hnat
  have hperNat : 0 < period := by
    dsimp [period]
    exact Nat.mul_pos hsa ht
  have hper : 0 < (period : Int) := by exact_mod_cast hperNat
  have hrootA : (root - alo) % (s : Int) = 0 := by
    dsimp [root]
    have hsub : alo + (s : Int) * k - alo = (s : Int) * k := by omega
    rw [hsub]
    simpa [Int.mul_comm] using Int.mul_emod_left k (s : Int)
  have hrootB : (root - blo) % (t : Int) = 0 := by
    have hdelta := crt_delta_mod s t hs ht (blo - alo) hgap
    have heq : root - blo = (s : Int) * k - (blo - alo) := by
      dsimp [root]
      omega
    rw [heq]
    simpa [k, g, sa, tb] using hdelta
  have hremS : ((root - low) % (period : Int)) % (s : Int) = (root - low) % (s : Int) :=
    Int.emod_emod_of_dvd _ hperS
  have hremT : ((root - low) % (period : Int)) % (t : Int) = (root - low) % (t : Int) :=
    Int.emod_emod_of_dvd _ hperT
  have hfirstS : first % (s : Int) = root % (s : Int) := by
    dsimp [first]
    rw [Int.add_emod, hremS]
    rw [← Int.add_emod]
    congr 1
    omega
  have hfirstT : first % (t : Int) = root % (t : Int) := by
    dsimp [first]
    rw [Int.add_emod, hremT]
    rw [← Int.add_emod]
    congr 1
    omega
  have hrootSA : root % (s : Int) = alo % (s : Int) :=
    (Int.emod_eq_emod_iff_emod_sub_eq_zero (m := root) (n := s) (k := alo)).mpr hrootA
  have hrootTB : root % (t : Int) = blo % (t : Int) :=
    (Int.emod_eq_emod_iff_emod_sub_eq_zero (m := root) (n := t) (k := blo)).mpr hrootB
  have hfirstA : (first - alo) % (s : Int) = 0 :=
    (Int.emod_eq_emod_iff_emod_sub_eq_zero (m := first) (n := s) (k := alo)).mp
      (hfirstS.trans hrootSA)
  have hfirstB : (first - blo) % (t : Int) = 0 :=
    (Int.emod_eq_emod_iff_emod_sub_eq_zero (m := first) (n := t) (k := blo)).mp
      (hfirstT.trans hrootTB)
  have hremNonneg : 0 ≤ (root - low) % (period : Int) := Int.emod_nonneg _ (by omega)
  have hremLt : (root - low) % (period : Int) < (period : Int) := Int.emod_lt_of_pos _ hper
  refine ⟨hperNat, by dsimp [first]; omega, by dsimp [first]; omega, hfirstA, hfirstB⟩

private theorem contains_const_iff (lo : Int) (n : Nat) (v : Int) :
    contains ⟨lo, 0, n⟩ v = true ↔ 0 < n ∧ v = lo := by
  rw [contains_iff_mem, List.mem_map]
  constructor
  · rintro ⟨i, hi, hv⟩
    have hprod : progVal ⟨lo, 0, n⟩ i = lo := by simp [progVal]
    have hi' : i < n := List.mem_range.mp hi
    have hnpos : 0 < n := by omega
    exact ⟨hnpos, by omega⟩
  · rintro ⟨hn, hv⟩
    exact ⟨0, List.mem_range.mpr hn, by simp [progVal, hv]⟩

private theorem contains_pos_iff (a : Progression) (ha : 0 < a.step) (hn : 0 < a.n) (v : Int) :
    contains a v = true ↔ a.lo ≤ v ∧ v ≤ lastVal a ∧ (a.step : Int) ∣ v - a.lo := by
  rw [contains_iff_mem, List.mem_map]
  constructor
  · rintro ⟨i, hi, hv⟩
    have hi' : i < a.n := List.mem_range.mp hi
    have hpos : (0 : Int) < a.step := by exact_mod_cast ha
    have hval : progVal a i = v := hv
    dsimp [progVal] at hval
    have hnonneg : (0 : Int) ≤ (i : Int) * (a.step : Int) :=
      Int.mul_nonneg (by omega) (Int.le_of_lt hpos)
    refine ⟨?_, ?_, ?_⟩
    · omega
    · have hile : (i : Int) ≤ ((a.n - 1 : Nat) : Int) := by exact_mod_cast (by omega)
      have hmul := Int.mul_le_mul_of_nonneg_right hile (Int.le_of_lt hpos)
      dsimp [lastVal]
      omega
    · refine ⟨(i : Int), ?_⟩
      have hcomm : (a.step : Int) * (i : Int) = (i : Int) * (a.step : Int) := Int.mul_comm _ _
      omega
  · rintro ⟨hlo, hhi, ⟨q, hq⟩⟩
    have hpos : (0 : Int) < a.step := by exact_mod_cast ha
    have hq' : v - a.lo = q * (a.step : Int) := by
      rw [Int.mul_comm]
      exact hq
    have hqnon : 0 ≤ q := Int.nonneg_of_mul_nonneg_right (by
      rw [← hq]
      omega) hpos
    have hqle : q ≤ ((a.n - 1 : Nat) : Int) := by
      dsimp [lastVal] at hhi
      have hupper : v - a.lo ≤ ((a.n - 1 : Nat) : Int) * (a.step : Int) := by
        have hh := Int.sub_le_sub_right hhi a.lo
        rw [show (a.lo + ((a.n - 1 : Nat) : Int) * (a.step : Int)) - a.lo =
          ((a.n - 1 : Nat) : Int) * (a.step : Int) by omega] at hh
        exact hh
      rw [hq'] at hupper
      exact Int.le_of_mul_le_mul_right hupper hpos
    refine ⟨q.toNat, List.mem_range.mpr ?_, ?_⟩
    · have hle : q.toNat ≤ a.n - 1 := (Int.toNat_le (m := q) (n := a.n - 1)).mpr hqle
      omega
    · dsimp [progVal]
      rw [Int.toNat_of_nonneg hqnon]
      rw [← hq']
      omega

private theorem sub_of_add_eq_add (e f alo blo : Int) (h : blo + e = alo + f) :
    blo - alo = f - e := by
  have heplus : e + (f - e) = f := by
    rw [Int.sub_eq_add_neg, ← Int.add_assoc, Int.add_comm e f, Int.add_assoc,
      Int.add_right_neg, Int.add_zero]
  have hcommassoc : e + (alo + (f - e)) = alo + (e + (f - e)) := by
    rw [← Int.add_assoc, Int.add_comm e alo, Int.add_assoc]
  have hblo : blo = alo + (f - e) := by
    apply Int.add_left_cancel (a := e)
    calc
      e + blo = blo + e := Int.add_comm e blo
      _ = alo + f := h
      _ = e + (alo + (f - e)) := by rw [hcommassoc, heplus]
  rw [hblo]
  rw [Int.add_comm alo (f - e), Int.add_sub_cancel]

private theorem common_gap_dvd (a b : Progression) (v : Int)
    (ha : contains a v = true) (hb : contains b v = true) :
    ((Nat.gcd a.step b.step : Nat) : Int) ∣ b.lo - a.lo := by
  obtain ⟨i, hi, hvi⟩ := List.mem_map.mp ((contains_iff_mem a v).mp ha)
  obtain ⟨j, hj, hvj⟩ := List.mem_map.mp ((contains_iff_mem b v).mp hb)
  let g := Nat.gcd a.step b.step
  have hgaNat : g ∣ a.step := by exact Nat.gcd_dvd_left a.step b.step
  have hgbNat : g ∣ b.step := by exact Nat.gcd_dvd_right a.step b.step
  obtain ⟨ka, hka⟩ := hgaNat
  obtain ⟨kb, hkb⟩ := hgbNat
  have hga : (g : Int) ∣ (a.step : Int) := by
    refine ⟨(ka : Int), ?_⟩
    exact_mod_cast hka
  have hgb : (g : Int) ∣ (b.step : Int) := by
    refine ⟨(kb : Int), ?_⟩
    exact_mod_cast hkb
  have hda : (g : Int) ∣ (i : Int) * (a.step : Int) := Int.dvd_mul_of_dvd_right hga
  have hdb : (g : Int) ∣ (j : Int) * (b.step : Int) := Int.dvd_mul_of_dvd_right hgb
  have hvi' : a.lo + (i : Int) * (a.step : Int) = v := by
    dsimp [progVal] at hvi
    exact hvi
  have hvj' : b.lo + (j : Int) * (b.step : Int) = v := by
    dsimp [progVal] at hvj
    exact hvj
  have hsum : b.lo + (j : Int) * (b.step : Int) =
      a.lo + (i : Int) * (a.step : Int) := by
    rw [hvj', hvi']
  have heq : b.lo - a.lo = (i : Int) * (a.step : Int) -
      (j : Int) * (b.step : Int) :=
    sub_of_add_eq_add ((j : Int) * (b.step : Int)) ((i : Int) * (a.step : Int)) a.lo b.lo hsum
  rw [heq]
  exact Int.dvd_sub hda hdb

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

private theorem span_contains_iff (first step high x : Int) (hs : 0 < step) :
    let n := if first ≤ high then ((high - first) / step).toNat + 1 else 0
    contains ⟨first, step.toNat, n⟩ x = true ↔
      first ≤ x ∧ x ≤ high ∧ (step : Int) ∣ x - first := by
  dsimp only
  constructor
  · intro hx
    have hm := (contains_iff_mem ⟨first, step.toNat,
      if first ≤ high then ((high - first) / step).toNat + 1 else 0⟩ x).mp hx
    obtain ⟨k, hk, hval⟩ := List.mem_map.mp hm
    dsimp [progVal] at hval
    have hstepcast : (step.toNat : Int) = step := Int.toNat_of_nonneg (by omega)
    rw [hstepcast] at hval
    by_cases hfh : first ≤ high
    · have hklt : k < (if first ≤ high then ((high - first) / step).toNat + 1 else 0) :=
        List.mem_range.mp hk
      rw [if_pos hfh] at hklt
      have ht : 0 ≤ (high - first) / step :=
        Int.ediv_nonneg (by omega) (Int.le_of_lt hs)
      have htcast : (((high - first) / step).toNat : Int) = (high - first) / step :=
        Int.toNat_of_nonneg ht
      have hkq : (k : Int) ≤ (high - first) / step := by
        apply (Int.le_ediv_iff_mul_le hs).mpr
        have hkNat : k ≤ ((high - first) / step).toNat := by omega
        have hkInt : (k : Int) ≤ (((high - first) / step).toNat : Int) := by exact_mod_cast hkNat
        calc
          (k : Int) * step ≤ (((high - first) / step).toNat : Int) * step :=
            Int.mul_le_mul_of_nonneg_right hkInt (Int.le_of_lt hs)
          _ = ((high - first) / step) * step := by rw [htcast]
          _ ≤ high - first := Int.ediv_mul_le _ (by omega)
      have hkle : (k : Int) * step ≤ high - first :=
        (Int.le_ediv_iff_mul_le hs).mp hkq
      refine ⟨?_, ?_, ?_⟩
      · rw [← hval]
        exact Int.le_add_of_nonneg_right (Int.mul_nonneg (by omega) (Int.le_of_lt hs))
      · calc
          x = first + (k : Int) * step := hval.symm
          _ ≤ first + (high - first) := Int.add_le_add_left hkle first
          _ = high := by omega
      · refine ⟨(k : Int), ?_⟩
        calc
          x - first = (k : Int) * step := by omega
          _ = step * (k : Int) := Int.mul_comm _ _
    · simp [hfh] at hk
  · rintro ⟨hlo, hhi, ⟨q, hq⟩⟩
    have hfh : first ≤ high := by omega
    rw [if_pos hfh]
    have hqnon : 0 ≤ q := Int.nonneg_of_mul_nonneg_right (by
      rw [← hq]
      omega) hs
    have htcast : (((high - first) / step).toNat : Int) = (high - first) / step :=
      Int.toNat_of_nonneg (Int.ediv_nonneg (by omega) (Int.le_of_lt hs))
    have hqle : q ≤ (high - first) / step := by
      apply (Int.le_ediv_iff_mul_le hs).mpr
      calc
        q * step = x - first := by rw [Int.mul_comm q step]; exact hq.symm
        _ ≤ high - first := Int.sub_le_sub_right hhi first
    apply (contains_iff_mem ⟨first, step.toNat, ((high - first) / step).toNat + 1⟩ x).mpr
    apply List.mem_map.mpr
    refine ⟨q.toNat, List.mem_range.mpr ?_, ?_⟩
    · have hle : q.toNat ≤ ((high - first) / step).toNat := by
        apply (Int.toNat_le (m := q) (n := ((high - first) / step).toNat)).mpr
        rw [htcast]
        exact hqle
      exact Nat.lt_succ_of_le hle
    · dsimp [progVal]
      rw [Int.toNat_of_nonneg hqnon]
      rw [Int.toNat_of_nonneg (by omega : 0 ≤ step)]
      rw [Int.mul_comm step q] at hq
      omega

private theorem intersection_contains (a b : Progression) (v : Int) :
    contains (intersection a b) v = true ↔ contains a v = true ∧ contains b v = true := by
  by_cases han0 : a.n = 0
  · rcases a with ⟨alo, ast, an⟩
    simp at han0
    simp [intersection, contains, han0]
  by_cases hbn0 : b.n = 0
  · rcases b with ⟨blo, bst, bn⟩
    simp at hbn0
    simp [intersection, contains, hbn0]
  have hanp : 0 < a.n := by omega
  have hbnp : 0 < b.n := by omega
  by_cases has0 : a.step = 0
  · have haConst : contains a v = true ↔ 0 < a.n ∧ v = a.lo := by
      rcases a with ⟨alo, ast, an⟩
      simp at has0 ⊢
      subst ast
      exact contains_const_iff alo an v
    have hinter : intersection a b = ⟨a.lo, 1, if contains b a.lo = true then 1 else 0⟩ := by
      unfold intersection
      simp [han0, hbn0, has0]
    rw [hinter, haConst]
    have hone : contains ⟨a.lo, 1, 1⟩ v = true ↔ a.lo = v := by
      rw [contains_iff_mem, List.mem_map]
      simp [progVal]
    by_cases hb : contains b a.lo = true
    · rw [if_pos hb, hone]
      constructor
      · intro hv
        exact ⟨⟨hanp, hv.symm⟩, by rw [← hv]; exact hb⟩
      · rintro ⟨⟨_, hv⟩, _⟩
        exact hv.symm
    · rw [if_neg hb]
      have hr : contains ⟨a.lo, 1, 0⟩ v = true ↔ False := by simp [contains]
      rw [hr]
      constructor
      · intro h
        exact h.elim
      · rintro ⟨hA, hB⟩
        have hv : a.lo = v := hA.2.symm
        exact hb (by rw [hv]; exact hB)
  by_cases hbs0 : b.step = 0
  · have hbConst : contains b v = true ↔ 0 < b.n ∧ v = b.lo := by
      rcases b with ⟨blo, bst, bn⟩
      simp at hbs0 ⊢
      subst bst
      exact contains_const_iff blo bn v
    have hinter : intersection a b = ⟨b.lo, 1, if contains a b.lo = true then 1 else 0⟩ := by
      unfold intersection
      simp [han0, hbn0, has0, hbs0]
    rw [hinter, hbConst]
    have hone : contains ⟨b.lo, 1, 1⟩ v = true ↔ b.lo = v := by
      rw [contains_iff_mem, List.mem_map]
      simp [progVal]
    by_cases ha : contains a b.lo = true
    · rw [if_pos ha, hone]
      constructor
      · intro hv
        exact ⟨by rw [← hv]; exact ha, ⟨hbnp, hv.symm⟩⟩
      · rintro ⟨_, ⟨_, hv⟩⟩
        exact hv.symm
    · rw [if_neg ha]
      have hr : contains ⟨b.lo, 1, 0⟩ v = true ↔ False := by simp [contains]
      rw [hr]
      constructor
      · intro h
        exact h.elim
      · rintro ⟨hA, hB⟩
        have hv : b.lo = v := hB.2.symm
        exact ha (by rw [hv]; exact hA)
  · have hasp : 0 < a.step := Nat.pos_of_ne_zero has0
    have hbsp : 0 < b.step := Nat.pos_of_ne_zero hbs0
    let g := Nat.gcd a.step b.step
    let gap := b.lo - a.lo
    let period := (a.step / g) * b.step
    let unit := bezout (a.step / g) (b.step / g)
    let root := a.lo + (a.step : Int) * ((gap / (g : Int) * unit.1) % ((b.step / g : Nat) : Int))
    let low := max a.lo b.lo
    let first := low + (root - low) % (period : Int)
    let high := min (lastVal a) (lastVal b)
    have hgapmodEq : gap % (g : Int) = (b.lo - a.lo) % (Nat.gcd a.step b.step : Int) := by
      rfl
    by_cases hgapmod : gap % (g : Int) = 0
    · have hcrt := crt_first_solution a.step b.step hasp hbsp a.lo b.lo (by
        simpa [g, gap] using hgapmod)
      dsimp only at hcrt
      have hperNat : 0 < period := by
        simpa [g, period, gap] using hcrt.1
      have hlowFirst : low ≤ first := by
        simpa [low, first, root, period, unit, gap] using hcrt.2.1
      have hfa : (first - a.lo) % (a.step : Int) = 0 := by
        simpa [low, first, root, period, unit, gap] using hcrt.2.2.2.1
      have hfb : (first - b.lo) % (b.step : Int) = 0 := by
        simpa [low, first, root, period, unit, gap] using hcrt.2.2.2.2
      have hp : (0 : Int) < period := by exact_mod_cast hperNat
      have hper_comm : b.step * (a.step / g) = a.step * (b.step / g) := by
        apply Nat.eq_of_mul_eq_mul_left (Nat.gcd_pos_of_pos_left b.step hasp)
        have h1 := Nat.mul_div_cancel' (Nat.gcd_dvd_left a.step b.step)
        have h2 := Nat.mul_div_cancel' (Nat.gcd_dvd_right a.step b.step)
        grind
      have hinter : intersection a b = ⟨first, period,
          if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ := by
        unfold intersection
        simp [han0, hbn0, has0, hbs0, hgapmod, first, period, root, unit, gap, low, high, g]
      rw [hinter]
      have hspan : contains ⟨first, period,
          if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ v = true ↔
          first ≤ v ∧ v ≤ high ∧ (period : Int) ∣ v - first := by
        simpa using span_contains_iff first (period : Int) high v hp
      rw [hspan]
      have hpa : (a.step : Int) ∣ (period : Int) := by
        refine ⟨((b.step / g : Nat) : Int), ?_⟩
        change (period : Int) = (a.step : Int) * ((b.step / g : Nat) : Int)
        have hnat : period = a.step * (b.step / g) := by
          dsimp [period]
          rw [Nat.mul_comm (a.step / g) b.step]
          exact hper_comm
        exact_mod_cast hnat
      have hpb : (b.step : Int) ∣ (period : Int) := by
        refine ⟨((a.step / g : Nat) : Int), ?_⟩
        change (period : Int) = (b.step : Int) * ((a.step / g : Nat) : Int)
        have hnat : period = b.step * (a.step / g) := by
          dsimp [period]
          rw [Nat.mul_comm]
        exact_mod_cast hnat
      have hfroot : (period : Int) ∣ first - root := by
        exact first_root_dvd low root first (period : Int) hp (by rfl)
      have hfroot' : (period : Int) ∣ root - first := by
        obtain ⟨k, hk⟩ := hfroot
        refine ⟨-k, ?_⟩
        rw [Int.mul_neg, ← hk]
        omega
      constructor
      · rintro ⟨hfirstx, hxhigh, hxfirst⟩
        have hxroot : (period : Int) ∣ v - root := by
          have heq : v - root = (v - first) + (first - root) := by omega
          rw [heq]
          exact Int.dvd_add hxfirst hfroot
        have hrootA : (a.step : Int) ∣ root - first := Int.dvd_trans hpa hfroot'
        have hrootB' : (b.step : Int) ∣ root - first := Int.dvd_trans hpb hfroot'
        have hxrootA : (a.step : Int) ∣ v - root := Int.dvd_trans hpa hxroot
        have hxrootB' : (b.step : Int) ∣ v - root := Int.dvd_trans hpb hxroot
        have hfaDvd : (a.step : Int) ∣ first - a.lo := Int.dvd_of_emod_eq_zero hfa
        have hfbDvd : (b.step : Int) ∣ first - b.lo := Int.dvd_of_emod_eq_zero hfb
        have hxalo : (a.step : Int) ∣ v - a.lo := by
          have heq : v - a.lo = (v - root) + (root - first) + (first - a.lo) := by omega
          rw [heq]
          exact Int.dvd_add (Int.dvd_add hxrootA hrootA) hfaDvd
        have hxblo : (b.step : Int) ∣ v - b.lo := by
          have heq : v - b.lo = (v - root) + (root - first) + (first - b.lo) := by omega
          rw [heq]
          exact Int.dvd_add (Int.dvd_add hxrootB' hrootB') hfbDvd
        have hxlow : low ≤ v := by omega
        constructor
        · apply (contains_pos_iff a hasp hanp v).mpr
          refine ⟨?_, ?_, hxalo⟩
          · have hml := Int.le_max_left a.lo b.lo
            omega
          · have hmn := Int.min_le_left (lastVal a) (lastVal b)
            omega
        · apply (contains_pos_iff b hbsp hbnp v).mpr
          refine ⟨?_, ?_, hxblo⟩
          · have hmr := Int.le_max_right a.lo b.lo
            omega
          · have hmn := Int.min_le_right (lastVal a) (lastVal b)
            omega
      · rintro ⟨haC, hbC⟩
        have haB := (contains_pos_iff a hasp hanp v).mp haC
        have hbB := (contains_pos_iff b hbsp hbnp v).mp hbC
        have hfaDvd : (a.step : Int) ∣ first - a.lo := Int.dvd_of_emod_eq_zero hfa
        have hfbDvd : (b.step : Int) ∣ first - b.lo := Int.dvd_of_emod_eq_zero hfb
        have hva : (a.step : Int) ∣ v - first := by
          have heq : (v - a.lo) - (first - a.lo) = v - first := by omega
          rw [← heq]
          exact Int.dvd_sub haB.2.2 hfaDvd
        have hvb : (b.step : Int) ∣ v - first := by
          have heq : (v - b.lo) - (first - b.lo) = v - first := by omega
          rw [← heq]
          exact Int.dvd_sub hbB.2.2 hfbDvd
        have hxfirst : (period : Int) ∣ v - first :=
          period_dvd_delta a.step b.step hasp hbsp (v - first) hva hvb
        have hxroot : (period : Int) ∣ v - root := by
          have heq : v - root = (v - first) + (first - root) := by omega
          rw [heq]
          exact Int.dvd_add hxfirst hfroot
        have hxlow : low ≤ v := by omega
        have hxhigh : v ≤ high := by
          by_cases hle : lastVal a ≤ lastVal b
          · have hmin : high = lastVal a := by dsimp [high]; exact Int.min_eq_left hle
            rw [hmin]
            exact haB.2.1
          · have hmin : high = lastVal b := by dsimp [high]; exact Int.min_eq_right (by omega)
            rw [hmin]
            exact hbB.2.1
        have hfirstx : first ≤ v :=
          first_le_of_congruence low root first v (period : Int) hp (by rfl) hxlow hxroot
        exact ⟨hfirstx, hxhigh, hxfirst⟩
    · have hgapne : gap % (g : Int) ≠ 0 := by omega
      have hinter : intersection a b = ⟨0, 1, 0⟩ := by
        unfold intersection
        simp [han0, hbn0, has0, hbs0, hgapmod, g, gap]
      rw [hinter]
      have hr : contains ⟨0, 1, 0⟩ v = true ↔ False := by simp [contains]
      rw [hr]
      constructor
      · intro h
        exact h.elim
      · rintro ⟨haC, hbC⟩
        have hg := common_gap_dvd a b v haC hbC
        exact hgapne (Int.emod_eq_zero_of_dvd (by simpa [g, gap] using hg))

/-! ## Counting elements of finite progressions -/

private theorem progVal_injective (p : Progression) (hp : 0 < p.step) :
    Function.Injective (progVal p) := by
  intro i j hij
  dsimp [progVal] at hij
  have hpos : (0 : Int) < p.step := by exact_mod_cast hp
  have hmul : (i : Int) * (p.step : Int) = (j : Int) * (p.step : Int) := by omega
  have hcast : (i : Int) = (j : Int) := by
    exact (Int.mul_eq_mul_right_iff (by omega : (p.step : Int) ≠ 0)).mp hmul
  exact Int.ofNat.inj hcast

private theorem nodup_map_of_injective {α β : Type} (f : α → β)
    (hf : Function.Injective f) : ∀ l : List α, l.Nodup → (l.map f).Nodup := by
  intro l hl
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hn := List.nodup_cons.mp hl
      rw [List.map_cons, List.nodup_cons]
      constructor
      · intro hm
        obtain ⟨y, hy, hfy⟩ := List.mem_map.mp hm
        have hyx : y = x := hf hfy
        subst y
        exact hn.1 hy
      · exact ih hn.2

private theorem nodup_filter_of_nodup {α : Type} (p : α → Bool) :
    ∀ l : List α, l.Nodup → (l.filter p).Nodup := by
  intro l hl
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hn := List.nodup_cons.mp hl
      by_cases hx : p x = true
      · simp [hx]
        exact ⟨hn.1, ih hn.2⟩
      · simp [hx]
        exact ih hn.2

private theorem count_eq_one_of_mem_nodup {α : Type} [BEq α] [LawfulBEq α]
    (l : List α) (hl : l.Nodup) (a : α) (ha : a ∈ l) : l.count a = 1 := by
  induction l with
  | nil => simp at ha
  | cons x xs ih =>
      have hn := List.nodup_cons.mp hl
      by_cases hxa : x = a
      · subst a
        rw [List.count_cons]
        simp
        exact List.count_eq_zero.mpr hn.1
      · have haxs : a ∈ xs := by
          simp at ha
          rcases ha with h | h
          · exact False.elim (hxa h.symm)
          · exact h
        rw [List.count_cons]
        have hb : (x == a) = false := beq_eq_false_iff_ne.mpr hxa
        rw [hb]
        simp
        simpa using ih hn.2 haxs

private theorem length_eq_of_nodup_mem {α : Type} [BEq α] [LawfulBEq α]
    (l₁ l₂ : List α) (h₁ : l₁.Nodup) (h₂ : l₂.Nodup)
    (hmem : ∀ x, x ∈ l₁ ↔ x ∈ l₂) : l₁.length = l₂.length := by
  apply List.Perm.length_eq
  apply List.perm_iff_count.mpr
  intro a
  by_cases ha : a ∈ l₁
  · have ha2 : a ∈ l₂ := (hmem a).mp ha
    rw [count_eq_one_of_mem_nodup l₁ h₁ a ha, count_eq_one_of_mem_nodup l₂ h₂ a ha2]
  · have ha2 : ¬ a ∈ l₂ := fun h => ha ((hmem a).mpr h)
    rw [List.count_eq_zero.mpr ha, List.count_eq_zero.mpr ha2]

private theorem range_filter_eq_singleton (n i : Nat) (hi : i < n) :
    (List.range n).filter (fun j => j == i) = [i] := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [List.range_succ, List.filter_append]
      by_cases hni : n = i
      · subst i
        have hfil : (List.range n).filter (fun j => j == n) = [] := by
          apply List.eq_nil_iff_forall_not_mem.mpr
          intro j hj
          have hpair := List.mem_filter.mp hj
          have hjlt : j < n := List.mem_range.mp hpair.1
          have hb : (j == n) = false := beq_eq_false_iff_ne.mpr (by omega)
          rw [hb] at hpair
          cases hpair.2
        rw [hfil]
        simp
      · by_cases hin : i < n
        · rw [ih hin]
          simp [hni]
        · have hgt : n < i := by omega
          have hfil : (List.range n).filter (fun j => j == i) = [] := by
            apply List.eq_nil_iff_forall_not_mem.mpr
            intro j hj
            have hpair := List.mem_filter.mp hj
            have hjlt : j < n := List.mem_range.mp hpair.1
            have hb : (j == i) = false := beq_eq_false_iff_ne.mpr (by omega)
            rw [hb] at hpair
            cases hpair.2
          rw [hfil]
          simp [hni]
          omega

private theorem countP_eq_single (n i : Nat) (hi : i < n) :
    (List.range n).countP (fun j => j == i) = 1 := by
  rw [List.countP_eq_length_filter, range_filter_eq_singleton n i hi]
  simp

private theorem intersection_step_pos (p q : Progression) (hp : 0 < p.step) (hq : 0 < q.step) :
    0 < (intersection p q).step := by
  by_cases hpn : p.n = 0
  · have hint : intersection p q = ⟨0, 1, 0⟩ := by
      unfold intersection
      simp [hpn]
    rw [hint]
    simp
  by_cases hqn : q.n = 0
  · have hint : intersection p q = ⟨0, 1, 0⟩ := by
      unfold intersection
      simp [hqn]
    rw [hint]
    simp
  have hp0 : p.step ≠ 0 := by omega
  have hq0 : q.step ≠ 0 := by omega
  let g := Nat.gcd p.step q.step
  let gap := q.lo - p.lo
  let period := (p.step / g) * q.step
  let unit := (bezout (p.step / g) (q.step / g)).1
  let root := p.lo + (p.step : Int) * ((gap / (g : Int) * unit) % ((q.step / g : Nat) : Int))
  let low := max p.lo q.lo
  let first := low + (root - low) % (period : Int)
  let high := min (lastVal p) (lastVal q)
  by_cases hgap : gap % (g : Int) = 0
  · have hint : intersection p q = ⟨first, period,
        if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ := by
      unfold intersection
      simp [hpn, hqn, hp0, hq0, hgap, g, gap, period, unit, root, low, first, high]
    rw [hint]
    have hg : 0 < g := Nat.gcd_pos_of_pos_left q.step hp
    exact Nat.mul_pos (Nat.div_pos (Nat.le_of_dvd hp (Nat.gcd_dvd_left p.step q.step)) hg) hq
  · have hint : intersection p q = ⟨0, 1, 0⟩ := by
      unfold intersection
      simp [hpn, hqn, hp0, hq0, hgap, g, gap]
    rw [hint]
    simp

private theorem intersection_count (p q : Progression) (hp : 0 < p.step) :
    (intersection p q).n = (List.range p.n).countP (fun i => contains q (progVal p i)) := by
  by_cases hq0 : q.step = 0
  · by_cases hqn : q.n = 0
    · have hint : intersection p q = ⟨0, 1, 0⟩ := by
        unfold intersection
        simp [hqn, hq0]
      rw [hint]
      have hfalse : ∀ i ∈ List.range p.n, contains q (progVal p i) = false := by
        intro i hi
        simp [contains, hqn]
      rw [List.countP_eq_zero.mpr (fun i hi => by rw [hfalse i hi]; simp)]
    · by_cases hpn : p.n = 0
      · have hint : intersection p q = ⟨0, 1, 0⟩ := by
          unfold intersection
          simp [hpn]
        rw [hint]
        simp [contains, hpn]
      have hp0 : p.step ≠ 0 := by omega
      have hint : intersection p q = ⟨q.lo, 1, if contains p q.lo = true then 1 else 0⟩ := by
        unfold intersection
        simp [hpn, hqn, hq0, hp0]
      rw [hint]
      have hconst : ∀ v, contains q v = true ↔ v = q.lo := by
        intro v
        rw [contains_iff_mem, List.mem_map]
        constructor
        · rintro ⟨i, hi, hv⟩
          have hv' : q.lo + (i : Int) * (q.step : Int) = v := hv
          rw [hq0] at hv'
          simp at hv'
          exact hv'.symm
        · intro hv
          refine ⟨0, List.mem_range.mpr (by omega), ?_⟩
          simp [hv, hq0, progVal]
      have hcnt : (List.range p.n).countP (fun i => contains q (progVal p i)) =
          (List.range p.n).countP (fun i => progVal p i == q.lo) := by
        apply List.countP_congr
        intro i hi
        simp [hconst, beq_iff_eq]
      rw [hcnt]
      have hpInj : Function.Injective (progVal p) := progVal_injective p hp
      have hnod1 := nodup_map_of_injective (progVal p) hpInj (List.range p.n) List.nodup_range
      have hnod2 : (List.range (if contains p q.lo = true then 1 else 0)).map
          (fun i : Nat => q.lo + (i : Int) * (1 : Int)) |>.Nodup := by
        apply nodup_map_of_injective
        · intro i j hij
          dsimp at hij
          omega
        · exact List.nodup_range
      have hcntEq : (List.range p.n).countP (fun i => progVal p i == q.lo) =
          if contains p q.lo = true then 1 else 0 := by
        by_cases hmem : contains p q.lo = true
        · rw [if_pos hmem]
          obtain ⟨i, hi, hvi⟩ := List.mem_map.mp ((contains_iff_mem p q.lo).mp hmem)
          have hiRange : i < p.n := List.mem_range.mp hi
          have hall : ∀ j ∈ List.range p.n, (progVal p j == q.lo) = (j == i) := by
            intro j hj
            by_cases hji : j = i
            · subst j
              simp [hvi]
            · have hpj : progVal p j ≠ q.lo := by
                intro heq
                have : progVal p j = progVal p i := by omega
                exact hji (progVal_injective p hp this)
              rw [beq_eq_false_iff_ne.mpr hpj, beq_eq_false_iff_ne.mpr hji]
          rw [← countP_eq_single p.n i hiRange]
          apply List.countP_congr
          intro j hj
          change ((progVal p j == q.lo) = true ↔ (j == i) = true)
          rw [hall j hj]
        · rw [if_neg hmem]
          apply List.countP_eq_zero.mpr
          intro i hi hv
          have hb : progVal p i = q.lo := beq_iff_eq.mp hv
          exact hmem ((contains_iff_mem p q.lo).mpr (List.mem_map.mpr ⟨i, hi, hb⟩))
      rw [hcntEq]
  · have hq : 0 < q.step := Nat.pos_of_ne_zero hq0
    let l1 := ((List.range p.n).filter (fun i => contains q (progVal p i))).map (progVal p)
    let r := intersection p q
    let l2 := (List.range r.n).map (progVal r)
    have hnod1 : l1.Nodup := by
      dsimp [l1]
      apply nodup_map_of_injective (progVal p) (progVal_injective p hp)
      apply nodup_filter_of_nodup
      exact List.nodup_range
    have hnod2 : l2.Nodup := by
      dsimp [l2]
      apply nodup_map_of_injective (progVal r) (progVal_injective r (intersection_step_pos p q hp hq))
      exact List.nodup_range
    have hmem : ∀ v, v ∈ l1 ↔ v ∈ l2 := by
      intro v
      dsimp [l1, l2]
      rw [List.mem_map, List.mem_map]
      constructor
      · rintro ⟨i, hi, hvi⟩
        have hiFilter := List.mem_filter.mp hi
        have hpi : contains p (progVal p i) = true := by
          apply (contains_iff_mem p (progVal p i)).mpr
          apply List.mem_map.mpr
          exact ⟨i, hiFilter.1, rfl⟩
        have hboth : contains p v = true ∧ contains q v = true := by
          rw [← hvi]
          exact ⟨hpi, hiFilter.2⟩
        have hrv : contains r v = true := (intersection_contains p q v).mpr hboth
        obtain ⟨k, hk, hkv⟩ := List.mem_map.mp ((contains_iff_mem r v).mp hrv)
        exact ⟨k, hk, hkv⟩
      · rintro ⟨k, hk, hkv⟩
        have hrv : contains r (progVal r k) = true := by
          apply (contains_iff_mem r (progVal r k)).mpr
          apply List.mem_map.mpr
          exact ⟨k, hk, rfl⟩
        have hboth := (intersection_contains p q (progVal r k)).mp hrv
        obtain ⟨i, hi, hvi⟩ := List.mem_map.mp ((contains_iff_mem p (progVal r k)).mp hboth.1)
        refine ⟨i, ?_, hvi.trans hkv⟩
        apply List.mem_filter.mpr
        refine ⟨hi, ?_⟩
        rw [hvi]
        exact hboth.2
    have hlen := length_eq_of_nodup_mem l1 l2 hnod1 hnod2 hmem
    have hlen1 : (List.filter (fun i => contains q (progVal p i)) (List.range p.n)).length = r.n := by
      simpa [l1, l2] using hlen
    rw [← List.countP_eq_length_filter] at hlen1
    simpa [r] using hlen1.symm

/-! ## Lane value membership and join counts -/

private theorem values_contains_iff (a : Lane) (v : Int) :
    contains (values a) v = true ↔ ∃ j, j < a.n ∧ a.base + a.step * ((j : Nat) : Int) = v := by
  rw [contains_iff_mem, List.mem_map]
  constructor
  · rintro ⟨k, hk, hval⟩
    have hklt : k < a.n := List.mem_range.mp hk
    by_cases hneg : a.step < 0
    · let j := a.n - 1 - k
      have hjlt : j < a.n := by dsimp [j]; omega
      refine ⟨j, hjlt, ?_⟩
      have hstep : ((a.step.natAbs : Nat) : Int) = -a.step := by
        rw [← Int.natAbs_neg a.step]
        exact Int.natAbs_of_nonneg (by omega)
      dsimp [values, progVal] at hval
      rw [if_pos hneg] at hval
      rw [hstep] at hval
      have hjcast : ((j : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (k : Int) := by
        dsimp [j]
        rw [Int.ofNat_sub (by omega : k ≤ a.n - 1)]
      rw [hjcast, Int.mul_sub]
      have hc1 : a.step * ((a.n - 1 : Nat) : Int) = ((a.n - 1 : Nat) : Int) * a.step := Int.mul_comm _ _
      have hc2 : a.step * (k : Int) = -((k : Int) * (-a.step)) := by
        rw [Int.mul_neg, Int.neg_neg]
        exact Int.mul_comm _ _
      have hsum : ((a.n - 1 : Nat) : Int) * (-a.step) =
          -(((a.n - 1 : Nat) : Int) * a.step) := by
        rw [Int.mul_neg]
      omega
    · refine ⟨k, hklt, ?_⟩
      have hstep : ((a.step.natAbs : Nat) : Int) = a.step := by
        exact Int.natAbs_of_nonneg (by omega)
      dsimp [values, progVal] at hval
      rw [if_neg hneg] at hval
      rw [hstep] at hval
      rw [Int.mul_comm]
      exact hval
  · rintro ⟨j, hjlt, hv⟩
    by_cases hneg : a.step < 0
    · let k := a.n - 1 - j
      have hjle : j ≤ a.n - 1 := by omega
      refine ⟨k, List.mem_range.mpr ?_, ?_⟩
      · have hnpos : 0 < a.n := by omega
        exact Nat.lt_of_le_of_lt (Nat.sub_le _ _) (Nat.sub_lt hnpos (by omega))
      have hstep : ((a.step.natAbs : Nat) : Int) = -a.step := by
        rw [← Int.natAbs_neg a.step]
        exact Int.natAbs_of_nonneg (by omega)
      dsimp [values, progVal]
      rw [if_pos hneg]
      rw [hstep]
      have hkcast : ((k : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (j : Int) := by
        dsimp [k]
        rw [Int.ofNat_sub (by omega : j ≤ a.n - 1)]
      rw [hkcast, Int.sub_mul]
      have hc1 : a.step * ((a.n - 1 : Nat) : Int) = ((a.n - 1 : Nat) : Int) * a.step := Int.mul_comm _ _
      have hc2 : a.step * (j : Int) = -((j : Int) * (-a.step)) := by
        rw [Int.mul_neg, Int.neg_neg]
        exact Int.mul_comm _ _
      have hsum : ((a.n - 1 : Nat) : Int) * (-a.step) =
          -(((a.n - 1 : Nat) : Int) * a.step) := by
        rw [Int.mul_neg]
      omega
    · refine ⟨j, List.mem_range.mpr hjlt, ?_⟩
      have hstep : ((a.step.natAbs : Nat) : Int) = a.step := by
        exact Int.natAbs_of_nonneg (by omega)
      dsimp [values, progVal]
      rw [if_neg hneg]
      rw [hstep]
      rw [Int.mul_comm]
      exact hv

private theorem count_lane_val (n : Nat) (base step v : Int) :
    (List.range n).countP (fun j => base + step * ((j : Nat) : Int) == v) =
      if step == 0 then (if base == v then n else 0)
      else if contains (values ⟨0, 1, n, base, step⟩) v then 1 else 0 := by
  induction n with
  | zero => simp [values, contains]
  | succ n ih =>
      rw [List.range_succ, List.countP_append, ih]
      clear ih
      by_cases hs0 : step == 0
      · have hs : step = 0 := beq_iff_eq.mp hs0
        subst step
        by_cases hbv : base == v
        · have hb : base = v := beq_iff_eq.mp hbv
          simp [hbv, hb]
        · have hb : base ≠ v := by simpa using hbv
          simp [hbv, hb]
      · have hs : step ≠ 0 := by simpa using hs0
        by_cases hlast : base + step * ((n : Nat) : Int) == v
        · have hlastEq : base + step * ((n : Nat) : Int) = v := beq_iff_eq.mp hlast
          have hprev : contains (values ⟨0, 1, n, base, step⟩) v = false := by
            rw [Bool.eq_false_iff]
            intro ht
            rw [values_contains_iff] at ht
            obtain ⟨j, hj, heq⟩ := ht
            have hEq : base + step * (j : Int) = base + step * (n : Int) :=
              heq.trans hlastEq.symm
            have hmul : step * (j : Int) = step * (n : Int) := Int.add_left_cancel hEq
            have hij : (j : Int) = (n : Int) := (Int.mul_eq_mul_left_iff hs).mp hmul
            have hjn : j = n := by exact_mod_cast hij
            exact (Nat.lt_irrefl j) (by simpa [hjn] using hj)
          have hnext : contains (values ⟨0, 1, n + 1, base, step⟩) v = true := by
            rw [values_contains_iff]
            exact ⟨n, Nat.lt_succ_self n, hlastEq⟩
          simp [hs0, hlast, hprev, hnext]
        · have hneq : base + step * ((n : Nat) : Int) ≠ v := by simpa using hlast
          by_cases hpv : contains (values ⟨0, 1, n, base, step⟩) v = true
          · have hpv' : contains (values ⟨0, 1, n + 1, base, step⟩) v = true := by
              rw [values_contains_iff] at hpv ⊢
              obtain ⟨j, hj, hval⟩ := hpv
              exact ⟨j, Nat.lt_trans hj (Nat.lt_succ_self n), hval⟩
            simp [hs0, hlast, hpv, hpv']
          · have hpv' : contains (values ⟨0, 1, n + 1, base, step⟩) v = false := by
              rw [Bool.eq_false_iff]
              intro ht
              rw [values_contains_iff] at ht
              obtain ⟨j, hj, hval⟩ := ht
              by_cases hjn : j = n
              · subst j
                exact hneq hval
              · have hjlt : j < n := Nat.lt_of_le_of_ne (Nat.le_of_lt_succ hj) hjn
                have ht : contains (values ⟨0, 1, n, base, step⟩) v = true := by
                  rw [values_contains_iff]
                  exact ⟨j, hjlt, hval⟩
                exact hpv ht
            simp [hs0, hlast, hpv, hpv']

private def laneVal (a : Lane) (i : Nat) : Int := a.base + a.step * ((i : Nat) : Int)
private def laneValList (a : Lane) : List Int := (List.range a.n).map (laneVal a)

private theorem contains_values_iff_lane (a : Lane) (v : Int) :
    contains (values a) v = true ↔ v ∈ laneValList a := by
  rw [values_contains_iff, laneValList, List.mem_map]
  constructor
  · rintro ⟨j, hj, hv⟩
    exact ⟨j, List.mem_range.mpr hj, hv⟩
  · rintro ⟨j, hj, hv⟩
    exact ⟨j, List.mem_range.mp hj, hv⟩

private theorem laneVal_injective (a : Lane) (hs : a.step ≠ 0) : Function.Injective (laneVal a) := by
  intro i j hij
  dsimp [laneVal] at hij
  have hmul : a.step * ((i : Nat) : Int) = a.step * ((j : Nat) : Int) := by
    exact Int.add_left_cancel hij
  have hcast : (i : Int) = (j : Int) := (Int.mul_eq_mul_left_iff hs).mp hmul
  exact Int.ofNat.inj hcast

private theorem laneValList_nodup (a : Lane) (hs : a.step ≠ 0) : (laneValList a).Nodup := by
  dsimp [laneValList]
  apply nodup_map_of_injective (laneVal a) (laneVal_injective a hs)
  exact List.nodup_range

private theorem perm_of_nodup_mem {α : Type} [BEq α] [LawfulBEq α]
    (l₁ l₂ : List α) (h₁ : l₁.Nodup) (h₂ : l₂.Nodup)
    (hmem : ∀ x, x ∈ l₁ ↔ x ∈ l₂) : l₁.Perm l₂ := by
  apply List.perm_iff_count.mpr
  intro a
  by_cases ha : a ∈ l₁
  · have ha2 : a ∈ l₂ := (hmem a).mp ha
    rw [count_eq_one_of_mem_nodup l₁ h₁ a ha, count_eq_one_of_mem_nodup l₂ h₂ a ha2]
  · have ha2 : ¬ a ∈ l₂ := fun h => ha ((hmem a).mpr h)
    rw [List.count_eq_zero.mpr ha, List.count_eq_zero.mpr ha2]

private theorem progressions_values_mem (a : Lane) (v : Int) :
    v ∈ laneValList a ↔ contains (values a) v = true := (contains_values_iff_lane a v).symm

private theorem countP_of_mem_eq (n : Nat) (base step : Int) (v : Int) (hs : step ≠ 0) :
    (List.range n).countP (fun j => base + step * ((j : Nat) : Int) == v) =
      if contains (values ⟨0, 1, n, base, step⟩) v then 1 else 0 := by
  rw [count_lane_val]
  simp [show (step == 0) = false by simpa using hs]

private theorem count_common_eq_inter (p q : Progression) (hp : 0 < p.step) :
    (List.range p.n).countP (fun i => contains q (progVal p i)) =
      (intersection p q).n := (intersection_count p q hp).symm

private theorem range_succ_cons (n : Nat) :
    List.range (n + 1) = 0 :: (List.range n).map (fun i => i + 1) := by
  apply List.ext_getElem?
  intro k
  cases k with
  | zero => simp
  | succ j =>
      rw [List.getElem?_cons_succ, List.getElem?_map]
      by_cases hj : j < n
      · rw [List.getElem?_range hj, List.getElem?_range (by omega : j + 1 < n + 1)]
        simp
      · have hjge : n ≤ j := by omega
        rw [List.getElem?_eq_none (by simpa using hjge : (List.range n).length ≤ j)]
        simp
        omega

private theorem countP_range_reflect (n : Nat) (p : Nat → Bool) :
    (List.range n).countP p = (List.range n).countP (fun i => p (n - 1 - i)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      calc
        (List.range (n + 1)).countP p =
            (List.range n).countP p + (if p n then 1 else 0) := by
          rw [List.range_succ, List.countP_append]
          simp
        _ = (List.range n).countP (fun i => p (n - 1 - i)) + (if p n then 1 else 0) := by
          rw [ih]
        _ = (List.range (n + 1)).countP (fun i => p ((n + 1) - 1 - i)) := by
          rw [range_succ_cons n, List.countP_cons, List.countP_map]
          have hq : (fun i => p (n - (i + 1))) = (fun i => p (n - 1 - i)) := by
            funext i
            congr 1
            omega
          have hsub : n + 1 - 1 = n := by omega
          rw [hsub]
          change (List.range n).countP (fun i => p (n - 1 - i)) + (if p n then 1 else 0) =
            (List.range n).countP (fun i => p (n - (i + 1))) + (if p n then 1 else 0)
          rw [hq]

private theorem contains_values_lane (b : Lane) (v : Int) :
    contains (values b) v = true ↔ ∃ i, i < b.n ∧ v = b.base + b.step * (i : Int) := by
  rw [values_contains_iff]
  constructor
  · rintro ⟨i, hi, h⟩
    exact ⟨i, hi, h.symm⟩
  · rintro ⟨i, hi, h⟩
    exact ⟨i, hi, h.symm⟩

private def hit (b : Lane) (v : Int) : Nat :=
  (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int))

private theorem countP_eq_sum_nat {α : Type} (l : List α) (p : α → Bool) :
    l.countP p = (l.map (fun x => if p x then 1 else 0)).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih] <;> omega

private theorem sum_range_const_nat (n k : Nat) :
    ((List.range n).map (fun _ => k)).sum = n * k := by
  induction n with
  | zero => simp
  | succ n ih => simp [List.range_succ, ih, Nat.succ_mul]

private theorem hit_step_zero (b : Lane) (v : Int) (hs : b.step = 0) :
    hit b v = if b.base == v then b.n else 0 := by
  dsimp [hit]
  rw [countP_eq_sum_nat]
  simp [hs]
  by_cases h : v = b.base
  · have hb : b.base = v := h.symm
    rw [show (fun _ : Nat => if v = b.base then 1 else 0) = (fun _ : Nat => 1) by
      funext x; simp [h]]
    rw [show (if b.base = v then b.n else 0) = b.n by simp [hb]]
    simpa using sum_range_const_nat b.n 1
  · have hb : ¬ b.base = v := by intro hh; exact h hh.symm
    rw [show (fun _ : Nat => if v = b.base then 1 else 0) = (fun _ : Nat => 0) by
      funext x; simp [h]]
    rw [show (if b.base = v then b.n else 0) = 0 by simp [hb]]
    induction b.n with
    | zero => simp
    | succ n ih => simp [List.range_succ, ih]

private theorem countP_range_unique (n : Nat) (p : Nat → Bool)
    (huniq : ∀ i j, i < n → j < n → p i = true → p j = true → i = j) :
    (List.range n).countP p = if (∃ i, i < n ∧ p i = true) then 1 else 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.countP_append]
      have htailuniq : ∀ i j, i < n → j < n → p i = true → p j = true → i = j := by
        intro i j hi hj; exact huniq i j (by omega) (by omega)
      rw [ih htailuniq]
      by_cases hpn : p n = true
      · have hnone : ¬ ∃ i, i < n ∧ p i = true := by
          rintro ⟨i, hi, hpi⟩
          have heq := huniq i n (by omega) (by omega) hpi hpn
          omega
        have hS : ∃ i, i < n+1 ∧ p i = true := ⟨n, by omega, hpn⟩
        simp [hpn, hnone, hS]
      · have hpnf : p n = false := by cases h : p n <;> simp_all
        have hiff : (∃ i, i < n+1 ∧ p i = true) ↔ (∃ i, i < n ∧ p i = true) := by
          constructor
          · rintro ⟨i, hi, hpi⟩
            have hine : i ≠ n := by intro heq; subst i; rw [hpnf] at hpi; cases hpi
            exact ⟨i, by omega, hpi⟩
          · rintro ⟨i, hi, hpi⟩
            exact ⟨i, by omega, hpi⟩
        by_cases hnone : ∃ i, i < n ∧ p i = true
        · have hS : (∃ i, i < n+1 ∧ p i = true) := hiff.mpr hnone
          simp [hpnf, hnone, hS]
        · have hS : ¬ (∃ i, i < n+1 ∧ p i = true) := by intro hh; exact hnone (hiff.mp hh)
          simp [hpnf, hnone, hS]

private theorem hit_step_ne (b : Lane) (v : Int) (hs : b.step ≠ 0) :
    hit b v = if (∃ i, i < b.n ∧ v = b.base + b.step * (i : Int)) then 1 else 0 := by
  dsimp [hit]
  rw [countP_range_unique b.n (fun i => v == b.base + b.step * (i : Int))]
  · have hiff : (∃ i, i < b.n ∧ (v == b.base + b.step * (i : Int)) = true) ↔
        (∃ i, i < b.n ∧ v = b.base + b.step * (i : Int)) := by
      constructor
      · rintro ⟨i, hi, hpi⟩
        exact ⟨i, hi, beq_iff_eq.mp hpi⟩
      · rintro ⟨i, hi, hval⟩
        exact ⟨i, hi, by simp [hval]⟩
    by_cases hex : ∃ i, i < b.n ∧ (v == b.base + b.step * (i : Int)) = true
    · have hex' : ∃ i, i < b.n ∧ v = b.base + b.step * (i : Int) := hiff.mp hex
      simp [hex, hex']
    · have hex' : ¬ (∃ i, i < b.n ∧ v = b.base + b.step * (i : Int)) := by
        intro hh; exact hex (hiff.mpr hh)
      simp [hex, hex']
  · intro i j hi hj hpi hpj
    have hv1 : v = b.base + b.step * (i : Int) := beq_iff_eq.mp hpi
    have hv2 : v = b.base + b.step * (j : Int) := beq_iff_eq.mp hpj
    have hm0 : b.step * (i : Int) = b.step * (j : Int) := by omega
    have hm : (i : Int) * b.step = (j : Int) * b.step := by simpa [Int.mul_comm] using hm0
    have hsint : (b.step : Int) ≠ 0 := by exact_mod_cast hs
    have hij : (i : Int) = (j : Int) := Int.eq_of_mul_eq_mul_right hsint hm
    exact Int.ofNat_inj.mp hij

private theorem hit_eq_contains_values (b : Lane) (v : Int) :
    hit b v = (if contains (values b) v then (if b.step == 0 then b.n else 1) else 0) := by
  by_cases hbz : b.step = 0
  · rw [hit_step_zero b v hbz]
    by_cases hbn : b.n = 0
    · simp [contains, hbn, hbz]
    · have hbnpos : 0 < b.n := by omega
      by_cases hveq : b.base = v
      · have hcv : contains (values b) v = true := by
          rw [contains_values_lane]
          exact ⟨0, hbnpos, by simp [hbz, hveq]⟩
        simp [hveq, hcv, hbz]
      · have hcv : ¬ contains (values b) v = true := by
          intro h
          rcases (contains_values_lane b v).mp h with ⟨i, hi, hv⟩
          exact hveq (by simpa [hbz] using hv.symm)
        have hbeq : (b.base == v) = false := beq_eq_false_iff_ne.mpr hveq
        simp [hbeq, hcv, hbz]
  · rw [hit_step_ne b v hbz]
    have hcv := contains_values_lane b v
    by_cases hex : ∃ i, i < b.n ∧ v = b.base + b.step * (i : Int)
    · have hc : contains (values b) v = true := hcv.mpr hex
      simp [hex, hc, hbz]
    · have hc : ¬ contains (values b) v = true := by intro h; exact hex (hcv.mp h)
      simp [hex, hc, hbz]

private theorem sum_range_ite_count (n k : Nat) (p : Nat → Bool) :
    ((List.range n).map (fun i => if p i then k else 0)).sum = k * (List.range n).countP p := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
      rw [List.countP_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, List.countP_cons,
        List.countP_nil]
      by_cases hp : p n
      · simp [hp, ih, Nat.mul_add, Nat.mul_one, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]
      · simp [hp, ih]

private theorem count_lane_values_contains (a b : Lane) (ha : a.step ≠ 0) :
    (List.range a.n).countP (fun i : Nat => contains (values b) (a.base + (i : Int) * a.step)) =
      (intersection (values a) (values b)).n := by
  by_cases hpos : 0 < a.step
  · have hnegfalse : ¬ a.step < 0 := by omega
    have hva : values a = ⟨a.base, a.step.natAbs, a.n⟩ := by
      simp [values, hnegfalse]
    rw [hva]
    have habs : (a.step.natAbs : Int) = a.step := Int.natAbs_of_nonneg (by omega)
    have habspos : 0 < a.step.natAbs := by
      have : (0 : Int) < (a.step.natAbs : Int) := by rw [habs]; exact hpos
      exact_mod_cast this
    have h := count_common_eq_inter
      (⟨a.base, a.step.natAbs, a.n⟩ : Progression) (values b) habspos
    simpa [progVal, habs, Int.mul_comm] using h
  · have hneg : a.step < 0 := by omega
    have habs : (a.step.natAbs : Int) = -a.step := by
      rw [← Int.natAbs_neg a.step]
      exact Int.natAbs_of_nonneg (by omega)
    have hva : values a = ⟨a.base + ((a.n - 1 : Nat) : Int) * a.step, a.step.natAbs, a.n⟩ := by
      simp [values, hneg]
    rw [hva]
    have href := countP_range_reflect a.n
      (fun i : Nat => contains (values b) (a.base + (i : Int) * a.step))
    rw [href]
    have habspos : 0 < a.step.natAbs := Nat.pos_of_ne_zero (by omega : a.step.natAbs ≠ 0)
    have h := count_common_eq_inter
      (⟨a.base + ((a.n - 1 : Nat) : Int) * a.step, a.step.natAbs, a.n⟩ : Progression)
      (values b) habspos
    have hfun : ∀ i ∈ List.range a.n,
        contains (values b) (a.base + ((a.n - 1 - i : Nat) : Int) * a.step) =
        contains (values b) (a.base + ((a.n - 1 : Nat) : Int) * a.step +
          (i : Int) * (a.step.natAbs : Int)) := by
      intro i hi
      have hi' : i < a.n := List.mem_range.mp hi
      have hcast : ((a.n - 1 - i : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (i : Int) := by omega
      rw [hcast, habs]
      congr 1
      grind
    rw [show (List.range a.n).countP
          (fun i : Nat => contains (values b) (a.base + ((a.n - 1 - i : Nat) : Int) * a.step)) =
        (List.range a.n).countP (fun i : Nat =>
          contains (values b) (a.base + ((a.n - 1 : Nat) : Int) * a.step +
            (i : Int) * (a.step.natAbs : Int))) by
          apply List.countP_congr
          intro i hi
          rw [hfun i hi]]
    simpa [progVal] using h

private theorem hit_sum_eq_inter (a b : Lane) (ha : a.step ≠ 0) :
    ((List.range a.n).map (fun i : Nat => hit b (a.base + (i : Int) * a.step))).sum =
      (intersection (values a) (values b)).n * (if b.step == 0 then b.n else 1) := by
  have hcnt := count_lane_values_contains a b ha
  by_cases hbz : b.step = 0
  · have hbcond : (b.step == 0) = true := by simp [hbz]
    rw [if_pos hbcond]
    have hval : ∀ i : Nat, hit b (a.base + (i : Int) * a.step) =
        (if contains (values b) (a.base + (i : Int) * a.step) then b.n else 0) := by
      intro i
      rw [hit_eq_contains_values]
      simp [hbz]
    rw [show ((List.range a.n).map (fun i : Nat => hit b (a.base + (i : Int) * a.step))) =
        ((List.range a.n).map (fun i : Nat =>
          if contains (values b) (a.base + (i : Int) * a.step) then b.n else 0)) by
        apply List.map_congr_left
        intro i hi
        exact hval i]
    rw [sum_range_ite_count, hcnt]
    exact Nat.mul_comm _ _
  · have hbcond : ¬ (b.step == 0) = true := by simp [hbz]
    rw [if_neg hbcond]
    have hval : ∀ i : Nat, hit b (a.base + (i : Int) * a.step) =
        (if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0) := by
      intro i
      rw [hit_eq_contains_values]
      simp [hbz]
    rw [show ((List.range a.n).map (fun i : Nat => hit b (a.base + (i : Int) * a.step))) =
        ((List.range a.n).map (fun i : Nat =>
          if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0)) by
        apply List.map_congr_left
        intro i hi
        exact hval i]
    rw [sum_range_ite_count, hcnt]
    simp

private theorem hit_eq_joinCount (a b : Lane) (ha : a.step = 0) :
    hit b a.base * a.n = joinCount a b := by
  by_cases han : a.n = 0
  · have hva : values a = ⟨a.base, 0, a.n⟩ := by simp [values, ha]
    simp [han, joinCount, hva, intersection]
  · have hva : values a = ⟨a.base, 0, a.n⟩ := by simp [values, ha]
    have hvn : (values b).n = b.n := rfl
    by_cases hbn : b.n = 0
    · simp [hit, joinCount, hbn, hva, hvn, intersection]
    · have hint : intersection (values a) (values b) =
          ⟨a.base, 1, if contains (values b) a.base then 1 else 0⟩ := by
        simp [intersection, han, hva, hvn, hbn]
      unfold joinCount
      rw [hint]
      simp only [ha, ite_true]
      by_cases hc : contains (values b) a.base = true
      · have hex := (contains_values_lane b a.base).mp hc
        by_cases hbz : b.step = 0
        · rw [hit_step_zero b a.base hbz]
          have hbase : b.base = a.base := by
            obtain ⟨i, hi, hval⟩ := hex
            simpa [hbz] using hval.symm
          simp [hbz, hbase, hc, hva, hvn, intersection, han]
          exact Nat.mul_comm _ _
        · rw [hit_step_ne b a.base hbz]
          simp [hbz, hex, hc]
      · have hno : ¬ (∃ i, i < b.n ∧ a.base = b.base + b.step * (i : Int)) := by
          intro hh
          exact hc ((contains_values_lane b a.base).mpr hh)
        by_cases hbz : b.step = 0
        · rw [hit_step_zero b a.base hbz]
          have hbase : b.base ≠ a.base := by
            intro heq
            apply hno
            refine ⟨0, by omega, ?_⟩
            simp [hbz, heq]
          simp [hbz, hbase, hc, hva, hvn, intersection, han]
        · rw [hit_step_ne b a.base hbz]
          simp [hbz, hno, hc]

private theorem joinCount_eq_raw (a b : Lane) :
    ((List.range a.n).map (fun i : Nat => hit b (a.base + (i : Int) * a.step))).sum =
      joinCount a b := by
  by_cases ha : a.step = 0
  · rw [show ((List.range a.n).map (fun i : Nat => hit b (a.base + (i : Int) * a.step))) =
        ((List.range a.n).map (fun _ : Nat => hit b a.base)) by
      apply List.map_congr_left
      intro i hi
      simp [ha]]
    rw [sum_range_const_nat]
    rw [Nat.mul_comm]
    exact hit_eq_joinCount a b ha
  · rw [hit_sum_eq_inter a b ha]
    unfold joinCount
    simp [ha]



namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.SparseSample.Remaining.E3Lib

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-! ## Generic list helpers -/

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem filterMap_congr' {α β : Type} {l : List α} {f g : α → Option β}
    (h : ∀ x ∈ l, f x = g x) : l.filterMap f = l.filterMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filterMap_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem range_filterMap_all {α : Type} (n : Nat) (f : Nat → Option α) (g : Nat → α)
    (h : ∀ i, i < n → f i = some (g i)) :
    (List.range n).filterMap f = (List.range n).map g := by
  rw [← filterMap_some_eq_map (List.range n) g]
  apply filterMap_congr'
  intro i hi
  exact h i (List.mem_range.mp hi)

private theorem rows_range_map (lay : Layout) (c : Config) (f : Nat → Int)
    (h : ∀ i, i < c.n → lay.read c i = some (f i)) :
    rows lay c = (List.range c.n).map (fun i => (i, f i)) := by
  unfold rows
  exact range_filterMap_all c.n (fun i => (lay.read c i).map (fun v => (i, v)))
    (fun i => (i, f i)) (by intro i hi; simp [h i hi])

private theorem decide_eq_of_iff {a b : Prop} [Decidable a] [Decidable b] (h : a ↔ b) :
    decide a = decide b := by
  by_cases ha : a
  · have hb : b := h.mp ha
    simp [ha, hb]
  · have hb : ¬ b := fun hb => ha (h.mpr hb)
    simp [ha, hb]

private theorem filter_congr_bool {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, p x = q x) : l.filter p = l.filter q := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filter_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem map_pair_filter {α β : Type} (xs : List α) (f : α → Nat) (g : α → β)
    (p : Nat → Bool) :
    (xs.map (fun x => (f x, g x))).filter (fun r => p r.1) =
      (xs.filter (fun x => p (f x))).map (fun x => (f x, g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => by_cases h : p (f x) <;> simp [h, ih]

private theorem map_congr {α β : Type} (l : List α) (f g : α → β)
    (h : ∀ x ∈ l, f x = g x) : l.map f = l.map g := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem list_sum_perm {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem list_sum_perm_nat {l₁ l₂ : List Nat} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem perm_length {α : Type} {l₁ l₂ : List α} (p : l₁.Perm l₂) :
    l₁.length = l₂.length := by
  induction p with
  | nil => rfl
  | cons a p ih => simp [ih]
  | swap a b l => simp
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem sum_map_add_int {α : Type} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_map_sub_int {α : Type} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x - g x)).sum = (l.map f).sum - (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_map_const_int {α : Type} (l : List α) (v : Int) :
    (l.map (fun _ => v)).sum = (l.length : Int) * v := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih]
      grind

private theorem sum_map_mul_int (l : List Int) (c : Int) :
    (l.map (fun x => c * x)).sum = c * l.sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.mul_add]

private theorem countP_perm {α : Type} (l : List α) (p : α → Bool) {l' : List α}
    (h : l.Perm l') : l.countP p = l'.countP p := by
  induction h with
  | nil => rfl
  | cons a h ih =>
      simp only [List.countP_cons]
      rw [ih]
  | swap a b l =>
      simp only [List.countP_cons]
      by_cases hpa : p a <;> by_cases hpb : p b <;> simp [hpa, hpb, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]
  | trans h₁ h₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem sum_ite_filter_nat {α : Type} (l : List α) (p : α → Bool) (v : Nat) :
    ((l.map (fun x => if p x then v else 0)).sum) = v * (l.filter p).length := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih, Nat.mul_add] <;> omega

private theorem sum_ite_filter_one {α : Type} (l : List α) (p : α → Bool) :
    ((l.map (fun x => if p x then 1 else 0)).sum) = (l.filter p).length := by
  simpa using sum_ite_filter_nat l p 1

private theorem countP_eq_sum_one {α : Type} (l : List α) (p : α → Bool) :
    l.countP p = ((l.map (fun x => if p x then 1 else 0)).sum) := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih] <;> omega

private theorem range_filter_eq_singleton (p a : Nat) (ha : a < p) :
    (List.range p).filter (fun r => r == a) = [a] := by
  induction p with
  | zero => omega
  | succ p ih =>
      rw [List.range_succ, List.filter_append]
      by_cases hap : a < p
      · have htail : (List.filter (fun r => r == a) [p]) = [] := by
          have hne : (p == a) = false := beq_eq_false_iff_ne.mpr (by omega)
          simp [hne]
        rw [ih hap, htail]
        simp
      · have haeq : a = p := by omega
        subst a
        have hpre : (List.range p).filter (fun r => r == p) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro r hr hbeq
          have hreq : r = p := beq_iff_eq.mp hbeq
          have hrlt : r < p := List.mem_range.mp hr
          omega
        rw [hpre]
        simp

private theorem countP_beq_eq_one_of_mem_nodup {α : Type} [BEq α] [LawfulBEq α]
    (l : List α) (hl : l.Nodup) (v : α) :
    l.countP (fun w => v == w) = if v ∈ l then 1 else 0 := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hxs : xs.Nodup := hl.of_cons
      by_cases hx : v = x
      · subst hx
        have hnot : v ∉ xs := (List.nodup_cons.mp hl).1
        have ihv := ih hxs
        simp [hnot, ihv]
      · have hx' : (v == x) = false := beq_eq_false_iff_ne.mpr hx
        have hmem : (v ∈ x :: xs) = (v ∈ xs) := by simp [hx]
        simp [hx', ih hxs, hmem]

private theorem countP_beq_const_list {α : Type} [BEq α] [LawfulBEq α]
    (n : Nat) (v w : α) [DecidableEq α] :
    ((List.range n).map (fun _ => v)).countP (fun x => w == x) =
      if w == v then n else 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.countP_append, ih]
      by_cases h : w == v <;> simp [h] <;> omega

private theorem sum_range_succ_int (f : Nat → Int) (n : Nat) :
    ((List.range (n + 1)).map f).sum = ((List.range n).map f).sum + f n := by
  rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
  simp

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
      simpa using Nat.mul_le_mul_left k (show 1 ≤ k by omega)
    have hcan : k * k - k + k = k * k := Nat.sub_add_cancel hle
    have hEq : (k + 1) * k = k * (k - 1) + 2 * k := by rw [hmul1, hmul2]; omega
    rw [hEq]
    exact Nat.add_mul_div_left (k * (k - 1)) k (by decide : 0 < 2)

private theorem tri_succ (k : Nat) : tri (k + 1) = tri k + (k : Int) := by
  dsimp [tri]
  rw [choose2_succ]
  simp [Int.natCast_add]

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
      rw [partialSum_succ, ih, List.range_succ, List.map_append, List.sum_append]
      simp

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

private theorem squares_eq_sum_sq (k : Nat) :
    ((List.range k).map (fun i : Nat => (i : Int) * (i : Int))).sum = squares k := by
  induction k with
  | zero => simp [squares]
  | succ k ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih, squares_succ]
      grind

private theorem sum_filter_complement {α : Type} (l : List α) (p : α → Bool) (f : α → Int) :
    ((l.filter p).map f).sum + ((l.filter (fun x => !p x)).map f).sum = (l.map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true
      · simp [h, ih]
        omega
      · simp [h, ih]
        omega

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
  have hremnon : 0 ≤ (root - low) % period := Int.emod_nonneg _ (by omega : (period : Int) ≠ 0)
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

private theorem contains_unit_le_one (lo : Int) (n : Nat) (v : Int) (hn : n ≤ 1) :
    contains ⟨lo, 1, n⟩ v = true ↔ 0 < n ∧ v = lo := by
  by_cases hn0 : n = 0
  · subst n
    simp [contains]
  · have hnpos : 0 < n := Nat.pos_of_ne_zero hn0
    constructor
    · intro h
      obtain ⟨k, hk, hval⟩ := (contains_iff_mem ⟨lo, 1, n⟩ v).mp h
      change k < n at hk
      have hk0 : k = 0 := by omega
      exact ⟨hnpos, by simpa [progVal, hk0] using hval⟩
    · rintro ⟨hpos, hvl⟩
      rw [hvl]
      exact (contains_iff_mem ⟨lo, 1, n⟩ lo).mpr ⟨0, hpos, by simp [progVal]⟩

private theorem contains_const_iff (lo : Int) (n : Nat) (v : Int) :
    contains ⟨lo, 0, n⟩ v = true ↔ 0 < n ∧ v = lo := by
  by_cases hn0 : n = 0
  · subst n
    simp [contains]
  · have hnpos : 0 < n := Nat.pos_of_ne_zero hn0
    constructor
    · intro h
      obtain ⟨k, hk, hval⟩ := (contains_iff_mem ⟨lo, 0, n⟩ v).mp h
      exact ⟨hnpos, by simpa [progVal] using hval⟩
    · rintro ⟨hpos, hvl⟩
      rw [hvl]
      exact (contains_iff_mem ⟨lo, 0, n⟩ lo).mpr ⟨0, hpos, by simp [progVal]⟩

private theorem contains_bounds (a : Progression) (v : Int) (h : contains a v = true) :
    a.lo ≤ v ∧ v ≤ lastVal a := by
  have hm := (contains_iff_mem a v).mp h
  obtain ⟨k, hk, hval⟩ := hm
  unfold progVal at hval
  have hkint : (k : Int) ≤ ((a.n - 1 : Nat) : Int) := by exact_mod_cast (by omega)
  have hstep : 0 ≤ (a.step : Int) := by exact_mod_cast (Nat.zero_le a.step)
  have hmul := Int.mul_le_mul_of_nonneg_right hkint hstep
  have hprod : 0 ≤ (k : Int) * (a.step : Int) := Int.mul_nonneg (by omega) hstep
  unfold lastVal
  constructor <;> omega

private theorem contains_dvd (a : Progression) (v : Int) (h : contains a v = true) :
    (a.step : Int) ∣ v - a.lo := by
  obtain ⟨k, _, hval⟩ := (contains_iff_mem a v).mp h
  unfold progVal at hval
  rw [Int.mul_comm (k : Int) a.step] at hval
  refine ⟨(k : Int), ?_⟩
  omega

private theorem common_gap_dvd (a b : Progression) (v : Int)
    (ha : contains a v = true) (hb : contains b v = true) :
    (a.step.gcd b.step : Int) ∣ b.lo - a.lo := by
  obtain ⟨i, _, hi⟩ := (contains_iff_mem a v).mp ha
  obtain ⟨j, _, hj⟩ := (contains_iff_mem b v).mp hb
  unfold progVal at hi hj
  have hg_dvd_b : ((a.step.gcd b.step : Nat) : Int) ∣ (b.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_right a.step b.step)
  have hg_dvd_a : ((a.step.gcd b.step : Nat) : Int) ∣ (a.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_left a.step b.step)
  have h1 : (a.step.gcd b.step : Int) ∣ (b.step : Int) * (j : Int) :=
    Int.dvd_mul_of_dvd_left (c := (j : Int)) hg_dvd_b
  have h2 : (a.step.gcd b.step : Int) ∣ (a.step : Int) * (i : Int) :=
    Int.dvd_mul_of_dvd_left (c := (i : Int)) hg_dvd_a
  rw [Int.mul_comm (i : Int) a.step] at hi
  rw [Int.mul_comm (j : Int) b.step] at hj
  have heq : (a.step : Int) * (i : Int) - (b.step : Int) * (j : Int) = b.lo - a.lo := by omega
  rw [← heq]
  exact Int.dvd_sub h2 h1

private theorem lcm_dvd_of_dvd_of_dvd (s t : Nat) (hs : 0 < s) (ht : 0 < t) (d : Int)
    (hsd : (s : Int) ∣ d) (htd : (t : Int) ∣ d) :
    (((s / s.gcd t) * t : Nat) : Int) ∣ d := by
  let g := s.gcd t
  let A := s / g
  let B := t / g
  have hg : 0 < g := Nat.gcd_pos_of_pos_left t hs
  have hA : 0 < A := Nat.div_gcd_pos_of_pos_left t hs
  have hB : 0 < B := Nat.div_gcd_pos_of_pos_right s ht
  have hcop : Nat.gcd A B = 1 := by
    have hc : A.Coprime B := by simpa [A, B, g] using Nat.coprime_div_gcd_div_gcd hg
    exact hc.gcd_eq_one
  obtain ⟨k1, hk1⟩ := hsd
  obtain ⟨k2, hk2⟩ := htd
  have hsEq : (s : Int) = (g : Int) * (A : Int) := by
    change (s : Int) = (s.gcd t : Int) * ((s / s.gcd t : Nat) : Int)
    exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_left s t)).symm
  have htEq : (t : Int) = (g : Int) * (B : Int) := by
    change (t : Int) = (s.gcd t : Int) * ((t / s.gcd t : Nat) : Int)
    exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_right s t)).symm
  have hAk : (A : Int) * k1 = (B : Int) * k2 := by
    have hmul : (g : Int) * ((A : Int) * k1) = (g : Int) * ((B : Int) * k2) := by grind
    exact (Int.mul_eq_mul_left_iff (by omega : (g : Int) ≠ 0)).mp hmul
  rcases hbez : bezout A B with ⟨u, vv⟩
  have hspec := bezout_spec A B
  have hcop_int : (A.gcd B : Int) = 1 := by exact_mod_cast hcop
  rw [hbez] at hspec
  rw [hcop_int] at hspec
  simp only [Int.ofNat_one] at hspec
  have hspec' : (A : Int) * u + (B : Int) * vv = 1 := hspec.symm
  have hBdvdAk1 : (B : Int) ∣ (A : Int) * k1 := ⟨k2, hAk⟩
  obtain ⟨m, hm⟩ := hBdvdAk1
  have hBdk1 : (B : Int) ∣ k1 := by
    refine ⟨m * u + k1 * vv, ?_⟩
    calc
      k1 = k1 * ((A : Int) * u + (B : Int) * vv) := by rw [hspec']; simp
      _ = ((A : Int) * k1) * u + (B : Int) * (k1 * vv) := by grind
      _ = (B : Int) * (m * u + k1 * vv) := by rw [hm]; grind
  obtain ⟨q, hq⟩ := hBdk1
  refine ⟨q, ?_⟩
  change d = ((A * t : Nat) : Int) * q
  rw [hk1, hq]
  push_cast
  grind

private theorem intersection_contains (a b : Progression) (v : Int) :
    contains (intersection a b) v = true ↔ contains a v = true ∧ contains b v = true := by
  by_cases han : a.n = 0
  · rcases a with ⟨an, ast, aN⟩
    simp_all [intersection, contains]
  · by_cases hbn : b.n = 0
    · rcases b with ⟨bn, bst, bN⟩
      simp_all [intersection, contains]
    · have hanp : 0 < a.n := by omega
      have hbnp : 0 < b.n := by omega
      have hanB : (a.n == 0) = false := beq_eq_false_iff_ne.mpr han
      have hbnB : (b.n == 0) = false := beq_eq_false_iff_ne.mpr hbn
      have hfirstB : (a.n == 0 || b.n == 0) = false := by simp [hanB, hbnB]
      by_cases has : a.step = 0
      · have hasB : (a.step == 0) = true := by simp [has]
        simp only [intersection]
        rw [if_neg (by simpa [hfirstB]), if_pos (by simpa [hasB])]
        change contains ⟨a.lo, 1, if contains b a.lo then 1 else 0⟩ v = true ↔ _
        rw [contains_iff_mem]
        constructor
        · rintro ⟨k, hk, hv⟩
          change k < (if contains b a.lo then 1 else 0) at hk
          have hb : contains b a.lo = true := by
            by_cases hb : contains b a.lo = true
            · exact hb
            · simp [hb] at hk
          have hklt1 : k < 1 := by simpa [hb] using hk
          have hk0 : k = 0 := by omega
          have hv' : v = a.lo := by
            change v = a.lo + (k : Int) * 1 at hv
            rw [hk0] at hv
            simpa using hv
          have ha : contains a v = true := by
            rw [hv']
            exact (contains_iff_mem a a.lo).mpr ⟨0, hanp, by simp [progVal, has]⟩
          have hbv : contains b v = true := by simpa [hv'] using hb
          exact ⟨ha, hbv⟩
        · rintro ⟨ha, hb⟩
          have hv : v = a.lo := by
            obtain ⟨k, hk, hval⟩ := (contains_iff_mem a v).mp ha
            simpa [progVal, has] using hval
          have hb' : contains b a.lo = true := by simpa [hv] using hb
          refine ⟨0, by simp [hb'], ?_⟩
          simpa [progVal, hv]
      · by_cases hbs : b.step = 0
        · have hasBf : (a.step == 0) = false := beq_eq_false_iff_ne.mpr has
          have hbsB : (b.step == 0) = true := by simp [hbs]
          simp only [intersection]
          rw [if_neg (by simpa [hfirstB]), if_neg (by simpa [hasBf]), if_pos (by simpa [hbsB])]
          change contains ⟨b.lo, 1, if contains a b.lo then 1 else 0⟩ v = true ↔ _
          rw [contains_iff_mem]
          constructor
          · rintro ⟨k, hk, hv⟩
            change k < (if contains a b.lo then 1 else 0) at hk
            have ha : contains a b.lo = true := by
              by_cases ha : contains a b.lo = true
              · exact ha
              · simp [ha] at hk
            have hklt1 : k < 1 := by simpa [ha] using hk
            have hk0 : k = 0 := by omega
            have hv' : v = b.lo := by
              change v = b.lo + (k : Int) * 1 at hv
              rw [hk0] at hv
              simpa using hv
            have hb : contains b v = true := by
              rw [hv']
              exact (contains_iff_mem b b.lo).mpr ⟨0, hbnp, by simp [progVal, hbs]⟩
            have hav : contains a v = true := by simpa [hv'] using ha
            exact ⟨hav, hb⟩
          · rintro ⟨ha, hb⟩
            have hv : v = b.lo := by
              obtain ⟨k, hk, hval⟩ := (contains_iff_mem b v).mp hb
              simpa [progVal, hbs] using hval
            have ha' : contains a b.lo = true := by simpa [hv] using ha
            refine ⟨0, by simp [ha'], ?_⟩
            simpa [progVal, hv]
        · have hasp : 0 < a.step := Nat.pos_of_ne_zero has
          have hbsp : 0 < b.step := Nat.pos_of_ne_zero hbs
          have hasBf : (a.step == 0) = false := beq_eq_false_iff_ne.mpr has
          have hbsBf : (b.step == 0) = false := beq_eq_false_iff_ne.mpr hbs
          let g := a.step.gcd b.step
          let gap := b.lo - a.lo
          let period := (a.step / g) * b.step
          let unit := (bezout (a.step / g) (b.step / g)).1
          let root := a.lo + (a.step : Int) * ((gap / (g : Int) * unit) % ((b.step / g : Nat) : Int))
          let low := max a.lo b.lo
          let first := low + (root - low) % (period : Int)
          let high := min (lastVal a) (lastVal b)
          have hperNat : 0 < period := by
            dsimp [period]
            exact Nat.mul_pos (Nat.div_gcd_pos_of_pos_left b.step hasp) hbsp
          have hp : (0 : Int) < period := by exact_mod_cast hperNat
          have hgapdef : gap = b.lo - a.lo := rfl
          by_cases hgapmod : gap % (g : Int) = 0
          · have hgap : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgapmod
            have hroot := root_dvd a.step b.step hasp hbsp a.lo b.lo gap hgapdef hgap
            have hfirst : first = low + (root - low) % (period : Int) := rfl
            have hfroot := first_root_dvd low root first period hp hfirst
            have hremnon : 0 ≤ (root - low) % (period : Int) := Int.emod_nonneg _ (by omega : (period : Int) ≠ 0)
            have hflow : low ≤ first := by dsimp [first]; omega
            have ha_eq : (a.step : Int) = (a.step.gcd b.step : Int) * ((a.step / a.step.gcd b.step : Nat) : Int) := by
              change (a.step : Int) = (a.step.gcd b.step : Int) * ((a.step / a.step.gcd b.step : Nat) : Int)
              exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_left a.step b.step)).symm
            have hb_eq : (b.step : Int) = (a.step.gcd b.step : Int) * ((b.step / a.step.gcd b.step : Nat) : Int) := by
              change (b.step : Int) = (a.step.gcd b.step : Int) * ((b.step / a.step.gcd b.step : Nat) : Int)
              exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_right a.step b.step)).symm
            have hperA : (period : Int) = (a.step : Int) * ((b.step / g : Nat) : Int) := by
              dsimp [period]
              grind
            have hperB : (period : Int) = (b.step : Int) * ((a.step / g : Nat) : Int) := by
              dsimp [period]
              grind
            have hpa : (a.step : Int) ∣ (period : Int) := by
              exact ⟨((b.step / g : Nat) : Int), hperA⟩
            have hpb : (b.step : Int) ∣ (period : Int) := by
              exact ⟨((a.step / g : Nat) : Int), hperB⟩
            have hgapB : (gap % (g : Int) != 0) = false := by simp [hgapmod]
            unfold intersection
            rw [if_neg (by simpa [hfirstB]), if_neg (by simpa [hasBf]), if_neg (by simpa [hbsBf]), if_neg (by simpa [hgapB])]
            change contains ⟨first, period, if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ v = true ↔ _
            have hspan : contains ⟨first, (period : Int).toNat,
                if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ v = true ↔
                first ≤ v ∧ v ≤ high ∧ (period : Int) ∣ v - first := by
              simpa using (span_contains_iff first (period : Int) high v hp)
            rw [show (period : Int).toNat = period by omega] at hspan
            rw [hspan]
            constructor
            · rintro ⟨hfirstx, hxhigh, hxfirst⟩
              have hxroot : (period : Int) ∣ v - root := by
                have heq : v - root = (v - first) + (first - root) := by omega
                rw [heq]
                exact Int.dvd_add hxfirst hfroot
              have hperA : (a.step : Int) ∣ v - root := Int.dvd_trans hpa hxroot
              have hperB : (b.step : Int) ∣ v - root := Int.dvd_trans hpb hxroot
              have hxalo : (a.step : Int) ∣ v - a.lo := by
                have hsum : (a.step : Int) ∣ (v - root) + (root - a.lo) := Int.dvd_add hperA hroot.1
                have heq3 : (v - root) + (root - a.lo) = v - a.lo := by omega
                rw [heq3] at hsum
                exact hsum
              have hxblo : (b.step : Int) ∣ v - b.lo := by
                have hsum : (b.step : Int) ∣ (v - root) + (root - b.lo) := Int.dvd_add hperB hroot.2
                have heq3 : (v - root) + (root - b.lo) = v - b.lo := by omega
                rw [heq3] at hsum
                exact hsum
              have hxlow : low ≤ v := by omega
              have ha_lo : a.lo ≤ v := by dsimp [low] at hflow; omega
              have ha_hi : v ≤ lastVal a := by dsimp [high] at hxhigh; omega
              have hb_lo : b.lo ≤ v := by dsimp [low] at hflow; omega
              have hb_hi : v ≤ lastVal b := by dsimp [high] at hxhigh; omega
              constructor
              · exact mem_of_bounds_dvd a v hanp hasp ha_lo ha_hi hxalo
              · exact mem_of_bounds_dvd b v hbnp hbsp hb_lo hb_hi hxblo
            · rintro ⟨ha, hb⟩
              have hxalo0 := contains_dvd a v ha
              have hxblo0 := contains_dvd b v hb
              have hdA : (a.step : Int) ∣ v - root := by
                have hdiff : (a.step : Int) ∣ (v - a.lo) - (root - a.lo) := Int.dvd_sub hxalo0 hroot.1
                have heq3 : (v - a.lo) - (root - a.lo) = v - root := by omega
                rw [heq3] at hdiff
                exact hdiff
              have hdB : (b.step : Int) ∣ v - root := by
                have hdiff : (b.step : Int) ∣ (v - b.lo) - (root - b.lo) := Int.dvd_sub hxblo0 hroot.2
                have heq3 : (v - b.lo) - (root - b.lo) = v - root := by omega
                rw [heq3] at hdiff
                exact hdiff
              have hxroot : (period : Int) ∣ v - root := by
                have hl := lcm_dvd_of_dvd_of_dvd a.step b.step hasp hbsp (v - root) hdA hdB
                change (period : Int) ∣ v - root
                simpa [period, g] using hl
              have hxfirst : (period : Int) ∣ v - first := by
                have heq : v - first = (v - root) - (first - root) := by omega
                rw [heq]
                exact Int.dvd_sub hxroot hfroot
              have hab := contains_bounds a v ha
              have hbb := contains_bounds b v hb
              have hvlow : low ≤ v := by dsimp [low]; omega
              have hvhig : v ≤ high := by dsimp [high]; omega
              have hfirstv : first ≤ v := first_le_of_congruence low root first v (period : Int) hp hfirst hvlow hxroot
              exact ⟨hfirstv, hvhig, hxfirst⟩
          · have hgapne : gap % (g : Int) ≠ 0 := by simpa using hgapmod
            have hgapB : (gap % (g : Int) != 0) = true := by simp [hgapmod]
            unfold intersection
            rw [if_neg (by simpa [hfirstB]), if_neg (by simpa [hasBf]), if_neg (by simpa [hbsBf]), if_pos (by simpa [hgapB])]
            simp only [contains]
            constructor
            · intro h
              simp at h
            · rintro ⟨ha, hb⟩
              have hg := common_gap_dvd a b v ha hb
              exact (hgapne (Int.emod_eq_zero_of_dvd hg)).elim


/-! ## Progressions as value lists -/

private def progVals (p : Progression) : List Int :=
  (List.range p.n).map (progVal p)

private theorem progMem_iff_mem_progVals (p : Progression) (v : Int) :
    progMem p v ↔ v ∈ progVals p := by
  simp [progMem, progVals, progVal, eq_comm]

private theorem contains_mem (p : Progression) (v : Int) :
    contains p v = true ↔ v ∈ progVals p :=
  (contains_iff_mem p v).trans (progMem_iff_mem_progVals p v)

private theorem nodup_map_inj {α β : Type} (l : List α) (f : α → β)
    (h : ∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) (hl : l.Nodup) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at hl
      rw [List.map_cons, List.nodup_cons]
      constructor
      · intro hm
        rcases List.mem_map.mp hm with ⟨b, hb, he⟩
        exact hl.1 (by rw [← h b (by simp [hb]) a (by simp) he]; exact hb)
      · exact ih (fun x hx y hy => h x (by simp [hx]) y (by simp [hy])) hl.2

private theorem progVals_nodup_of_pos (p : Progression) (hs : 0 < p.step) :
    (progVals p).Nodup := by
  unfold progVals
  apply nodup_map_inj
  · intro a ha b hb hEq
    unfold progVal at hEq
    have hm' : (a : Int) * (p.step : Int) = (b : Int) * (p.step : Int) := by omega
    have hm : (p.step : Int) * (a : Int) = (p.step : Int) * (b : Int) := by
      simpa [Int.mul_comm] using hm'
    have hab : (a : Int) = (b : Int) :=
      (Int.mul_eq_mul_left_iff (by exact_mod_cast (Nat.ne_of_gt hs) : (p.step : Int) ≠ 0)).mp hm
    exact_mod_cast hab
  · exact List.nodup_range

private theorem progVals_of_step_zero (p : Progression) (hs : p.step = 0) :
    progVals p = (List.range p.n).map (fun _ => p.lo) := by
  simp [progVals, progVal, hs]

private theorem intersection_step_pos (p q : Progression) (hp : 0 < p.step) :
    0 < (intersection p q).step := by
  by_cases hn0 : p.n = 0
  · have hnB : (p.n == 0) = true := by simp [hn0]
    simp [intersection, hnB]
  · by_cases hm0 : q.n = 0
    · have hnB : (p.n == 0) = false := beq_eq_false_iff_ne.mpr hn0
      have hmB : (q.n == 0) = true := by simp [hm0]
      simp [intersection, hnB, hmB]
    · have hp0 : p.step ≠ 0 := by omega
      have hnB : (p.n == 0) = false := beq_eq_false_iff_ne.mpr hn0
      have hmB : (q.n == 0) = false := beq_eq_false_iff_ne.mpr hm0
      have hpB : (p.step == 0) = false := beq_eq_false_iff_ne.mpr hp0
      by_cases hq0 : q.step = 0
      · have hqB : (q.step == 0) = true := by simp [hq0]
        simp [intersection, hnB, hmB, hpB, hqB]
      · have hqB : (q.step == 0) = false := beq_eq_false_iff_ne.mpr hq0
        by_cases hgap : ((q.lo - p.lo) % (p.step.gcd q.step : Int) != 0) = true
        · simp [intersection, hnB, hmB, hpB, hqB, hgap]
        · simp [intersection, hnB, hmB, hpB, hqB, hgap]
          exact Nat.mul_pos (Nat.div_gcd_pos_of_pos_left q.step hp) (Nat.pos_of_ne_zero hq0)

private theorem count_common (p q : Progression) (hp : 0 < p.step) :
    ((progVals p).filter (contains q)).length = (intersection p q).n := by
  have hleft : ((progVals p).filter (contains q)).Nodup :=
    (progVals_nodup_of_pos p hp).filter _
  have hright : (progVals (intersection p q)).Nodup :=
    progVals_nodup_of_pos _ (intersection_step_pos p q hp)
  have hperm : ((progVals p).filter (contains q)).Perm
      (progVals (intersection p q)) := by
    apply (List.perm_ext_iff_of_nodup hleft hright).mpr
    intro v
    simp only [List.mem_filter]
    constructor
    · intro h
      exact (contains_mem (intersection p q) v).mp
        ((intersection_contains p q v).mpr ⟨(contains_mem p v).mpr h.1, by simpa using h.2⟩)
    · intro h
      have hi := (contains_mem (intersection p q) v).mpr h
      have hb := (intersection_contains p q v).mp hi
      exact ⟨(contains_mem p v).mp hb.1, by simpa using hb.2⟩
  rw [perm_length hperm]
  simp [progVals]

private theorem beq_comm_bool {α : Type} [BEq α] [LawfulBEq α] (a b : α) :
    (a == b) = (b == a) := by
  by_cases h : a = b
  · subst b
    simp
  · rw [beq_eq_false_iff_ne.mpr h, beq_eq_false_iff_ne.mpr (Ne.symm h)]

private theorem countP_beq_progVals (q : Progression) (v : Int) :
    (progVals q).countP (fun w => v == w) =
      (if contains q v then 1 else 0) * (if q.step = 0 then q.n else 1) := by
  by_cases hq0 : q.step = 0
  · rw [progVals_of_step_zero q hq0, countP_beq_const_list]
    have hc : contains q v = true ↔ 0 < q.n ∧ v = q.lo := by
      have hqeq : q = ⟨q.lo, 0, q.n⟩ := by
        cases q with
        | mk lo st n =>
            simp at hq0
            subst st
            rfl
      rw [hqeq]
      exact contains_const_iff q.lo q.n v
    by_cases hv : v == q.lo
    · have hvv : v = q.lo := beq_iff_eq.mp hv
      by_cases hnpos : 0 < q.n
      · have hcv : contains q v = true := hc.mpr ⟨hnpos, hvv⟩
        simp [hv, hcv, hq0]
      · have hcv : contains q v ≠ true := by intro hcv; exact hnpos (hc.mp hcv).1
        have hnzero : q.n = 0 := by omega
        simp [hv, hcv, hq0, hnzero]
    · have hvv : v ≠ q.lo := by intro he; exact hv (by rw [he]; simp)
      have hcv : contains q v ≠ true := by intro hcv; exact hvv (hc.mp hcv).2
      simp [hv, hcv, hq0]
  · have hnod : (progVals q).Nodup := progVals_nodup_of_pos q (Nat.pos_of_ne_zero hq0)
    rw [countP_beq_eq_one_of_mem_nodup (progVals q) hnod v]
    by_cases hvq : v ∈ progVals q
    · have hc : contains q v = true := (contains_mem q v).mpr hvq
      simp [hvq, hc, hq0]
    · have hc : contains q v ≠ true := by intro h; exact hvq ((contains_mem q v).mp h)
      simp [hvq, hc, hq0]

private theorem sum_range_const_nat (m v : Nat) :
    ((List.range m).map (fun _ => v)).sum = m * v := by
  induction m with
  | zero => simp
  | succ m ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      simp [Nat.succ_mul]

private theorem intersection_const_left (p q : Progression) (hp : p.step = 0)
    (hpn : 0 < p.n) :
    (intersection p q).n = if contains q p.lo then 1 else 0 := by
  have hnB : (p.n == 0) = false := beq_eq_false_iff_ne.mpr (by omega : p.n ≠ 0)
  have hpB : (p.step == 0) = true := by simp [hp]
  by_cases hqn0 : q.n = 0
  · have hfalse : contains q p.lo ≠ true := by
      intro h
      obtain ⟨k, hk, _⟩ := (contains_iff_mem q p.lo).mp h
      omega
    have hmB : (q.n == 0) = true := by simp [hqn0]
    simp [intersection, hnB, hmB, hpB, hfalse]
  · have hmB : (q.n == 0) = false := beq_eq_false_iff_ne.mpr hqn0
    simp [intersection, hnB, hmB, hpB]

private theorem sum_indicator_common (p q : Progression) :
    ((progVals p).map (fun v => if contains q v then 1 else 0)).sum =
      (if p.step = 0 then p.n else 1) * (intersection p q).n := by
  by_cases hp0 : p.step = 0
  · by_cases hpn0 : p.n = 0
    · simp [progVals, hpn0, hp0]
    · have hpn : 0 < p.n := by omega
      rw [progVals_of_step_zero p hp0, List.map_map]
      change ((List.range p.n).map (fun _ => if contains q p.lo then 1 else 0)).sum =
        (if p.step = 0 then p.n else 1) * (intersection p q).n
      rw [sum_range_const_nat]
      rw [intersection_const_left p q hp0 hpn]
      have hB : (p.step == 0) = true := by simp [hp0]
      simp [hp0]
  · rw [sum_ite_filter_nat]
    rw [count_common p q (Nat.pos_of_ne_zero hp0)]
    have hB : (p.step == 0) = false := beq_eq_false_iff_ne.mpr hp0
    simp [hp0]

private theorem prog_join_count (p q : Progression) :
    ((progVals p).map (fun v => (progVals q).countP (fun w => v == w))).sum =
      (intersection p q).n * (if p.step = 0 then p.n else 1) * (if q.step = 0 then q.n else 1) := by
  rw [show (progVals p).map (fun v => (progVals q).countP (fun w => v == w)) =
      (progVals p).map (fun v => (if contains q v then 1 else 0) *
        (if q.step = 0 then q.n else 1)) by
        apply List.map_congr_left
        intro v hv
        rw [countP_beq_progVals q v]]
  rw [show ((progVals p).map (fun v => (if contains q v then 1 else 0) *
        (if q.step = 0 then q.n else 1))).sum =
      ((progVals p).map (fun v => if contains q v then 1 else 0)).sum *
        (if q.step = 0 then q.n else 1) by
        induction progVals p with
        | nil => simp
        | cons x xs ih => simp [ih, Nat.mul_add, Nat.add_mul] <;> grind]
  rw [sum_indicator_common p q]
  by_cases hq0 : q.step = 0 <;> simp [hq0] <;> grind


/-! ## Lane points and residue partitions -/

private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun j => (a.start + j * a.stride, a.base + (j : Int) * a.step))

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

private theorem residue_count_eq_zero_of_ge (N r p : Nat) (hr : N ≤ r) (hp : 0 < p) :
    (N - r + p - 1) / p = 0 := by
  have hsub : N - r = 0 := by omega
  rw [hsub, Nat.zero_add]
  exact Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))

private theorem residue_lt_of_count (N p r q : Nat) (hp : 0 < p) (hr : r < p)
    (hq : q < (N - r + p - 1) / p) : r + q * p < N := by
  by_cases hle : r ≤ N
  · have hmul : q * p < N - r + p - 1 - (p - 1) := (Nat.lt_div_iff_mul_lt hp).mp hq
    have hdiff : N - r + p - 1 - (p - 1) = N - r := by omega
    rw [hdiff] at hmul
    omega
  · have hz : (N - r + p - 1) / p = 0 := residue_count_eq_zero_of_ge N r p (by omega) hp
    omega

private theorem mem_of_mem_filter {α : Type} {l : List α} {p : α → Bool} {a : α}
    (h : a ∈ l.filter p) : a ∈ l := (List.mem_filter.mp h).1

private theorem mem_of_mem_flatMap {α β : Type} {l : List α} {f : α → List β} {b : β}
    (h : b ∈ l.flatMap f) : ∃ a ∈ l, b ∈ f a := List.mem_flatMap.mp h

private theorem nodup_filter {α : Type} (l : List α) (p : α → Bool) (hl : l.Nodup) :
    (l.filter p).Nodup := hl.filter _

private theorem pairwise_range_rel (p : Nat) (R : Nat → Nat → Prop)
    (h : ∀ a < p, ∀ b < p, a ≠ b → R a b) : (List.range p).Pairwise R := by
  induction p with
  | zero => simp
  | succ p ih =>
      rw [show p + 1 = Nat.succ p by omega, List.range_succ, List.pairwise_append]
      refine ⟨ih (fun a ha b hb hab => h a (by omega) b (by omega) hab), by simp, ?_⟩
      intro a ha
      have ha' : a < p := List.mem_range.mp ha
      intro b hb
      have hb' : b = p := by simpa using hb
      subst b
      exact h a (by omega) p (by omega) (by omega)

private theorem residue_flatMap_nodup_gen (n p k : Nat) (hp : 0 < p) (hk : k ≤ p)
    (f : Nat → Int) :
    (((List.range (min k n)).flatMap fun r =>
      (List.range ((n - r + p - 1) / p)).map fun q =>
        (r + q*p, f (r + q*p)))).Nodup := by
  let F : Nat → List (Nat × Int) := fun r =>
    (List.range ((n - r + p - 1) / p)).map fun q => (r + q*p, f (r+q*p))
  change List.Pairwise (fun a b : Nat × Int => a ≠ b) (List.flatMap F (List.range (min k n)))
  rw [List.pairwise_flatMap]
  constructor
  · intro r _
    unfold F
    apply nodup_map_inj
    · intro q _ q' _ he
      have hh : r + q*p = r + q'*p := congrArg Prod.fst he
      exact Nat.eq_of_mul_eq_mul_left hp (by simpa [Nat.mul_comm] using Nat.add_left_cancel hh)
    · exact List.nodup_range
  · apply pairwise_range_rel (min k n)
    intro a ha b hb hab x hx y hy he
    rcases List.mem_map.mp hx with ⟨qx, hqx, rfl⟩
    rcases List.mem_map.mp hy with ⟨qy, hqy, rfl⟩
    have ha' : a < p := by omega
    have hb' : b < p := by omega
    have hpos : a + qx*p = b + qy*p := congrArg Prod.fst he
    have hamod : (a + qx*p) % p = a := by
      rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt ha']
    have hbmod : (b + qy*p) % p = b := by
      rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hb']
    have hab' : a = b := by rw [← hamod, ← hbmod, hpos]
    exact hab hab'

private theorem residue_rows_perm_gen (n p k : Nat) (hp : 0 < p) (hk : k ≤ p)
    (f : Nat → Int) :
    (((List.range n).filter (fun i => i % p < k)).map (fun i => (i, f i))).Perm
      (((List.range (min k n)).flatMap fun r =>
        (List.range ((n - r + p - 1) / p)).map fun q =>
          (r + q*p, f (r + q*p)))) := by
  refine (List.perm_ext_iff_of_nodup ?_ ?_).mpr ?_
  · exact nodup_map_inj _ _ (by intro a _ b _ he; exact congrArg Prod.fst he)
      (nodup_filter _ _ List.nodup_range)
  · exact residue_flatMap_nodup_gen n p k hp hk f
  · intro z
    constructor
    · intro hz
      rcases List.mem_map.mp hz with ⟨i, hi, rfl⟩
      have hin : i < n := List.mem_range.mp (mem_of_mem_filter hi)
      have hik : i % p < k := by
        have hb := (List.mem_filter.mp hi).2
        exact Nat.lt_of_lt_of_le (by simpa using hb) (by omega)
      have hrK : i % p < min k n := Nat.lt_min.mpr ⟨hik, by
        have := Nat.mod_le i p
        omega⟩
      have hq : i / p < (n - i % p + p - 1) / p := by
        have hdecomp : i % p + (i / p) * p = i := by
          have hd := Nat.div_add_mod i p
          rw [Nat.mul_comm p (i / p)] at hd
          omega
        exact (pos_lt_iff (i % p) p (i / p) n hp).mp (by rw [hdecomp]; exact hin)
      apply List.mem_flatMap.mpr
      refine ⟨i % p, List.mem_range.mpr hrK, ?_⟩
      apply List.mem_map.mpr
      refine ⟨i / p, List.mem_range.mpr hq, ?_⟩
      have hdecomp : i % p + (i / p) * p = i := by
        have hd := Nat.div_add_mod i p
        rw [Nat.mul_comm p (i / p)] at hd
        omega
      simp [hdecomp]
    · intro hz
      rcases mem_of_mem_flatMap hz with ⟨r, hr, hz⟩
      rcases List.mem_map.mp hz with ⟨q, hq, rfl⟩
      have hrp : r < p := by have := List.mem_range.mp hr; omega
      have hrk : r < k := by have := List.mem_range.mp hr; omega
      have hrn : r + q*p < n := residue_lt_of_count n p r q hp hrp (List.mem_range.mp hq)
      apply List.mem_map.mpr
      refine ⟨r + q*p, List.mem_filter.mpr ⟨List.mem_range.mpr hrn, ?_⟩, rfl⟩
      have hmod : (r + q*p) % p = r := by
        rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hrp]
      rw [hmod]
      simp [hrk]


/-! ## Lane value lists -/

private def laneVals (a : Lane) : List Int :=
  (List.range a.n).map (fun j : Nat => a.base + (Int.ofNat j) * a.step)

private theorem mem_laneVals_iff (a : Lane) (v : Int) :
    v ∈ laneVals a ↔ ∃ j, j < a.n ∧ v = a.base + (Int.ofNat j) * a.step := by
  simp [laneVals, eq_comm]

private theorem values_step_zero (a : Lane) : (values a).step = 0 ↔ a.step = 0 := by
  dsimp [values]
  exact Int.natAbs_eq_zero

private theorem progMem_values_lane (a : Lane) (v : Int) :
    progMem (values a) v ↔ v ∈ laneVals a := by
  rw [mem_laneVals_iff]
  unfold values
  by_cases hn0 : a.n = 0
  · constructor
    · rintro ⟨k, hk, _⟩
      change k < a.n at hk
      omega
    · rintro ⟨j, hj, _⟩
      omega
  · have hnpos : 0 < a.n := by omega
    by_cases hneg : a.step < 0
    · simp only [if_pos hneg]
      have hsabs : ((a.step.natAbs : Nat) : Int) = -a.step :=
        Int.ofNat_natAbs_of_nonpos (by omega)
      constructor
      · rintro ⟨k, hk, hval⟩
        unfold progVal at hval
        change k < a.n at hk
        change v = (a.base + (Int.ofNat (a.n - 1)) * a.step) + (Int.ofNat k) * (a.step.natAbs) at hval
        let j := a.n - 1 - k
        have hjle : k ≤ a.n - 1 := by omega
        have hjlt : j < a.n := by dsimp [j]; omega
        refine ⟨j, hjlt, ?_⟩
        dsimp [j]
        simp only [Int.ofNat_eq_natCast] at hval ⊢
        rw [Int.ofNat_sub hjle]
        rw [hsabs] at hval
        grind
      · rintro ⟨j, hj, hval⟩
        let k := a.n - 1 - j
        have hjle : j ≤ a.n - 1 := by omega
        have hklt : k < a.n := by dsimp [k]; omega
        refine ⟨k, hklt, ?_⟩
        unfold progVal
        change v = (a.base + (Int.ofNat (a.n - 1)) * a.step) + (Int.ofNat k) * (a.step.natAbs : Int) at *
        simp only [Int.ofNat_eq_natCast] at hval ⊢
        rw [hsabs]
        rw [Int.ofNat_sub hjle]
        grind
    · simp only [if_neg hneg]
      have hsabs : ((a.step.natAbs : Nat) : Int) = a.step :=
        Int.natAbs_of_nonneg (by omega)
      constructor
      · rintro ⟨k, hk, hval⟩
        unfold progVal at hval
        change k < a.n at hk
        refine ⟨k, hk, ?_⟩
        repeat rw [Int.ofNat_eq_natCast] at hval ⊢
        rw [hsabs] at hval
        simpa using hval
      · rintro ⟨j, hj, hval⟩
        refine ⟨j, hj, ?_⟩
        unfold progVal
        repeat rw [Int.ofNat_eq_natCast] at hval ⊢
        rw [← hsabs] at hval
        simpa using hval

private theorem laneVals_nodup_of_step (a : Lane) (hs : a.step ≠ 0) :
    (laneVals a).Nodup := by
  unfold laneVals
  apply nodup_map_inj
  · intro i _ j _ hEq
    have hm' : (Int.ofNat i) * a.step = (Int.ofNat j) * a.step := by omega
    have hm : a.step * (Int.ofNat i) = a.step * (Int.ofNat j) := by
      rw [Int.mul_comm a.step (Int.ofNat i), Int.mul_comm a.step (Int.ofNat j)]
      exact hm'
    have hij : (Int.ofNat i) = (Int.ofNat j) :=
      (Int.mul_eq_mul_left_iff (by exact_mod_cast hs : (a.step : Int) ≠ 0)).mp hm
    exact Int.ofNat.inj hij
  · exact List.nodup_range

private theorem progVals_values_perm (a : Lane) :
    (progVals (values a)).Perm (laneVals a) := by
  by_cases hs : a.step = 0
  · have h1 : progVals (values a) = (List.range a.n).map (fun _ => a.base) := by
      simp [values, progVals, progVal, hs]
    rw [h1]
    have h2 : laneVals a = (List.range a.n).map (fun _ => a.base) := by
      simp [laneVals, hs]
    rw [h2]
  · have hstep : 0 < (values a).step := by
      dsimp [values]
      by_cases hneg : a.step < 0
      · simp [hneg]
        exact hs
      · simp [hneg]
        exact hs
    apply (List.perm_ext_iff_of_nodup
      (progVals_nodup_of_pos _ hstep)
      (laneVals_nodup_of_step a hs)).mpr
    intro v
    rw [← progMem_iff_mem_progVals, progMem_values_lane]

private theorem joinCount_lane (a b : Lane) :
    ((laneVals a).map (fun v => (laneVals b).countP (fun w => v == w))).sum = joinCount a b := by
  unfold joinCount
  have hpa := progVals_values_perm a
  have hpb := progVals_values_perm b
  have hcount : ∀ v : Int,
      (laneVals b).countP (fun w => v == w) =
        (progVals (values b)).countP (fun w => v == w) := by
    intro v
    exact (countP_perm (progVals (values b)) (fun w => v == w) hpb).symm
  calc
    ((laneVals a).map (fun v => (laneVals b).countP (fun w => v == w))).sum =
        ((laneVals a).map (fun v => (progVals (values b)).countP (fun w => v == w))).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro v hv
          exact hcount v
    _ = ((progVals (values a)).map (fun v => (progVals (values b)).countP (fun w => v == w))).sum := by
          exact (list_sum_perm_nat (hpa.map (fun v => (progVals (values b)).countP (fun w => v == w)))).symm
    _ = (intersection (values a) (values b)).n *
          (if (values a).step = 0 then (values a).n else 1) *
          (if (values b).step = 0 then (values b).n else 1) := prog_join_count _ _
    _ = (intersection (values a) (values b)).n *
          (if a.step = 0 then a.n else 1) *
          (if b.step = 0 then b.n else 1) := by
          have hva := values_step_zero a
          have hvb := values_step_zero b
          have hna : (values a).n = a.n := rfl
          have hnb : (values b).n = b.n := rfl
          rw [hna, hnb]
          simp [hva, hvb]
    _ = (intersection (values a) (values b)).n *
          (if (a.step == 0) = true then a.n else 1) *
          (if (b.step == 0) = true then b.n else 1) := by
          by_cases ha : a.step = 0 <;> by_cases hb : b.step = 0 <;> simp [ha, hb]


/-! ## Deleted arithmetic progressions -/

private def lanePosVals (a : Lane) : List Int :=
  (List.range a.n).map (fun j : Nat => ((a.start + j * a.stride : Nat) : Int))

private def posF (a : Lane) (v : Int) : Int :=
  a.base + (((v - (a.start : Int)) / (a.stride : Int)).toNat : Int) * a.step

private theorem posF_at (a : Lane) (ha : 0 < a.stride) (j : Nat) :
    posF a ((a.start + j * a.stride : Nat) : Int) = a.base + (j : Int) * a.step := by
  unfold posF
  have hsub : ((a.start + j * a.stride : Nat) : Int) - (a.start : Int) =
      (j : Int) * (a.stride : Int) := by
    push_cast
    omega
  rw [hsub]
  rw [Int.mul_ediv_cancel (j : Int) (by exact_mod_cast (Nat.ne_of_gt ha))]
  rfl

private theorem inter_positions_perm (a : Lane) (ha : 0 < a.stride) (m : Progression) :
    (progVals (intersection (positions a) m)).Perm
      ((lanePosVals a).filter (fun v => contains m v)) := by
  have hleft : (progVals (intersection (positions a) m)).Nodup :=
    progVals_nodup_of_pos _ (intersection_step_pos (positions a) m (by exact ha))
  have hright : ((lanePosVals a).filter (fun v => contains m v)).Nodup := by
    unfold lanePosVals
    exact nodup_filter _ _ (nodup_map_inj _ _ (by
      intro i _ j _ hEq
      have hEq' : a.start + i * a.stride = a.start + j * a.stride := by exact_mod_cast hEq
      have hij : i * a.stride = j * a.stride := Nat.add_left_cancel hEq'
      exact Nat.eq_of_mul_eq_mul_left ha (by simpa [Nat.mul_comm] using hij)) List.nodup_range)
  apply (List.perm_ext_iff_of_nodup hleft hright).mpr
  intro v
  unfold lanePosVals
  simp only [List.mem_filter]
  constructor
  · intro hz
    have hq := (contains_mem (intersection (positions a) m) v).mpr hz
    have hb := (intersection_contains (positions a) m v).mp hq
    rcases List.mem_map.mp ((contains_mem (positions a) v).mp hb.1) with ⟨j, hj, hjv⟩
    have hj' : j ∈ List.range a.n := by simpa [positions] using hj
    have hjv' : ((a.start + j * a.stride : Nat) : Int) = v := by
      simpa [progVal, positions] using hjv
    exact ⟨List.mem_map.mpr ⟨j, hj', hjv'⟩, hb.2⟩
  · rintro ⟨hz, hm⟩
    rcases List.mem_map.mp hz with ⟨j, hj, hjv⟩
    have hj' : j ∈ List.range (positions a).n := by simpa [positions] using hj
    have hjv' : progVal (positions a) j = v := by
      simpa [progVal, positions] using hjv
    apply (contains_mem (intersection (positions a) m) v).mp
    apply (intersection_contains (positions a) m v).mpr
    refine ⟨(contains_mem (positions a) v).mpr ?_, hm⟩
    exact List.mem_map.mpr ⟨j, hj', hjv'⟩

private theorem posF_inter_of (a : Lane) (ha : 0 < a.stride) (q : Progression)
    (hqa : ∀ v, contains q v = true → contains (positions a) v = true)
    (v : Int) (hv : v ∈ progVals q) :
    ∃ k, k < q.n ∧
      v = q.lo + (k : Int) * (q.step : Int) ∧
      posF a v = firstAt a q + (k : Int) * slopeAt a q := by
  have hqmem : v ∈ progVals q := hv
  rcases List.mem_map.mp hqmem with ⟨k, hk, hkv⟩
  have hklt : k < q.n := List.mem_range.mp hk
  have hkval : v = q.lo + (k : Int) * (q.step : Int) := by
    simpa [progVal] using hkv.symm
  have hqlo : q.lo ∈ progVals q := by
    exact List.mem_map.mpr ⟨0, List.mem_range.mpr (by omega), by simp [progVal]⟩
  have hqloPos : contains (positions a) q.lo = true :=
    hqa q.lo ((contains_mem q q.lo).mpr hqlo)
  rcases List.mem_map.mp ((contains_mem (positions a) q.lo).mp hqloPos) with ⟨j0, hj0, hj0v⟩
  have hj0val : q.lo = (a.start : Int) + (j0 : Int) * (a.stride : Int) := by
    have htmp : q.lo = (positions a).lo + (j0 : Int) * (positions a).step := by
      simpa [progVal] using hj0v.symm
    change q.lo = (a.start : Int) + (j0 : Int) * (a.stride : Int) at htmp
    exact htmp
  have hsub0 : q.lo - (a.start : Int) = (j0 : Int) * (a.stride : Int) := by omega
  have hdiv0 : (((q.lo - (a.start : Int)) / (a.stride : Int)).toNat) = j0 := by
    rw [hsub0, Int.mul_ediv_cancel (j0 : Int) (by exact_mod_cast (Nat.ne_of_gt ha))]
    simp
  by_cases hk0 : k = 0
  · subst k
    refine ⟨0, hklt, by simpa using hkval, ?_⟩
    have hnon : 0 ≤ (q.lo - (a.start : Int)) / (a.stride : Int) := by
      apply Int.ediv_nonneg
      · rw [hsub0]
        exact Int.mul_nonneg (Int.natCast_nonneg _) (Int.natCast_nonneg _)
      · exact_mod_cast (Nat.zero_le a.stride)
    have hdiv0' : (q.lo - (a.start : Int)) / (a.stride : Int) = (j0 : Int) := by
      rw [← Int.toNat_of_nonneg hnon, hdiv0]
    have hkval0 : v = q.lo := by simpa using hkval
    unfold posF firstAt slopeAt
    rw [hkval0]
    rw [Int.toNat_of_nonneg hnon, hdiv0']
    simp
  · have hkpos : 0 < k := by omega
    have hq1mem : q.lo + (q.step : Int) ∈ progVals q := by
      exact List.mem_map.mpr ⟨1, List.mem_range.mpr (by omega), by simp [progVal]⟩
    have hq1Pos : contains (positions a) (q.lo + (q.step : Int)) = true :=
      hqa (q.lo + (q.step : Int)) ((contains_mem q (q.lo + (q.step : Int))).mpr hq1mem)
    rcases List.mem_map.mp ((contains_mem (positions a) (q.lo + (q.step : Int))).mp hq1Pos) with
      ⟨j1, hj1, hj1v⟩
    have hj1val : q.lo + (q.step : Int) = (a.start : Int) + (j1 : Int) * (a.stride : Int) := by
      have htmp : q.lo + (q.step : Int) = (positions a).lo + (j1 : Int) * (positions a).step := by
        simpa [progVal] using hj1v.symm
      change q.lo + (q.step : Int) = (a.start : Int) + (j1 : Int) * (a.stride : Int) at htmp
      exact htmp
    have hqstep_nonneg : 0 ≤ (q.step : Int) := by exact_mod_cast (Nat.zero_le q.step)
    have hj0le1 : j0 ≤ j1 := by
      by_cases h : j0 ≤ j1
      · exact h
      · have hjlt : j1 < j0 := by omega
        have hmul : j1 * a.stride < j0 * a.stride := Nat.mul_lt_mul_of_pos_right hjlt ha
        omega
    have hstepEq : (q.step : Int) = ((j1 - j0 : Nat) : Int) * (a.stride : Int) := by
      have hsubcast : ((j1 - j0 : Nat) : Int) = (j1 : Int) - (j0 : Int) := by
        exact Int.ofNat_sub hj0le1
      rw [hsubcast, Int.sub_mul]
      grind
    have hstepNatEq : q.step = (j1 - j0) * a.stride := by exact_mod_cast hstepEq
    have hstepNat : q.step / a.stride = j1 - j0 := by
      rw [hstepNatEq]
      exact Nat.mul_div_cancel _ ha
    have hsubv : v - (a.start : Int) = ((j0 + k * (j1 - j0) : Nat) : Int) * (a.stride : Int) := by
      rw [hkval, hstepEq]
      grind
    have hdivv : (((v - (a.start : Int)) / (a.stride : Int)).toNat) = j0 + k * (j1 - j0) := by
      rw [hsubv, Int.mul_ediv_cancel ((j0 + k * (j1 - j0) : Nat) : Int)
        (by exact_mod_cast (Nat.ne_of_gt ha))]
      rw [Int.toNat_natCast]
    refine ⟨k, hklt, hkval, ?_⟩
    change posF a v = firstAt a q + (k : Int) * slopeAt a q
    have hnon0 : 0 ≤ (q.lo - (a.start : Int)) / (a.stride : Int) := by
      apply Int.ediv_nonneg
      · rw [hsub0]
        exact Int.mul_nonneg (Int.natCast_nonneg _) (Int.natCast_nonneg _)
      · exact_mod_cast (Nat.zero_le a.stride)
    have hnonv : 0 ≤ (v - (a.start : Int)) / (a.stride : Int) := by
      apply Int.ediv_nonneg
      · rw [hsubv]
        exact Int.mul_nonneg (Int.natCast_nonneg _) (Int.natCast_nonneg _)
      · exact_mod_cast (Nat.zero_le a.stride)
    have hdiv0' : (q.lo - (a.start : Int)) / (a.stride : Int) = (j0 : Int) := by
      rw [← Int.toNat_of_nonneg hnon0, hdiv0]
    have hdivv' : (v - (a.start : Int)) / (a.stride : Int) =
        ((j0 + k * (j1 - j0) : Nat) : Int) := by
      rw [← Int.toNat_of_nonneg hnonv, hdivv]
    unfold posF firstAt slopeAt
    change a.base + (((v - (a.start : Int)) / (a.stride : Int)).toNat : Int) * a.step =
      a.base + (q.lo - (a.start : Int)) / (a.stride : Int) * a.step +
        (k : Int) * (((q.step / a.stride : Nat) : Int) * a.step)
    rw [Int.toNat_of_nonneg hnonv, hdivv', hdiv0', hstepNat]
    grind


private theorem posF_inter (a : Lane) (ha : 0 < a.stride) (m : Progression)
    (v : Int) (hv : v ∈ progVals (intersection (positions a) m)) :
    ∃ k, k < (intersection (positions a) m).n ∧
      v = (intersection (positions a) m).lo + (k : Int) * ((intersection (positions a) m).step : Int) ∧
      posF a v = firstAt a (intersection (positions a) m) +
        (k : Int) * slopeAt a (intersection (positions a) m) := by
  exact posF_inter_of a ha (intersection (positions a) m)
    (fun v h => ((intersection_contains (positions a) m v).mp h).1) v hv

private theorem progVal_inj_of_pos (p : Progression) (hs : 0 < p.step)
    (i j : Nat) (hi : i < p.n) (hj : j < p.n)
    (hEq : progVal p i = progVal p j) : i = j := by
  unfold progVal at hEq
  have hm' : (i : Int) * (p.step : Int) = (j : Int) * (p.step : Int) := by omega
  have hm : (p.step : Int) * (i : Int) = (p.step : Int) * (j : Int) := by
    rw [Int.mul_comm (p.step : Int) (i : Int), Int.mul_comm (p.step : Int) (j : Int)]
    exact hm'
  have hij : (i : Int) = (j : Int) :=
    (Int.mul_eq_mul_left_iff (by exact_mod_cast (Nat.ne_of_gt hs) : (p.step : Int) ≠ 0)).mp hm
  exact_mod_cast hij

private theorem sum_posF_inter (a : Lane) (ha : 0 < a.stride) (m : Progression) :
    ((progVals (intersection (positions a) m)).map (posF a)).sum = deletedSum a m := by
  unfold deletedSum
  let q := intersection (positions a) m
  have hqstep : 0 < q.step := intersection_step_pos (positions a) m (by exact ha)
  change ((progVals q).map (posF a)).sum = (q.n : Int) * firstAt a q + tri q.n * slopeAt a q
  have hval : ∀ k, k < q.n →
      posF a (progVal q k) = firstAt a q + (k : Int) * slopeAt a q := by
    intro k hk
    obtain ⟨k', hk', hvk', heq⟩ := posF_inter a ha m (progVal q k)
      (List.mem_map.mpr ⟨k, List.mem_range.mpr hk, rfl⟩)
    have hvk'' : progVal q k = q.lo + (k' : Int) * (q.step : Int) := by
      change progVal q k = (intersection (positions a) m).lo + (k' : Int) * ((intersection (positions a) m).step : Int) at hvk'
      exact hvk'
    have heq' : posF a (progVal q k) = firstAt a q + (k' : Int) * slopeAt a q := by
      change posF a (progVal q k) = firstAt a q + (k' : Int) * slopeAt a q at heq
      exact heq
    have hEqv : progVal q k' = progVal q k := by
      change q.lo + (k' : Int) * (q.step : Int) = progVal q k
      rw [hvk'']
    have hk'eq : k' = k := progVal_inj_of_pos q hqstep k' k hk' hk hEqv
    rw [hk'eq] at heq'
    exact heq'
  let L : Lane := ⟨0, 1, q.n, firstAt a q, slopeAt a q⟩
  calc
    ((progVals q).map (posF a)).sum =
        ((List.range q.n).map (fun k : Nat => firstAt a q + (k : Int) * slopeAt a q)).sum := by
      rw [show progVals q = (List.range q.n).map (fun k : Nat => progVal q k) by rfl, List.map_map]
      change ((List.range q.n).map (fun k : Nat => posF a (progVal q k))).sum =
        ((List.range q.n).map (fun k : Nat => firstAt a q + (k : Int) * slopeAt a q)).sum
      apply congrArg List.sum
      apply List.map_congr_left
      intro k hk
      exact hval k (List.mem_range.mp hk)
    _ = partialSum L q.n := (partialSum_eq_sum L q.n).symm
    _ = (q.n : Int) * firstAt a q + tri q.n * slopeAt a q := rfl

private theorem filter_map_eq {α β : Type} (l : List α) (f : α → β) (p : β → Bool) :
    (l.map f).filter p = (l.filter (fun x => p (f x))).map f := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p (f x) <;> simp [h, ih]

private theorem filter_lanePosVals (a : Lane) (m : Progression) :
    (lanePosVals a).filter (fun v => contains m v) =
      ((List.range a.n).filter (fun j : Nat => contains m ((a.start + j * a.stride : Nat) : Int))).map
        (fun j : Nat => ((a.start + j * a.stride : Nat) : Int)) := by
  unfold lanePosVals
  rw [filter_map_eq]

private theorem deletedSum_lane (a : Lane) (ha : 0 < a.stride) (m : Progression) :
    (((List.range a.n).filter (fun j : Nat => !contains m ((a.start + j * a.stride : Nat) : Int))).map
      (fun j : Nat => a.base + (j : Int) * a.step)).sum =
      partialSum a a.n - deletedSum a m := by
  let p : Nat → Bool := fun j : Nat => contains m ((a.start + j * a.stride : Nat) : Int)
  let val : Nat → Int := fun j => a.base + (j : Int) * a.step
  have hcomp := sum_filter_complement (List.range a.n) p val
  have hdel : (((List.range a.n).filter p).map val).sum = deletedSum a m := by
    rw [show ((List.range a.n).filter p).map val =
        (((lanePosVals a).filter (fun v => contains m v)).map (posF a)) by
      rw [filter_lanePosVals, List.map_map]
      apply List.map_congr_left
      intro j hj
      exact (posF_at a ha j).symm]
    exact (list_sum_perm ((inter_positions_perm a ha m).map (posF a))).symm.trans (sum_posF_inter a ha m)
  have htot : ((List.range a.n).map val).sum = partialSum a a.n := (partialSum_eq_sum a a.n).symm
  have hcomp' : (((List.range a.n).filter p).map val).sum +
      (((List.range a.n).filter (fun j : Nat => !p j)).map val).sum =
      ((List.range a.n).map val).sum := by simpa using hcomp
  have hgoal : (((List.range a.n).filter (fun j : Nat => !p j)).map val).sum =
      partialSum a a.n - deletedSum a m := by omega
  simpa [p, val] using hgoal


/-! ## Aligned products -/

private def bvalAt (b : Lane) (v : Int) : Int :=
  if contains (positions b) v then
    b.base + b.step * (((v - (b.start : Int)) / (b.stride : Int)).toNat : Int)
  else 0

private theorem bvalAt_at (b : Lane) (hb : 0 < b.stride) (i : Nat) (hi : i < b.n) :
    bvalAt b ((b.start + i * b.stride : Nat) : Int) = b.base + b.step * (i : Int) := by
  unfold bvalAt
  have hc : contains (positions b) ((b.start + i * b.stride : Nat) : Int) = true := by
    apply (contains_mem (positions b) _).mpr
    change ((b.start + i * b.stride : Nat) : Int) ∈
      (List.range b.n).map (fun k : Nat => progVal (positions b) k)
    apply List.mem_map.mpr
    refine ⟨i, List.mem_range.mpr hi, ?_⟩
    simp [progVal, positions]
  rw [if_pos hc]
  have hsub : ((b.start + i * b.stride : Nat) : Int) - (b.start : Int) =
      (i : Int) * (b.stride : Int) := by push_cast; omega
  rw [hsub, Int.mul_ediv_cancel (i : Int) (by exact_mod_cast (Nat.ne_of_gt hb))]
  rfl

private theorem sum_map_zero_int {α : Type} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem sum_eq_single_int (p r : Nat) (hr : r < p) (f : Nat → Int) :
    ((List.range p).map (fun x => if x == r then f x else 0)).sum = f r := by
  induction p with
  | zero => omega
  | succ p ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      by_cases heq : p = r
      · subst r
        have hpre : ((List.range p).map (fun x => if x == p then f x else 0)) =
            (List.range p).map (fun _ => (0 : Int)) := by
          apply List.map_congr_left
          intro x hx
          have hxp : x < p := List.mem_range.mp hx
          rw [beq_eq_false_iff_ne.mpr (by omega)]
          simp
        rw [hpre, sum_map_zero_int]
        simp
      · rw [if_neg (by simp [heq])]
        rw [ih (by omega)]
        omega

private theorem inner_aligned (a b : Lane) (hb : 0 < b.stride) (j : Nat) :
    ((List.range b.n).map (fun i : Nat =>
      if a.start + j * a.stride == b.start + i * b.stride then
        (a.base + (j : Int) * a.step) * (b.base + b.step * (i : Int))
      else 0)).sum =
    (a.base + (j : Int) * a.step) * bvalAt b ((a.start + j * a.stride : Nat) : Int) := by
  let pos : Int := ((a.start + j * a.stride : Nat) : Int)
  let aval : Int := a.base + (j : Int) * a.step
  by_cases hc : contains (positions b) pos = true
  · rcases List.mem_map.mp ((contains_mem (positions b) pos).mp hc) with ⟨i, hi, hival⟩
    have hival' : pos = (b.start : Int) + (i : Int) * (b.stride : Int) := by
      simpa [progVal, positions] using hival.symm
    have huniq : ∀ k < b.n, pos == b.start + k * b.stride → k = i := by
      intro k hk heqk
      have hEq : (b.start : Int) + (k : Int) * (b.stride : Int) =
          (b.start : Int) + (i : Int) * (b.stride : Int) := by
        have hcast : ((b.start + k * b.stride : Nat) : Int) = (b.start : Int) + (k : Int) * (b.stride : Int) := by
          push_cast
          rfl
        rw [← hcast]
        exact (beq_iff_eq.mp heqk).symm.trans hival'
      have hmul : k * b.stride = i * b.stride := by exact_mod_cast (by omega : (k : Int) * (b.stride : Int) = (i : Int) * (b.stride : Int))
      exact Nat.eq_of_mul_eq_mul_left hb (by simpa [Nat.mul_comm] using hmul)
    have hsum : ((List.range b.n).map (fun k : Nat =>
        if a.start + j * a.stride == b.start + k * b.stride then
          aval * (b.base + b.step * (k : Int))
        else 0)).sum = aval * (b.base + b.step * (i : Int)) := by
      rw [show (List.range b.n).map (fun k : Nat =>
          if a.start + j * a.stride == b.start + k * b.stride then
            aval * (b.base + b.step * (k : Int))
          else 0) =
          (List.range b.n).map (fun k : Nat => if k == i then
            aval * (b.base + b.step * (i : Int)) else 0) by
        apply List.map_congr_left
        intro k hk
        by_cases hkEq : k == i
        · have hki : k = i := beq_iff_eq.mp hkEq
          subst k
          have hnat : a.start + j * a.stride = b.start + i * b.stride := by omega
          rw [if_pos (beq_iff_eq.mpr hnat)]
          simp
        · have hne : a.start + j * a.stride ≠ b.start + k * b.stride := by
            intro hEq
            have hbeq : pos == b.start + k * b.stride := by
              apply beq_iff_eq.mpr
              change ((a.start + j * a.stride : Nat) : Int) = ((b.start + k * b.stride : Nat) : Int)
              exact_mod_cast hEq
            have hki := huniq k (List.mem_range.mp hk) hbeq
            exact hkEq (by rw [hki]; simp)
          rw [beq_eq_false_iff_ne.mpr hne]
          by_cases hki : k = i
          · subst k
            exact (hkEq (by simp)).elim
          · simp [hki]]
      rw [sum_eq_single_int b.n i (List.mem_range.mp hi)]
    have hbval : bvalAt b pos = b.base + b.step * (i : Int) := by
      unfold bvalAt
      rw [if_pos hc]
      have hdiv : (((pos - (b.start : Int)) / (b.stride : Int)).toNat) = i := by
        rw [hival']
        have hsub : ((b.start : Int) + (i : Int) * (b.stride : Int) - (b.start : Int)) =
            (i : Int) * (b.stride : Int) := by omega
        rw [hsub, Int.mul_ediv_cancel (i : Int) (by exact_mod_cast (Nat.ne_of_gt hb))]
        simp
      rw [hdiv]
    rw [hsum, hbval]
  · have hzero : ((List.range b.n).map (fun k : Nat =>
        if a.start + j * a.stride == b.start + k * b.stride then
          aval * (b.base + b.step * (k : Int))
        else 0)).sum = 0 := by
      rw [show (List.range b.n).map (fun k : Nat =>
          if a.start + j * a.stride == b.start + k * b.stride then
            aval * (b.base + b.step * (k : Int))
          else 0) = (List.range b.n).map (fun _ : Nat => (0 : Int)) by
        apply List.map_congr_left
        intro k hk
        by_cases hEq : a.start + j * a.stride == b.start + k * b.stride
        · have hEq' : a.start + j * a.stride = b.start + k * b.stride := beq_iff_eq.mp hEq
          have hc' : contains (positions b) pos = true := by
            apply (contains_mem (positions b) pos).mpr
            unfold progVals
            apply List.mem_map.mpr
            refine ⟨k, hk, ?_⟩
            have hcast : ((b.start + k * b.stride : Nat) : Int) = pos := by
              change ((b.start + k * b.stride : Nat) : Int) = ((a.start + j * a.stride : Nat) : Int)
              exact_mod_cast hEq'.symm
            change ((b.start + k * b.stride : Nat) : Int) = pos
            exact hcast
          exact (hc hc').elim
        · rw [if_neg hEq]]
      rw [sum_map_zero_int]
    have hbval : bvalAt b pos = 0 := by
      unfold bvalAt
      rw [if_neg hc]
    rw [hzero, hbval]
    simp

private theorem moment_sum (n : Nat) (x y dx dy : Int) :
    ((List.range n).map (fun k : Nat => (x + (k : Int) * dx) * (y + (k : Int) * dy))).sum =
      (n : Int) * x * y + tri n * (x * dy + y * dx) + squares n * dx * dy := by
  induction n with
  | zero => simp [tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      rw [tri_succ, squares_succ]
      simp [Int.natCast_add, Int.natCast_mul]
      grind


private theorem aligned_lane (a b : Lane) (ha : 0 < a.stride) (hb : 0 < b.stride) :
    ((List.range a.n).map (fun j : Nat =>
      ((List.range b.n).map (fun i : Nat =>
        if a.start + j * a.stride == b.start + i * b.stride then
          (a.base + (j : Int) * a.step) * (b.base + b.step * (i : Int))
        else 0)).sum)).sum = aligned a b := by
  rw [show (List.range a.n).map (fun j : Nat =>
      ((List.range b.n).map (fun i : Nat =>
        if a.start + j * a.stride == b.start + i * b.stride then
          (a.base + (j : Int) * a.step) * (b.base + b.step * (i : Int))
        else 0)).sum) =
      (List.range a.n).map (fun j : Nat =>
        (a.base + (j : Int) * a.step) * bvalAt b ((a.start + j * a.stride : Nat) : Int)) by
    apply List.map_congr_left
    intro j hj
    exact inner_aligned a b hb j]
  rw [show ((List.range a.n).map (fun j : Nat =>
      (a.base + (j : Int) * a.step) * bvalAt b ((a.start + j * a.stride : Nat) : Int))).sum =
      ((lanePosVals a).map (fun v => posF a v * bvalAt b v)).sum by
    rw [lanePosVals, List.map_map]
    apply congrArg List.sum
    apply List.map_congr_left
    intro j hj
    change (a.base + (j : Int) * a.step) * bvalAt b ((a.start + j * a.stride : Nat) : Int) =
      posF a ((a.start + j * a.stride : Nat) : Int) * bvalAt b ((a.start + j * a.stride : Nat) : Int)
    rw [posF_at a ha j]]
  let q := intersection (positions a) (positions b)
  have hqstep : 0 < q.step := intersection_step_pos (positions a) (positions b) (by exact ha)
  have hsplit : ((lanePosVals a).map (fun v => posF a v * bvalAt b v)).sum =
      (((lanePosVals a).filter (fun v => contains (positions b) v)).map
        (fun v => posF a v * bvalAt b v)).sum := by
    have hc := sum_filter_complement (lanePosVals a) (fun v => contains (positions b) v)
      (fun v => posF a v * bvalAt b v)
    have hz : (((lanePosVals a).filter (fun v => !contains (positions b) v)).map
        (fun v => posF a v * bvalAt b v)).sum = 0 := by
      rw [show ((lanePosVals a).filter (fun v => !contains (positions b) v)).map
          (fun v => posF a v * bvalAt b v) =
          ((lanePosVals a).filter (fun v => !contains (positions b) v)).map (fun _ => (0 : Int)) by
        apply List.map_congr_left
        intro v hv
        have hnv : ¬ contains (positions b) v := by
          have hb := (List.mem_filter.mp hv).2
          simpa using hb
        unfold bvalAt
        rw [if_neg hnv]
        simp]
      exact sum_map_zero_int _
    omega
  rw [hsplit]
  rw [← list_sum_perm ((inter_positions_perm a ha (positions b)).map
    (fun v => posF a v * bvalAt b v))]
  have hbval_inter : ∀ v ∈ progVals q, bvalAt b v = posF b v := by
    intro v hv
    have hc := (contains_mem q v).mpr hv
    have hbboth := (intersection_contains (positions a) (positions b) v).mp hc
    unfold bvalAt posF
    rw [if_pos hbboth.2]
    grind
  rw [show ((progVals q).map (fun v => posF a v * bvalAt b v)).sum =
      ((progVals q).map (fun v => posF a v * posF b v)).sum by
    apply congrArg List.sum
    apply List.map_congr_left
    intro v hv
    rw [hbval_inter v hv]]
  have hval : ∀ k, k < q.n →
      posF a (progVal q k) * posF b (progVal q k) =
        (firstAt a q + (k : Int) * slopeAt a q) *
          (firstAt b q + (k : Int) * slopeAt b q) := by
    intro k hk
    obtain ⟨ka, hka, hvka, heqa⟩ := posF_inter a ha (positions b) (progVal q k)
      (List.mem_map.mpr ⟨k, List.mem_range.mpr hk, rfl⟩)
    change progVal q k = q.lo + (ka : Int) * (q.step : Int) at hvka
    change posF a (progVal q k) = firstAt a q + (ka : Int) * slopeAt a q at heqa
    change ka < q.n at hka
    have hcq : contains q (progVal q k) = true := (contains_mem q (progVal q k)).mpr
      (List.mem_map.mpr ⟨k, List.mem_range.mpr hk, rfl⟩)
    obtain ⟨kb, hkb, hvkb, heqb⟩ := posF_inter_of b hb q
      (fun v h => ((intersection_contains (positions a) (positions b) v).mp h).2)
      (progVal q k) (List.mem_map.mpr ⟨k, List.mem_range.mpr hk, rfl⟩)
    have hkab : ka = kb := by
      have hEq : progVal q ka = progVal q kb := by
        change q.lo + (ka : Int) * (q.step : Int) = q.lo + (kb : Int) * (q.step : Int)
        rw [← hvka, ← hvkb]
      exact progVal_inj_of_pos q hqstep ka kb hka hkb hEq
    have heqb' : posF b (progVal q k) = firstAt b q + (ka : Int) * slopeAt b q := by
      rw [← hkab] at heqb
      exact heqb
    have hkaka : ka = k := by
      apply progVal_inj_of_pos q hqstep ka k hka hk
      change q.lo + (ka : Int) * (q.step : Int) = q.lo + (k : Int) * (q.step : Int)
      rw [← hvka]
      rfl
    rw [hkaka] at heqa heqb'
    rw [heqa, heqb']
  unfold aligned
  change ((progVals q).map (fun v => posF a v * posF b v)).sum =
    (q.n : Int) * firstAt a (intersection (positions a) (positions b)) *
        firstAt b (intersection (positions a) (positions b)) +
      tri q.n * (firstAt a q * slopeAt b q + firstAt b q * slopeAt a q) +
      squares q.n * slopeAt a q * slopeAt b q
  rw [show progVals q = (List.range q.n).map (fun k : Nat => progVal q k) by rfl, List.map_map]
  change ((List.range q.n).map (fun k : Nat =>
    posF a (progVal q k) * posF b (progVal q k))).sum = _
  rw [show (List.range q.n).map (fun k : Nat =>
      posF a (progVal q k) * posF b (progVal q k)) =
      (List.range q.n).map (fun k : Nat =>
        (firstAt a q + (k : Int) * slopeAt a q) *
          (firstAt b q + (k : Int) * slopeAt b q)) by
    apply List.map_congr_left
    intro k hk
    exact hval k (List.mem_range.mp hk)]
  exact moment_sum q.n (firstAt a q) (firstAt b q) (slopeAt a q) (slopeAt b q)


/-! ## Generic goal wrappers -/

private theorem flatMap_map_sum {α : Type} (ls : List α) (f : α → List (Nat × Int))
    (g : Nat × Int → Int) :
    ((ls.flatMap f).map g).sum = (ls.map (fun a => ((f a).map g).sum)).sum := by
  induction ls with
  | nil => simp
  | cons a as ih => simp [ih]

private theorem flatMap_map_sum_nat {α : Type} (ls : List α) (f : α → List (Nat × Int))
    (g : Nat × Int → Nat) :
    ((ls.flatMap f).map g).sum = (ls.map (fun a => ((f a).map g).sum)).sum := by
  induction ls with
  | nil => simp
  | cons a as ih => simp [ih]

private theorem laneVals_count_eq (b : Lane) (v : Int) :
    (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)) =
      (laneVals b).countP (fun w => v == w) := by
  unfold laneVals
  rw [List.countP_map]
  apply congrArg (fun p => List.countP p (List.range b.n))
  funext i
  simp [Int.mul_comm]

private theorem lanePoints_count_eq_aux (l : List Nat) (a b : Lane) :
    ((l.map (fun j : Nat => (a.start + j * a.stride, a.base + (j : Int) * a.step))).map
      (fun r => (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum =
    ((l.map (fun j : Nat => a.base + (j : Int) * a.step)).map
      (fun v => (laneVals b).countP (fun w => v == w))).sum := by
  induction l with
  | nil => simp
  | cons j js ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      rw [laneVals_count_eq]

private theorem lanePoints_count_eq (a b : Lane) :
    ((lanePoints a).map (fun r => (List.range b.n).countP
      (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum =
    ((laneVals a).map (fun v => (laneVals b).countP (fun w => v == w))).sum := by
  unfold lanePoints laneVals
  exact lanePoints_count_eq_aux (List.range a.n) a b

private theorem lanePoints_filter_snd (a : Lane) (m : Progression) :
    ((lanePoints a).filter (fun r => !contains m r.1)).map Prod.snd =
      ((List.range a.n).filter (fun j : Nat => !contains m ((a.start + j * a.stride : Nat) : Int))).map
        (fun j : Nat => a.base + (j : Int) * a.step) := by
  unfold lanePoints
  rw [filter_map_eq]
  simp

private theorem residueJoin_generic (lay : Layout) (c : Config) (b : Lane)
    (hperm : (rows lay c).Perm ((lay.lanes c).flatMap lanePoints)) :
    ((rows lay c).map (fun r => (List.range b.n).countP
      (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum =
    ((lay.lanes c).map (fun a => joinCount a b)).sum := by
  let f : Nat × Int → Nat := fun r => (List.range b.n).countP
    (fun i : Nat => r.2 == b.base + b.step * (i : Int))
  calc
    ((rows lay c).map f).sum = (((lay.lanes c).flatMap lanePoints).map f).sum :=
      list_sum_perm_nat (hperm.map f)
    _ = ((lay.lanes c).map (fun a => ((lanePoints a).map f).sum)).sum :=
      flatMap_map_sum_nat (lay.lanes c) lanePoints f
    _ = ((lay.lanes c).map (fun a => joinCount a b)).sum := by
      apply congrArg List.sum
      apply List.map_congr_left
      intro a ha
      rw [lanePoints_count_eq]
      exact joinCount_lane a b

private theorem deleteProgression_generic (lay : Layout) (c : Config) (m : Progression)
    (hperm : (rows lay c).Perm ((lay.lanes c).flatMap lanePoints))
    (hs : ∀ a ∈ lay.lanes c, 0 < a.stride) :
    (((rows lay c).filter (fun r => !contains m r.1)).map Prod.snd).sum =
      ((lay.lanes c).map (fun a => partialSum a a.n - deletedSum a m)).sum := by
  let p : Nat × Int → Bool := fun r => !contains m r.1
  have hfilt := hperm.filter p
  have hf : ((lay.lanes c).flatMap lanePoints).filter p =
      (lay.lanes c).flatMap (fun a => (lanePoints a).filter p) := by
    induction lay.lanes c with
    | nil => simp
    | cons a as ih => simp [ih]
  calc
    (((rows lay c).filter p).map Prod.snd).sum =
        ((((lay.lanes c).flatMap lanePoints).filter p).map Prod.snd).sum :=
      list_sum_perm (hfilt.map (fun r => r.2))
    _ = ((lay.lanes c).map (fun a =>
          (((lanePoints a).filter p).map Prod.snd).sum)).sum := by
      rw [hf]
      exact flatMap_map_sum (lay.lanes c)
        (fun a => (lanePoints a).filter p) (fun r : Nat × Int => r.2)
    _ = ((lay.lanes c).map (fun a => partialSum a a.n - deletedSum a m)).sum := by
      apply congrArg List.sum
      apply List.map_congr_left
      intro a ha
      change (((lanePoints a).filter (fun r => !contains m r.1)).map Prod.snd).sum =
        partialSum a a.n - deletedSum a m
      rw [lanePoints_filter_snd]
      exact deletedSum_lane a (hs a ha) m

private theorem alignedProduct_generic (lay : Layout) (c : Config) (b : Lane)
    (hperm : (rows lay c).Perm ((lay.lanes c).flatMap lanePoints))
    (hs : ∀ a ∈ lay.lanes c, 0 < a.stride) (hb : 0 < b.stride) :
    ((rows lay c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)).sum =
    ((lay.lanes c).map (aligned · b)).sum := by
  let g : Nat × Int → Int := fun r => ((List.range b.n).map (fun i : Nat =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum
  calc
    ((rows lay c).map g).sum = (((lay.lanes c).flatMap lanePoints).map g).sum :=
      list_sum_perm (hperm.map g)
    _ = ((lay.lanes c).map (fun a => ((lanePoints a).map g).sum)).sum :=
      flatMap_map_sum (lay.lanes c) lanePoints g
    _ = ((lay.lanes c).map (aligned · b)).sum := by
      apply congrArg List.sum
      apply List.map_congr_left
      intro a ha
      change ((lanePoints a).map g).sum = aligned a b
      unfold lanePoints g
      rw [List.map_map]
      change ((List.range a.n).map (fun j : Nat => ((List.range b.n).map (fun i : Nat =>
          if a.start + j * a.stride == b.start + i * b.stride then
            (a.base + (j : Int) * a.step) * (b.base + b.step * (i : Int))
          else 0)).sum)).sum = aligned a b
      exact aligned_lane a b (hs a ha) hb

private theorem flatMap_congr_left {α β : Type} (l : List α) (f g : α → List β)
    (h : ∀ x ∈ l, f x = g x) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]


/-! ## Rows as permuted lanes -/

private theorem rows_lanes_DictionaryCycle (c : Config) :
    (rows DictionaryCycle.layout c).Perm
      ((DictionaryCycle.layout.lanes c).flatMap lanePoints) := by
  by_cases hne : c.dict = []
  · simp [rows, DictionaryCycle.layout, lanePoints, hne]
  · let p := c.dict.length
    have hp : 0 < p := by dsimp [p]; exact List.length_pos_iff.mpr hne
    have hrows : rows DictionaryCycle.layout c =
        (List.range c.n).map (fun i : Nat => (i, c.dict[i % p]?.getD 0)) := by
      unfold rows
      apply range_filterMap_all
      intro i hi
      have hlt : i % c.dict.length < c.dict.length := Nat.mod_lt i hp
      have hget : c.dict[i % c.dict.length]? = some (c.dict[i % c.dict.length]) := by
        rw [List.getElem?_eq_some_iff]
        exact ⟨hlt, rfl⟩
      simp [DictionaryCycle.layout, hget, p]
    rw [hrows]
    simp only [DictionaryCycle.layout]
    have hpdef : c.dict.length = p := rfl
    rw [hpdef]
    have hflat : ((List.range (min p c.n)).map (fun r => lane c.n r p (c.dict[r]?.getD 0) 0)).flatMap lanePoints =
        ((List.range (min p c.n)).flatMap (fun r =>
          (List.range ((c.n - r + p - 1) / p)).map (fun q =>
            (r + q * p, c.dict[(r + q * p) % p]?.getD 0)))) := by
      rw [List.flatMap_map]
      apply flatMap_congr_left
      intro r hr
      have hrp : r < p := by have := List.mem_range.mp hr; omega
      unfold lanePoints lane
      dsimp
      congr 1
      funext q
      have hmod : (r + q * p) % p = r := by
        rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hrp]
      rw [hmod]
      simp
    rw [hflat]
    have hperm := residue_rows_perm_gen c.n p (min p c.n) hp (Nat.min_le_left _ _)
      (fun i : Nat => c.dict[i % p]?.getD 0)
    have hfilter : (List.range c.n).filter (fun i : Nat => i % p < min p c.n) = List.range c.n := by
      apply List.filter_eq_self.mpr
      intro i hi
      have hiN : i < c.n := List.mem_range.mp hi
      by_cases hpn : p ≤ c.n
      · rw [Nat.min_eq_left hpn]
        simp [Nat.mod_lt i hp]
      · rw [Nat.min_eq_right (by omega)]
        simp [Nat.lt_of_le_of_lt (Nat.mod_le i p) hiN]
    simpa [hfilter] using hperm


private theorem rows_lanes_DictionaryDrift (c : Config) :
    (rows DictionaryDrift.layout c).Perm
      ((DictionaryDrift.layout.lanes c).flatMap lanePoints) := by
  by_cases hne : c.dict = []
  · simp [rows, DictionaryDrift.layout, lanePoints, hne]
  · let p := c.dict.length
    have hp : 0 < p := by dsimp [p]; exact List.length_pos_iff.mpr hne
    have hrows : rows DictionaryDrift.layout c =
        (List.range c.n).map (fun i : Nat =>
          (i, c.dict[i % p]?.getD 0 + c.step * ((i / p : Nat) : Int))) := by
      unfold rows
      apply range_filterMap_all
      intro i hi
      have hlt : i % c.dict.length < c.dict.length := Nat.mod_lt i hp
      have hget : c.dict[i % c.dict.length]? = some (c.dict[i % c.dict.length]) := by
        rw [List.getElem?_eq_some_iff]
        exact ⟨hlt, rfl⟩
      simp [DictionaryDrift.layout, hget, p]
    rw [hrows]
    simp only [DictionaryDrift.layout]
    have hpdef : c.dict.length = p := rfl
    rw [hpdef]
    have hflat : ((List.range (min p c.n)).map (fun r => lane c.n r p (c.dict[r]?.getD 0) c.step)).flatMap lanePoints =
        ((List.range (min p c.n)).flatMap (fun r =>
          (List.range ((c.n - r + p - 1) / p)).map (fun q =>
            (r + q * p, c.dict[(r + q * p) % p]?.getD 0 +
              c.step * (((r + q * p) / p : Nat) : Int))))) := by
      rw [List.flatMap_map]
      apply flatMap_congr_left
      intro r hr
      have hrp : r < p := by have := List.mem_range.mp hr; omega
      unfold lanePoints lane
      dsimp
      congr 1
      funext q
      have hmod : (r + q * p) % p = r := by
        rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hrp]
      have hdiv : (r + q * p) / p = q := by
        rw [Nat.add_mul_div_right r q hp, Nat.div_eq_of_lt hrp]
        simp
      rw [hmod]
      have hdivInt : ((r + q * p : Nat) : Int) / (p : Int) = (q : Int) := by
        exact_mod_cast hdiv
      have hdivInt' : ((r : Int) + (q : Int) * (p : Int)) / (p : Int) = (q : Int) := by
        rw [← show ((r + q * p : Nat) : Int) = (r : Int) + (q : Int) * (p : Int) by push_cast; rfl]
        exact hdivInt
      rw [hdivInt']
      simp [Int.mul_comm]
    rw [hflat]
    have hperm := residue_rows_perm_gen c.n p (min p c.n) hp (Nat.min_le_left _ _)
      (fun i : Nat => c.dict[i % p]?.getD 0 + c.step * ((i / p : Nat) : Int))
    have hfilter : (List.range c.n).filter (fun i : Nat => i % p < min p c.n) = List.range c.n := by
      apply List.filter_eq_self.mpr
      intro i hi
      have hiN : i < c.n := List.mem_range.mp hi
      by_cases hpn : p ≤ c.n
      · rw [Nat.min_eq_left hpn]
        simp [Nat.mod_lt i hp]
      · rw [Nat.min_eq_right (by omega)]
        simp [Nat.lt_of_le_of_lt (Nat.mod_le i p) hiN]
    simpa [hfilter] using hperm


private theorem rows_lanes_AlternatingSign (c : Config) :
    (rows AlternatingSign.layout c).Perm
      ((AlternatingSign.layout.lanes c).flatMap lanePoints) := by
  let f : Nat → Int := fun i =>
    (if i % 2 == 0 then (1 : Int) else -1) * (c.base + c.step * (i : Int))
  have hrows : rows AlternatingSign.layout c = (List.range c.n).map (fun i : Nat => (i, f i)) := by
    unfold rows AlternatingSign.layout f
    simp
  rw [hrows]
  by_cases hn0 : c.n = 0
  · simp [hn0, AlternatingSign.layout, lanePoints, lane]
  · have hnpos : 0 < c.n := by omega
    by_cases hn1 : c.n = 1
    · simp [hn1, AlternatingSign.layout, lanePoints, lane, f]
    · have hn2 : 2 ≤ c.n := by omega
      have hp : 0 < 2 := by omega
      have hperm := residue_rows_perm_gen c.n 2 (min 2 c.n) hp (by omega) f
      have hmin : min 2 c.n = 2 := Nat.min_eq_left hn2
      rw [hmin] at hperm
      have hfilter : (List.range c.n).filter (fun i : Nat => i % 2 < 2) = List.range c.n := by
        apply List.filter_eq_self.mpr
        intro i hi
        simp [Nat.mod_lt i hp]
      rw [hfilter] at hperm
      have hRHS : ((List.range 2).flatMap (fun r =>
          (List.range ((c.n - r + 2 - 1) / 2)).map (fun q => (r + q * 2, f (r + q * 2))))) =
          (AlternatingSign.layout.lanes c).flatMap lanePoints := by
        simp only [show List.range 2 = [0, 1] by decide, List.flatMap_cons, List.flatMap_nil,
          AlternatingSign.layout, lanePoints, lane]
        simp only [Nat.add_zero, Nat.zero_add]
        congr 1
        · apply List.map_congr_left
          intro q hq
          simp [f, Int.mul_comm, Int.add_comm, Int.add_left_comm, Int.add_assoc]
          grind
        · simp only [List.append_nil]
          apply List.map_congr_left
          intro q hq
          simp [f, Int.mul_comm, Int.add_comm, Int.add_left_comm, Int.add_assoc]
          grind
      rw [← hRHS]
      simpa [hmin] using hperm


/-! ## The nine theorems -/

private theorem stride_DictionaryCycle (c : Config) :
    ∀ a ∈ DictionaryCycle.layout.lanes c, 0 < a.stride := by
  intro a ha
  by_cases hne : c.dict = []
  · simp [DictionaryCycle.layout, hne] at ha
  · have hp : 0 < c.dict.length := List.length_pos_iff.mpr hne
    simp [DictionaryCycle.layout] at ha
    rcases ha with ⟨r, hr, rfl⟩
    simpa [lane] using hp

private theorem stride_DictionaryDrift (c : Config) :
    ∀ a ∈ DictionaryDrift.layout.lanes c, 0 < a.stride := by
  intro a ha
  by_cases hne : c.dict = []
  · simp [DictionaryDrift.layout, hne] at ha
  · have hp : 0 < c.dict.length := List.length_pos_iff.mpr hne
    simp [DictionaryDrift.layout] at ha
    rcases ha with ⟨r, hr, rfl⟩
    simpa [lane] using hp

private theorem stride_AlternatingSign (c : Config) :
    ∀ a ∈ AlternatingSign.layout.lanes c, 0 < a.stride := by
  intro a ha
  simp [AlternatingSign.layout] at ha
  rcases ha with rfl | rfl <;> simp [lane] <;> omega

theorem DictionaryCycle_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryCycle.ResidueJoin.Goal := by
  intro c b
  exact residueJoin_generic DictionaryCycle.layout c b
    (rows_lanes_DictionaryCycle c)

theorem DictionaryCycle_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryCycle.DeleteProgression.Goal := by
  intro c m
  exact deleteProgression_generic DictionaryCycle.layout c m
    (rows_lanes_DictionaryCycle c) (stride_DictionaryCycle c)

theorem DictionaryCycle_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryCycle.AlignedProduct.Goal := by
  intro c b hb
  exact alignedProduct_generic DictionaryCycle.layout c b
    (rows_lanes_DictionaryCycle c) (stride_DictionaryCycle c) hb

theorem DictionaryDrift_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryDrift.ResidueJoin.Goal := by
  intro c b
  exact residueJoin_generic DictionaryDrift.layout c b
    (rows_lanes_DictionaryDrift c)

theorem DictionaryDrift_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryDrift.DeleteProgression.Goal := by
  intro c m
  exact deleteProgression_generic DictionaryDrift.layout c m
    (rows_lanes_DictionaryDrift c) (stride_DictionaryDrift c)

theorem DictionaryDrift_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryDrift.AlignedProduct.Goal := by
  intro c b hb
  exact alignedProduct_generic DictionaryDrift.layout c b
    (rows_lanes_DictionaryDrift c) (stride_DictionaryDrift c) hb

theorem AlternatingSign_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.AlternatingSign.ResidueJoin.Goal := by
  intro c b
  exact residueJoin_generic AlternatingSign.layout c b
    (rows_lanes_AlternatingSign c)

theorem AlternatingSign_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.AlternatingSign.DeleteProgression.Goal := by
  intro c m
  exact deleteProgression_generic AlternatingSign.layout c m
    (rows_lanes_AlternatingSign c) (stride_AlternatingSign c)

theorem AlternatingSign_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.AlternatingSign.AlignedProduct.Goal := by
  intro c b hb
  exact alignedProduct_generic AlternatingSign.layout c b
    (rows_lanes_AlternatingSign c) (stride_AlternatingSign c) hb


private theorem filterMap_ite_eq {α β : Type} (l : List α) (p : α → Bool) (f g : α → β) :
    l.filterMap (fun x => if p x then some (f x) else some (g x)) =
      l.map (fun x => if p x then f x else g x) := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

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
    have hsub : n - r + p - 1 = n / p * p + (n % p + p - 1 - r) := by grind
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
        · have heq : n % p + 1 = p := by
            have hs' := Nat.mod_lt n hp
            omega
          have hlt : r < n % p := by omega
          have hltnew : ¬ r < 0 := by omega
          simp [hn, h, hlt, hltnew]

private theorem filterMap_ite_some_eq {α β : Type} (l : List α) (p : α → Bool) (f : α → β) :
    l.filterMap (fun i => if p i then some (f i) else none) = (l.filter p).map f := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem rows_sparse_lanes (c : Config) :
    (rows TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.layout c).Perm
      ((TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.layout.lanes c).flatMap lanePoints) := by
  open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample in
    let w := width c
  let w := TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c
  let r := c.split % w
  have hw : 0 < w := by dsimp [w, TinylambOpt.ResearchGoals.Atlas.EncodedPages.width]; omega
  have hr : r < w := by dsimp [r]; exact Nat.mod_lt _ hw
  have hrows : rows TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.layout c =
      ((List.range c.n).filter (fun i => i % w == r)).map
        (fun i => (i, c.base + c.step*((i / w : Nat) : Int))) := by
    unfold rows
    simp only [TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.layout]
    rw [← filterMap_ite_some_eq]
    apply filterMap_congr'
    intro i hi
    by_cases h : i % TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c ==
        c.split % TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c <;>
      simp [h, w, r]
  rw [hrows, range_filter_mod_eq c.n w r hw hr]
  simp only [List.map_map, Function.comp_apply]
  simp only [TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.layout,
    List.flatMap_cons, List.flatMap_nil, List.append_nil]
  unfold lanePoints TinylambOpt.ResearchGoals.Atlas.EncodedPages.lane
  dsimp
  have hmapEq :
      (List.map ((fun i : Nat => (i, c.base + c.step * (((i : Nat) : Int) /
        (TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c : Int)))) ∘
        (fun q => c.split % TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c +
          q * TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c))
        (List.range ((c.n - c.split % TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c +
          TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c - 1) /
          TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c))) =
      List.map (fun q => (c.split % TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c +
        q * TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c,
        c.base + c.step * (q : Int)))
        (List.range ((c.n - c.split % TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c +
          TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c - 1) /
          TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c)) := by
    apply List.map_congr_left
    intro q hq
    have hw' : 0 < TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c := by
      dsimp [TinylambOpt.ResearchGoals.Atlas.EncodedPages.width]
      omega
    have hcastx : ((c.split % TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c +
        q * TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c : Nat) : Int) =
        (c.split % TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c : Int) +
          (TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c : Int) * (q : Int) := by
      push_cast
      rw [Int.mul_comm (q : Int) (TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c : Int)]
    have hdivInt : (((c.split % TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c +
        q * TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c : Nat) : Int) /
        (TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c : Int)) = (q : Int) := by
      rw [hcastx, Int.add_mul_ediv_left _ (q : Int) (by omega)]
      rw [Int.ediv_eq_zero_of_lt (by exact_mod_cast Nat.zero_le _)
        (by exact_mod_cast Nat.mod_lt _ hw')]
      simp
    change (c.split % TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c +
      q * TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c,
      c.base + c.step *
        ((((c.split % TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c +
          q * TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c : Nat) : Int)) /
          (TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c : Int))) =
      (c.split % TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c +
        q * TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c,
        c.base + c.step * (q : Int))
    rw [hdivInt]
  rw [hmapEq]
  have hmapEq2 : List.map (fun q => (c.split % TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c +
        q * TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c,
        c.base + c.step * (q : Int)))
        (List.range ((c.n - c.split % TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c +
          TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c - 1) /
          TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c)) =
      List.map (fun j => (c.split % TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c +
        j * TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c,
        c.base + (j : Int) * c.step))
        (List.range ((c.n - c.split % TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c +
          TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c - 1) /
          TinylambOpt.ResearchGoals.Atlas.EncodedPages.width c)) := by
    apply List.map_congr_left
    intro j hj
    simp [Int.mul_comm]
  rw [hmapEq2]

private theorem sound_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.ResidueJoin.Goal := by
  intro c b
  exact residueJoin_generic TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.layout c b
    (rows_sparse_lanes c)

private theorem sound_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.DeleteProgression.Goal := by
  intro c mask
  apply deleteProgression_generic TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.layout c mask
    (rows_sparse_lanes c)
  intro a ha
  simp only [TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.layout,
    List.mem_singleton] at ha
  subst a
  dsimp [TinylambOpt.ResearchGoals.Atlas.EncodedPages.lane,
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.width]
  omega

private theorem sound_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.AlignedProduct.Goal := by
  intro c b hb
  apply alignedProduct_generic TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.layout c b
    (rows_sparse_lanes c)
  · intro a ha
    simp only [TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.layout,
      List.mem_singleton] at ha
    subst a
    dsimp [TinylambOpt.ResearchGoals.Atlas.EncodedPages.lane,
      TinylambOpt.ResearchGoals.Atlas.EncodedPages.width]
    omega
  · exact hb

end TinylambOpt.ResearchProofs.Atlas.EncodedPages.SparseSample.Remaining.E3Lib

theorem sound_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.ResidueJoin.Goal :=
  TinylambOpt.ResearchProofs.Atlas.EncodedPages.SparseSample.Remaining.E3Lib.sound_ResidueJoin

theorem sound_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.DeleteProgression.Goal :=
  TinylambOpt.ResearchProofs.Atlas.EncodedPages.SparseSample.Remaining.E3Lib.sound_DeleteProgression

theorem sound_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.AlignedProduct.Goal :=
  TinylambOpt.ResearchProofs.Atlas.EncodedPages.SparseSample.Remaining.E3Lib.sound_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.EncodedPages.SparseSample.Remaining

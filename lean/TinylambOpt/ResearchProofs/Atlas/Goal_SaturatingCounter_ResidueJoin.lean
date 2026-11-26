import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `SaturatingCounter.ResidueJoin` — `∀ c (b : Lane), ((rows layout c).map (fun r =>
  (List.range b.n).countP (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum =
  ((layout.lanes c).map (joinCount · b)).sum`
-/

namespace TinylambOpt.ResearchProofs.Atlas.Goal_SaturatingCounter_ResidueJoin

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SaturatingCounter

set_option maxHeartbeats 0
set_option maxRecDepth 10000

private theorem contains_iff_index (p : Progression) (hs : 0 < p.step) (v : Int) :
    contains p v = true ↔ ∃ k : Nat, k < p.n ∧ v = p.lo + (k : Int) * (p.step : Int) := by
  have hs0 : (p.step == 0) = false := by simp [Nat.ne_of_gt hs]
  have hspos : 0 < (p.step : Int) := by exact_mod_cast hs
  constructor
  · intro h
    simp [contains, hs0] at h
    rcases h with ⟨⟨⟨hn, hlo⟩, hhi⟩, hmod⟩
    have hmod0 : (v - p.lo) % (p.step : Int) = 0 := by simpa using hmod
    let q : Int := (v - p.lo) / (p.step : Int)
    have hqmul : q * (p.step : Int) = v - p.lo := by
      dsimp [q]
      have h := Int.ediv_mul_add_emod (v - p.lo) (p.step : Int)
      omega
    have hspos_nonneg : 0 ≤ (p.step : Int) := by omega
    have hqnonneg : 0 ≤ q := by
      dsimp [q]
      exact Int.ediv_nonneg (by omega) hspos_nonneg
    have hhi' : v ≤ p.lo + ((p.n - 1 : Nat) : Int) * (p.step : Int) := by
      simpa [lastVal] using hhi
    have hle_int : v - p.lo ≤ ((p.n - 1 : Nat) : Int) * (p.step : Int) := by omega
    have hdivle : (v - p.lo) / (p.step : Int) ≤
        (((p.n - 1 : Nat) : Int) * (p.step : Int)) / (p.step : Int) :=
      Int.ediv_le_ediv hspos hle_int
    have hcancel : (((p.n - 1 : Nat) : Int) * (p.step : Int)) / (p.step : Int) =
        ((p.n - 1 : Nat) : Int) := by
      rw [Int.mul_ediv_cancel _ (by omega : (p.step : Int) ≠ 0)]
    have hbound : q ≤ ((p.n - 1 : Nat) : Int) := by dsimp [q] at hdivle ⊢; omega
    have hklt : q.toNat < p.n := by
      have hleNat : q.toNat ≤ p.n - 1 := (Int.toNat_le).mpr hbound
      omega
    refine ⟨q.toNat, hklt, ?_⟩
    have hcastq : (q.toNat : Int) = q := Int.toNat_of_nonneg hqnonneg
    rw [hcastq]
    omega
  · rintro ⟨k, hk, rfl⟩
    have hn : 0 < p.n := by omega
    have hk_le : k ≤ p.n - 1 := by omega
    have hspos_nonneg : 0 ≤ (p.step : Int) := by omega
    have hnonneg : p.lo ≤ p.lo + (k : Int) * (p.step : Int) :=
      Int.le_add_of_nonneg_right (Int.mul_nonneg (by omega) hspos_nonneg)
    have hkcast : (k : Int) ≤ ((p.n - 1 : Nat) : Int) := by exact_mod_cast hk_le
    have hle : p.lo + (k : Int) * (p.step : Int) ≤
        p.lo + ((p.n - 1 : Nat) : Int) * (p.step : Int) := by
      have := Int.mul_le_mul_of_nonneg_right hkcast hspos_nonneg
      omega
    have hmod : (p.lo + (k : Int) * (p.step : Int) - p.lo) % (p.step : Int) = 0 := by
      apply Int.emod_eq_zero_of_dvd
      refine ⟨k, ?_⟩
      rw [Int.mul_comm (k : Int) (p.step : Int)]
      omega
    have hvhi : p.lo + (k : Int) * (p.step : Int) ≤ lastVal p := by
      simpa [lastVal] using hle
    have hbool : ((p.lo + (k : Int) * (p.step : Int) - p.lo) % (p.step : Int) == 0) = true := by
      rw [hmod]
      simp
    simp [contains, hs0, hn, hnonneg, hvhi, hbool]

private theorem contains_zero (p : Progression) (v : Int) :
    contains ⟨p.lo, 0, p.n⟩ v = true ↔ 0 < p.n ∧ v = p.lo := by
  constructor
  · intro h
    simp [contains, lastVal] at h
    rcases h with ⟨⟨hn, hlo⟩, hhi⟩
    omega
  · intro h
    rcases h with ⟨hn, hv⟩
    subst v
    simp [contains, lastVal, hn]

private theorem contains_index_inj (p : Progression) (hs : 0 < p.step) {k l : Nat}
    (h : p.lo + (k : Int) * (p.step : Int) = p.lo + (l : Int) * (p.step : Int)) : k = l := by
  have hsne : (p.step : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hs)
  have hmul : (k : Int) * (p.step : Int) = (l : Int) * (p.step : Int) := by omega
  have hcast : (k : Int) = (l : Int) := Int.eq_of_mul_eq_mul_right hsne hmul
  exact_mod_cast hcast

private theorem bezout_correct : ∀ a b : Nat,
    (bezout a b).1 * (a : Int) + (bezout a b).2 * (b : Int) = (Nat.gcd a b : Int) := by
  intro a b
  revert a
  refine Nat.strongRecOn b ?_
  intro b ih a
  by_cases hb : b = 0
  · subst b
    simp [bezout]
  · have hmodlt : a % b < b := Nat.mod_lt a (Nat.pos_of_ne_zero hb)
    rcases hbez : bezout b (a % b) with ⟨x, y⟩
    have hxy : x * (b : Int) + y * ((a % b : Nat) : Int) = (Nat.gcd b (a % b) : Int) := by
      simpa [hbez] using ih (a % b) hmodlt b
    have hgcd : Nat.gcd b (a % b) = Nat.gcd a b := by
      rw [Nat.gcd_comm b (a % b)]
      rw [← Nat.gcd_rec b a]
      rw [Nat.gcd_comm a b]
    have hdivNat : (a : Int) = ((a / b : Nat) : Int) * (b : Int) + ((a % b : Nat) : Int) := by
      have hh := Nat.div_add_mod a b
      have hh' : (b : Int) * ((a / b : Nat) : Int) + ((a % b : Nat) : Int) = (a : Int) := by
        exact_mod_cast hh
      rw [Int.mul_comm] at hh'
      omega
    have hdivmod : (a : Int) = ((a : Int) / (b : Int)) * (b : Int) + ((a % b : Nat) : Int) := by
      rw [Int.ofNat_ediv_ofNat]
      exact hdivNat
    have hbezab : bezout a b = (y, x - ((a : Int) / (b : Int)) * y) := by
      unfold bezout
      rw [dif_neg hb, hbez]
    rw [hbezab]
    rw [hgcd] at hxy
    let q : Int := (a : Int) / (b : Int)
    let r : Int := ((a % b : Nat) : Int)
    have hqr : q * (b : Int) + r = (a : Int) := by
      dsimp [q, r]
      exact hdivmod.symm
    change y * (a : Int) + (x - q * y) * (b : Int) = (Nat.gcd a b : Int)
    have hcalc : y * (a : Int) + (x - q * y) * (b : Int) = x * (b : Int) + y * r := by
      rw [← hqr, Int.mul_add, Int.sub_mul]
      rw [← Int.mul_assoc y q (b : Int)]
      rw [Int.mul_comm y q]
      omega
    rw [hcalc]
    have hxy' : x * (b : Int) + y * r = (Nat.gcd a b : Int) := by simpa [r] using hxy
    rw [hxy']

private theorem bezout_coprime_unit (s t : Nat) (hg : Nat.gcd s t = 1) :
    (t : Int) ∣ (bezout s t).1 * (s : Int) - 1 := by
  have hbt := bezout_correct s t
  have hgcd1 : (Nat.gcd s t : Int) = 1 := by rw [hg]; simp
  rw [hgcd1] at hbt
  have hzero : (bezout s t).2 * (t : Int) + (-(bezout s t).2) * (t : Int) = 0 := by
    rw [Int.neg_mul]
    omega
  refine ⟨-(bezout s t).2, ?_⟩
  have hsw : (bezout s t).1 * (s : Int) - 1 = -(bezout s t).2 * (t : Int) := by omega
  rw [hsw, Int.mul_comm]

private def ModEq (m x y : Int) : Prop := m ∣ x - y

private theorem modeq_symm {m x y : Int} (h : ModEq m x y) : ModEq m y x := by
  dsimp [ModEq] at h ⊢
  rcases h with ⟨k, hk⟩
  refine ⟨-k, ?_⟩
  rw [Int.mul_neg]
  omega

private theorem modeq_trans {m x y z : Int} (h1 : ModEq m x y) (h2 : ModEq m y z) :
    ModEq m x z := by
  dsimp [ModEq] at h1 h2 ⊢
  rcases h1 with ⟨k1, hk1⟩
  rcases h2 with ⟨k2, hk2⟩
  refine ⟨k1 + k2, ?_⟩
  rw [Int.mul_add]
  omega

private theorem modeq_add {m x y c : Int} (h : ModEq m x y) : ModEq m (x + c) (y + c) := by
  dsimp [ModEq] at h ⊢
  rcases h with ⟨k, hk⟩
  refine ⟨k, ?_⟩
  omega

private theorem modeq_mul_left {m x y c : Int} (h : ModEq m x y) : ModEq m (c * x) (c * y) := by
  dsimp [ModEq] at h ⊢
  rcases h with ⟨k, hk⟩
  refine ⟨c * k, ?_⟩
  rw [← Int.mul_sub, hk]
  rw [← Int.mul_assoc, Int.mul_comm c m, Int.mul_assoc]

private theorem modeq_emod (m x : Int) : ModEq m x (x % m) := by
  dsimp [ModEq]
  refine ⟨x / m, ?_⟩
  have h := Int.ediv_mul_add_emod x m
  rw [Int.mul_comm (x / m) m] at h
  omega

private theorem modeq_of_emod_eq {m x y : Int} (h : x % m = y % m) : ModEq m x y := by
  dsimp [ModEq]
  exact Int.dvd_of_emod_eq_zero (Int.emod_eq_emod_iff_emod_sub_eq_zero.mp h)

private theorem emod_eq_of_modeq {m x y : Int} (h : ModEq m x y) : x % m = y % m := by
  dsimp [ModEq] at h
  exact Int.emod_eq_emod_iff_emod_sub_eq_zero.mpr (Int.emod_eq_zero_of_dvd h)

private theorem modeq_of_dvd_sub {m x y : Int} (h : m ∣ x - y) : ModEq m x y := h

private theorem gcd_div_coprime (s t : Nat) (hs : 0 < s) (ht : 0 < t) :
    Nat.gcd (s / Nat.gcd s t) (t / Nat.gcd s t) = 1 := by
  have hgpos : 0 < Nat.gcd s t := Nat.gcd_pos_of_pos_left t hs
  have h1 : Nat.gcd s t ∣ s := Nat.gcd_dvd_left s t
  have h2 : Nat.gcd s t ∣ t := Nat.gcd_dvd_right s t
  rw [Nat.gcd_div h1 h2]
  have hdiv : Nat.gcd s t / Nat.gcd s t = 1 := Nat.div_self hgpos
  rw [hdiv]

private theorem crt_root (s t : Nat) (hs : 0 < s) (ht : 0 < t)
    (la lb : Int) (hgdiv : (Nat.gcd s t : Int) ∣ lb - la) :
    let g := Nat.gcd s t
    let sp := s / g
    let tp := t / g
    let unit := (bezout sp tp).1
    let q : Int := (lb - la) / (g : Int)
    let k0 : Int := (q * unit) % (tp : Int)
    let root : Int := la + (s : Int) * k0
    ModEq (s : Int) root la ∧ ModEq (t : Int) root lb := by
  dsimp only
  let g := Nat.gcd s t
  let sp := s / g
  let tp := t / g
  let unit := (bezout sp tp).1
  let q : Int := (lb - la) / (g : Int)
  let k0 : Int := (q * unit) % (tp : Int)
  let root : Int := la + (s : Int) * k0
  have hgpos : 0 < g := Nat.gcd_pos_of_pos_left t hs
  have hsppos : 0 < sp := Nat.div_pos (Nat.le_of_dvd hs (Nat.gcd_dvd_left s t)) hgpos
  have htppos : 0 < tp := Nat.div_pos (Nat.le_of_dvd ht (Nat.gcd_dvd_right s t)) hgpos
  have hgcd1 : Nat.gcd sp tp = 1 := by
    dsimp [sp, tp, g]
    exact gcd_div_coprime s t hs ht
  have hunit : (tp : Int) ∣ unit * (sp : Int) - 1 := by
    dsimp [unit]
    exact bezout_coprime_unit sp tp hgcd1
  have hgq : (g : Int) * q = lb - la := by
    dsimp [q]
    exact Int.mul_ediv_cancel' hgdiv
  have htk0 : ModEq (tp : Int) (q * unit) k0 := by
    dsimp [k0]
    exact modeq_emod (tp : Int) (q * unit)
  have hspk : ModEq (tp : Int) ((sp : Int) * k0) q := by
    have hleft : ModEq (tp : Int) (k0) (q * unit) := modeq_symm htk0
    have hmul := modeq_mul_left (c := (sp : Int)) hleft
    have hterm1 : ModEq (tp : Int) ((sp : Int) * k0) ((sp : Int) * (q * unit)) := by
      simpa using hmul
    have hterm2 : ModEq (tp : Int) ((sp : Int) * (q * unit)) q := by
      dsimp [ModEq]
      rcases hunit with ⟨w, hw⟩
      refine ⟨q * w, ?_⟩
      have hcomm : (sp : Int) * (q * unit) = q * (unit * (sp : Int)) := by
        rw [← Int.mul_assoc, Int.mul_comm (sp : Int) q, Int.mul_assoc, Int.mul_comm (sp : Int) unit]
      have hcalc : (sp : Int) * (q * unit) - q = q * (unit * (sp : Int) - 1) := by
        rw [Int.mul_sub]
        omega
      rw [hcalc, hw]
      rw [← Int.mul_assoc, Int.mul_comm q (tp : Int), Int.mul_assoc]
    exact modeq_trans hterm1 hterm2
  have hscast : (s : Int) = (g : Int) * (sp : Int) := by
    have hgdvd : g ∣ s := Nat.gcd_dvd_left s t
    rcases hgdvd with ⟨u, hu⟩
    have hu' : (s : Int) = (g : Int) * (u : Int) := by exact_mod_cast hu
    have hspu : sp = u := by
      dsimp [sp]
      rw [hu]
      rw [Nat.mul_comm]
      exact Nat.mul_div_left u hgpos
    rw [hspu]
    exact hu'
  have htcast : (t : Int) = (g : Int) * (tp : Int) := by
    have hgdvd : g ∣ t := Nat.gcd_dvd_right s t
    rcases hgdvd with ⟨u, hu⟩
    have hu' : (t : Int) = (g : Int) * (u : Int) := by exact_mod_cast hu
    have htpu : tp = u := by
      dsimp [tp]
      rw [hu]
      rw [Nat.mul_comm]
      exact Nat.mul_div_left u hgpos
    rw [htpu]
    exact hu'
  constructor
  · change ModEq (s : Int) (la + (s : Int) * k0) la
    dsimp [ModEq]
    refine ⟨k0, ?_⟩
    omega
  · change ModEq (t : Int) (la + (s : Int) * k0) lb
    dsimp [ModEq]
    rcases hspk with ⟨w, hw⟩
    refine ⟨w, ?_⟩
    have hcalc2 : (s : Int) * k0 - (lb - la) = (g : Int) * ((sp : Int) * k0 - q) := by
      rw [hscast, ← hgq]
      rw [Int.mul_assoc]
      rw [← Int.mul_sub]
    have hcalc : (la + (s : Int) * k0) - lb = (g : Int) * ((sp : Int) * k0 - q) := by omega
    rw [hcalc, hw]
    rw [htcast]
    rw [Int.mul_assoc]

private theorem intersection_eq_empty_of_gap (a b : Progression)
    (ha : 0 < a.n) (hb : 0 < b.n) (hsa : 0 < a.step) (hsb : 0 < b.step)
    (hgap : (b.lo - a.lo) % (Nat.gcd a.step b.step : Int) != 0) :
    intersection a b = ⟨0, 1, 0⟩ := by
  have hn0a : (a.n == 0) = false := by simp [Nat.ne_of_gt ha]
  have hn0b : (b.n == 0) = false := by simp [Nat.ne_of_gt hb]
  have hs0a : (a.step == 0) = false := by simp [Nat.ne_of_gt hsa]
  have hs0b : (b.step == 0) = false := by simp [Nat.ne_of_gt hsb]
  simp [intersection, hn0a, hn0b, hs0a, hs0b, hgap]

private theorem intersection_eq_formula (a b : Progression)
    (ha : 0 < a.n) (hb : 0 < b.n) (hsa : 0 < a.step) (hsb : 0 < b.step)
    (hgap : (b.lo - a.lo) % (Nat.gcd a.step b.step : Int) == 0) :
    let g := Nat.gcd a.step b.step
    let sp := a.step / g
    let tp := b.step / g
    let period := sp * b.step
    let unit := (bezout sp tp).1
    let q : Int := (b.lo - a.lo) / (g : Int)
    let k0 : Int := (q * unit) % (tp : Int)
    let root : Int := a.lo + (a.step : Int) * k0
    let low := max a.lo b.lo
    let first : Int := low + (root - low) % (period : Int)
    let high := min (lastVal a) (lastVal b)
    intersection a b = ⟨first, period,
      if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ := by
  dsimp only
  let g := Nat.gcd a.step b.step
  let sp := a.step / g
  let tp := b.step / g
  let period := sp * b.step
  let unit := (bezout sp tp).1
  let q : Int := (b.lo - a.lo) / (g : Int)
  let k0 : Int := (q * unit) % (tp : Int)
  let root : Int := a.lo + (a.step : Int) * k0
  let low := max a.lo b.lo
  let first : Int := low + (root - low) % (period : Int)
  let high := min (lastVal a) (lastVal b)
  have hn0a : (a.n == 0) = false := by simp [Nat.ne_of_gt ha]
  have hn0b : (b.n == 0) = false := by simp [Nat.ne_of_gt hb]
  have hs0a : (a.step == 0) = false := by simp [Nat.ne_of_gt hsa]
  have hs0b : (b.step == 0) = false := by simp [Nat.ne_of_gt hsb]
  have hg0 : (b.lo - a.lo) % (g : Int) = 0 := by
    have ht : ((b.lo - a.lo) % (Nat.gcd a.step b.step : Int) == 0) = true := hgap
    have heq := beq_iff_eq.mp ht
    simpa [g] using heq
  have hgap' : ((b.lo - a.lo) % (g : Int) != 0) = false := by simp [hg0]
  simp [intersection, hn0a, hn0b, hs0a, hs0b, hgap', g, sp, tp, period, unit, q, k0, root,
    low, first, high]

private theorem modeq_of_dvd_mod {m1 m2 x y : Int} (hdiv : m1 ∣ m2)
    (h : ModEq m2 x y) : ModEq m1 x y := by
  dsimp [ModEq] at h ⊢
  exact Int.dvd_trans hdiv h

private theorem modeq_lcm (s t : Nat) {v f : Int}
    (h1 : ModEq (s : Int) v f) (h2 : ModEq (t : Int) v f) :
    ModEq (Nat.lcm s t : Int) v f := by
  dsimp [ModEq] at h1 h2 ⊢
  have h1' : s ∣ (v - f).natAbs := (Int.natAbs_dvd_natAbs).mpr h1
  have h2' : t ∣ (v - f).natAbs := (Int.natAbs_dvd_natAbs).mpr h2
  have hl := Nat.lcm_dvd h1' h2'
  exact (Int.natAbs_dvd_natAbs).mp (by simpa using hl)

private theorem modeq_of_contains (p : Progression) (hs : 0 < p.step) {v : Int}
    (hv : contains p v = true) : ModEq (p.step : Int) v p.lo := by
  rcases (contains_iff_index p hs v).mp hv with ⟨k, hk, rfl⟩
  dsimp [ModEq]
  refine ⟨(k : Int), ?_⟩
  rw [Int.mul_comm]
  omega

private theorem contains_of_mod_bounds (p : Progression) (hs : 0 < p.step) (hn : 0 < p.n)
    {v : Int} (hvlo : p.lo ≤ v) (hvhi : v ≤ lastVal p)
    (hm : ModEq (p.step : Int) v p.lo) :
    contains p v = true := by
  have hs0 : (p.step == 0) = false := by simp [Nat.ne_of_gt hs]
  have hmod : (v - p.lo) % (p.step : Int) = 0 := by
    dsimp [ModEq] at hm
    exact Int.emod_eq_zero_of_dvd hm
  have hb : ((v - p.lo) % (p.step : Int) == 0) = true := by rw [hmod]; simp
  simp [contains, hs0, hn, hvlo, hvhi, hb]

private theorem dvd_gap_of_common (a b : Progression) (hsa : 0 < a.step) (hsb : 0 < b.step)
    {v : Int} (hav : contains a v = true) (hbv : contains b v = true) :
    (Nat.gcd a.step b.step : Int) ∣ b.lo - a.lo := by
  rcases (contains_iff_index a hsa v).mp hav with ⟨i, hi, hia⟩
  rcases (contains_iff_index b hsb v).mp hbv with ⟨j, hj, hjb⟩
  let g := Nat.gcd a.step b.step
  have hgda : (g : Int) ∣ (a.step : Int) := by
    rcases Nat.gcd_dvd_left a.step b.step with ⟨u, hu⟩
    refine ⟨(u : Int), ?_⟩
    exact_mod_cast hu
  have hgdb : (g : Int) ∣ (b.step : Int) := by
    rcases Nat.gcd_dvd_right a.step b.step with ⟨u, hu⟩
    refine ⟨(u : Int), ?_⟩
    exact_mod_cast hu
  have hpart1 : (g : Int) ∣ (i : Int) * (a.step : Int) := by
    rcases hgda with ⟨u, hu⟩
    refine ⟨(i : Int) * u, ?_⟩
    rw [hu, ← Int.mul_assoc, Int.mul_comm (i : Int) g, Int.mul_assoc]
  have hpart2 : (g : Int) ∣ (j : Int) * (b.step : Int) := by
    rcases hgdb with ⟨u, hu⟩
    refine ⟨(j : Int) * u, ?_⟩
    rw [hu, ← Int.mul_assoc, Int.mul_comm (j : Int) g, Int.mul_assoc]
  have hgap : b.lo - a.lo = (i : Int) * (a.step : Int) - (j : Int) * (b.step : Int) := by omega
  rw [hgap]
  exact Int.dvd_sub hpart1 hpart2

private theorem period_eq_lcm (a b : Progression) (hsa : 0 < a.step) :
    let g := Nat.gcd a.step b.step
    let sp := a.step / g
    sp * b.step = Nat.lcm a.step b.step := by
  dsimp only
  let g := Nat.gcd a.step b.step
  let sp := a.step / g
  have hgpos : 0 < g := Nat.gcd_pos_of_pos_left b.step hsa
  have hgda : g ∣ a.step := Nat.gcd_dvd_left a.step b.step
  have hgsp : g * sp = a.step := by
    dsimp [sp]
    exact Nat.mul_div_cancel' hgda
  have hleft : g * (sp * b.step) = a.step * b.step := by
    rw [← Nat.mul_assoc, hgsp]
  have hright : g * Nat.lcm a.step b.step = a.step * b.step := Nat.gcd_mul_lcm a.step b.step
  exact Nat.eq_of_mul_eq_mul_left hgpos (by rw [hleft, hright])

private theorem intersection_mem_spec (a b : Progression) (ha : 0 < a.n) (hb : 0 < b.n)
    (hsa : 0 < a.step) (hsb : 0 < b.step) :
    ∀ v : Int, contains (intersection a b) v = true ↔
      (contains a v = true ∧ contains b v = true) := by
  intro v
  let g := Nat.gcd a.step b.step
  by_cases hbad : ((b.lo - a.lo) % (g : Int) != 0) = true
  · have hform := intersection_eq_empty_of_gap a b ha hb hsa hsb (by simpa [g] using hbad)
    rw [hform]
    constructor
    · intro hv
      have hn : ¬ (contains ⟨(0 : Int), 1, 0⟩ v = true) := by simp [contains]
      exact False.elim (hn hv)
    · intro hcommon
      have hgdiv := dvd_gap_of_common a b hsa hsb hcommon.1 hcommon.2
      have hmod0 : (b.lo - a.lo) % (g : Int) = 0 :=
        Int.emod_eq_zero_of_dvd (by simpa [g] using hgdiv)
      have hbadf : ((b.lo - a.lo) % (g : Int) != 0) = false := by simp [hmod0]
      rw [hbadf] at hbad
      cases hbad
  · have hbadf : ((b.lo - a.lo) % (g : Int) != 0) = false := by
      cases h : ((b.lo - a.lo) % (g : Int) != 0) <;> simp_all
    have hz : (b.lo - a.lo) % (g : Int) = 0 := by simpa using hbadf
    have hgapbool : ((b.lo - a.lo) % (g : Int) == 0) = true := by simp [hz]
    have hgdiv : (g : Int) ∣ b.lo - a.lo := Int.dvd_of_emod_eq_zero hz
    let sp := a.step / g
    let tp := b.step / g
    let period := sp * b.step
    let unit := (bezout sp tp).1
    let q : Int := (b.lo - a.lo) / (g : Int)
    let k0 : Int := (q * unit) % (tp : Int)
    let root : Int := a.lo + (a.step : Int) * k0
    let low := max a.lo b.lo
    let first : Int := low + (root - low) % (period : Int)
    let high := min (lastVal a) (lastVal b)
    have hform : intersection a b = ⟨first, period,
        if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ := by
      dsimp [g, sp, tp, period, unit, q, k0, root, low, first, high]
      exact intersection_eq_formula a b ha hb hsa hsb (by simpa [g] using hgapbool)
    rw [hform]
    have hgpos : 0 < g := Nat.gcd_pos_of_pos_left b.step hsa
    have hsppos : 0 < sp := Nat.div_pos (Nat.le_of_dvd hsa (Nat.gcd_dvd_left a.step b.step)) hgpos
    have hperiodpos : 0 < period := Nat.mul_pos hsppos hsb
    have hperiodposI : 0 < (period : Int) := by exact_mod_cast hperiodpos
    have hcrt := crt_root a.step b.step hsa hsb a.lo b.lo (by simpa [g] using hgdiv)
    have hcrt' : ModEq (a.step : Int) root a.lo ∧ ModEq (b.step : Int) root b.lo := by
      simpa [g, sp, tp, q, k0, root, unit] using hcrt
    have hfirstroot : ModEq (period : Int) first root := by
      dsimp [first]
      have hm := modeq_emod (period : Int) (root - low)
      dsimp [ModEq] at hm ⊢
      rcases hm with ⟨k, hk⟩
      refine ⟨-k, ?_⟩
      rw [Int.mul_neg]
      omega
    have hperiod_lcm : period = Nat.lcm a.step b.step := by
      dsimp [period, sp]
      exact period_eq_lcm a b hsa
    have hperiod_dvd_a : (a.step : Int) ∣ (period : Int) := by
      rw [hperiod_lcm]
      exact_mod_cast Nat.dvd_lcm_left a.step b.step
    have hperiod_dvd_b : (b.step : Int) ∣ (period : Int) := by
      rw [hperiod_lcm]
      exact_mod_cast Nat.dvd_lcm_right a.step b.step
    have hfirst_a : ModEq (a.step : Int) first a.lo :=
      modeq_trans (modeq_of_dvd_mod hperiod_dvd_a hfirstroot) hcrt'.1
    have hfirst_b : ModEq (b.step : Int) first b.lo :=
      modeq_trans (modeq_of_dvd_mod hperiod_dvd_b hfirstroot) hcrt'.2
    have hfirstlo : low ≤ first := by
      dsimp [first]
      have hn := Int.emod_nonneg (root - low) (by omega : (period : Int) ≠ 0)
      omega
    have hfirstlt : first < low + (period : Int) := by
      dsimp [first]
      have hlt := Int.emod_lt_of_pos (root - low) hperiodposI
      omega
    have common_facts : ∀ {w : Int}, contains a w = true → contains b w = true →
        low ≤ w ∧ w ≤ high ∧ ModEq (period : Int) w first ∧ first ≤ w := by
      intro w hav hbv
      have hva := modeq_of_contains a hsa hav
      have hvb := modeq_of_contains b hsb hbv
      have hvfirst_a : ModEq (a.step : Int) w first := modeq_trans hva (modeq_symm hfirst_a)
      have hvfirst_b : ModEq (b.step : Int) w first := modeq_trans hvb (modeq_symm hfirst_b)
      have hvlcm := modeq_lcm a.step b.step hvfirst_a hvfirst_b
      have hvperiod : ModEq (period : Int) w first := by
        rw [hperiod_lcm]
        exact hvlcm
      have hvr : ModEq (period : Int) w root := modeq_trans hvperiod hfirstroot
      have hvlow : low ≤ w := by
        dsimp [low]
        rcases (contains_iff_index a hsa w).mp hav with ⟨i, hi, hiw⟩
        have hsa_nonneg : 0 ≤ (a.step : Int) := by exact_mod_cast (Nat.le_of_lt hsa)
        have hva_nonneg : 0 ≤ (i : Int) * (a.step : Int) :=
          Int.mul_nonneg (by omega) hsa_nonneg
        have hb_lo : b.lo ≤ w := by
          rcases (contains_iff_index b hsb w).mp hbv with ⟨j, hj, hjw⟩
          have hsb_nonneg : 0 ≤ (b.step : Int) := by exact_mod_cast (Nat.le_of_lt hsb)
          have hnonneg_b : 0 ≤ (j : Int) * (b.step : Int) :=
            Int.mul_nonneg (by omega) hsb_nonneg
          omega
        omega
      have hvhigh : w ≤ high := by
        dsimp [high]
        rcases (contains_iff_index a hsa w).mp hav with ⟨i, hi, hiw⟩
        rcases (contains_iff_index b hsb w).mp hbv with ⟨j, hj, hjw⟩
        have hlasta : a.lo + (i : Int) * (a.step : Int) ≤ lastVal a := by
          have hi_le : i ≤ a.n - 1 := by omega
          have hicast : (i : Int) ≤ ((a.n - 1 : Nat) : Int) := by exact_mod_cast hi_le
          have hsa_nonneg : 0 ≤ (a.step : Int) := by exact_mod_cast (Nat.le_of_lt hsa)
          have hm := Int.mul_le_mul_of_nonneg_right hicast hsa_nonneg
          have hh : a.lo + (i : Int) * (a.step : Int) ≤
              a.lo + ((a.n - 1 : Nat) : Int) * (a.step : Int) := by omega
          simpa [lastVal] using hh
        have hlastb : b.lo + (j : Int) * (b.step : Int) ≤ lastVal b := by
          have hj_le : j ≤ b.n - 1 := by omega
          have hjcast : (j : Int) ≤ ((b.n - 1 : Nat) : Int) := by exact_mod_cast hj_le
          have hsb_nonneg : 0 ≤ (b.step : Int) := by exact_mod_cast (Nat.le_of_lt hsb)
          have hm := Int.mul_le_mul_of_nonneg_right hjcast hsb_nonneg
          have hh : b.lo + (j : Int) * (b.step : Int) ≤
              b.lo + ((b.n - 1 : Nat) : Int) * (b.step : Int) := by omega
          simpa [lastVal] using hh
        omega
      have hemodeq : (w - low) % (period : Int) = (root - low) % (period : Int) := by
        have hvm := modeq_add (m := (period : Int)) hvr (c := -low)
        exact emod_eq_of_modeq hvm
      have hfirstle : first ≤ w := by
        dsimp [first]
        have hwnonneg : 0 ≤ w - low := by omega
        have hdecomp := Int.ediv_mul_add_emod (w - low) (period : Int)
        have hnonnegmod : 0 ≤ (w - low) % (period : Int) := Int.emod_nonneg _ (by omega)
        have hqnonneg : 0 ≤ (w - low) / (period : Int) :=
          Int.ediv_nonneg hwnonneg (by omega)
        have hprodnonneg : 0 ≤ ((w - low) / (period : Int)) * (period : Int) :=
          Int.mul_nonneg hqnonneg (by omega)
        omega
      exact ⟨hvlow, hvhigh, hvperiod, hfirstle⟩
    by_cases hfh : first ≤ high
    · rw [if_pos hfh]
      let H := ((high - first) / (period : Int)).toNat
      change contains ⟨first, period, H + 1⟩ v = true ↔
        (contains a v = true ∧ contains b v = true)
      have hHnonneg : 0 ≤ (high - first) / (period : Int) :=
        Int.ediv_nonneg (by omega) (by omega)
      have hHcast : (H : Int) = (high - first) / (period : Int) := Int.toNat_of_nonneg hHnonneg
      have hHbound : first + (H : Int) * (period : Int) ≤ high := by
        have hdecomp := Int.ediv_mul_add_emod (high - first) (period : Int)
        have hrem : 0 ≤ (high - first) % (period : Int) := Int.emod_nonneg _ (by omega)
        rw [hHcast]
        omega
      constructor
      · intro hvR
        rcases (contains_iff_index ⟨first, period, H + 1⟩ (by exact_mod_cast hperiodpos) v).mp hvR with
          ⟨k, hk, hvk⟩
        have hk_le : k ≤ H := Nat.le_of_lt_succ hk
        have hvlo : low ≤ v := by
          rw [hvk]
          have hkprod : 0 ≤ (k : Int) * (period : Int) :=
            Int.mul_nonneg (by omega) (by omega)
          calc
            low ≤ first := hfirstlo
            _ ≤ first + (k : Int) * (period : Int) := Int.le_add_of_nonneg_right hkprod
        have hvhi : v ≤ high := by
          rw [hvk]
          have hkcast : (k : Int) ≤ (H : Int) := by exact_mod_cast hk_le
          have hm := Int.mul_le_mul_of_nonneg_right hkcast (by omega : 0 ≤ (period : Int))
          calc
            first + (k : Int) * (period : Int) ≤ first + (H : Int) * (period : Int) := by omega
            _ ≤ high := hHbound
        have hvperiod' : ModEq (period : Int) v first := by
          dsimp [ModEq]
          refine ⟨(k : Int), ?_⟩
          have hvk' : v = first + (period : Int) * (k : Int) := by
            rw [hvk]
            exact congrArg (fun z : Int => first + z) (Int.mul_comm (k : Int) (period : Int))
          rw [hvk']
          omega
        have hva : ModEq (a.step : Int) v a.lo :=
          modeq_trans (modeq_of_dvd_mod hperiod_dvd_a hvperiod') hfirst_a
        have hvb : ModEq (b.step : Int) v b.lo :=
          modeq_trans (modeq_of_dvd_mod hperiod_dvd_b hvperiod') hfirst_b
        have hvalasta : v ≤ lastVal a := by
          have hm : min (lastVal a) (lastVal b) ≤ lastVal a := Int.min_le_left _ _
          dsimp [high] at hvhi
          omega
        have hvalastb : v ≤ lastVal b := by
          have hm : min (lastVal a) (lastVal b) ≤ lastVal b := Int.min_le_right _ _
          dsimp [high] at hvhi
          omega
        have hva_lo : a.lo ≤ v := by dsimp [low] at hvlo; omega
        have hvb_lo : b.lo ≤ v := by dsimp [low] at hvlo; omega
        exact ⟨contains_of_mod_bounds a hsa ha hva_lo hvalasta hva,
          contains_of_mod_bounds b hsb hb hvb_lo hvalastb hvb⟩
      · intro hcommon
        rcases common_facts hcommon.1 hcommon.2 with ⟨hvlow, hvhigh, hvperiod, hfirstle⟩
        have hvfirstnonneg : 0 ≤ v - first := by omega
        have hvmod0 : (v - first) % (period : Int) = 0 := by
          dsimp [ModEq] at hvperiod
          exact Int.emod_eq_zero_of_dvd hvperiod
        let kk : Int := (v - first) / (period : Int)
        have hkknonneg : 0 ≤ kk := by
          dsimp [kk]
          exact Int.ediv_nonneg hvfirstnonneg (by omega)
        have hkkmul : kk * (period : Int) = v - first := by
          dsimp [kk]
          have htmp := Int.ediv_mul_add_emod (v - first) (period : Int)
          omega
        have hkkle : kk ≤ (high - first) / (period : Int) := by
          dsimp [kk]
          exact Int.ediv_le_ediv hperiodposI (by omega)
        have hkkNat : kk.toNat < H + 1 := by
          have hleH : kk.toNat ≤ H := by
            exact (Int.toNat_le).mpr (by rw [hHcast]; exact hkkle)
          omega
        apply (contains_iff_index ⟨first, period, H + 1⟩ (by exact_mod_cast hperiodpos) v).mpr
        refine ⟨kk.toNat, hkkNat, ?_⟩
        change v = first + (kk.toNat : Int) * (period : Int)
        have hcast : (kk.toNat : Int) = kk := Int.toNat_of_nonneg hkknonneg
        rw [hcast]
        have hrev : v = first + (v - first) := by omega
        rw [hrev, ← hkkmul]
    · rw [if_neg hfh]
      constructor
      · intro hv
        have hn : ¬ (contains ⟨first, period, 0⟩ v = true) := by simp [contains]
        exact False.elim (hn hv)
      · intro hcommon
        rcases common_facts hcommon.1 hcommon.2 with ⟨hvlow, hvhigh, hvperiod, hfirstle⟩
        omega

private theorem intersection_step_pos (p q : Progression) (hp : 0 < p.step) (hq : 0 < q.step) :
    0 < (intersection p q).step := by
  by_cases hpn : p.n = 0
  · simp [intersection, hpn]
  · by_cases hqn : q.n = 0
    · simp [intersection, hpn, hqn]
    · have hpn0 : (p.n == 0) = false := by simp [hpn]
      have hqn0 : (q.n == 0) = false := by simp [hqn]
      have hps0 : (p.step == 0) = false := by simp [Nat.ne_of_gt hp]
      have hqs0 : (q.step == 0) = false := by simp [Nat.ne_of_gt hq]
      by_cases hgapbad : ((q.lo - p.lo) % (Nat.gcd p.step q.step : Int) != 0) = true
      · simp [intersection, hpn0, hqn0, hps0, hqs0, hgapbad]
      · have hgapfalse : ((q.lo - p.lo) % (Nat.gcd p.step q.step : Int) != 0) = false := by
          cases h : ((q.lo - p.lo) % (Nat.gcd p.step q.step : Int) != 0) <;> simp_all
        have hz : (q.lo - p.lo) % (Nat.gcd p.step q.step : Int) = 0 := by simpa using hgapfalse
        have hgapbool : ((q.lo - p.lo) % (Nat.gcd p.step q.step : Int) == 0) = true := by simp [hz]
        have hform := intersection_eq_formula p q (Nat.pos_of_ne_zero hpn) (Nat.pos_of_ne_zero hqn)
          hp hq hgapbool
        rw [hform]
        dsimp only
        let g := Nat.gcd p.step q.step
        have hgpos : 0 < g := Nat.gcd_pos_of_pos_left q.step hp
        have hsppos : 0 < p.step / g :=
          Nat.div_pos (Nat.le_of_dvd hp (Nat.gcd_dvd_left p.step q.step)) hgpos
        exact Nat.mul_pos hsppos hq

private theorem nodup_map_of_injective {α β : Type} (f : α → β) (xs : List α)
    (hf : Function.Injective f) (hd : xs.Nodup) : (xs.map f).Nodup := by
  exact List.Pairwise.map f (fun a b hne heq => hne (hf heq)) hd

private theorem sum_if_eq_countP (xs : List Nat) (f : Nat → Int) (g : Int → Bool) :
    (xs.map (fun k => if g (f k) then 1 else 0)).sum = (xs.map f).countP g := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases h : g (f x)
      · simp [h, ih]
        omega
      · simp [h, ih]

private theorem prog_value_injective (p : Progression) (hp : 0 < p.step) :
    Function.Injective (fun k : Nat => p.lo + (k : Int) * (p.step : Int)) := by
  intro a b heq
  have hsne : (p.step : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hp)
  have hmul : (a : Int) * (p.step : Int) = (b : Int) * (p.step : Int) := Int.add_left_cancel heq
  have hcast : (a : Int) = (b : Int) := Int.eq_of_mul_eq_mul_right hsne hmul
  exact_mod_cast hcast

private theorem count_common_prog (p q : Progression) (hp : 0 < p.step) (hq : 0 < q.step)
    (hpn : 0 < p.n) (hqn : 0 < q.n) :
    ((List.range p.n).map (fun (k : Nat) => if contains q (p.lo + (k : Int) * (p.step : Int)) then 1 else 0)).sum =
      (intersection p q).n := by
  let r := intersection p q
  let lp : List Int := ((List.range p.n).map (fun (k : Nat) => p.lo + (k : Int) * (p.step : Int))).filter
    (fun v => contains q v)
  let lr : List Int := (List.range r.n).map (fun (k : Nat) => r.lo + (k : Int) * (r.step : Int))
  have hrstep : 0 < r.step := by dsimp [r]; exact intersection_step_pos p q hp hq
  have hlp : lp.Nodup := by
    dsimp [lp]
    exact List.Pairwise.sublist List.filter_sublist
      (nodup_map_of_injective (fun k : Nat => p.lo + (k : Int) * (p.step : Int))
        (List.range p.n) (prog_value_injective p hp) List.nodup_range)
  have hlr : lr.Nodup := by
    dsimp [lr]
    exact nodup_map_of_injective (fun k : Nat => r.lo + (k : Int) * (r.step : Int))
      (List.range r.n) (prog_value_injective r hrstep) List.nodup_range
  have hmem : ∀ v : Int, v ∈ lp ↔ v ∈ lr := by
    intro v
    dsimp [lp, lr]
    constructor
    · intro hv
      rw [List.mem_filter] at hv
      rcases hv with ⟨hvmem, hvq⟩
      rcases List.mem_map.mp hvmem with ⟨k, hk, rfl⟩
      have hklt : k < p.n := by simpa using hk
      have hvp : contains p (p.lo + (k : Int) * (p.step : Int)) = true :=
        (contains_iff_index p hp _).mpr ⟨k, hklt, rfl⟩
      have hvcommon : contains r (p.lo + (k : Int) * (p.step : Int)) = true :=
        (intersection_mem_spec p q hpn hqn hp hq _).mpr ⟨hvp, hvq⟩
      rcases (contains_iff_index r hrstep _).mp hvcommon with ⟨k', hk', hvk'⟩
      apply List.mem_map.mpr
      refine ⟨k', by simpa using hk', ?_⟩
      exact hvk'.symm
    · intro hv
      rcases List.mem_map.mp hv with ⟨k', hk', hvk'⟩
      have hk'lt : k' < r.n := by simpa using hk'
      have hvr : contains r v = true :=
        (contains_iff_index r hrstep v).mpr ⟨k', hk'lt, hvk'.symm⟩
      rcases (intersection_mem_spec p q hpn hqn hp hq v).mp hvr with ⟨hvp, hvq⟩
      rw [List.mem_filter]
      constructor
      · rcases (contains_iff_index p hp v).mp hvp with ⟨k, hk, hvk⟩
        apply List.mem_map.mpr
        refine ⟨k, by simpa using hk, hvk.symm⟩
      · exact hvq
  have hperm : lp.Perm lr := (List.perm_ext_iff_of_nodup hlp hlr).mpr hmem
  have hlen := hperm.length_eq
  have hsum : ((List.range p.n).map (fun (k : Nat) => if contains q (p.lo + (k : Int) * (p.step : Int)) then 1 else 0)).sum =
      lp.length := by
    rw [sum_if_eq_countP]
    dsimp [lp]
    rw [List.countP_eq_length_filter]
  rw [hsum]
  dsimp [lr] at hlen
  simpa using hlen

private theorem lane_values_idx (a : Lane) (ha : a.step ≠ 0) (i : Nat) (hi : i < a.n) :
    ∃ k : Nat, k < a.n ∧
      a.base + a.step * (i : Int) = (values a).lo + (k : Int) * ((values a).step : Int) := by
  by_cases hneg : a.step < 0
  · have habs : ((a.step.natAbs : Nat) : Int) = -a.step := by
      rcases Int.natAbs_eq a.step with h | h
      · omega
      · omega
    refine ⟨a.n - 1 - i, by omega, ?_⟩
    dsimp [values]
    simp [hneg]
    have hle : i ≤ a.n - 1 := by omega
    have hcast : (((a.n - 1 - i : Nat) : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (i : Int) :=
      Int.ofNat_sub hle
    rw [hcast, habs, Int.mul_neg, Int.sub_mul]
    rw [Int.mul_comm (i : Int) a.step]
    omega
  · have habs : ((a.step.natAbs : Nat) : Int) = a.step := Int.natAbs_of_nonneg (by omega)
    refine ⟨i, hi, ?_⟩
    dsimp [values]
    simp [hneg, habs, Int.mul_comm]

private theorem values_lane_idx (a : Lane) (ha : a.step ≠ 0) (k : Nat) (hk : k < a.n) :
    ∃ i : Nat, i < a.n ∧
      (values a).lo + (k : Int) * ((values a).step : Int) = a.base + a.step * (i : Int) := by
  by_cases hneg : a.step < 0
  · have habs : ((a.step.natAbs : Nat) : Int) = -a.step := by
      rcases Int.natAbs_eq a.step with h | h
      · omega
      · omega
    refine ⟨a.n - 1 - k, by omega, ?_⟩
    dsimp [values]
    simp [hneg]
    have hle : k ≤ a.n - 1 := by omega
    have hcast : (((a.n - 1 - k : Nat) : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (k : Int) :=
      Int.ofNat_sub hle
    rw [hcast, habs, Int.mul_neg, Int.mul_sub]
    have h1 : ((a.n - 1 : Nat) : Int) * a.step = a.step * ((a.n - 1 : Nat) : Int) := Int.mul_comm _ _
    have h2 : (k : Int) * a.step = a.step * (k : Int) := Int.mul_comm _ _
    omega
  · have habs : ((a.step.natAbs : Nat) : Int) = a.step := Int.natAbs_of_nonneg (by omega)
    refine ⟨k, hk, ?_⟩
    dsimp [values]
    simp [hneg, habs, Int.mul_comm]

private theorem lane_value_injective (a : Lane) (ha : a.step ≠ 0) :
    Function.Injective (fun i : Nat => a.base + a.step * (i : Int)) := by
  intro i j heq
  have hsne : (a.step : Int) ≠ 0 := by exact_mod_cast ha
  have hmul : (a.step : Int) * (i : Int) = (a.step : Int) * (j : Int) := Int.add_left_cancel heq
  have hcast : (i : Int) = (j : Int) := Int.eq_of_mul_eq_mul_left hsne hmul
  exact_mod_cast hcast

private theorem lane_values_perm (a : Lane) (ha : a.step ≠ 0) :
    ((List.range a.n).map (fun (i : Nat) => a.base + a.step * (i : Int))).Perm
      ((List.range (values a).n).map
        (fun (k : Nat) => (values a).lo + (k : Int) * ((values a).step : Int))) := by
  have hstep : 0 < (values a).step := by
    dsimp [values]
    exact Int.natAbs_pos.mpr ha
  have hd1 : ((List.range a.n).map (fun (i : Nat) => a.base + a.step * (i : Int))).Nodup :=
    nodup_map_of_injective _ _ (lane_value_injective a ha) List.nodup_range
  have hd2 : ((List.range (values a).n).map
      (fun (k : Nat) => (values a).lo + (k : Int) * ((values a).step : Int))).Nodup :=
    nodup_map_of_injective _ _ (prog_value_injective (values a) hstep) List.nodup_range
  apply (List.perm_ext_iff_of_nodup hd1 hd2).mpr
  intro v
  constructor
  · intro hv
    rcases List.mem_map.mp hv with ⟨i, hi, rfl⟩
    have hilt : i < a.n := by simpa using hi
    rcases lane_values_idx a ha i hilt with ⟨k, hk, hval⟩
    apply List.mem_map.mpr
    refine ⟨k, by simpa [values] using hk, hval.symm⟩
  · intro hv
    rcases List.mem_map.mp hv with ⟨k, hk, hvk⟩
    have hklt : k < a.n := by simpa [values] using hk
    rcases values_lane_idx a ha k hklt with ⟨i, hi, hval⟩
    apply List.mem_map.mpr
    refine ⟨i, by simpa using hi, ?_⟩
    rw [← hvk, hval]

private theorem count_lane_eq_count_prog (a : Lane) (ha : a.step ≠ 0) (q : Progression) :
    ((List.range a.n).map (fun (i : Nat) => if contains q (a.base + a.step * (i : Int)) then 1 else 0)).sum =
      ((List.range (values a).n).map
        (fun (k : Nat) => if contains q ((values a).lo + (k : Int) * ((values a).step : Int)) then 1 else 0)).sum := by
  have hperm := lane_values_perm a ha
  have hc := List.Perm.countP_eq (fun v : Int => contains q v) hperm
  rw [sum_if_eq_countP, sum_if_eq_countP]
  exact hc

private theorem countP_range_eq_one_of_inj (f : Nat → Int) (hf : Function.Injective f)
    (n : Nat) (v : Int) :
    (List.range n).countP (fun i => f i == v) = if ∃ i, i < n ∧ f i = v then 1 else 0 := by
  classical
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.countP_append, ih]
      by_cases hex : ∃ i, i < n ∧ f i = v
      · have hnot : f n ≠ v := by
          intro hfv
          rcases hex with ⟨i, hi, hiv⟩
          have hij : i = n := hf (by rw [hiv, hfv])
          omega
        have hb : (f n == v) = false := by simp [hnot]
        have hex' : ∃ i, i < n + 1 ∧ f i = v := by
          rcases hex with ⟨i, hi, hiv⟩
          exact ⟨i, by omega, hiv⟩
        simp [hex, hex', hb]
      · by_cases hfv : f n = v
        · have hb : (f n == v) = true := by simp [hfv]
          have hex' : ∃ i, i < n + 1 ∧ f i = v := ⟨n, by omega, hfv⟩
          simp [hex, hex', hb]
        · have hb : (f n == v) = false := by simp [hfv]
          have hnone : ¬ ∃ i, i < n + 1 ∧ f i = v := by
            rintro ⟨i, hi, hiv⟩
            by_cases hin : i < n
            · exact hex ⟨i, hin, hiv⟩
            · have hin' : i = n := by omega
              exact hfv (by simpa [hin'] using hiv)
          simp [hex, hnone, hb]

private theorem countP_range_const (n : Nat) (b : Bool) :
    (List.range n).countP (fun _ => b) = if b then n else 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.countP_append, ih]
      by_cases h : b <;> simp [h] <;> omega

private theorem count_value_lane (a : Lane) (v : Int) :
    (List.range a.n).countP (fun i : Nat => a.base + a.step * (i : Int) == v) =
      if a.step == 0 then (if a.base == v then a.n else 0)
      else (if contains (values a) v then 1 else 0) := by
  classical
  by_cases ha0 : a.step = 0
  · have hb : (a.step == 0) = true := by simp [ha0]
    rw [hb]
    rw [show (fun i : Nat => a.base + a.step * (i : Int) == v) = (fun _ : Nat => a.base == v) by
      funext i; simp [ha0]]
    rw [countP_range_const]
    by_cases heq : a.base = v <;> simp [heq]
  · have hb : (a.step == 0) = false := by simp [ha0]
    rw [hb]
    rw [countP_range_eq_one_of_inj (fun i : Nat => a.base + a.step * (i : Int))
      (lane_value_injective a ha0)]
    dsimp
    have hstep : 0 < (values a).step := by
      dsimp [values]
      exact Int.natAbs_pos.mpr ha0
    have hiff : (∃ i, i < a.n ∧ a.base + a.step * (i : Int) = v) ↔
        contains (values a) v = true := by
      constructor
      · rintro ⟨i, hi, hiv⟩
        rcases lane_values_idx a ha0 i hi with ⟨k, hk, hval⟩
        have hvprog : v = (values a).lo + (k : Int) * ((values a).step : Int) := by omega
        exact (contains_iff_index (values a) hstep v).mpr ⟨k, hk, hvprog⟩
      · intro hv
        rcases (contains_iff_index (values a) hstep v).mp hv with ⟨k, hk, hvk⟩
        rcases values_lane_idx a ha0 k hk with ⟨i, hi, hval⟩
        refine ⟨i, hi, ?_⟩
        rw [hval] at hvk
        exact hvk.symm
    by_cases hex : ∃ i, i < a.n ∧ a.base + a.step * (i : Int) = v
    · have hc : contains (values a) v = true := hiff.mp hex
      simp [hex, hc]
    · have hc : ¬ contains (values a) v = true := by
        intro h; exact hex (hiff.mpr h)
      simp [hex, hc]

private theorem sum_range_const_int (n : Nat) (v : Int) :
    ((List.range n).map (fun _ => v)).sum = (n : Int) * v := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      rw [ih]
      rw [show (Nat.succ n : Int) = (n : Int) + 1 by omega, Int.add_mul]
      omega

private theorem sum_ite_nat_as_int (xs : List Nat) (p : Nat → Bool) (v : Nat) :
    (xs.map (fun x => if p x then (v : Int) else 0)).sum = (v : Int) * (xs.countP p : Int) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x
      · simp [h, ih]
        rw [Int.mul_add, Int.mul_one]
        omega
      · simp [h, ih]

private theorem sum_if_int_eq_nat (xs : List Nat) (p : Nat → Bool) :
    (xs.map (fun x => if p x then (1 : Int) else 0)).sum =
      ((xs.map (fun x => if p x then 1 else 0)).sum : Nat) := by
  induction xs with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem sum_map_zero_int (l : List Nat) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem lane_join (a b : Lane) :
    ((List.range a.n).map (fun (i : Nat) => Int.ofNat ((List.range b.n).countP
      (fun (j : Nat) => a.base + a.step * (i : Int) == b.base + b.step * (j : Int))))).sum =
    joinCount a b := by
  classical
  by_cases han : a.n = 0
  · simp [han, joinCount, intersection, values]
  by_cases hbn : b.n = 0
  · simp [hbn, joinCount, intersection, values]
    apply sum_map_zero_int
  have hanpos : 0 < a.n := Nat.pos_of_ne_zero han
  have hbnpos : 0 < b.n := Nat.pos_of_ne_zero hbn
  have han0 : (a.n == 0) = false := by simp [han]
  have hbn0 : (b.n == 0) = false := by simp [hbn]
  by_cases ha0 : a.step = 0
  · have has0 : (a.step == 0) = true := by simp [ha0]
    simp only [ha0, Int.zero_mul, Int.add_zero]
    let cnt : Nat := (List.range b.n).countP (fun (j : Nat) =>
      b.base + b.step * (j : Int) == a.base)
    have hrev : (List.range b.n).countP (fun (j : Nat) =>
        a.base == b.base + b.step * (j : Int)) = cnt := by
      dsimp [cnt]
      rw [show (fun (j : Nat) => a.base == b.base + b.step * (j : Int)) =
        (fun (j : Nat) => b.base + b.step * (j : Int) == a.base) by
          funext j; exact Bool.beq_comm]
    rw [hrev]
    rw [show ((List.range a.n).map (fun (_ : Nat) => Int.ofNat cnt)).sum = (a.n : Int) * (cnt : Int) by
      change ((List.range a.n).map (fun (_ : Nat) => (cnt : Int))).sum = (a.n : Int) * (cnt : Int)
      exact sum_range_const_int a.n (cnt : Int)]
    have hcnt : cnt = if b.step == 0 then (if b.base == a.base then b.n else 0)
        else (if contains (values b) a.base then 1 else 0) := by
      dsimp [cnt]
      simpa using count_value_lane b a.base
    rw [hcnt]
    have hinter : (intersection (values a) (values b)).n =
        if contains (values b) a.base then 1 else 0 := by
      dsimp [values]
      simp [intersection, han0, hbn0, ha0]
      rfl
    dsimp [joinCount]
    rw [hinter]
    simp [ha0]
    by_cases hb0 : b.step = 0
    · simp [hb0]
      by_cases heq : b.base = a.base
      · have hc : contains (values b) a.base = true := by
          dsimp [values]
          rw [hb0]
          exact (contains_zero ⟨b.base, 0, b.n⟩ a.base).mpr ⟨hbnpos, heq.symm⟩
        simp [heq, hc]
      · have hne : b.base ≠ a.base := heq
        have hc : ¬ contains (values b) a.base = true := by
          intro h
          have hh := (contains_zero ⟨b.base, 0, b.n⟩ a.base).mp (by simpa [values, hb0] using h)
          exact hne hh.2.symm
        simp [heq, hc]
    · simp [hb0, Int.mul_comm]
  · by_cases hb0' : b.step == 0
    · have hb0 : b.step = 0 := beq_iff_eq.mp hb0'
      simp only [hb0, Int.zero_mul, Int.add_zero]
      let cnt : Nat := (List.range a.n).countP (fun (i : Nat) =>
        a.base + a.step * (i : Int) == b.base)
      have hrev : (List.range a.n).countP (fun (i : Nat) =>
          a.base + a.step * (i : Int) == b.base) = cnt := by rfl
      have hcnt : cnt = if contains (values a) b.base then 1 else 0 := by
        dsimp [cnt]
        rw [count_value_lane a b.base]
        simp [ha0]
      have hper (i : Nat) : (List.range b.n).countP (fun (_ : Nat) =>
          a.base + a.step * (i : Int) == b.base) =
          if a.base + a.step * (i : Int) == b.base then b.n else 0 := by
        rw [countP_range_const]
      have hsum : ((List.range a.n).map (fun (i : Nat) =>
          Int.ofNat ((List.range b.n).countP (fun (_ : Nat) =>
            a.base + a.step * (i : Int) == b.base)))).sum =
          (b.n : Int) * (cnt : Int) := by
        rw [show ((List.range a.n).map (fun (i : Nat) =>
            Int.ofNat ((List.range b.n).countP (fun (_ : Nat) =>
              a.base + a.step * (i : Int) == b.base)))) =
            ((List.range a.n).map (fun (i : Nat) =>
              if a.base + a.step * (i : Int) == b.base then (b.n : Int) else 0)) by
              apply List.map_congr_left
              intro i hi
              by_cases hp : a.base + a.step * (i : Int) == b.base
              · simp [hp, hper i]
              · simp [hp, hper i]]
        rw [sum_ite_nat_as_int]
      rw [hsum, hcnt]
      have hinter : (intersection (values a) (values b)).n =
          if contains (values a) b.base then 1 else 0 := by
        dsimp [values]
        rw [hb0]
        simp [intersection, han0, hbn0, ha0]
        rfl
      dsimp [joinCount]
      rw [hinter]
      simp [ha0, hb0]
      rw [Int.mul_comm]
    · have hcnt (i : Nat) : (List.range b.n).countP (fun (j : Nat) =>
          a.base + a.step * (i : Int) == b.base + b.step * (j : Int)) =
          if contains (values b) (a.base + a.step * (i : Int)) then 1 else 0 := by
        rw [show (fun (j : Nat) => a.base + a.step * (i : Int) == b.base + b.step * (j : Int)) =
          (fun (j : Nat) => b.base + b.step * (j : Int) == a.base + a.step * (i : Int)) by
            funext j; exact Bool.beq_comm]
        rw [count_value_lane b (a.base + a.step * (i : Int))]
        simp [hb0']
      have hsum : ((List.range a.n).map (fun (i : Nat) => Int.ofNat ((List.range b.n).countP
          (fun (j : Nat) => a.base + a.step * (i : Int) == b.base + b.step * (j : Int))))).sum =
          ((List.range a.n).map (fun (i : Nat) =>
            if contains (values b) (a.base + a.step * (i : Int)) then 1 else 0)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro i hi
        by_cases h : contains (values b) (a.base + a.step * (i : Int)) = true
        · simp [h, hcnt i]
        · simp [h, hcnt i]
      rw [hsum, sum_if_int_eq_nat, count_lane_eq_count_prog a ha0 (values b)]
      have hp : 0 < (values a).step := by dsimp [values]; exact Int.natAbs_pos.mpr ha0
      have hbne : b.step ≠ 0 := by simpa using hb0'
      have hq : 0 < (values b).step := by dsimp [values]; exact Int.natAbs_pos.mpr hbne
      rw [count_common_prog (values a) (values b) hp hq (by simpa [values] using hanpos)
        (by simpa [values] using hbnpos)]
      dsimp [joinCount]
      rw [show (if a.step == 0 then a.n else 1) = 1 by simp [ha0],
        show (if b.step == 0 then b.n else 1) = 1 by simp [hb0']]
      simp

private theorem range_split (m n : Nat) (h : m ≤ n) :
    List.range n = List.range m ++ List.map (fun x : Nat => m + x) (List.range (n - m)) := by
  have hlen : m + (n - m) = n := Nat.add_sub_of_le h
  exact (congrArg List.range hlen.symm).trans List.range_add

private theorem filterMap_some_eq_map {α β : Type} (f : α → β) (l : List α) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem rows_saturating_split_le (c : Config) (h : c.split ≤ c.n) :
    rows layout c =
      (List.range c.split).map (fun (i : Nat) => (i, c.base + c.step * (i : Int))) ++
      (List.range (c.n - c.split)).map (fun (k : Nat) =>
        (c.split + k, c.base + c.step * (c.split : Int))) := by
  unfold rows
  simp only [layout, List.filterMap_cons, List.filterMap_nil]
  rw [show (fun i : Nat => Option.map (fun x => (i, x))
      (if i < c.split then some (c.base + c.step * (i : Int))
       else some (c.base + c.step * (c.split : Int)))) =
      (fun i : Nat => some (i, if i < c.split then c.base + c.step * (i : Int)
        else c.base + c.step * (c.split : Int))) by
        funext i
        by_cases hi : i < c.split <;> simp [hi]]
  rw [filterMap_some_eq_map]
  have hrange : List.range c.n = (List.range c.split) ++
      (List.map (fun x : Nat => c.split + x) (List.range (c.n - c.split))) :=
    range_split c.split c.n h
  rw [hrange, List.map_append]
  congr 1
  · apply List.map_congr_left
    intro i hi
    have hilt : i < c.split := by simpa using hi
    simp [hilt]
  · rw [List.map_map]
    apply List.map_congr_left
    intro k hk
    have hklt : k < c.n - c.split := by simpa using hk
    have hge : c.split ≤ c.split + k := by omega
    have hnot : ¬ c.split + k < c.split := by omega
    simp [hnot]

private theorem rows_saturating_split_gt (c : Config) (h : c.n < c.split) :
    rows layout c = (List.range c.n).map (fun (i : Nat) => (i, c.base + c.step * (i : Int))) := by
  unfold rows
  simp only [layout, List.filterMap_cons, List.filterMap_nil]
  rw [show (fun i : Nat => Option.map (fun x => (i, x))
      (if i < c.split then some (c.base + c.step * (i : Int))
       else some (c.base + c.step * (c.split : Int)))) =
      (fun i : Nat => some (i, if i < c.split then c.base + c.step * (i : Int)
        else c.base + c.step * (c.split : Int))) by
        funext i
        by_cases hi : i < c.split <;> simp [hi]]
  rw [filterMap_some_eq_map]
  apply List.map_congr_left
  intro i hi
  have hilt : i < c.split := by
    have hi' := List.mem_range.mp hi
    omega
  simp [hilt]

private theorem sum_ofNat_map {α : Type} (f : α → Nat) (l : List α) :
    (l.map (fun x => Int.ofNat (f x))).sum = Int.ofNat ((l.map f).sum) := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      simp

private theorem map_countP_comp (l : List Nat) (g : Nat → Nat × Int) (b : Lane) :
    l.map ((fun r : Nat × Int => Int.ofNat ((List.range b.n).countP
      (fun (j : Nat) => r.2 == b.base + b.step * (j : Int)))) ∘ g) =
    l.map (fun i => Int.ofNat ((List.range b.n).countP
      (fun (j : Nat) => (g i).2 == b.base + b.step * (j : Int)))) := by
  apply List.map_congr_left
  intro i hi
  rfl

theorem sound_SaturatingCounter_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SaturatingCounter.ResidueJoin.Goal := by
  intro c b
  classical
  have hInt : ((rows layout c).map (fun r => Int.ofNat ((List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base + b.step * (i : Int))))).sum =
      ((layout.lanes c).map (fun a => Int.ofNat (joinCount a b))).sum := by
    by_cases h : c.split ≤ c.n
    · rw [rows_saturating_split_le c h]
      simp only [List.map_append, List.sum_append]
      rw [List.map_map, List.map_map]
      rw [map_countP_comp (List.range c.split) (fun i => (i, c.base + c.step * (i : Int))) b]
      rw [map_countP_comp (List.range (c.n - c.split))
        (fun k => (c.split + k, c.base + c.step * (c.split : Int))) b]
      have hA : span c 0 c.split c.base c.step = ⟨0, 1, c.split, c.base, c.step⟩ := by
        simp [span, lane, h]
      have hB : span c c.split c.n (c.base + c.step * c.split) 0 =
          ⟨c.split, 1, c.n - c.split, c.base + c.step * c.split, 0⟩ := by
        simp [span, lane]
      simp [layout, hA, hB]
      have hjA := lane_join ⟨0, 1, c.split, c.base, c.step⟩ b
      change ((List.range c.split).map (fun (i : Nat) => Int.ofNat ((List.range b.n).countP
        (fun (j : Nat) => c.base + c.step * (i : Int) == b.base + b.step * (j : Int))))).sum =
        Int.ofNat (joinCount ⟨0, 1, c.split, c.base, c.step⟩ b) at hjA
      have hjB := lane_join ⟨c.split, 1, c.n - c.split, c.base + c.step * c.split, 0⟩ b
      change ((List.range (c.n - c.split)).map (fun (i : Nat) => Int.ofNat ((List.range b.n).countP
        (fun (j : Nat) => (c.base + c.step * (c.split : Int)) + 0 * (i : Int) ==
          b.base + b.step * (j : Int))))).sum =
        Int.ofNat (joinCount ⟨c.split, 1, c.n - c.split, c.base + c.step * c.split, 0⟩ b) at hjB
      have hjB' : ((List.range (c.n - c.split)).map (fun (i : Nat) => Int.ofNat ((List.range b.n).countP
          (fun (j : Nat) => c.base + c.step * (c.split : Int) == b.base + b.step * (j : Int))))).sum =
        Int.ofNat (joinCount ⟨c.split, 1, c.n - c.split, c.base + c.step * c.split, 0⟩ b) := by
        rw [← hjB]
        apply congrArg List.sum
        apply List.map_congr_left
        intro i hi
        congr 1
        apply List.countP_congr
        intro j hj
        simp
      change ((List.range c.split).map (fun (i : Nat) => Int.ofNat ((List.range b.n).countP
        (fun (j : Nat) => c.base + c.step * (i : Int) == b.base + b.step * (j : Int))))).sum +
        ((List.range (c.n - c.split)).map (fun (i : Nat) => Int.ofNat ((List.range b.n).countP
          (fun (j : Nat) => c.base + c.step * (c.split : Int) == b.base + b.step * (j : Int))))).sum =
        Int.ofNat (joinCount ⟨0, 1, c.split, c.base, c.step⟩ b) +
        Int.ofNat (joinCount ⟨c.split, 1, c.n - c.split, c.base + c.step * c.split, 0⟩ b)
      rw [hjA, hjB']
    · have hgt : c.n < c.split := by omega
      rw [rows_saturating_split_gt c hgt]
      rw [List.map_map]
      rw [map_countP_comp (List.range c.n) (fun i => (i, c.base + c.step * (i : Int))) b]
      have hmin : min c.n c.split = c.n := Nat.min_eq_left (Nat.le_of_lt hgt)
      have hsub : c.n - c.split = 0 := by omega
      have hA : span c 0 c.split c.base c.step = ⟨0, 1, c.n, c.base, c.step⟩ := by
        simp [span, lane, hmin]
      have hB : span c c.split c.n (c.base + c.step * c.split) 0 =
          ⟨c.split, 1, 0, c.base + c.step * c.split, 0⟩ := by
        simp [span, lane, hsub]
      simp [layout, hA, hB]
      have hz : joinCount ⟨c.split, 1, 0, c.base + c.step * c.split, 0⟩ b = 0 := by
        simp [joinCount, intersection]
      rw [hz]
      simp [hmin]
      have hjA := lane_join ⟨0, 1, c.n, c.base, c.step⟩ b
      change ((List.range c.n).map (fun (i : Nat) => Int.ofNat ((List.range b.n).countP
        (fun (j : Nat) => c.base + c.step * (i : Int) == b.base + b.step * (j : Int))))).sum =
        Int.ofNat (joinCount ⟨0, 1, c.n, c.base, c.step⟩ b) at hjA
      omega
  rw [sum_ofNat_map, sum_ofNat_map] at hInt
  exact Int.ofNat_inj.mp hInt

end TinylambOpt.ResearchProofs.Atlas.Goal_SaturatingCounter_ResidueJoin

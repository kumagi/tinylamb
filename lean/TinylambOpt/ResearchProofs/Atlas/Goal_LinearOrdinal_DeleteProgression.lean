import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `LinearOrdinal.DeleteProgression` — `∀ c mask, (((rows layout c).filter (fun r =>
  !contains mask r.1)).map Prod.snd).sum = ((layout.lanes c).map (fun a => partialSum
  a a.n - deletedSum a mask)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_LinearOrdinal_DeleteProgression

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal

/-! ## Generic list helpers -/

private theorem rows_linear (c : Config) :
    rows layout c = (List.range c.n).map (fun i : Nat => (i, c.base + c.step * (i : Int))) := by
  unfold rows
  induction List.range c.n with
  | nil => simp
  | cons i is ih => simp [layout]

private theorem lanes_linear (c : Config) :
    layout.lanes c = [⟨0, 1, c.n, c.base, c.step⟩] := by
  simp [layout, lane]

private theorem sum_filter_compl {α : Type} (l : List α) (p : α → Bool) (f : α → Int) :
    ((l.filter (fun x => !p x)).map f).sum + ((l.filter p).map f).sum = (l.map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h] <;> omega

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
      rw [partialSum_succ, ih]
      rw [List.range_succ, List.map_append, List.sum_append]
      simp

/-! ## Extended Euclid and the CRT intersection -/

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
        simp only
        have hgcd : a.gcd b = b.gcd (a % b) := by
          rw [Nat.gcd_comm a b, Nat.gcd_rec b a, Nat.gcd_comm]
        rw [hgcd, hrec]
        have hd : (a : Int) = (b : Int) * ((a / b : Nat) : Int) + ((a % b : Nat) : Int) := by
          exact_mod_cast (Nat.div_add_mod a b).symm
        rw [hd]
        grind

private theorem bezout_one_mod (S : Nat) :
    (S : Int) ∣ (bezout 1 S).1 - 1 := by
  have hspec := bezout_spec 1 S
  simp at hspec
  refine ⟨-(bezout 1 S).2, ?_⟩
  grind

private theorem F_congr (mlo : Int) (S : Nat) (hS : 0 < S) :
    let low := max 0 mlo
    let root := mlo * (bezout 1 S).1
    let F := low + (root - low) % (S : Int)
    (S : Int) ∣ F - mlo := by
  dsimp only
  let low := max 0 mlo
  let root := mlo * (bezout 1 S).1
  have hSpos : (0 : Int) < S := by exact_mod_cast hS
  have hu := bezout_one_mod S
  have hmul : (S : Int) ∣ mlo * ((bezout 1 S).1 - 1) :=
    Int.dvd_mul_of_dvd_right (b := mlo) hu
  have hrootmlo : (S : Int) ∣ root - mlo := by
    dsimp [root]
    simpa [Int.mul_sub] using hmul
  have hFroot : (S : Int) ∣ low + (root - low) % (S : Int) - root := by
    have hm := Int.mul_ediv_add_emod (root - low) (S : Int)
    refine ⟨-((root - low) / (S : Int)), ?_⟩
    grind
  have heq : (low + (root - low) % (S : Int) - root) + (root - mlo) =
      low + (root - low) % (S : Int) - mlo := by omega
  rw [← heq]
  exact Int.dvd_add hFroot hrootmlo

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

private theorem ediv_sub_one_of_dvd (d S : Int) (hS : 0 < S) (hd : S ∣ d) :
    (d - 1) / S = d / S - 1 := by
  let q := d / S
  have hq : d = q * S := by
    have hh := Int.ediv_mul_cancel_of_dvd hd
    dsimp [q]
    exact hh.symm
  have hcalc : d - 1 = (q - 1) * S + (S - 1) := by
    rw [hq]
    rw [Int.sub_mul, Int.one_mul]
    omega
  rw [hcalc]
  rw [Int.mul_add_ediv_right (q - 1) (S - 1) (by omega : S ≠ 0)]
  have hsmall : (S - 1) / S = 0 := by
    apply Int.ediv_eq_zero_of_lt <;> omega
  rw [hsmall]
  dsimp [q]
  omega

private theorem ediv_sub_one_of_not_dvd (d S : Int) (hS : 0 < S) (hd : ¬ S ∣ d) :
    (d - 1) / S = d / S := by
  let q := d / S
  let r := d % S
  have hrnon : 0 ≤ r := Int.emod_nonneg _ (by omega)
  have hrlt : r < S := Int.emod_lt_of_pos _ hS
  have hrpos : 0 < r := by
    by_cases hr0 : r = 0
    · exfalso
      apply hd
      apply Int.dvd_of_emod_eq_zero
      simpa [r] using hr0
    · omega
  have hcalc : d - 1 = q * S + (r - 1) := by
    have hh := Int.mul_ediv_add_emod d S
    rw [Int.mul_comm S (d / S)] at hh
    dsimp [q, r] at hh ⊢
    omega
  rw [hcalc, Int.mul_add_ediv_right q (r - 1) (by omega : S ≠ 0)]
  have hsmall : (r - 1) / S = 0 := by
    apply Int.ediv_eq_zero_of_lt <;> omega
  rw [hsmall]
  omega

private theorem count_succ_pos (N : Nat) (hN : 0 < N) (mlo : Int) (S mn : Nat)
    (hS : 0 < S) (hmn : 0 < mn) :
    let low := max 0 mlo
    let root := mlo * (bezout 1 S).1
    let F := low + (root - low) % (S : Int)
    let L := mlo + ((mn - 1 : Nat) : Int) * (S : Int)
    let HN := min ((N - 1 : Nat) : Int) L
    let HS := min (N : Int) L
    let KN := if F ≤ HN then ((HN - F) / (S : Int)).toNat + 1 else 0
    let KS := if F ≤ HS then ((HS - F) / (S : Int)).toNat + 1 else 0
    KS = KN + if contains ⟨mlo, S, mn⟩ (N : Int) then 1 else 0 := by
  let low := max 0 mlo
  let root := mlo * (bezout 1 S).1
  let F := low + (root - low) % (S : Int)
  let L := mlo + ((mn - 1 : Nat) : Int) * (S : Int)
  let HN := min ((N - 1 : Nat) : Int) L
  let HS := min (N : Int) L
  change (if F ≤ HS then ((HS - F) / (S : Int)).toNat + 1 else 0) =
    (if F ≤ HN then ((HN - F) / (S : Int)).toNat + 1 else 0) +
      (if contains ⟨mlo, S, mn⟩ (N : Int) then 1 else 0)
  have hSpos : (0 : Int) < S := by exact_mod_cast hS
  have hFlow : low ≤ F := by
    dsimp [F]
    have := Int.emod_nonneg (root - low) (by omega : (S : Int) ≠ 0)
    omega
  have hFcongr := F_congr mlo S hS
  dsimp only at hFcongr
  have hrootcongr : (S : Int) ∣ root - mlo := by
    have hFroot := first_root_dvd low root F (S:Int) hSpos (by rfl)
    have hd := Int.dvd_sub (a := (S:Int)) (b := F - mlo) (c := F - root) hFcongr hFroot
    have heq : (F - mlo) - (F - root) = root - mlo := by omega
    rw [← heq]
    exact hd
  by_cases hdel : contains ⟨mlo, S, mn⟩ (N : Int) = true
  · have hdel' : mlo ≤ (N : Int) ∧ (N : Int) ≤ L ∧ (S : Int) ∣ (N : Int) - mlo := by
      have hh := hdel
      simp [contains, lastVal, Nat.ne_of_gt hS, hmn] at hh
      rcases hh with ⟨⟨hlo, hhi⟩, hmod⟩
      have hmod' : ((N : Int) - mlo) % (S : Int) = 0 := by simpa using hmod
      exact ⟨hlo, of_decide_eq_true hhi, Int.dvd_of_emod_eq_zero hmod'⟩
    have hlowN : low ≤ (N : Int) := by dsimp [low]; omega
    have hNr : (S : Int) ∣ (N : Int) - root := by
      have hd := Int.dvd_sub (a := (S:Int)) (b := (N:Int) - mlo) (c := root - mlo) hdel'.2.2 hrootcongr
      have heq : ((N : Int) - mlo) - (root - mlo) = (N : Int) - root := by omega
      rw [← heq]
      exact hd
    have hFN : F ≤ (N : Int) := first_le_of_congruence low root F (N:Int) (S:Int) hSpos (by rfl) hlowN hNr
    have hHSeq : HS = (N : Int) := by dsimp [HS]; omega
    have hHNeq : HN = (N - 1 : Nat) := by dsimp [HN, L]; omega
    rw [hHSeq]
    rw [if_pos hFN]
    by_cases hFNeq : F = (N : Int)
    · have hHNlt : ¬ F ≤ HN := by rw [hHNeq]; omega
      rw [if_neg hHNlt]
      simp [hdel]
      have hz : ((N:Int) - F) / (S:Int) = 0 := by rw [hFNeq]; simp
      rw [hz]
      simp
    · have hFNlt : F < (N : Int) := by omega
      have hHNge : F ≤ HN := by rw [hHNeq]; omega
      rw [if_pos hHNge]
      simp [hdel]
      have hdvdNF : (S : Int) ∣ (N : Int) - F := by
        have hd := Int.dvd_sub (a := (S:Int)) (b := (N:Int) - root) (c := F - root) hNr
          (first_root_dvd low root F (S:Int) hSpos (by rfl))
        have heq : ((N : Int) - root) - (F - root) = (N : Int) - F := by omega
        rw [← heq]
        exact hd
      have hdiv := ediv_sub_one_of_dvd ((N:Int) - F) (S:Int) hSpos hdvdNF
      have hHNval : HN - F = (N : Int) - F - 1 := by rw [hHNeq]; omega
      rw [hHNval, hdiv]
      have hqeq := Int.ediv_mul_cancel_of_dvd hdvdNF
      have hqpos : 1 ≤ ((N : Int) - F) / (S : Int) := by
        by_cases hle : ((N : Int) - F) / (S : Int) ≤ 0
        · have hnonpos : ((N : Int) - F) / (S : Int) * (S : Int) ≤ 0 :=
            Int.mul_nonpos_of_nonpos_of_nonneg hle (Int.le_of_lt hSpos)
          omega
        · omega
      omega
  · have hdel' : ¬ (mlo ≤ (N : Int) ∧ (N : Int) ≤ L ∧ (S : Int) ∣ (N : Int) - mlo) := by
      intro h
      apply hdel
      have hmod : ((N : Int) - mlo) % (S : Int) = 0 := Int.emod_eq_zero_of_dvd h.2.2
      have hhi : (N : Int) ≤ mlo + ((mn - 1 : Nat) : Int) * (S : Int) := by
        dsimp [L] at h
        exact h.2.1
      simp [contains, lastVal, hmn, h.1, hhi, hmod]
    simp [hdel]
    by_cases hNL : L < (N : Int)
    · have hHNeq : HN = L := by dsimp [HN]; omega
      have hHSeq : HS = L := by dsimp [HS]; omega
      rw [hHNeq, hHSeq]
    · have hNL' : (N : Int) ≤ L := by omega
      by_cases hNmlo : (N : Int) < mlo
      · have hHSlt : ¬ F ≤ HS := by dsimp [HS]; omega
        have hHNlt : ¬ F ≤ HN := by dsimp [HN]; omega
        rw [if_neg hHSlt, if_neg hHNlt]
      · have hNmlo' : mlo ≤ (N : Int) := by omega
        have hnotdvd : ¬ (S : Int) ∣ (N : Int) - mlo := by
          intro hd
          exact hdel' ⟨hNmlo', hNL', hd⟩
        have hnotdvdF : ¬ (S : Int) ∣ (N : Int) - F := by
          intro hd
          apply hnotdvd
          have heq : ((N : Int) - F) + (F - mlo) = (N : Int) - mlo := by omega
          rw [← heq]
          exact Int.dvd_add hd hFcongr
        have hHSeq : HS = (N : Int) := by dsimp [HS]; omega
        have hHNeq : HN = (N - 1 : Nat) := by dsimp [HN]; omega
        rw [hHSeq, hHNeq]
        by_cases hFbig : (N : Int) < F
        · have hnle : ¬ F ≤ (N : Int) := by omega
          rw [if_neg hnle]
          have hHNlt : ¬ F ≤ (N - 1 : Nat) := by omega
          rw [if_neg hHNlt]
        · have hFN : F ≤ (N : Int) := by omega
          have hFNlt : F < (N : Int) := by
            by_cases heqF : F = (N : Int)
            · exfalso
              apply hnotdvdF
              rw [heqF]
              simp
            · omega
          rw [if_pos hFN]
          have hHNge : F ≤ (N - 1 : Nat) := by omega
          rw [if_pos hHNge]
          have hdiv := ediv_sub_one_of_not_dvd ((N:Int) - F) (S:Int) hSpos hnotdvdF
          have hHNval : (N - 1 : Nat) - F = (N : Int) - F - 1 := by omega
          rw [hHNval, hdiv]

private theorem pos_point_eq (N : Nat) (hN : 0 < N) (mlo : Int) (S mn : Nat)
    (hS : 0 < S) (hmn : 0 < mn) :
    let low := max 0 mlo
    let root := mlo * (bezout 1 S).1
    let F := low + (root - low) % (S : Int)
    let L := mlo + ((mn - 1 : Nat) : Int) * (S : Int)
    let KN := if F ≤ min ((N - 1 : Nat) : Int) L then
      ((min ((N - 1 : Nat) : Int) L - F) / (S : Int)).toNat + 1 else 0
    contains ⟨mlo, S, mn⟩ (N : Int) = true → (N : Int) = F + (KN : Int) * (S : Int) := by
  dsimp only
  intro hdel
  let low := max 0 mlo
  let root := mlo * (bezout 1 S).1
  let F := low + (root - low) % (S : Int)
  let L := mlo + ((mn - 1 : Nat) : Int) * (S : Int)
  let HN := min ((N - 1 : Nat) : Int) L
  change (N : Int) = F + ((if F ≤ HN then ((HN - F) / (S : Int)).toNat + 1 else 0 : Nat) : Int) * (S : Int)
  have hSpos : (0 : Int) < S := by exact_mod_cast hS
  have hdel' : mlo ≤ (N : Int) ∧ (N : Int) ≤ L ∧ (S : Int) ∣ (N : Int) - mlo := by
    have hh := hdel
    simp [contains, lastVal, Nat.ne_of_gt hS, hmn] at hh
    rcases hh with ⟨⟨hlo, hhi⟩, hmod⟩
    exact ⟨hlo, of_decide_eq_true hhi, Int.dvd_of_emod_eq_zero hmod⟩
  have hFcongr := F_congr mlo S hS
  dsimp only at hFcongr
  have hFroot := first_root_dvd low root F (S:Int) hSpos (by rfl)
  have hrootcongr : (S : Int) ∣ root - mlo := by
    have hd := Int.dvd_sub (a := (S:Int)) (b := F - mlo) (c := F - root) hFcongr hFroot
    have heq : (F - mlo) - (F - root) = root - mlo := by omega
    rw [← heq]
    exact hd
  have hNr : (S : Int) ∣ (N : Int) - root := by
    have hd := Int.dvd_sub (a := (S:Int)) (b := (N:Int) - mlo) (c := root - mlo) hdel'.2.2 hrootcongr
    have heq : ((N : Int) - mlo) - (root - mlo) = (N : Int) - root := by omega
    rw [← heq]
    exact hd
  have hlowN : low ≤ (N : Int) := by dsimp [low]; omega
  have hFN : F ≤ (N : Int) := first_le_of_congruence low root F (N:Int) (S:Int) hSpos (by rfl) hlowN hNr
  have hdvdNF : (S : Int) ∣ (N : Int) - F := by
    have hd := Int.dvd_sub (a := (S:Int)) (b := (N:Int) - root) (c := F - root) hNr hFroot
    have heq : ((N : Int) - root) - (F - root) = (N : Int) - F := by omega
    rw [← heq]
    exact hd
  have hHNeq : HN = (N - 1 : Nat) := by
    dsimp [HN]
    have h1 : ((N - 1 : Nat) : Int) ≤ (N : Int) := by omega
    have h2 : (N : Int) ≤ L := hdel'.2.1
    omega
  by_cases hFe : F = (N : Int)
  · rw [if_neg (by rw [hHNeq]; omega)]
    simp [hFe]
  · have hFNlt : F < (N : Int) := by omega
    have hHNge : F ≤ HN := by rw [hHNeq]; omega
    rw [if_pos hHNge]
    have hHNval : HN - F = (N : Int) - F - 1 := by rw [hHNeq]; omega
    have hdiv := ediv_sub_one_of_dvd ((N:Int) - F) (S:Int) hSpos hdvdNF
    rw [hHNval, hdiv]
    have hqeq : (N : Int) - F = ((N : Int) - F) / (S : Int) * (S : Int) := by
      have hh := Int.ediv_mul_cancel_of_dvd hdvdNF
      exact hh.symm
    have hqpos : 1 ≤ ((N : Int) - F) / (S : Int) := by
      by_cases hle : ((N : Int) - F) / (S : Int) ≤ 0
      · have hnonpos : ((N : Int) - F) / (S : Int) * (S : Int) ≤ 0 :=
          Int.mul_nonpos_of_nonpos_of_nonneg hle (Int.le_of_lt hSpos)
        omega
      · omega
    have htonat : (((((N : Int) - F) / (S : Int) - 1).toNat + 1 : Nat) : Int) =
        ((N : Int) - F) / (S : Int) := by
      have hq1 : 0 ≤ ((N : Int) - F) / (S : Int) - 1 := by omega
      have ht := Int.toNat_of_nonneg hq1
      omega
    rw [htonat]
    omega

private theorem deletedSum_const (N : Nat) (base step : Int) (mlo : Int) (mn : Nat)
    (hmn : 0 < mn) :
    deletedSum ⟨0, 1, N, base, step⟩ ⟨mlo, 0, mn⟩ =
      if 0 < mn ∧ 0 ≤ mlo ∧ mlo < (N : Int) then base + mlo * step else 0 := by
  by_cases hN0 : N = 0
  · subst N
    simp [deletedSum, intersection, positions, contains, tri,
      TinylambOpt.ResearchGoals.PairAggregates.choose2] <;> omega
  · have hN : 0 < N := Nat.pos_of_ne_zero hN0
    have hIr : intersection (positions ⟨0,1,N,base,step⟩) ⟨mlo,0,mn⟩ =
        ⟨mlo, 1, if contains ⟨0,1,N⟩ mlo then 1 else 0⟩ := by
      simp [intersection, positions, lastVal, Nat.ne_of_gt hN, Nat.ne_of_gt hmn]
      rfl
    rw [deletedSum, hIr]
    have hc : contains ⟨0,1,N⟩ mlo = true ↔ 0 ≤ mlo ∧ mlo < (N : Int) := by
      simp [contains, lastVal]
      omega
    by_cases hc' : contains ⟨0,1,N⟩ mlo = true
    · rw [if_pos hc']
      simp [firstAt, slopeAt, tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
      have hcok := hc.mp hc'
      have hcond : 0 < mn ∧ 0 ≤ mlo ∧ mlo < (N : Int) := ⟨hmn, hcok⟩
      omega
    · rw [if_neg hc']
      simp [tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
      have hcond : ¬ (0 < mn ∧ 0 ≤ mlo ∧ mlo < (N : Int)) := by
        intro hh
        apply hc'
        simp [contains, lastVal]
        omega
      omega

private theorem deletedSum_zero_succ (base step : Int) (mlo : Int) (S mn : Nat)
    (hS : 0 < S) (hmn : 0 < mn) :
    deletedSum ⟨0,1,1,base,step⟩ ⟨mlo,S,mn⟩ =
      if contains ⟨mlo,S,mn⟩ (0 : Int) then base else 0 := by
  let low := max 0 mlo
  let root := mlo * (bezout 1 S).1
  let F := low + (root - low) % (S : Int)
  let L := mlo + ((mn - 1 : Nat) : Int) * (S : Int)
  have hSpos : (0 : Int) < S := by exact_mod_cast hS
  have hIr : intersection (positions ⟨0,1,1,base,step⟩) ⟨mlo,S,mn⟩ =
      ⟨F, S, if F ≤ min (0 : Int) L then ((min (0 : Int) L - F) / (S : Int)).toNat + 1 else 0⟩ := by
    dsimp [F, L, root, low]
    simp [intersection, positions, lastVal, Nat.ne_of_gt hmn, Nat.ne_of_gt hS]
  rw [deletedSum, hIr]
  have hFlow : low ≤ F := by
    dsimp [F]
    have := Int.emod_nonneg (root - low) (by omega : (S : Int) ≠ 0)
    omega
  have hFnonneg : 0 ≤ F := by
    have hlow : 0 ≤ low := by dsimp [low]; omega
    omega
  have hFcongr := F_congr mlo S hS
  dsimp only at hFcongr
  have hFroot := first_root_dvd low root F (S:Int) hSpos (by rfl)
  have hrootcongr : (S : Int) ∣ root - mlo := by
    have hd := Int.dvd_sub (a := (S:Int)) (b := F - mlo) (c := F - root) hFcongr hFroot
    have heq : (F - mlo) - (F - root) = root - mlo := by omega
    rw [← heq]
    exact hd
  by_cases hdel : contains ⟨mlo,S,mn⟩ (0 : Int) = true
  · rw [if_pos hdel]
    have hd' : mlo ≤ (0 : Int) ∧ (0 : Int) ≤ L ∧ (S : Int) ∣ (0 : Int) - mlo := by
      have hh := hdel
      simp [contains, lastVal, Nat.ne_of_gt hS, hmn] at hh
      rcases hh with ⟨⟨hlo, hhi⟩, hmod⟩
      have hmod' : ((0 : Int) - mlo) % (S : Int) = 0 := by simpa using hmod
      exact ⟨hlo, of_decide_eq_true hhi, Int.dvd_of_emod_eq_zero hmod'⟩
    have hlow0 : low ≤ (0 : Int) := by dsimp [low]; omega
    have h0r : (S : Int) ∣ (0 : Int) - root := by
      have hd := Int.dvd_sub (a := (S:Int)) (b := (0:Int) - mlo) (c := root - mlo) hd'.2.2 hrootcongr
      have heq : ((0 : Int) - mlo) - (root - mlo) = (0 : Int) - root := by omega
      rw [← heq]
      exact hd
    have hF0 : F ≤ 0 := first_le_of_congruence low root F 0 (S:Int) hSpos (by rfl) hlow0 h0r
    have hFeq : F = 0 := by omega
    have hcondK : F ≤ min (0 : Int) L := by
      rw [hFeq]
      omega
    have hmin0 : min (0 : Int) L = 0 := by omega
    have hK : (if F ≤ min (0 : Int) L then ((min (0 : Int) L - F) / (S : Int)).toNat + 1 else 0) = 1 := by
      rw [if_pos hcondK, hmin0, hFeq]
      simp
    rw [hK]
    rw [firstAt, slopeAt]
    simp [hFeq, tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  · rw [if_neg hdel]
    by_cases hcond : F ≤ min (0 : Int) L
    · exfalso
      apply hdel
      have hFeq : F = 0 := by
        have hmin : min (0 : Int) L ≤ 0 := (Std.min_le_left : min (0 : Int) L ≤ 0)
        omega
      have hFleL : F ≤ L := by
        have hmr : min (0 : Int) L ≤ L := (Std.min_le_right : min (0 : Int) L ≤ L)
        omega
      have hml : mlo ≤ (0 : Int) := by
        dsimp [low] at hFlow
        omega
      have hdvd : (S : Int) ∣ (0 : Int) - mlo := by
        have hFzero : (S : Int) ∣ F - 0 := by rw [hFeq]; exact ⟨0, by omega⟩
        have hd := Int.dvd_sub (a := (S:Int)) (b := F - mlo) (c := F - 0) hFcongr hFzero
        have heq : (F - mlo) - (F - 0) = (0 : Int) - mlo := by omega
        rw [← heq]
        exact hd
      have hmod : ((0 : Int) - mlo) % (S : Int) = 0 := Int.emod_eq_zero_of_dvd hdvd
      have hleL0 : (0 : Int) ≤ L := by rw [← hFeq]; exact hFleL
      have hleL : (0 : Int) ≤ mlo + ((mn - 1 : Nat) : Int) * (S : Int) := by
        dsimp [L] at hleL0
        exact hleL0
      have hhmod : (-mlo) % (S : Int) = 0 := by simpa using hmod
      have hzero : (S : Int) ∣ (0 : Int) - 0 := ⟨0, by omega⟩
      simp [contains, lastVal, hmn, hml, hleL, hhmod]
    · rw [if_neg hcond]
      simp [tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]

private theorem arith_step (K : Nat) (base F S step : Int) :
    (((K + 1 : Nat) : Int) * (base + F * step) + tri (K + 1) * (S * step)) =
      ((K : Int) * (base + F * step) + tri K * (S * step)) + base +
        (step * F + (K : Int) * (S * step)) := by
  rw [tri_succ]
  rw [Int.natCast_add, Int.natCast_one]
  rw [Int.add_mul, Int.add_mul]
  have hmul : (K : Int) * (S * step) = (K : Int) * S * step := by rw [Int.mul_assoc]
  rw [hmul, Int.mul_comm step F]
  omega

private theorem deletedSum_succ (N : Nat) (base step : Int) (m : Progression) :
    deletedSum ⟨0,1,N+1,base,step⟩ m =
      deletedSum ⟨0,1,N,base,step⟩ m +
        (if contains m (N : Int) then base + step * (N : Int) else 0) := by
  rcases m with ⟨mlo, S, mn⟩
  by_cases hmn0 : mn = 0
  · simp [deletedSum, intersection, contains, hmn0, tri,
      TinylambOpt.ResearchGoals.PairAggregates.choose2]
  · have hmn : 0 < mn := Nat.pos_of_ne_zero hmn0
    by_cases hS0 : S = 0
    · subst S
      rw [deletedSum_const N base step mlo mn hmn, deletedSum_const (N+1) base step mlo mn hmn]
      let condN : Prop := 0 < mn ∧ 0 ≤ mlo ∧ mlo < (N : Int)
      let condS : Prop := 0 < mn ∧ 0 ≤ mlo ∧ mlo < ((N + 1 : Nat) : Int)
      change (if condS then base + mlo * step else 0) =
        (if condN then base + mlo * step else 0) +
          (if contains ⟨mlo,0,mn⟩ (N:Int) then base + step * (N:Int) else 0)
      have hdel : contains ⟨mlo,0,mn⟩ (N:Int) = true ↔ 0 ≤ mlo ∧ (N:Int) = mlo := by
        simp [contains, lastVal, hmn]
        omega
      by_cases hd : contains ⟨mlo,0,mn⟩ (N:Int) = true
      · have hd' := hdel.mp hd
        have hcondN : ¬ condN := by dsimp [condN]; omega
        have hcondS : condS := by dsimp [condS]; omega
        rw [if_pos hcondS, if_neg hcondN, if_pos hd]
        rw [← hd'.2, Int.mul_comm (N : Int) step]
        simp
      · rw [if_neg hd]
        have heqconds : (if condN then base + mlo * step else 0) =
            (if condS then base + mlo * step else 0) := by
          by_cases hcN : condN
          · have hcS : condS := by dsimp [condN, condS] at hcN ⊢; omega
            rw [if_pos hcN, if_pos hcS]
          · have hcS : ¬ condS := by
              intro hcS
              apply hcN
              dsimp [condN, condS] at hcS ⊢
              have hlo : 0 ≤ mlo := hcS.2.1
              by_cases hEq : mlo = (N : Int)
              · exfalso
                apply hd
                exact hdel.mpr ⟨hlo, hEq.symm⟩
              · omega
            rw [if_neg hcN, if_neg hcS]
        rw [← heqconds]
        simp
    · have hS : 0 < S := Nat.pos_of_ne_zero hS0
      by_cases hN0 : N = 0
      · subst N
        rw [deletedSum_zero_succ base step mlo S mn hS hmn]
        simp [deletedSum, intersection, positions, lastVal, tri,
          TinylambOpt.ResearchGoals.PairAggregates.choose2,
          Nat.ne_of_gt hmn, Nat.ne_of_gt hS]
      · have hN : 0 < N := Nat.pos_of_ne_zero hN0
        let low := max 0 mlo
        let root := mlo * (bezout 1 S).1
        let F := low + (root - low) % (S : Int)
        let L := mlo + ((mn - 1 : Nat) : Int) * (S : Int)
        let HN := min ((N - 1 : Nat) : Int) L
        let HS := min (N : Int) L
        let KN := if F ≤ HN then ((HN - F) / (S : Int)).toNat + 1 else 0
        let KS := if F ≤ HS then ((HS - F) / (S : Int)).toNat + 1 else 0
        have hIrN : intersection (positions ⟨0,1,N,base,step⟩) ⟨mlo,S,mn⟩ =
            ⟨F, S, KN⟩ := by
          dsimp [F, L, root, low, HN, KN]
          simp [intersection, positions, lastVal, Nat.ne_of_gt hN, Nat.ne_of_gt hmn, Nat.ne_of_gt hS]
        have hIrS : intersection (positions ⟨0,1,N+1,base,step⟩) ⟨mlo,S,mn⟩ =
            ⟨F, S, KS⟩ := by
          dsimp [F, L, root, low, HS, KS]
          simp [intersection, positions, lastVal, Nat.ne_of_gt hmn, Nat.ne_of_gt hS]
        have hdN : deletedSum ⟨0,1,N,base,step⟩ ⟨mlo,S,mn⟩ =
            KN * (base + F * step) + tri KN * ((S : Int) * step) := by
          rw [deletedSum, hIrN]
          simp [firstAt, slopeAt]
        have hdS : deletedSum ⟨0,1,N+1,base,step⟩ ⟨mlo,S,mn⟩ =
            KS * (base + F * step) + tri KS * ((S : Int) * step) := by
          rw [deletedSum, hIrS]
          simp [firstAt, slopeAt]
        rw [hdN, hdS]
        have hKS := count_succ_pos N hN mlo S mn hS hmn
        dsimp only at hKS
        have hKSeq : KS = KN + if contains ⟨mlo,S,mn⟩ (N:Int) then 1 else 0 := by
          simpa [low, root, F, L, HN, HS, KN, KS] using hKS
        rw [hKSeq]
        have htri := tri_succ KN
        by_cases hd : contains ⟨mlo,S,mn⟩ (N:Int) = true
        · rw [if_pos hd]
          have hpt := pos_point_eq N hN mlo S mn hS hmn
          dsimp only at hpt
          have hpt' : (N:Int) = F + (KN:Int)*(S:Int) := by
            simpa [low, root, F, L, HN, KN] using hpt hd
          have hmulN : step * (N : Int) = step * F + (KN : Int) * (S * step) := by
            rw [hpt']
            grind
          rw [if_pos hd]
          rw [arith_step]
          rw [← hmulN]
          omega
        · simp [hd]

private theorem goal_aux (n : Nat) (base step : Int) (mask : Progression) :
    let xs := (List.range n).map (fun i : Nat => (i, base + step * (i : Int)))
    ((xs.filter (fun r => !contains mask (r.1 : Int))).map Prod.snd).sum =
      partialSum ⟨0,1,n,base,step⟩ n - deletedSum ⟨0,1,n,base,step⟩ mask := by
  dsimp only
  induction n with
  | zero =>
      simp [partialSum, tri, TinylambOpt.ResearchGoals.PairAggregates.choose2,
        deletedSum, intersection, positions]
  | succ n ih =>
      rw [List.range_succ]
      simp only [List.map_append, List.filter_append, List.map_append, List.sum_append]
      have hdel := deletedSum_succ n base step mask
      have hps0 := partialSum_succ ⟨0,1,n,base,step⟩ n
      have hps : partialSum ⟨0,1,n+1,base,step⟩ (n+1) =
          partialSum ⟨0,1,n,base,step⟩ n + (base + (n : Int) * step) := by
        change partialSum ⟨0,1,n,base,step⟩ (n+1) = _
        exact hps0
      by_cases hp : contains mask (n : Int) = true
      · simp [hp]
        rw [ih, hdel, hps]
        simp [hp]
        rw [Int.mul_comm (n : Int) step]
        omega
      · simp [hp]
        rw [ih, hdel, hps]
        simp [hp]
        rw [Int.mul_comm (n : Int) step]
        omega

theorem sound_LinearOrdinal_DeleteProgression : LinearOrdinal.DeleteProgression.Goal := by
  intro c mask
  rw [rows_linear c, lanes_linear c]
  simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
  simpa using goal_aux c.n c.base c.step mask

end TinylambOpt.ResearchProofs.Atlas.Goal_LinearOrdinal_DeleteProgression

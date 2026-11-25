import TinylambOpt.ResearchGoals.Atlas.TextExecution

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Quantile

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Wavelet

private theorem kth_succ (d : Nat) (xs : Word) (k : Nat) :
    kth (d + 1) xs k =
      if k < (lower d xs).length then kth d (lower d xs) k
      else 2 ^ d + kth d (upper d xs) (k - (lower d xs).length) := rfl

private theorem kth_lowbits_aux : ∀ d (xs : Word) k,
    kth d xs k = kth d (xs.map (fun x => x % 2 ^ d)) k := by
  intro d
  induction d with
  | zero => intro xs k; rfl
  | succ d ih =>
      intro xs k
      have hmodbit : ∀ x : Nat, (x % 2 ^ (d + 1)).testBit d = x.testBit d := by
        intro x
        simpa using Nat.testBit_mod_two_pow x (d + 1) d
      have hlow : lower d (xs.map (fun x => x % 2 ^ (d + 1))) =
          (lower d xs).map (fun x => x % 2 ^ (d + 1)) := by
        unfold lower
        rw [List.filter_map]
        apply congrArg (List.map (fun x => x % 2 ^ (d + 1)))
        apply congrArg (fun p : Nat → Bool => xs.filter p)
        funext x
        exact congrArg Bool.not (hmodbit x)
      have hupp : upper d (xs.map (fun x => x % 2 ^ (d + 1))) =
          (upper d xs).map (fun x => x % 2 ^ (d + 1)) := by
        unfold upper
        rw [List.filter_map]
        apply congrArg (List.map (fun x => x % 2 ^ (d + 1)))
        apply congrArg (fun p : Nat → Bool => xs.filter p)
        funext x
        exact hmodbit x
      rw [kth_succ, kth_succ d (xs.map (fun x => x % 2 ^ (d + 1))) k]
      rw [hlow, hupp, List.length_map]
      have hcommLow : ((lower d xs).map (fun x => x % 2 ^ (d + 1))).map
          (fun x => x % 2 ^ d) = (lower d xs).map (fun x => x % 2 ^ d) := by
        rw [List.map_map]
        apply List.map_congr_left
        intro x _
        exact Nat.mod_mod_of_dvd x (Nat.pow_dvd_pow 2 (by omega))
      have hcommUp : ((upper d xs).map (fun x => x % 2 ^ (d + 1))).map
          (fun x => x % 2 ^ d) = (upper d xs).map (fun x => x % 2 ^ d) := by
        rw [List.map_map]
        apply List.map_congr_left
        intro x _
        exact Nat.mod_mod_of_dvd x (Nat.pow_dvd_pow 2 (by omega))
      by_cases hkz : k < (lower d xs).length
      · rw [if_pos hkz, if_pos hkz]
        calc
          kth d (lower d xs) k = kth d ((lower d xs).map (fun x => x % 2 ^ d)) k :=
            ih (lower d xs) k
          _ = kth d (((lower d xs).map (fun x => x % 2 ^ (d + 1))).map
              (fun x => x % 2 ^ d)) k := by rw [hcommLow]
          _ = kth d ((lower d xs).map (fun x => x % 2 ^ (d + 1))) k :=
            (ih ((lower d xs).map (fun x => x % 2 ^ (d + 1))) k).symm
      · rw [if_neg hkz, if_neg hkz]
        congr 1
        calc
          kth d (upper d xs) (k - (lower d xs).length) =
              kth d ((upper d xs).map (fun x => x % 2 ^ d)) (k - (lower d xs).length) :=
            ih (upper d xs) (k - (lower d xs).length)
          _ = kth d (((upper d xs).map (fun x => x % 2 ^ (d + 1))).map
              (fun x => x % 2 ^ d)) (k - (lower d xs).length) := by rw [hcommUp]
          _ = kth d ((upper d xs).map (fun x => x % 2 ^ (d + 1)))
              (k - (lower d xs).length) :=
            (ih ((upper d xs).map (fun x => x % 2 ^ (d + 1)))
              (k - (lower d xs).length)).symm

private theorem mod_eq_sub_of_band (d x : Nat) (hlo : 2 ^ d ≤ x)
    (hhi : x < 2 ^ (d + 1)) : x % 2 ^ d = x - 2 ^ d := by
  have hpos : 0 < 2 ^ d := Nat.two_pow_pos d
  have hdiv : x / 2 ^ d = 1 := by
    apply (Nat.div_eq_iff hpos).2
    rw [Nat.pow_succ] at hhi
    constructor
    · simpa using hlo
    · omega
  rw [Nat.mod_eq_sub_div_mul, hdiv]
  simp

private theorem kth_sub_pow (d : Nat) (xs : Word) k
    (hx : ∀ x ∈ xs, 2 ^ d ≤ x ∧ x < 2 ^ (d + 1)) :
    kth d xs k = kth d (xs.map (fun x => x - 2 ^ d)) k := by
  have hmap : xs.map (fun x => x % 2 ^ d) =
      xs.map (fun x => x - 2 ^ d) := by
    apply List.map_congr_left
    intro x hxmem
    have h := hx x hxmem
    exact mod_eq_sub_of_band d x h.1 h.2
  have hlow := kth_lowbits_aux d xs k
  exact hlow.trans (by rw [hmap])

private theorem sorted_select (xs : Word) (hp : xs.Pairwise (· ≤ ·)) :
    ∀ v k, v ∈ xs →
      xs.countP (fun x => decide (x < v)) ≤ k →
      k < xs.countP (fun x => decide (x < v)) + xs.count v → k < xs.length →
      xs[k]? = some v := by
  induction xs with
  | nil => intro v k hv hl hh hk; simp at hk
  | cons x xs ih =>
      intro v k hv hl hh hk
      have hp' := List.pairwise_cons.mp hp
      by_cases hxv : x < v
      · simp only [List.countP_cons, decide_eq_true hxv, ↓reduceIte] at hl hh
        have hkpos : 0 < k := by omega
        let k' := k - 1
        have hkeq : k = k' + 1 := by omega
        rw [hkeq]
        simp only [List.getElem?_cons_succ]
        have hc : (x :: xs).count v = xs.count v := by
          simp [List.count_cons, Nat.ne_of_lt hxv]
        rw [hc] at hh
        apply ih hp'.2 v k'
        · rcases List.mem_cons.mp hv with rfl | hv'
          · exact False.elim (Nat.lt_irrefl _ hxv)
          · exact hv'
        · omega
        · omega
        · simp only [List.length_cons] at hk; omega
      · have hvx : v ≤ x := by omega
        by_cases heq : v = x
        · subst v
          have htail : xs.countP (fun y => decide (y < x)) = 0 := by
            apply List.countP_eq_zero.mpr
            intro y hy hlt
            exact (Nat.not_le_of_gt (of_decide_eq_true hlt)) (hp'.1 y hy)
          have hself : decide (x < x) = false := by simp
          have hc : (x :: xs).count x = xs.count x + 1 := by simp
          have hh' : k < xs.count x + 1 := by
            simpa only [List.countP_cons, hself, Bool.false_eq_true, ↓reduceIte,
              htail, Nat.zero_add, hc] using hh
          by_cases hk0 : k = 0
          · simp [hk0]
          · have hkpos : 0 < k := by omega
            have hcountpos : 0 < xs.count x := by omega
            have hxmem : x ∈ xs := List.count_pos_iff.mp hcountpos
            let k' := k - 1
            have hkeq : k = k' + 1 := by omega
            rw [hkeq]
            simp only [List.getElem?_cons_succ]
            apply ih hp'.2 x k' hxmem (by omega) (by omega)
            simp only [List.length_cons] at hk
            omega
        · have hvlt : v < x := by omega
          rcases List.mem_cons.mp hv with rfl | hv'
          · exact False.elim (hxv (by omega))
          · exact False.elim (Nat.not_le_of_gt hvlt (hp'.1 v hv'))

private theorem countP_map_sub_lt (P : Nat) (xs : Word) (u : Nat)
    (hP : ∀ x ∈ xs, P ≤ x) :
    xs.countP (fun x => decide (x < P + u)) =
      (xs.map (fun x => x - P)).countP (fun y => decide (y < u)) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have hx := hP x (by simp)
      have ih' := ih (by intro y hy; exact hP y (by simp [hy]))
      by_cases hlt : x < P + u
      · have hlt' : x - P < u := by omega
        simp [List.countP_cons, hlt, hlt', ih']
      · have hlt' : ¬ x - P < u := by omega
        simp [List.countP_cons, hlt, hlt', ih']

private theorem count_map_sub_eq (P : Nat) (xs : Word) (u : Nat)
    (hP : ∀ x ∈ xs, P ≤ x) :
    xs.count (P + u) = (xs.map (fun x => x - P)).count u := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have hx := hP x (by simp)
      have ih' := ih (by intro y hy; exact hP y (by simp [hy]))
      by_cases heq : x = P + u
      · have heq' : x - P = u := by omega
        simp [List.count_cons, heq, heq', ih']
      · have heq' : x - P ≠ u := by omega
        simp [List.count_cons, heq, heq', ih']

private theorem count_eq_length_of_all_eq (xs : Word) (v : Nat)
    (hx : ∀ x ∈ xs, x = v) : xs.count v = xs.length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have hxv := hx x (by simp)
      subst x
      simp [ih (by intro y hy; exact hx y (by simp [hy]))]

private theorem kth_spec_aux : ∀ d (xs : Word) k,
    (∀ x ∈ xs, x < 2 ^ d) → k < xs.length →
      let v := kth d xs k
      v ∈ xs ∧
        xs.countP (fun x => decide (x < v)) ≤ k ∧
        k < xs.countP (fun x => decide (x < v)) + xs.count v := by
  intro d
  induction d with
  | zero =>
      intro xs k hx hk
      have hzero : ∀ x ∈ xs, x = 0 := by
        intro x hxmem
        have hlt := hx x hxmem
        simp only [Nat.pow_zero] at hlt
        omega
      have he := List.getElem?_eq_getElem hk
      rw [hzero _ (List.mem_of_getElem? he)] at he
      have hv : (0 : Nat) ∈ xs := List.mem_of_getElem? he
      change (0 : Nat) ∈ xs ∧
        xs.countP (fun x => decide (x < 0)) ≤ k ∧
        k < xs.countP (fun x => decide (x < 0)) + xs.count 0
      have hz : xs.countP (fun x => decide (x < 0)) = 0 := by
        apply List.countP_eq_zero.mpr
        intro x hxmem hlt
        exact Nat.not_lt_zero _ (of_decide_eq_true hlt)
      refine ⟨hv, ?_, ?_⟩
      · rw [hz]
        exact Nat.zero_le _
      · have hc : xs.count 0 = xs.length := count_eq_length_of_all_eq xs 0 hzero
        rw [hz, hc]
        simpa using hk
  | succ d ih =>
      intro xs k hx hk
      let zs := lower d xs
      let us := upper d xs
      have hz : ∀ x ∈ zs, x < 2 ^ d := by
        intro x hxmem
        have hy := List.mem_filter.mp hxmem
        have hbit : x.testBit d = false := by simpa using hy.2
        have hn : ¬ 2 ^ d ≤ x := by
          intro hge
          have ht := Nat.testBit_of_two_pow_le_and_two_pow_add_one_gt hge (hx x hy.1)
          rw [hbit] at ht
          contradiction
        omega
      have huBand : ∀ x ∈ us, 2 ^ d ≤ x ∧ x < 2 ^ (d + 1) := by
        intro x hxmem
        have hy := List.mem_filter.mp hxmem
        exact ⟨Nat.ge_two_pow_of_testBit hy.2, hx x hy.1⟩
      have hperm : (zs ++ us).Perm xs := by
        have hp := List.filter_append_perm (fun x : Nat => x.testBit d) xs
        exact List.perm_append_comm.trans hp
      have hlen : zs.length + us.length = xs.length := by
        simpa using hperm.length_eq
      rw [kth_succ]
      by_cases hkz : k < zs.length
      · rw [if_pos hkz]
        have hrec := ih zs k hz hkz
        obtain ⟨hv, hl, hh⟩ := hrec
        have hvxs : kth d zs k ∈ xs := hperm.mem_iff.mp (List.mem_append_left _ hv)
        refine ⟨hvxs, ?_, ?_⟩
        · have hzero : us.countP (fun x => decide (x < kth d zs k)) = 0 := by
            apply List.countP_eq_zero.mpr
            intro x hxmem hlt
            exact (Nat.not_le_of_gt (of_decide_eq_true hlt))
              (Nat.le_trans (Nat.le_of_lt (hz _ hv)) (huBand x hxmem).1)
          have hcnt : us.count (kth d zs k) = 0 := by
            apply List.count_eq_zero.mpr
            intro hm
            exact (Nat.not_le_of_gt (hz _ hv)) (huBand _ hm).1
          rw [← hperm.countP_eq (fun x => decide (x < kth d zs k)),
            List.countP_append, hzero]
          exact hl
        · have hzero : us.countP (fun x => decide (x < kth d zs k)) = 0 := by
            apply List.countP_eq_zero.mpr
            intro x hxmem hlt
            exact (Nat.not_le_of_gt (of_decide_eq_true hlt))
              (Nat.le_trans (Nat.le_of_lt (hz _ hv)) (huBand x hxmem).1)
          have hcnt : us.count (kth d zs k) = 0 := by
            apply List.count_eq_zero.mpr
            intro hm
            exact (Nat.not_le_of_gt (hz _ hv)) (huBand _ hm).1
          rw [← hperm.countP_eq (fun x => decide (x < kth d zs k)),
            ← hperm.count_eq (kth d zs k), List.countP_append, List.count_append,
            hzero, hcnt]
          omega
      · rw [if_neg hkz]
        let ys := us.map (fun x => x - 2 ^ d)
        let j := k - zs.length
        have hj : j < ys.length := by
          simp only [ys, List.length_map]
          omega
        have hys : ∀ x ∈ ys, x < 2 ^ d := by
          intro x hxmem
          rcases List.mem_map.mp hxmem with ⟨y, hy, rfl⟩
          have h := huBand y hy
          omega
        have hrec := ih ys j hys hj
        obtain ⟨hu, hl, hh⟩ := hrec
        let u := kth d ys j
        have hkth : kth d us j = u := by
          dsimp [u, ys]
          exact kth_sub_pow d us j huBand
        have huHigh : 2 ^ d + u ∈ us := by
          change kth d (us.map (fun x => x - 2 ^ d)) j ∈
            us.map (fun x => x - 2 ^ d) at hu
          rcases List.mem_map.mp hu with ⟨x, hxmem, hxu⟩
          change x - 2 ^ d = u at hxu
          have h := huBand x hxmem
          have heq : 2 ^ d + u = x := by omega
          rw [heq]
          exact hxmem
        have hvxs : 2 ^ d + u ∈ xs :=
          hperm.mem_iff.mp (List.mem_append_right _ huHigh)
        rw [hkth]
        refine ⟨hvxs, ?_, ?_⟩
        · have hzlt : zs.countP (fun x => decide (x < 2 ^ d + u)) = zs.length := by
            rw [List.countP_eq_length_filter]
            apply congrArg List.length
            apply List.filter_eq_self.mpr
            intro x hxmem
            exact decide_eq_true (Nat.lt_of_lt_of_le (hz x hxmem) (Nat.le_add_right _ _))
          have hlt := countP_map_sub_lt (2 ^ d) us u (fun x hxmem => (huBand x hxmem).1)
          dsimp [u, ys, j] at hlt
          rw [← hperm.countP_eq (fun x => decide (x < 2 ^ d + u)),
            List.countP_append, hzlt, hlt]
          dsimp [ys, j] at hl
          omega
        · have hzlt : zs.countP (fun x => decide (x < 2 ^ d + u)) = zs.length := by
            rw [List.countP_eq_length_filter]
            apply congrArg List.length
            apply List.filter_eq_self.mpr
            intro x hxmem
            exact decide_eq_true (Nat.lt_of_lt_of_le (hz x hxmem) (Nat.le_add_right _ _))
          have hzcnt : zs.count (2 ^ d + u) = 0 := by
            apply List.count_eq_zero.mpr
            intro hm
            exact (Nat.not_le_of_gt (hz _ hm)) (Nat.le_add_right _ _)
          have hlt := countP_map_sub_lt (2 ^ d) us u (fun x hxmem => (huBand x hxmem).1)
          have hcnt := count_map_sub_eq (2 ^ d) us u (fun x hxmem => (huBand x hxmem).1)
          dsimp [u, ys, j] at hlt hcnt
          rw [← hperm.countP_eq (fun x => decide (x < 2 ^ d + u)),
            ← hperm.count_eq (2 ^ d + u), List.countP_append, List.count_append,
            hzlt, hlt, hzcnt, hcnt]
          dsimp [ys, j] at hh
          omega

/-- `kth` は安定マージソートの第 k 要素に等しい。下位ビット枝の再帰的仕様
    （値が属すること・自身より小さい要素数が k 以下であること・同値要素数と合わせると k より大きいこと）
    を示し、ソート済み列での位置決定補題に渡す。 -/
theorem sound_Quantile : Wavelet.Quantile.Goal := by
  intro d xs k hx hk
  have hspec := kth_spec_aux d xs k hx hk
  obtain ⟨hv, hl, hh⟩ := hspec
  let ys := xs.mergeSort (· ≤ ·)
  have hperm : ys.Perm xs := List.mergeSort_perm xs (· ≤ ·)
  have hmem : kth d xs k ∈ ys := hperm.mem_iff.mpr hv
  have hlt : ys.countP (fun x => decide (x < kth d xs k)) =
      xs.countP (fun x => decide (x < kth d xs k)) := hperm.countP_eq _
  have hcnt : ys.count (kth d xs k) = xs.count (kth d xs k) := hperm.count_eq _
  have hlen : ys.length = xs.length := hperm.length_eq
  have hsortB : ys.Pairwise (fun a b => decide (a ≤ b) = true) :=
    List.pairwise_mergeSort (le := fun a b => decide (a ≤ b))
      (fun _ _ _ h1 h2 => by
        simpa using Nat.le_trans (of_decide_eq_true h1) (of_decide_eq_true h2))
      (fun a b => by
        simp only [Bool.or_eq_true, decide_eq_true_eq]
        omega) xs
  have hsort : ys.Pairwise (· ≤ ·) :=
    hsortB.imp (fun h => of_decide_eq_true h)
  exact sorted_select ys hsort (kth d xs k) k hmem (by omega) (by omega) (by omega)

end TinylambOpt.ResearchProofs.Atlas.Goal_Quantile

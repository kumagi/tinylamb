import TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.CheckedRewrites.Goal_SquareDistance_FirstFault

open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem fit_bounds {w : Nat} {v : Int} (h : fits w v) : -limit w ≤ v ∧ v < limit w := by
  simp only [fits, Bool.and_eq_true, decide_eq_true_eq] at h
  exact h

theorem fit_of_bounds {w : Nat} {v : Int} (h : -limit w ≤ v ∧ v < limit w) : fits w v := by
  simp only [fits, Bool.and_eq_true, decide_eq_true_eq]
  exact h

theorem limit_pos (w : Nat) : 0 < limit w := by
  unfold limit
  exact_mod_cast (Nat.pow_pos (by decide : 0 < 2) : 0 < 2 ^ (w - 1))

theorem limit_mono {n w : Nat} (h : n ≤ w) : limit n ≤ limit w := by
  unfold limit
  exact_mod_cast (Nat.pow_le_pow_right (by decide : 0 < 2) (Nat.sub_le_sub_right h 1))

theorem fits_mono {n w : Nat} {v : Int} (_hn : 1 < n) (hnw : n ≤ w) (h : fits n v) : fits w v := by
  apply fit_of_bounds
  have hb := fit_bounds h
  have hm := limit_mono hnw
  have hp := limit_pos n
  omega

theorem scalar_eq_fast_of_guard (r : Rule) (hv : foldValue r) (hs : sufficient r)
    {w : Nat} {x y : Int} (hw : 1 < w) (hsafe : safeInputs w (x, y)) (hg : r.guard w x y) :
    scalar r w x y = fast r w x y := by
  have hi := hs w x y hw hsafe hg
  have hvxy := hv x y
  simp [scalar, fast, check, hi]
  change check w 1 (r.finish x y (r.intermediate x y)) = check w 1 (r.folded x y)
  rw [hvxy]

theorem firstFault_of (r : Rule) (hv : foldValue r) (hs : sufficient r) : firstFault r := by
  intro w x y hw hsafe
  unfold dispatch
  split
  · exact (scalar_eq_fast_of_guard r hv hs hw hsafe ‹_›).symm
  · rfl

theorem badRow_from_congr (f g : Int × Int → Except Nat Int) (rs : List (Int × Int)) (i : Nat)
    (h : ∀ p ∈ rs, f p = g p) :
    List.findSome? (fun q => match f q.1 with | .error pc => some (q.2, pc) | .ok _ => none) (rs.zipIdx i) =
    List.findSome? (fun q => match g q.1 with | .error pc => some (q.2, pc) | .ok _ => none) (rs.zipIdx i) := by
  induction rs generalizing i with
  | nil => simp
  | cons p ps ih =>
      have hp : f p = g p := h p (by simp)
      simp only [List.zipIdx_cons, List.findSome?_cons]
      cases hfp : f p with
      | error pc =>
          cases hgp : g p with
          | error pc' =>
              simp [hfp, hgp] at hp ⊢
              subst pc'
              rfl
          | ok a =>
              simp [hfp, hgp] at hp
      | ok a =>
          cases hgp : g p with
          | error pc =>
              simp [hfp, hgp] at hp
          | ok b =>
              simp [hfp, hgp]
              exact ih (i + 1) (by intro q hq; exact h q (by simp [hq]))

theorem badRow_congr (f g : Int × Int → Except Nat Int) (rs : List (Int × Int))
    (h : ∀ p ∈ rs, f p = g p) : badRow f rs = badRow g rs := by
  exact badRow_from_congr f g rs 0 h

theorem hoistGuard_of (r : Rule) (hv : foldValue r) (hs : sufficient r) : hoistGuard r := by
  intro w rows hw hsafe hguard
  apply badRow_congr
  intro p hp
  have hsf : safeInputs w p := by
    have hsall := hsafe
    simp only [List.all_eq_true] at hsall
    exact hsall p hp
  have hgd : r.guard w p.1 p.2 = true := by
    have hgall := hguard
    simp only [List.all_eq_true] at hgall
    exact hgall p hp
  exact (scalar_eq_fast_of_guard r hv hs hw hsf hgd).symm

theorem narrowVersion_of (r : Rule) (hv : foldValue r) (hs : sufficient r) : narrowVersion r := by
  intro n w x y hn hnw hsafe hg hfold
  have hin := hs n x y (by omega) hsafe hg
  have hiw := fits_mono hn hnw hin
  have hfw := fits_mono hn hnw hfold
  simp [fast, scalar, check, hiw, hfold, hfw]
  change Except.ok (r.folded x y) = check w 1 (r.finish x y (r.intermediate x y))
  rw [hv x y]
  simp [check, hfw]

theorem le_trans_int {α : Int} {β : Int} {γ : Int} (h1 : α ≤ β) (h2 : β ≤ γ) : α ≤ γ := by omega

theorem intAbs_of_nonneg {z : Int} (h : 0 ≤ z) : (z.natAbs : Int) = z := Int.natAbs_of_nonneg h

theorem intAbs_of_neg {z : Int} (h : z < 0) : (z.natAbs : Int) = -z := by
  rw [← Int.natAbs_neg]
  exact Int.natAbs_of_nonneg (by omega)

theorem natAbs_pos_of_ne_zero {z : Int} (h : z ≠ 0) : 0 < z.natAbs := by
  cases hn : z.natAbs with
  | zero =>
      have hz : z = 0 ∨ z = -0 := Int.natAbs_eq_iff.mp (by omega)
      omega
  | succ n => omega

theorem fits_of_natAbs_lt {w : Nat} {z : Int} (h : z.natAbs < (limit w).toNat) : fits w z := by
  apply fit_of_bounds
  have hp := limit_pos w
  have h' : (z.natAbs : Int) < limit w := by
    have hc : ((z.natAbs : Nat) : Int) < ((limit w).toNat : Int) := by exact_mod_cast h
    rwa [Int.toNat_of_nonneg (by omega)] at hc
  by_cases hz : 0 ≤ z
  · rw [intAbs_of_nonneg hz] at h'
    omega
  · rw [intAbs_of_neg (by omega)] at h'
    omega

theorem double_limit {w : Nat} (hw : 1 < w) : limit w = 2 * limit (w - 1) := by
  have hnat : (2 : Nat) ^ (w - 1) = 2 * 2 ^ (w - 2) := by
    rw [show w - 1 = (w - 2) + 1 by omega, Nat.pow_succ, Nat.mul_comm]
  unfold limit
  exact_mod_cast hnat

theorem half_limit {w : Nat} (hw : 1 < w) : limit w / 2 = limit (w - 1) := by
  rw [double_limit hw]
  exact Int.mul_ediv_cancel_left (limit (w - 1)) (by decide : (2 : Int) ≠ 0)

theorem two_mul_half_limit {w : Nat} : 2 * (limit w / 2) ≤ limit w := by
  have hp := limit_pos w
  omega

-- FoldValue proofs

theorem mul_nonpos_of_xor {x y : Int} (h : (x < 0) != (y < 0)) : x * y ≤ 0 := by
  by_cases hx : x < 0 <;> by_cases hy : y < 0 <;> simp [hx, hy] at h ⊢
  · exact Int.mul_nonpos_of_nonpos_of_nonneg (by omega) (by omega)
  · exact Int.mul_nonpos_of_nonneg_of_nonpos (by omega) (by omega)

theorem mul_self_ge_sq {t L : Int} (hp : 0 < L) (ht : L ≤ t) : L * L ≤ t * t := by
  have hL0 : 0 ≤ L := by omega
  have ht0 : 0 ≤ t := by omega
  have h1 : L * L ≤ t * L := Int.mul_le_mul_of_nonneg_right ht hL0
  have h2 : t * L ≤ t * t := Int.mul_le_mul_of_nonneg_left ht ht0
  omega

theorem mul_self_ge_sq_neg {t L : Int} (hp : 0 < L) (ht : t ≤ -L) : L * L ≤ t * t := by
  have hL0 : 0 ≤ L := by omega
  have hnt0 : 0 ≤ -t := by omega
  have h1 : L ≤ -t := by omega
  have ha : L * L ≤ (-t) * L := Int.mul_le_mul_of_nonneg_right h1 hL0
  have hb : (-t) * L ≤ (-t) * (-t) := Int.mul_le_mul_of_nonneg_left h1 hnt0
  have hc : L * L ≤ (-t) * (-t) := by omega
  rwa [show (-t) * (-t) = t * t by rw [Int.neg_mul, Int.mul_neg, Int.neg_neg]] at hc

theorem scalar_eq_check_of_fits {r : Rule} {w : Nat} {x y : Int}
    (hi : fits w (r.intermediate x y)) :
    scalar r w x y = check w 1 (r.finish x y (r.intermediate x y)) := by
  simp [scalar, check, hi]
  change check w 1 (r.finish x y (r.intermediate x y)) = check w 1 (r.finish x y (r.intermediate x y))
  rfl

theorem scalar_error_of_not_fits {r : Rule} {w : Nat} {x y : Int}
    (hi : ¬ fits w (r.intermediate x y)) : scalar r w x y = .error 0 := by
  simp [scalar, check, hi]
  change Except.error 0 = Except.error 0
  rfl

theorem natAbs_neg_limit (w : Nat) (hp : 0 < limit w) : (-limit w).natAbs = (limit w).toNat := by
  rw [Int.natAbs_neg]
  exact (Int.natAbs_eq_iff).2 (Or.inl (by rw [Int.toNat_of_nonneg (by omega : 0 ≤ limit w)]))

theorem mul_bounds (a b c d x y : Int) (hax : a ≤ x) (hxb : x ≤ b)
    (hcy : c ≤ y) (hyd : y ≤ d) :
    min (min (a * c) (a * d)) (min (b * c) (b * d)) ≤ x * y ∧
      x * y ≤ max (max (a * c) (a * d)) (max (b * c) (b * d)) := by
  have hay_lo : min (a * c) (a * d) ≤ a * y := by
    by_cases ha : 0 ≤ a
    · exact le_trans_int (Int.min_le_left _ _) (Int.mul_le_mul_of_nonneg_left hcy ha)
    · exact le_trans_int (Int.min_le_right _ _) (Int.mul_le_mul_of_nonpos_left (by omega : a ≤ 0) hyd)
  have hay_hi : a * y ≤ max (a * c) (a * d) := by
    by_cases ha : 0 ≤ a
    · exact le_trans_int (Int.mul_le_mul_of_nonneg_left hyd ha) (Int.le_max_right _ _)
    · exact le_trans_int (Int.mul_le_mul_of_nonpos_left (by omega : a ≤ 0) hcy) (Int.le_max_left _ _)
  have hby_lo : min (b * c) (b * d) ≤ b * y := by
    by_cases hb : 0 ≤ b
    · exact le_trans_int (Int.min_le_left _ _) (Int.mul_le_mul_of_nonneg_left hcy hb)
    · exact le_trans_int (Int.min_le_right _ _) (Int.mul_le_mul_of_nonpos_left (by omega : b ≤ 0) hyd)
  have hby_hi : b * y ≤ max (b * c) (b * d) := by
    by_cases hb : 0 ≤ b
    · exact le_trans_int (Int.mul_le_mul_of_nonneg_left hyd hb) (Int.le_max_right _ _)
    · exact le_trans_int (Int.mul_le_mul_of_nonpos_left (by omega : b ≤ 0) hcy) (Int.le_max_left _ _)
  constructor
  · by_cases hy : 0 ≤ y
    · have haxy : a * y ≤ x * y := Int.mul_le_mul_of_nonneg_right hax hy
      have hm : min (min (a*c) (a*d)) (min (b*c) (b*d)) ≤ min (a*c) (a*d) := Int.min_le_left _ _
      omega
    · have hbxy : b * y ≤ x * y := Int.mul_le_mul_of_nonpos_right hxb (by omega : y ≤ 0)
      have hm : min (min (a*c) (a*d)) (min (b*c) (b*d)) ≤ min (b*c) (b*d) := Int.min_le_right _ _
      omega
  · by_cases hy : 0 ≤ y
    · have hxby : x * y ≤ b * y := Int.mul_le_mul_of_nonneg_right hxb hy
      have hm : max (b*c) (b*d) ≤ max (max (a*c) (a*d)) (max (b*c) (b*d)) := Int.le_max_right _ _
      omega
    · have hxay : x * y ≤ a * y := Int.mul_le_mul_of_nonpos_right hax (by omega : y ≤ 0)
      have hm : max (a*c) (a*d) ≤ max (max (a*c) (a*d)) (max (b*c) (b*d)) := Int.le_max_left _ _
      omega

theorem fv_SquareDistance : SquareDistance.FoldValue.Goal := by
  intro x y
  simp [SquareDistance.rule]
  rw [Int.sub_mul, Int.mul_sub, Int.mul_sub, Int.mul_comm y x, Int.mul_assoc]
  omega

theorem su_SquareDistance : SquareDistance.Sufficient.Goal := by
  intro w x y hw hsafe hg
  have hs := hsafe
  simp only [safeInputs, Bool.and_eq_true, decide_eq_true_eq] at hs
  have hx := fit_bounds hs.1
  by_cases hy : 0 ≤ y
  · have hg' : y - limit w ≤ x := by simpa [SquareDistance.rule, hy] using hg
    simp [SquareDistance.rule]
    apply fit_of_bounds
    omega
  · have hg' : x < limit w + y := by simpa [SquareDistance.rule, hy] using hg
    simp [SquareDistance.rule]
    apply fit_of_bounds
    omega

theorem sound : SquareDistance.FirstFault.Goal := by
  exact firstFault_of SquareDistance.rule fv_SquareDistance su_SquareDistance

end TinylambOpt.ResearchProofs.JitFrontiers.CheckedRewrites.Goal_SquareDistance_FirstFault

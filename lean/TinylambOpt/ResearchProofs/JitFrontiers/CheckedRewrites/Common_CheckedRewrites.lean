import TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.CheckedRewrites.Common

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

end TinylambOpt.ResearchProofs.JitFrontiers.CheckedRewrites.Common

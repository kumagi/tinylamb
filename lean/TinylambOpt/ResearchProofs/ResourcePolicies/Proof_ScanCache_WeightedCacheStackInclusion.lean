import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache

namespace TinylambOpt.ResearchProofs.ResourcePolicies.ScanCache.Goal_WeightedCacheStackInclusion

open TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache


/-!
  A smaller byte cap leaves a prefix of the larger cap's cache.  This is the
  weighted analogue of LRU stack inclusion: lowering the scan-cache byte budget
  can drop MRU entries, but it never reorders the entries that remain.
  -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache

namespace Helpers

def bs (bytes : Nat → Nat) (l : List Nat) : Nat := (l.map bytes).sum

theorem fit_prefix (bytes : Nat → Nat) : ∀ (cap : Nat) (l : List Nat),
    fit bytes cap l <+: l
  | _, [] => by simp [fit]
  | cap, i :: is => by
      by_cases h : bytes i ≤ cap
      · simpa [fit, h] using fit_prefix bytes (cap - bytes i) is
      · simp [fit, h]

theorem fit_sum_le (bytes : Nat → Nat) : ∀ (cap : Nat) (l : List Nat),
    bs bytes (fit bytes cap l) ≤ cap
  | _, [] => by simp [fit, bs]
  | cap, i :: is => by
      by_cases h : bytes i ≤ cap
      · simp only [fit, h, ite_true]
        simp [bs]
        have ih := fit_sum_le bytes (cap - bytes i) is
        simp [bs] at ih
        omega
      · simp [fit, h, bs]

theorem fit_max (bytes : Nat → Nat) : ∀ (cap : Nat) (l p : List Nat),
    p <+: l → bs bytes p ≤ cap → p <+: fit bytes cap l
  | cap, [], p, hp, hs => by
      cases p with
      | nil => simp [fit]
      | cons j p' =>
          rcases hp with ⟨t, ht⟩
          simp at ht
  | cap, i :: is, p, hp, hs => by
      cases p with
      | nil => exact List.nil_prefix
      | cons j p' =>
          rcases hp with ⟨t, ht⟩
          simp only [List.cons_append] at ht
          cases ht
          have hsum_aux : bytes i + (List.map bytes p').sum ≤ cap := by
            simpa [bs] using hs
          have hsum : bs bytes p' ≤ cap - bytes i := by
            simp [bs]
            omega
          by_cases h : bytes i ≤ cap
          · simpa [fit, h] using
              (fit_max bytes (cap - bytes i) (p' ++ t) p' ⟨t, rfl⟩ hsum)
          · omega

theorem fit_mono_cap (bytes : Nat → Nat) {a b : Nat} (hab : a ≤ b) (l : List Nat) :
    fit bytes a l <+: fit bytes b l := by
  exact fit_max bytes b l (fit bytes a l) (fit_prefix bytes a l)
    (Nat.le_trans (fit_sum_le bytes a l) hab)

theorem fit_mono_list (bytes : Nat → Nat) (cap : Nat) {l₁ l₂ : List Nat}
    (h : l₁ <+: l₂) : fit bytes cap l₁ <+: fit bytes cap l₂ := by
  exact fit_max bytes cap l₂ (fit bytes cap l₁)
    ((fit_prefix bytes cap l₁).trans h) (fit_sum_le bytes cap l₁)

theorem fit_fit (bytes : Nat → Nat) {a b : Nat} (hab : a ≤ b) (l : List Nat) :
    fit bytes a (fit bytes b l) = fit bytes a l := by
  apply List.IsPrefix.eq_of_length_le (fit_mono_list bytes a (fit_prefix bytes b l))
  exact (fit_max bytes a (fit bytes b l) (fit bytes a l)
    (fit_mono_cap bytes hab l) (fit_sum_le bytes a l)).length_le

theorem shift (bytes : Nat → Nat) (k : Nat) : ∀ (d : Nat) (xs : List Nat),
    fit bytes d (xs.erase k) = fit bytes d ((fit bytes (d + bytes k) xs).erase k)
  | d, [] => by simp [fit]
  | d, x :: xs => by
      by_cases hxk : x = k
      · subst x
        simp [fit, Nat.add_sub_cancel_left]
        exact (fit_fit bytes (Nat.le_refl d) xs).symm
      · have hne : (x == k) = false := by simp [hxk]
        simp only [List.erase_cons, hne, ite_false]
        by_cases hxd : bytes x ≤ d
        · have hxbig : bytes x ≤ d + bytes k := by omega
          simp [fit, hxd, hxbig, hne]
          have hs := shift bytes k (d - bytes x) xs
          have hcap : d - bytes x + bytes k = d + bytes k - bytes x := by omega
          simpa [hcap] using hs
        · by_cases hxbig : bytes x ≤ d + bytes k
          · simp [fit, hxd, hxbig, hne]
          · simp [fit, hxd, hxbig, hne]

theorem fit_cons_erase_fit (bytes : Nat → Nat) (cap k : Nat) (c : List Nat) :
    fit bytes cap (k :: c.erase k) = fit bytes cap (k :: (fit bytes cap c).erase k) := by
  by_cases hbk : bytes k ≤ cap
  · have hcap : cap - bytes k + bytes k = cap := by omega
    simpa [fit, hbk, hcap] using shift bytes k (cap - bytes k) c
  · simp [fit, hbk]

theorem touch_commute (bytes : Nat → Nat) {a b : Nat} (hab : a ≤ b)
    (c : List Nat) (k : Nat) :
    fit bytes a (touch bytes b c k) = touch bytes a (fit bytes a c) k := by
  unfold touch
  rw [fit_fit bytes hab]
  exact fit_cons_erase_fit bytes a k c

theorem run_shrink (bytes : Nat → Nat) {a b : Nat} (hab : a ≤ b) :
    ∀ (c : List Nat) (trace : List Nat),
      fit bytes a (run bytes b c trace) = run bytes a (fit bytes a c) trace
  | c, [] => by simp [run]
  | c, k :: ks => by
      change fit bytes a (run bytes b (touch bytes b c k) ks) =
        run bytes a (touch bytes a (fit bytes a c) k) ks
      rw [run_shrink bytes hab (touch bytes b c k) ks]
      rw [touch_commute bytes hab c k]


end Helpers

open Helpers

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache.WeightedCacheStackInclusion.Goal := by
  intro bytes a b trace hab
  have hs : fit bytes a (run bytes b [] trace) = run bytes a [] trace := by
    simpa [fit] using run_shrink bytes hab [] trace
  rw [← hs]
  exact fit_prefix bytes a (run bytes b [] trace)
end TinylambOpt.ResearchProofs.ResourcePolicies.ScanCache.Goal_WeightedCacheStackInclusion

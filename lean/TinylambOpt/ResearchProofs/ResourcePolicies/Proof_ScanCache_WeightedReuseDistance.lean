import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache

namespace TinylambOpt.ResearchProofs.ResourcePolicies.ScanCache.Goal_WeightedReuseDistance

open TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache


/-!
  After reversing the trace, the resident set is the maximal byte-bounded prefix
  of the distinct intervening keys followed by the target.  Therefore a key is a
  hit exactly when its own bytes plus the distinct intervening bytes fit in the
  cap; counting intervening keys is insufficient for heterogeneous blocks.
  -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

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



theorem eraseDups_erase (x : Nat) (l : List Nat) :
    (l.eraseDups).erase x = (l.filter (fun y => ! y == x)).eraseDups := by
  match l with
  | [] => simp
  | y :: ys =>
      rw [List.eraseDups_cons]
      by_cases hy : y = x
      · subst y
        simp
      · have hne : (y == x) = false := by simp [hy]
        have hfilt : List.filter (fun z : Nat => ! z == x) (y :: ys) =
            y :: List.filter (fun z : Nat => ! z == x) ys := by simp [hne]
        rw [hfilt, List.eraseDups_cons]
        have ih0 := eraseDups_erase x ys
        have ih1 := eraseDups_erase x (List.filter (fun z : Nat => ! z == y) ys)
        have hcomm : List.filter (fun a : Nat => !a == x && !a == y) ys =
            List.filter (fun a : Nat => !a == y && !a == x) ys := by
          simp [Bool.and_comm]
        simpa [List.erase_cons, hne, ih1, List.filter_filter] using congrArg List.eraseDups hcomm
termination_by l.length
decreasing_by
  all_goals
    have hc : (y :: ys).length = ys.length + 1 := by simp
    have h0 : ys.length < (y :: ys).length := by omega
    have h1 := List.length_filter_le (fun z : Nat => ! z == y) ys
    omega


theorem run_reverse (bytes : Nat → Nat) (cap : Nat) : ∀ r : List Nat,
    run bytes cap [] r.reverse = fit bytes cap r.eraseDups
  | [] => by simp [run, fit]
  | x :: r => by
      rw [List.reverse_cons]
      change run bytes cap [] (r.reverse ++ [x]) = fit bytes cap (x :: r).eraseDups
      simp only [run, List.foldl_append, List.foldl_cons, List.foldl_nil]
      change touch bytes cap (run bytes cap [] r.reverse) x = fit bytes cap (x :: r).eraseDups
      rw [run_reverse bytes cap r]
      have herase : (x :: r).eraseDups = x :: (r.eraseDups).erase x := by
        rw [List.eraseDups_cons]
        congr 1
        exact (eraseDups_erase x r).symm
      rw [herase]
      unfold touch
      exact (fit_cons_erase_fit bytes cap x r.eraseDups).symm


theorem mem_of_prefix {a : Nat} {l p : List Nat} (h : p <+: l) (ha : a ∈ p) : a ∈ l := by
  rcases h with ⟨t, ht⟩
  rw [← ht]
  simp [ha]

theorem prefix_append_singleton (p H : List Nat) (k : Nat) :
    p <+: H ++ [k] → p <+: H ∨ p = H ++ [k] := by
  induction H generalizing p with
  | nil =>
      intro hp
      cases p with
      | nil => exact Or.inl (by simp)
      | cons x xs =>
          rcases hp with ⟨t, ht⟩
          simp at ht
          rcases ht with ⟨hxs, ht'⟩
          subst k
          rcases ht' with ⟨hxs', htnil⟩
          subst xs
          subst t
          exact Or.inr rfl
  | cons x xs ih =>
      intro hp
      cases p with
      | nil => exact Or.inl (by simp)
      | cons y ys =>
          rcases hp with ⟨t, ht⟩
          simp only [List.cons_append] at ht
          injection ht with hy htail
          subst y
          rcases ih ys ⟨t, htail⟩ with hys | hys
          · exact Or.inl (by simpa using hys)
          · right
            simpa [hys]

theorem mem_fit_last_iff (bytes : Nat → Nat) (cap : Nat) (H : List Nat) (k : Nat)
    (hkH : k ∉ H) :
    k ∈ fit bytes cap (H ++ [k]) ↔ bs bytes H + bytes k ≤ cap := by
  constructor
  · intro hk
    rcases prefix_append_singleton (fit bytes cap (H ++ [k])) H k (fit_prefix bytes cap (H ++ [k])) with hH | heq
    · exact False.elim (hkH (mem_of_prefix hH hk))
    · have hs := fit_sum_le bytes cap (H ++ [k])
      rw [heq] at hs
      simpa [bs] using hs
  · intro hs
    have hs' : bs bytes (H ++ [k]) ≤ cap := by simpa [bs] using hs
    have hp : H ++ [k] <+: fit bytes cap (H ++ [k]) :=
      fit_max bytes cap (H ++ [k]) (H ++ [k]) List.prefix_rfl hs'
    exact mem_of_prefix hp (by simp)

theorem perm_sum (f : Nat → Nat) : ∀ {l₁ l₂ : List Nat},
    l₁.Perm l₂ → (l₁.map f).sum = (l₂.map f).sum
  | _, _, .nil => rfl
  | _, _, .cons a p => by simpa using perm_sum f p
  | _, _, .swap a b l => by simp [Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]
  | _, _, .trans p q => by rw [perm_sum f p, perm_sum f q]

theorem not_mem_erase_self_of_nodup {a : Nat} : ∀ l : List Nat,
    l.Nodup → a ∉ l.erase a
  | [], hnd => by simp
  | x :: xs, hnd => by
      by_cases hxa : x = a
      · subst x
        simpa using (List.nodup_cons.mp hnd).1
      · have hax : a ≠ x := fun h => hxa h.symm
        have hne : (x == a) = false := by simp [hxa]
        simp [List.erase_cons_tail, hne, hax, not_mem_erase_self_of_nodup (a := a) xs (List.nodup_cons.mp hnd).2]

theorem perm_of_nodup_mem : ∀ (l₁ l₂ : List Nat),
    l₁.Nodup → l₂.Nodup → (∀ x, x ∈ l₁ ↔ x ∈ l₂) → l₁.Perm l₂
  | [], l₂, _, hnd₂, hm => by
      cases l₂ with
      | nil => exact .nil
      | cons y ys =>
          have hy : y ∈ ([] : List Nat) := (hm y).mpr (by simp)
          simp at hy
  | a :: xs, l₂, hnd₁, hnd₂, hm => by
      have ha_not : a ∉ xs := (List.nodup_cons.mp hnd₁).1
      have hxs_nd : xs.Nodup := (List.nodup_cons.mp hnd₁).2
      have ha_mem : a ∈ l₂ := (hm a).mp (by simp)
      have hp : l₂.Perm (a :: l₂.erase a) := List.perm_cons_erase ha_mem
      have hnd_erase : (l₂.erase a).Nodup := List.Nodup.erase a hnd₂
      have hm_erase : ∀ x, x ∈ xs ↔ x ∈ l₂.erase a := by
        intro x
        by_cases hxa : x = a
        · subst x
          constructor
          · intro hx
            exact False.elim (ha_not hx)
          · intro hx
            exact False.elim (not_mem_erase_self_of_nodup (a := a) l₂ hnd₂ hx)
        · rw [List.mem_erase_of_ne hxa]
          have hxmem := hm x
          simp [hxa] at hxmem
          exact hxmem
      have hp_xs : xs.Perm (l₂.erase a) := perm_of_nodup_mem xs (l₂.erase a) hxs_nd hnd_erase hm_erase
      exact (List.Perm.cons a hp_xs).trans hp.symm

theorem nodup_eraseDups (l : List Nat) : l.eraseDups.Nodup := by
  match l with
  | [] => simp
  | x :: xs =>
      rw [List.eraseDups_cons]
      refine List.nodup_cons.mpr ⟨?_, nodup_eraseDups (xs.filter (fun y => ! y == x))⟩
      intro h
      have hm := List.mem_eraseDups.mp h
      simp at hm
termination_by l.length
decreasing_by
  all_goals
    have hle := List.length_filter_le (fun y : Nat => ! y == x) xs
    simp at hle ⊢
    omega

theorem sum_eraseDups_reverse (bytes : Nat → Nat) (l : List Nat) :
    bs bytes l.reverse.eraseDups = bs bytes l.eraseDups := by
  have hp : l.reverse.eraseDups.Perm l.eraseDups := by
    apply perm_of_nodup_mem
    · exact nodup_eraseDups l.reverse
    · exact nodup_eraseDups l
    · intro x
      rw [List.mem_eraseDups, List.mem_eraseDups, List.mem_reverse]
  exact perm_sum bytes hp

theorem singleton_removeAll_eq_of_not_mem (k : Nat) : ∀ H : List Nat,
    k ∉ H → [k].removeAll H = [k]
  | [] => by simp
  | x :: xs => by
      intro hk
      have hxk : x ≠ k := by
        intro h
        exact hk (by simp [h])
      have hkx : k ≠ x := fun h => hk (by simp [h])
      have hne : (k == x) = false := by simp [hkx]
      simp [List.removeAll_cons, hne, singleton_removeAll_eq_of_not_mem k xs (by
        intro h
        exact hk (by simp [h]))]

theorem eraseDups_append_singleton_of_not_mem (H : List Nat) (k : Nat) (hk : k ∉ H) :
    (H ++ [k]).eraseDups = H.eraseDups ++ [k] := by
  rw [List.eraseDups_append]
  rw [singleton_removeAll_eq_of_not_mem k H hk]
  rw [List.eraseDups_cons]
  simp


end Helpers

open Helpers
open TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache WeightedReuseDistance

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache.WeightedReuseDistance.Goal := by
  intro bytes cap k between hkb
  have hkrev : k ∉ between.reverse.eraseDups := by
    intro hk
    have : k ∈ between.reverse := List.mem_eraseDups.mp hk
    exact hkb (by simpa using this)
  have hrun : run bytes cap [] (k :: between) =
      fit bytes cap (between.reverse.eraseDups ++ [k]) := by
    rw [← eraseDups_append_singleton_of_not_mem between.reverse k (by simpa using hkb)]
    rw [← run_reverse bytes cap (between.reverse ++ [k])]
    simp
  rw [hrun]
  rw [mem_fit_last_iff bytes cap between.reverse.eraseDups k hkrev]
  rw [sum_eraseDups_reverse]
  simpa [bs, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]
end TinylambOpt.ResearchProofs.ResourcePolicies.ScanCache.Goal_WeightedReuseDistance

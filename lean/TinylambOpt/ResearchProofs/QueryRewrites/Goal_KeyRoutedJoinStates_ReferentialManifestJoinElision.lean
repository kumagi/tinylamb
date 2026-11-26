import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
# Goal_KeyRoutedJoinStates_ReferentialManifestJoinElision

A complete unique manifest makes every file lookup return exactly one row, so
the executor can omit the lookup while preserving row count and nullable
identifier sum.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_ReferentialManifestJoinElision

open TinylambOpt.ResearchGoals.QueryRewrites

private theorem count_eq_one_of_nodup_mem {xs : List Nat} (hnd : xs.Nodup) {a : Nat} (ha : a ∈ xs) :
    xs.count a = 1 := by
  induction xs with
  | nil => simp at ha
  | cons x xs ih =>
      change List.Pairwise (fun a b : Nat => a ≠ b) (x :: xs) at hnd
      rw [List.pairwise_cons] at hnd
      by_cases hax : a = x
      · subst a
        have hnot : ¬ x ∈ xs := by
          intro hx
          exact hnd.1 x hx rfl
        simp [hnot]
        exact List.count_eq_zero.mpr hnot
      · have hain : a ∈ xs := by
          cases ha with
          | head => exact False.elim (hax rfl)
          | tail a h => exact h
        have hne : x ≠ a := hnd.1 a hain
        simp [hax, hne, ih hnd.2 hain]

private theorem nsum_none_cons (xs : List (Option Int)) : nsum (none :: xs) = nsum xs := by
  simp [nsum]

private theorem sum_filterMap_eq_of_nsum_eq {xs ys : List (Option Int)} (h : nsum xs = nsum ys) :
    (xs.filterMap id).sum = (ys.filterMap id).sum := by
  unfold nsum at h
  by_cases hx : (xs.filterMap id).isEmpty
  · rw [List.isEmpty_iff] at hx
    by_cases hy : (ys.filterMap id).isEmpty
    · rw [List.isEmpty_iff] at hy
      simp [hx, hy]
    · simp [hx, hy] at h
  · by_cases hy : (ys.filterMap id).isEmpty
    · rw [List.isEmpty_iff] at hy
      simp [hx, hy] at h
    · simp [hx, hy] at h ⊢
      exact h

theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.ReferentialManifestJoinElision.Goal := by
  intro rows files hnodup hmem
  change
    let expanded := rows.flatMap fun (k, id) => List.replicate (files.count k) id
    (expanded.length, nsum expanded) = (rows.length, nsum (rows.map Prod.snd))
  dsimp only
  induction rows with
  | nil => simp [nsum]
  | cons r rs ih =>
      rcases r with ⟨k, id⟩
      have hk : k ∈ files := hmem (k, id) (by simp)
      have hc : files.count k = 1 := count_eq_one_of_nodup_mem hnodup hk
      have hmem' : ∀ r ∈ rs, r.1 ∈ files := by
        intro r hr
        exact hmem r (by simp [hr])
      have ih' := ih hmem'
      have ih1 := congrArg Prod.fst ih'
      have ih2 := congrArg Prod.snd ih'
      simp at ih1 ih2
      apply Prod.ext
      · simp [hc, ih1]
      · cases id with
        | none =>
            simp [hc]
            rw [nsum_none_cons, nsum_none_cons]
            exact ih2
        | some v =>
            have hsum := sum_filterMap_eq_of_nsum_eq ih2
            simp [hc, nsum, hsum]

end TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_ReferentialManifestJoinElision

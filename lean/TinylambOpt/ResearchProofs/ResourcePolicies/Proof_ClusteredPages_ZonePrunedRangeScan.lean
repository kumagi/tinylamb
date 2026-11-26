import TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages

/-!
Proof contribution for ResourcePolicies_ClusteredPages_ZonePrunedRangeScan. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
namespace TinylambOpt.ResearchProofs.ResourcePolicies.ClusteredPages.Goal_ZonePrunedRangeScan

open TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages


private theorem candidate_of_range {lo hi k : Nat} {p : Page}
    (hk : k ∈ p.keys) (hlo : lo ≤ k) (hhi : k ≤ hi) : candidate lo hi p = true := by
  unfold candidate
  simp only [Bool.and_eq_true, Option.any_eq_true]
  have hne : p.keys ≠ [] := List.ne_nil_of_mem hk
  have hmin_some : p.keys.min?.isSome = true := (List.isSome_min?_iff).mpr hne
  have hmax_some : p.keys.max?.isSome = true := (List.isSome_max?_iff).mpr hne
  obtain ⟨mn, hmn⟩ := Option.isSome_iff_exists.mp hmin_some
  obtain ⟨mx, hmx⟩ := Option.isSome_iff_exists.mp hmax_some
  refine ⟨⟨mn, hmn, ?_⟩, ⟨mx, hmx, ?_⟩⟩
  · have hle : mn ≤ k := (List.min?_eq_some_iff.mp hmn).2 k hk
    exact decide_eq_true_iff.mpr (by omega)
  · have hle : k ≤ mx := (List.max?_eq_some_iff.mp hmx).2 k hk
    exact decide_eq_true_iff.mpr (by omega)

private theorem rangeRows_append (lo hi : Nat) (xs ys : List Nat) :
    rangeRows lo hi (xs ++ ys) = rangeRows lo hi xs ++ rangeRows lo hi ys := by
  unfold rangeRows
  rw [List.filter_append]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages.ZonePrunedRangeScan.Goal := by
  intro ps lo hi
  induction ps with
  | nil => simp [rangeRows]
  | cons p ps ih =>
      rw [List.filter_cons]
      by_cases hc : candidate lo hi p = true
      · simp [hc, ih, rangeRows_append]
      · have hfilter : rangeRows lo hi p.keys = [] := by
          unfold rangeRows
          apply (List.filter_eq_nil_iff).mpr
          intro k hk hrange
          simp only [Bool.and_eq_true, decide_eq_true_iff] at hrange
          exact hc (candidate_of_range hk hrange.1 hrange.2)
        simp [hc, ih, hfilter, rangeRows_append]

end TinylambOpt.ResearchProofs.ResourcePolicies.ClusteredPages.Goal_ZonePrunedRangeScan

import TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ClusteredPages_ZonePrunedRangeScan
open TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages ZonePrunedRangeScan

private theorem candidate_of_range {lo hi k : Nat} {p : Page}
    (hk : k ∈ p.keys) (hlo : lo ≤ k) (hhi : k ≤ hi) : candidate lo hi p = true := by
  unfold candidate
  simp only [Bool.and_eq_true, Option.any_eq_true]
  have hne : p.keys ≠ [] := List.ne_nil_of_mem hk
  have hmin_some : p.keys.min?.isSome = true := by rw [List.isSome_min?_iff]; exact hne
  have hmax_some : p.keys.max?.isSome = true := by rw [List.isSome_max?_iff]; exact hne
  obtain ⟨mn, hmn⟩ := Option.isSome_iff_exists.mp hmin_some
  obtain ⟨mx, hmx⟩ := Option.isSome_iff_exists.mp hmax_some
  refine ⟨⟨mn, hmn, ?_⟩, ⟨mx, hmx, ?_⟩⟩
  · exact decide_eq_true_iff.mpr (Nat.le_trans ((List.min?_eq_some_iff.mp hmn).2 k hk) hhi)
  · exact decide_eq_true_iff.mpr (Nat.le_trans hlo ((List.max?_eq_some_iff.mp hmx).2 k hk))

theorem sound : Goal := by
  intro ps lo hi
  unfold rangeRows
  induction ps with
  | nil => simp
  | cons p ps ih =>
    by_cases hc : candidate lo hi p = true
    · simp [List.filter, hc, ih]
    · simp only [List.filter, hc, List.flatMap_cons, List.filter_append, ite_false, List.nil_append]
      have hempty : List.filter (fun k => lo ≤ k && k ≤ hi) p.keys = [] := by
        rw [List.filter_eq_nil_iff]
        intro k hk
        simp only [Bool.and_eq_false_imp, Bool.and_eq_true, decide_eq_true_eq]
        intro h
        exact hc (candidate_of_range hk h.1 h.2)
      rw [hempty, List.nil_append]
      exact ih
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ClusteredPages_ZonePrunedRangeScan

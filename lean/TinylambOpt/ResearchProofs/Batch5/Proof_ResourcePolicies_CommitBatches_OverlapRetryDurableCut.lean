import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

/-!
Proof contribution for ResourcePolicies_CommitBatches_OverlapRetryDurableCut. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CommitBatches.Goal_OverlapRetryDurableCut

open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches


open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

/-- An overlapping retry adds no newly covered byte, so coverage of the
    concatenated completion list is pointwise identical to the original. -/
theorem cover_append_eq (ws extra : List Write)
    (h : ∀ i, cover extra i = true → cover ws i = true) :
    ∀ i, cover (ws ++ extra) i = cover ws i := by
  intro i
  simp only [cover, List.any_append]
  by_cases hws : (ws.any (fun w => w.start ≤ i && i < w.start + w.count)) = true
  · simp [hws]
  · by_cases he : (extra.any (fun w => w.start ≤ i && i < w.start + w.count)) = true
    · have hwt : cover ws i = true := h i (by simpa [cover] using he)
      simp [cover, hws] at hwt
    · simp [hws, he]

/-- Retried writes whose bytes were already confirmed cannot move the first
    uncovered byte, hence the safe fsync cut is unchanged. -/
theorem sound : OverlapRetryDurableCut.Goal := by
  intro ws extra base limit h
  have hcov := cover_append_eq ws extra h
  have hfun : (fun i => cover (ws ++ extra) i) = cover ws := by
    funext i
    exact hcov i
  simp [prefixEnd, hfun]
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CommitBatches.Goal_OverlapRetryDurableCut

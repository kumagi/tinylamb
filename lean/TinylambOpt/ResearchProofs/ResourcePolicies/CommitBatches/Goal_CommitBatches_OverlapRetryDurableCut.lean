import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! WAL group-commit claim for OverlapRetryDurableCut. The proof fixes what a contiguous durable
cut may acknowledge while keeping the same client-visible commit order. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CommitBatches_OverlapRetryDurableCut

open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

theorem sound : OverlapRetryDurableCut.Goal := by
  intro ws extra base limit hsub
  have hcov : ∀ i, cover (ws ++ extra) i = cover ws i := by
    intro i
    unfold cover
    rw [List.any_append]
    by_cases hw : ws.any (fun w => w.start ≤ i && i < w.start + w.count) = true
    · simp [hw]
    · by_cases he : extra.any (fun w => w.start ≤ i && i < w.start + w.count) = true
      · have hce : cover extra i = true := by simpa [cover] using he
        have hcw : cover ws i = true := hsub i hce
        simp [cover] at hcw
        simp [hcw]
      · simp [hw, he]
  unfold prefixEnd
  have hfun : (fun i => cover (ws ++ extra) (base + i)) = (fun i => cover ws (base + i)) := by
    funext i
    exact hcov (base + i)
  rw [hfun]

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CommitBatches_OverlapRetryDurableCut

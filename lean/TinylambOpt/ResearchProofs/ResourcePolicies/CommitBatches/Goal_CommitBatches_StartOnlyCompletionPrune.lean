import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! WAL group-commit claim for StartOnlyCompletionPrune. The proof fixes what a contiguous durable
cut may acknowledge while keeping the same client-visible commit order. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CommitBatches_StartOnlyCompletionPrune

open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

theorem refute : ¬ StartOnlyCompletionPrune.Goal := by
  intro h
  have hc := h ([⟨0,5⟩] : List Write) 3 10
  simp at hc
  exact (by native_decide : ¬ (prefixEnd [⟨0,5⟩] 3 10 = prefixEnd ([] : List Write) 3 10)) hc.symm

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CommitBatches_StartOnlyCompletionPrune

import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! WAL group-commit claim for LargestCompletionWakesPrefix. The proof fixes what a contiguous durable
cut may acknowledge while keeping the same client-visible commit order. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CommitBatches_LargestCompletionWakesPrefix

open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

theorem refute : ¬ LargestCompletionWakesPrefix.Goal := by
  intro h
  have hc := h ([⟨4,1⟩, ⟨0,1⟩] : List Write) 5 (by decide) (by decide)
  exact (by native_decide : ¬ (prefixEnd [⟨4,1⟩, ⟨0,1⟩] 0 5 = 5)) hc

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CommitBatches_LargestCompletionWakesPrefix

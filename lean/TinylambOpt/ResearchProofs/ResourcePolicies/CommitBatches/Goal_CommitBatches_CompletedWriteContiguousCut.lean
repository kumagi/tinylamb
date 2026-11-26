import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches
import TinylambOpt.ResearchProofs.ResourcePolicies.Common_CommitBatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! WAL group-commit claim for CompletedWriteContiguousCut. The proof fixes what a contiguous durable
cut may acknowledge while keeping the same client-visible commit order. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CommitBatches_CompletedWriteContiguousCut

open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

theorem sound : CompletedWriteContiguousCut.Goal := by
  intro ws base limit t hbase ht
  exact TinylambOpt.ResearchProofs.ResourcePolicies.Common_CommitBatches.prefixEnd_iff
    ws base limit t hbase ht

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CommitBatches_CompletedWriteContiguousCut

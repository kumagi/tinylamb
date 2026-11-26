import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches
import TinylambOpt.ResearchProofs.ResourcePolicies.Common_CommitBatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! WAL group-commit claim for IncrementalCompletionScan. The proof fixes what a contiguous durable
cut may acknowledge while keeping the same client-visible commit order. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CommitBatches_IncrementalCompletionScan

open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

theorem sound : IncrementalCompletionScan.Goal := by
  intro a b base limit hbl
  let X := a ++ b
  let q := prefixEnd a base limit
  have hbase_q : base ≤ q := by change base ≤ prefixEnd a base limit; unfold prefixEnd; omega
  have hq_limit : q ≤ limit :=
    TinylambOpt.ResearchProofs.ResourcePolicies.Common_CommitBatches.prefixEnd_le_limit a base limit hbl
  have hp1_limit : prefixEnd X base limit ≤ limit :=
    TinylambOpt.ResearchProofs.ResourcePolicies.Common_CommitBatches.prefixEnd_le_limit X base limit hbl
  have hq_cov : ∀ i, base ≤ i → i < q → cover X i = true := by
    intro i hib hiq
    have hca := (TinylambOpt.ResearchProofs.ResourcePolicies.Common_CommitBatches.prefixEnd_iff
      a base limit q hbase_q hq_limit).mp (by omega) i hib hiq
    have hca' : a.any (fun w => w.start ≤ i && i < w.start + w.count) = true := by
      simpa [cover] using hca
    unfold X
    unfold cover
    rw [List.any_append]
    simp [hca']
  have hq_le_p1 : q ≤ prefixEnd X base limit :=
    (TinylambOpt.ResearchProofs.ResourcePolicies.Common_CommitBatches.prefixEnd_iff
      X base limit q hbase_q hq_limit).mpr hq_cov
  apply Nat.le_antisymm
  · have hq_le_p2 : q ≤ prefixEnd X q limit := by change q ≤ prefixEnd X q limit; unfold prefixEnd; omega
    have hbase_p2 : base ≤ prefixEnd X q limit := by omega
    apply (TinylambOpt.ResearchProofs.ResourcePolicies.Common_CommitBatches.prefixEnd_iff
      X base limit (prefixEnd X q limit) hbase_p2
      (TinylambOpt.ResearchProofs.ResourcePolicies.Common_CommitBatches.prefixEnd_le_limit X q limit hq_limit)).mpr
    intro i hbi hip2
    by_cases hiq : i < q
    · exact hq_cov i hbi hiq
    · exact TinylambOpt.ResearchProofs.ResourcePolicies.Common_CommitBatches.cover_of_lt_prefixEnd
        X q limit i (by omega) hip2
  · have hp2_limit : prefixEnd X q limit ≤ limit :=
      TinylambOpt.ResearchProofs.ResourcePolicies.Common_CommitBatches.prefixEnd_le_limit X q limit hq_limit
    apply (TinylambOpt.ResearchProofs.ResourcePolicies.Common_CommitBatches.prefixEnd_iff
      X q limit (prefixEnd X base limit) hq_le_p1 hp1_limit).mpr
    intro i hqi hip1
    exact TinylambOpt.ResearchProofs.ResourcePolicies.Common_CommitBatches.cover_of_lt_prefixEnd
      X base limit i (by omega) hip1

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CommitBatches_IncrementalCompletionScan

import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! WAL group-commit claim for OrderedCommitWaitPrefix. The proof fixes what a contiguous durable
cut may acknowledge while keeping the same client-visible commit order. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CommitBatches_OrderedCommitWaitPrefix

open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

theorem sound : OrderedCommitWaitPrefix.Goal := by
  intro ws d hp
  induction ws with
  | nil => simp [wake]
  | cons w ws ih =>
      rw [List.pairwise_cons] at hp
      by_cases hw : w.stop ≤ d
      · rw [List.takeWhile_cons]
        simp [hw]
        rw [show wake (w::ws) d = w :: wake ws d by simp [wake, hw]]
        rw [ih hp.2]
      · have hnone : wake ws d = [] := by
          unfold wake
          rw [List.filter_eq_nil_iff]
          intro w' hw'
          have hw'le : w.stop ≤ w'.stop := hp.1 w' (by simp [hw'])
          simp
          omega
        rw [List.takeWhile_cons]
        simp [hw]
        rw [show wake (w::ws) d = wake ws d by simp [wake, hw]]
        exact hnone

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CommitBatches_OrderedCommitWaitPrefix

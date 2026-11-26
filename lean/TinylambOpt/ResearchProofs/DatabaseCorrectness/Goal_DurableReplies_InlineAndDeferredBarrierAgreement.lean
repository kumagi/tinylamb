import TinylambOpt.ResearchGoals.DatabaseCorrectness.DurableReplies

/-! A deferred reply ticket waiting for both barriers is admissible exactly
    when every dependency end and the writer's own end are durable. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.DurableReplies.Goal_InlineAndDeferredBarrierAgreement

open TinylambOpt.ResearchGoals.DatabaseCorrectness.DurableReplies

private theorem maxD_le_iff (l : List Nat) (d : Nat) :
    l.max?.getD 0 ≤ d ↔ ∀ x ∈ l, x ≤ d := by
  cases hm : l.max? with
  | none =>
      have hl : l = [] := List.max?_eq_none_iff.mp hm
      simp [hl]
  | some a =>
      simp only [Option.getD_some]
      exact List.max?_le_iff hm

theorem sound : InlineAndDeferredBarrierAgreement.Goal := by
  intro t d
  by_cases hs : t.sync <;>
    apply Bool.eq_iff_iff.mpr <;>
    simp [sequentialWaits, target, hs, List.all_eq, maxD_le_iff, Nat.max_le,
      decide_eq_true_eq, Bool.and_eq_true]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.DurableReplies.Goal_InlineAndDeferredBarrierAgreement

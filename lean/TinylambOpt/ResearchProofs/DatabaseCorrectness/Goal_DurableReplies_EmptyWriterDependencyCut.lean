import TinylambOpt.ResearchGoals.DatabaseCorrectness.DurableReplies

/-! A read-only ticket with no observed dependencies waits for nothing; with
    dependencies it waits for their maximum, independently of the sync flag. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.DurableReplies.Goal_EmptyWriterDependencyCut

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

theorem sound : EmptyWriterDependencyCut.Goal := by
  intro seen d sync
  change max (if sync then 0 else 0) (seen.max?.getD 0) ≤ d ↔ ∀ endPos ∈ seen, endPos ≤ d
  simpa using maxD_le_iff seen d

end TinylambOpt.ResearchProofs.DatabaseCorrectness.DurableReplies.Goal_EmptyWriterDependencyCut

import TinylambOpt.ResearchGoals.DatabaseCorrectness.DurableReplies

/-! A success reply is only emitted by a published, append-and-install-complete
    writer after all required ends have become durable. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.DurableReplies.Goal_WriteReplyRequiresPublicationAndBarrier

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

private theorem seq_wait_eq (t : Ticket) (d : Nat) :
    sequentialWaits t d = decide (target t ≤ d) := by
  by_cases hs : t.sync <;>
    apply Bool.eq_iff_iff.mpr <;>
    simp [sequentialWaits, target, hs, List.all_eq, maxD_le_iff, Nat.max_le,
      decide_eq_true_eq, Bool.and_eq_true]

theorem sound : WriteReplyRequiresPublicationAndBarrier.Goal := by
  intro a p t d ok hsucc
  by_cases ha : a = true
  · by_cases hp : p = true
    · by_cases hok : ok = true
      · have htarget : target t ≤ d := by
          by_cases ht : target t ≤ d
          · exact ht
          · exfalso
            simp [launch, complete, ha, hp, hok, ht] at hsucc
        refine ⟨ha, hp, hok, ?_⟩
        rw [seq_wait_eq t d]
        simp [htarget]
      · exfalso
        simp [launch, complete, ha, hp, hok] at hsucc
    · exfalso
      simp [launch, complete, ha, hp] at hsucc
  · exfalso
    simp [launch, complete, ha] at hsucc

end TinylambOpt.ResearchProofs.DatabaseCorrectness.DurableReplies.Goal_WriteReplyRequiresPublicationAndBarrier

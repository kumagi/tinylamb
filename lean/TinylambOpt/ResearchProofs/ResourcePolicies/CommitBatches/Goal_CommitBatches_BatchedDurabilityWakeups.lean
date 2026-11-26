import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! WAL group-commit claim for BatchedDurabilityWakeups. The proof fixes what a contiguous durable
cut may acknowledge while keeping the same client-visible commit order. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CommitBatches_BatchedDurabilityWakeups

open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

theorem sound : BatchedDurabilityWakeups.Goal := by
  intro ws a b hab
  induction ws with
  | nil => simp [wake, pending]
  | cons w ws ih =>
      by_cases ha : w.stop ≤ a
      · have hb : w.stop ≤ b := Nat.le_trans ha hab
        rw [show wake (w::ws) a = w :: wake ws a by simp [wake, ha],
            show pending (w::ws) a = pending ws a by simp [pending, ha],
            show wake (w::ws) b = w :: wake ws b by simp [wake, hb]]
        exact List.Perm.cons w ih
      · by_cases hb : w.stop ≤ b
        · have halt : a < w.stop := Nat.lt_of_not_ge ha
          rw [show wake (w::ws) a = wake ws a by simp [wake, ha],
              show pending (w::ws) a = w :: pending ws a by simp [pending, ha, halt],
              show wake (w::ws) b = w :: wake ws b by simp [wake, hb],
              show wake (w :: pending ws a) b = w :: wake (pending ws a) b by simp [wake, hb]]
          exact List.perm_middle.trans (List.Perm.cons w ih)
        · have halt : a < w.stop := Nat.lt_of_not_ge ha
          rw [show wake (w::ws) a = wake ws a by simp [wake, ha],
              show pending (w::ws) a = w :: pending ws a by simp [pending, ha, halt],
              show wake (w::ws) b = wake ws b by simp [wake, hb],
              show wake (w :: pending ws a) b = wake (pending ws a) b by simp [wake, hb]]
          exact ih

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CommitBatches_BatchedDurabilityWakeups

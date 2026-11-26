import TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

/-!
A commit batching policy must preserve the durability and visibility guarantees of
committed SQL transactions.

Proved

- `StickyAck` — Once a waiter can reply it stays eligible across retries and concurrent
  appends.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_StickyAck

open TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

theorem sound : StickyAck.Goal := by
  intro s es ws w hw
  induction es generalizing s with
  | nil => exact hw
  | cons e es ih =>
      have hd : s.durable ≤ (step s e).durable := by
        rcases s with ⟨a,wr,d,f⟩
        cases e <;> simp [step]
        cases f <;> simp_all [Nat.le_max_left]
      apply ih (step s e)
      simp [ready, List.mem_filter] at hw ⊢
      exact ⟨hw.1, Nat.le_trans hw.2 hd⟩

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_StickyAck

import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.

Proved

- `ObjectRanges.CoalescePaidTail` — `∀ u f a b c, a≤b → b≤c → (span a b+u)/(u+1)=(span
  a c+u)/(u+1) → price u f a c=price u f a b`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_ObjectRanges_CoalescePaidTail

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.ObjectRanges

-- truth probe on small inputs
#eval (List.range 6).all (fun u =>
  (List.range 4).all (fun f =>
    (List.range 10).all (fun a =>
      (List.range 10).all (fun b =>
        (List.range 10).all (fun c =>
          if a ≤ b && b ≤ c then
            if (span a b + u) / (u + 1) = (span a c + u) / (u + 1) then
              price u f a c == price u f a b
            else true
          else true)))))

/-- With unit u+1, align charges ((n+u)/(u+1))*(u+1): the `+unit-1` offset
    simplifies to `+u` because u+1 is positive. -/
private theorem align_succ (u n : Nat) :
    TinylambOpt.ResearchGoals.Compression.Wire.align (u + 1) n = ((n + u) / (u + 1)) * (u + 1) := by
  unfold TinylambOpt.ResearchGoals.Compression.Wire.align
  have h : n + (u + 1) - 1 = n + u := by omega
  rw [h]

theorem sound_CoalescePaidTail : ObjectRanges.CoalescePaidTail.Goal := by
  intro u f a b c hab hbc hquot
  unfold price
  rw [align_succ, align_succ]
  rw [hquot]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_ObjectRanges_CoalescePaidTail

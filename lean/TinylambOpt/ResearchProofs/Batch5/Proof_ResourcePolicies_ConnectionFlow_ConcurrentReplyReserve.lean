import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ConnectionFlow.Goal_ConcurrentReplyReserve

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow


/-! Per-connection slack accounting: when every admitted load is at most
`hi + maxFrame`, reserving one extra control frame of `control` bytes per
connection keeps each contribution within `hi + maxFrame + control`, so the sum
over connections is bounded by the connection count times that limit. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace CrpProof

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

/-- Induction over the connection list, with the fixed per-connection bound. -/
theorem reserve_sum : ∀ (loads : List Nat) (hi maxFrame control : Nat),
    (∀ n ∈ loads, n ≤ hi + maxFrame) →
    (loads.map (· + control)).sum ≤ loads.length * (hi + maxFrame + control)
  | [], _, _, _, _ => by simp
  | n :: ns, hi, maxFrame, control, hbound => by
      have hn : n ≤ hi + maxFrame := hbound n (by simp)
      have hns : ∀ m ∈ ns, m ≤ hi + maxFrame := by
        intro m hm
        exact hbound m (by simp [hm])
      have ih := reserve_sum ns hi maxFrame control hns
      simp only [List.map_cons, List.sum_cons, List.length_cons, Nat.succ_mul]
      omega

end CrpProof

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow.ConcurrentReplyReserve.Goal :=
  CrpProof.reserve_sum
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ConnectionFlow.Goal_ConcurrentReplyReserve

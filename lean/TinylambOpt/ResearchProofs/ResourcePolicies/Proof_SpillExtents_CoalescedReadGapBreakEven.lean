import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.Goal_CoalescedReadGapBreakEven

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents


/-! Scratch proof: coalescing adjacent extent reads replaces one start-up cost
    per chunk with the bytes of the gaps. The stated break-even inequality is
    just cancellation of the common payload total, so the rewrite is safe
    exactly when the gap bytes fit in the saved start costs. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents.CoalescedReadGapBreakEven.Goal := by
  intro sizes gaps startCost hlen
  rw [hlen, Nat.add_mul, Nat.one_mul]
  omega
end TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.Goal_CoalescedReadGapBreakEven

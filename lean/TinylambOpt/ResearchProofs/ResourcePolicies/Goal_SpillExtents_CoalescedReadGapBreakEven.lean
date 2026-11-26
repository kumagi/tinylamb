import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents
/-!
Registers the checked result for `SpillExtents.CoalescedReadGapBreakEven.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_CoalescedReadGapBreakEven
open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents CoalescedReadGapBreakEven

theorem sound : Goal := by
  intro sizes gaps startCost hlen
  rw [hlen]
  -- (gaps.length + 1) * startCost = gaps.length * startCost + startCost
  have hmul : (gaps.length + 1) * startCost = gaps.length * startCost + startCost := by
    rw [Nat.add_mul, Nat.one_mul]
  constructor <;> intro h <;> omega
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_CoalescedReadGapBreakEven

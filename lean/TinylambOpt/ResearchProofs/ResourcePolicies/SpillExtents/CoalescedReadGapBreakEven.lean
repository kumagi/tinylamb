import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.CoalescedReadGapBreakEven

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

theorem sound : CoalescedReadGapBreakEven.Goal := by
  intro sizes gaps startCost hlen
  have hlen' : sizes.length = gaps.length + 1 := hlen
  constructor
  · intro h
    -- startCost + sizes.sum + gaps.sum ≤ sizes.length * startCost + sizes.sum
    -- => startCost + gaps.sum ≤ (gaps.length + 1) * startCost
    -- => gaps.sum ≤ gaps.length * startCost
    have h2 : startCost + gaps.sum ≤ sizes.length * startCost := by omega
    rw [hlen'] at h2
    -- (gaps.length + 1) * startCost = gaps.length * startCost + startCost
    have h3 : (gaps.length + 1) * startCost = gaps.length * startCost + startCost := by
      rw [Nat.add_mul, Nat.one_mul]
    omega
  · intro h
    rw [hlen']
    have h3 : (gaps.length + 1) * startCost = gaps.length * startCost + startCost := by
      rw [Nat.add_mul, Nat.one_mul]
    omega

end TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.CoalescedReadGapBreakEven

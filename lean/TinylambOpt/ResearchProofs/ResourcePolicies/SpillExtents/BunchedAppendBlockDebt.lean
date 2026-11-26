import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.BunchedAppendBlockDebt

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

private theorem blocks_mono (u : Nat) {n m : Nat} (h : n ≤ m) : blocks u n ≤ blocks u m := by
  unfold blocks
  apply Nat.div_le_div_right
  omega

theorem sound : BunchedAppendBlockDebt.Goal := by
  intro u n a b hu
  have h1 : blocks u n ≤ blocks u (n + a) := blocks_mono u (Nat.le_add_right n a)
  have h2 : blocks u (n + a) ≤ blocks u (n + a + b) := blocks_mono u (by omega)
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.BunchedAppendBlockDebt

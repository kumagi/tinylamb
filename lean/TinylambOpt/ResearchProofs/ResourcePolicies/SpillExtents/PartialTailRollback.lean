import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.PartialTailRollback

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

theorem sound : PartialTailRollback.Goal := by
  intro old payload written
  simp [List.take_append_of_le_length (Nat.le_refl old.length)]

end TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.PartialTailRollback

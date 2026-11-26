import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.Goal_ExtentGenerationReadEquivalence

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents


/-! Scratch proof: a reader pinned to the old extent mapping sees the same
    bytes after private compaction. Relocation collisions are harmless because
    the hypothesis forces the copied payload to agree for every directory
    entry, and flatMap preserves directory order. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

private theorem flatMap_relocate_eq (directory : List Nat) (old new : Nat → List Nat)
    (relocate : Nat → Nat)
    (h : ∀ id ∈ directory, new (relocate id) = old id) :
    (directory.map relocate).flatMap new = directory.flatMap old := by
  induction directory with
  | nil => simp
  | cons id ids ih =>
      have hid : new (relocate id) = old id := h id (by simp)
      have htail : ∀ j ∈ ids, new (relocate j) = old j := by
        intro j hj
        exact h j (by simp [hj])
      simp [ih htail, hid]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents.ExtentGenerationReadEquivalence.Goal := by
  intro directory old new relocate start len h
  rw [flatMap_relocate_eq directory old new relocate h]
end TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.Goal_ExtentGenerationReadEquivalence

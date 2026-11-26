import TinylambOpt.ResearchGoals.DatabaseCorrectness.SnapshotIndex
import TinylambOpt.ResearchProofs.DatabaseCorrectness.SnapshotIndexCommon

/-!
Appending any nonempty field suffix to a matched nonempty prefix keeps the
encoded compound key strictly inside the prefix fence: it is longer than the
bare prefix and its first extra byte (the VARCHAR tag 2) is below the 0xFF
sentinel used by `EncodeEndParts`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SnapshotIndex_AllDescendantsBelowPrefixFence

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SnapshotIndex
open TinylambOpt.ResearchProofs.DatabaseCorrectness.SnapshotIndexCommon

theorem sound : AllDescendantsBelowPrefixFence.Goal := by
  intro pre rest hpre hrest
  have hlenpos : 0 < rest.length := by
    cases rest with
    | nil => contradiction
    | cons x xs => simp
  constructor
  · rw [image_append]
    exact lt_append_self (image pre) (image rest) (image_nonempty rest hrest)
  · have harity : pre.length < pre.length + rest.length := by omega
    rw [image_append]
    simp [upper, harity, hpre]
    exact image_head_lt_ff rest hrest

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SnapshotIndex_AllDescendantsBelowPrefixFence

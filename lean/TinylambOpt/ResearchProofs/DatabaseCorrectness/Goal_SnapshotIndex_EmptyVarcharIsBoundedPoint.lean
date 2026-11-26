import TinylambOpt.ResearchGoals.DatabaseCorrectness.SnapshotIndex
import TinylambOpt.ResearchProofs.DatabaseCorrectness.SnapshotIndexCommon

/-!
A one-column range whose lower and upper bound are both the encoded empty
VARCHAR is a point range: it admits exactly the empty value. The empty field
has the minimal binary encoding, so no nonempty value can satisfy the upper
bound.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SnapshotIndex_EmptyVarcharIsBoundedPoint

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SnapshotIndex
open TinylambOpt.ResearchGoals.DatabaseCorrectness.VarcharKeys (encode)
open TinylambOpt.ResearchProofs.DatabaseCorrectness.SnapshotIndexCommon

theorem sound : EmptyVarcharIsBoundedPoint.Goal := by
  intro v
  have hle : encode [] ≤ encode v := encode_empty_le v
  have heq : encode v = encode [] ↔ v = [] := encode_eq_empty v
  by_cases hv : v = []
  · subst v
    simp [mayRead, image, upper]
  · have hle_rev : encode v ≤ encode [] ↔ v = [] := by
      constructor
      · intro h
        exact heq.mp (List.le_antisymm h hle)
      · intro h
        subst v
        simp
    simp [mayRead, image, upper, hle, hle_rev]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SnapshotIndex_EmptyVarcharIsBoundedPoint

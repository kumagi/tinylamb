import TinylambOpt.ResearchGoals.DatabaseCorrectness.ChunkSlices

/-! Slicing a compact constant column before expansion yields the same logical
    cells as slicing the expanded flat column, even past its end. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.ChunkSlices.Goal_ConstantOrFlatSliceDecodesExactly

open TinylambOpt.ResearchGoals.DatabaseCorrectness.ChunkSlices

theorem sound : ConstantOrFlatSliceDecodesExactly.Goal := by
  intro c i n
  cases c with
  | flat xs => rfl
  | constant size x =>
      change List.replicate (min n (size - i)) x = ((List.replicate size x).drop i).take n
      rw [List.drop_replicate, List.take_replicate]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.ChunkSlices.Goal_ConstantOrFlatSliceDecodesExactly

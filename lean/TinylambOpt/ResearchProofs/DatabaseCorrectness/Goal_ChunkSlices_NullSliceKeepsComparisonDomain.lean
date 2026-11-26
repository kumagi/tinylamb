import TinylambOpt.ResearchGoals.DatabaseCorrectness.ChunkSlices

/-! A slice containing only NULLs still carries the column collation, because
    comparison domain metadata travels with the chunk, not with payload bytes. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.ChunkSlices.Goal_NullSliceKeepsComparisonDomain

open TinylambOpt.ResearchGoals.DatabaseCorrectness.ChunkSlices

private theorem rows_portion (c : Layout) (i n : Nat) :
    rows (portion c i n) = ((rows c).drop i).take n := by
  cases c with
  | flat xs => rfl
  | constant size x =>
      change List.replicate (min n (size - i)) x = ((List.replicate size x).drop i).take n
      rw [List.drop_replicate, List.take_replicate]

theorem sound : NullSliceKeepsComparisonDomain.Goal := by
  intro c off n h
  rcases c with ⟨coll, column, positions⟩
  simp [slice, rows_portion]
  simpa [List.all_eq_true] using h

end TinylambOpt.ResearchProofs.DatabaseCorrectness.ChunkSlices.Goal_NullSliceKeepsComparisonDomain

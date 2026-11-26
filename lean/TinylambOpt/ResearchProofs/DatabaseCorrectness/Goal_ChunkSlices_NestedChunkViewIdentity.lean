import TinylambOpt.ResearchGoals.DatabaseCorrectness.ChunkSlices

/-! A nested chunk view is the same view as one slice with combined offsets and
    the second limit clipped by the first; collation and positions follow too. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.ChunkSlices.Goal_NestedChunkViewIdentity

open TinylambOpt.ResearchGoals.DatabaseCorrectness.ChunkSlices

private theorem take_drop_take {α : Type} (xs : List α) (i n j k : Nat) :
    (((xs.drop i).take n).drop j).take k = (xs.drop (i + j)).take (min k (n - j)) := by
  rw [List.drop_take, List.drop_drop, List.take_take]

private theorem min_min_sub (size i n j k : Nat) :
    min k (min n (size - i) - j) = min (min k (n - j)) (size - (i + j)) := by
  rw [← Nat.sub_min_sub_right n (size - i) j, Nat.sub_sub]
  exact (Nat.min_assoc k (n - j) (size - (i + j))).symm

theorem sound : NestedChunkViewIdentity.Goal := by
  intro c i n j k
  rcases c with ⟨coll, column, positions⟩
  cases column with
  | flat xs =>
      cases positions <;> simp [slice, portion, take_drop_take]
  | constant size x =>
      cases positions <;> simp [slice, portion, min_min_sub, take_drop_take]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.ChunkSlices.Goal_NestedChunkViewIdentity

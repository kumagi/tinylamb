import TinylambOpt.ResearchGoals.DatabaseCorrectness.ChunkSlices

/-! Physical row positions stay paired with their data cells across a spill and
    a projected slice. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.ChunkSlices.Goal_SpillPositionsStayWithSlicedRows

open TinylambOpt.ResearchGoals.DatabaseCorrectness.ChunkSlices

private theorem rows_portion (c : Layout) (i n : Nat) :
    rows (portion c i n) = ((rows c).drop i).take n := by
  cases c with
  | flat xs => rfl
  | constant size x =>
      change List.replicate (min n (size - i)) x = ((List.replicate size x).drop i).take n
      rw [List.drop_replicate, List.take_replicate]

private theorem zip_drop_take {α β : Type} (xs : List α) (ys : List β) (off n : Nat) :
    (((xs.zip ys).drop off).take n) =
      ((xs.drop off).take n).zip ((ys.drop off).take n) := by
  rw [show (xs.zip ys).drop off = (xs.drop off).zip (ys.drop off) by
    simpa [List.zip] using (List.drop_zipWith (f := Prod.mk) (l := xs) (l' := ys) (i := off))]
  rw [show ((xs.drop off).zip (ys.drop off)).take n =
      ((xs.drop off).take n).zip ((ys.drop off).take n) by
    simpa [List.zip] using (List.take_zipWith (f := Prod.mk) (l := xs.drop off) (l' := ys.drop off) (i := n))]

theorem sound : SpillPositionsStayWithSlicedRows.Goal := by
  intro c ids off n hpos
  rcases c with ⟨coll, column, positions⟩
  change positions = some ids at hpos
  subst positions
  simp [slice, rows_portion]
  exact (zip_drop_take (rows column) ids off n).symm

end TinylambOpt.ResearchProofs.DatabaseCorrectness.ChunkSlices.Goal_SpillPositionsStayWithSlicedRows

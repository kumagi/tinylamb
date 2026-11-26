import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

/-! A prefix scan across concatenated frames seeds the second frame with the
first frame's endpoint. The scan accumulator is the base plus the frame sum, so
SQL can resume a morsel from one carry value. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_HorizontalScanChunkSeed

open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

theorem foldlAdd (b : Int) (a : List Int) : List.foldl (· + ·) b a = b + a.sum := by
  induction a generalizing b with
  | nil => simp
  | cons x xs ih => simp [ih, List.sum_cons, Int.add_assoc]

theorem sound : HorizontalScanChunkSeed.Goal := by
  intro b a c
  unfold values
  rw [List.scanl_append]
  rw [foldlAdd]
  simp only [List.drop_append]
  have hz : 1 - (List.scanl (fun x y : Int => x + y) b a).length = 0 := by
    rw [List.length_scanl]
    omega
  rw [hz]
  simp [List.drop_one]

end TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_HorizontalScanChunkSeed

import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_HorizontalScanChunkSeed

open TinylambOpt.ResearchGoals.JitKernels

theorem sound : TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.HorizontalScanChunkSeed.Goal := by
  intro b a c
  have hfold : a.foldl (fun x y : Int => x + y) b = b + a.sum := by
    induction a generalizing b with
    | nil => simp
    | cons x xs ih => simp [ih, Int.add_assoc]
  simp [DeltaLaneScan.values, List.scanl_append, hfold]

end TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_HorizontalScanChunkSeed

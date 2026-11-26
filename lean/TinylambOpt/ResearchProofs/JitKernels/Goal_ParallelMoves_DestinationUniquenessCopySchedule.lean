import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ParallelMoves_DestinationUniquenessCopySchedule

open TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.ParallelMoves.DestinationUniquenessCopySchedule.Goal := by
  intro h
  let ms : List Move := [(1, 0), (0, 1)]
  let s : File := fun r => r
  have hnd : (ms.map Prod.fst).Nodup := by native_decide
  have heq := h ms s hnd
  have hbad : serial s ms 0 ≠ parallelCopy s ms 0 := by native_decide
  exact hbad (congrFun heq 0)

end TinylambOpt.ResearchProofs.JitKernels.Goal_ParallelMoves_DestinationUniquenessCopySchedule

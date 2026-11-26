import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-!
Uniqueness of destinations is not sufficient for a straight serial lowering:
an earlier destination may still be the source of a later move. The counterexample
has distinct destinations 1 and 3, but the first move clobbers register 1.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_ParallelMoves_DestinationUniquenessCopySchedule
open TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

theorem refute : ¬ DestinationUniquenessCopySchedule.Goal := by
  intro h
  have hnodup : (([(1, 2), (3, 1)] : List Move).map Prod.fst).Nodup := by native_decide
  have heq := h [(1, 2), (3, 1)] (fun r => r) hnodup
  exact (by native_decide :
    serial (fun r => r) [(1, 2), (3, 1)] 3 ≠
      parallelCopy (fun r => r) [(1, 2), (3, 1)] 3) (congrFun heq 3)

end Nov26.Proof_ParallelMoves_DestinationUniquenessCopySchedule

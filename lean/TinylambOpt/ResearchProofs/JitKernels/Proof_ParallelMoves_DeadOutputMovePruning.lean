import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-!
A move whose destination is dead at the exit cannot always be deleted: it may
clobber the register read by a later live move. In the counterexample the dead
move `(1,0)` supplies the value used by the live move `(2,1)`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_ParallelMoves_DeadOutputMovePruning
open TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

theorem refute : ¬ DeadOutputMovePruning.Goal := by
  intro h
  have heq := h (fun r => r) [(1, 0), (2, 1)] [2] 2 (by native_decide)
  exact (by native_decide :
    serial (fun r => r) ([(1, 0), (2, 1)].filter (fun m => [2].contains m.1)) 2 ≠
      serial (fun r => r) [(1, 0), (2, 1)] 2) heq

end Nov26.Proof_ParallelMoves_DeadOutputMovePruning

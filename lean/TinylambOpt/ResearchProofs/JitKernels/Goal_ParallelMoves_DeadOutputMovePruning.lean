import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

-- Refutation: a move to a dead register can still feed a later move to a live register.

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ParallelMoves_DeadOutputMovePruning

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.ParallelMoves.DeadOutputMovePruning.Goal := by
  intro h
  have c := h (fun r : Nat => if r == 0 then 5 else 7) ([(0, 1), (1, 0)] : List (Nat × Nat))
    [1] 1 (by native_decide)
  exact (by native_decide : ¬ (
    TinylambOpt.ResearchGoals.JitKernels.ParallelMoves.serial
      (fun r : Nat => if r == 0 then 5 else 7)
      (([(0, 1), (1, 0)] : List (Nat × Nat)).filter
        (fun m => [1].contains m.1)) 1 =
    TinylambOpt.ResearchGoals.JitKernels.ParallelMoves.serial
      (fun r : Nat => if r == 0 then 5 else 7) ([(0, 1), (1, 0)] : List (Nat × Nat)) 1)) c

end TinylambOpt.ResearchProofs.JitKernels.Goal_ParallelMoves_DeadOutputMovePruning

import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

-- Refutation: a borrow from a matched zero byte contaminates the next subtract-mask lane.

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_BorrowMasks_AdjacentDelimiterIntersection

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.BorrowMasks.AdjacentDelimiterIntersection.Goal := by
  intro h
  have c := h (0x10007 : BitVec 64) (0 : BitVec 8) (0 : BitVec 8)
  exact (by native_decide : ¬ (
    ((List.range 7).filter (fun i =>
      (TinylambOpt.ResearchGoals.JitKernels.BorrowMasks.marked
        (TinylambOpt.ResearchGoals.JitKernels.BorrowMasks.fast
          ((0x10007 : BitVec 64) ^^^ TinylambOpt.ResearchGoals.JitKernels.BorrowMasks.repeatByte 0))).contains i &&
      (TinylambOpt.ResearchGoals.JitKernels.BorrowMasks.marked
        (TinylambOpt.ResearchGoals.JitKernels.BorrowMasks.fast
          ((0x10007 : BitVec 64) ^^^ TinylambOpt.ResearchGoals.JitKernels.BorrowMasks.repeatByte 0))).contains (i + 1))) =
    ((List.range 7).filter (fun i =>
      TinylambOpt.ResearchGoals.JitKernels.BorrowMasks.lane (0x10007 : BitVec 64) i == 0 &&
      TinylambOpt.ResearchGoals.JitKernels.BorrowMasks.lane (0x10007 : BitVec 64) (i + 1) == 0)))) c

end TinylambOpt.ResearchProofs.JitKernels.Goal_BorrowMasks_AdjacentDelimiterIntersection

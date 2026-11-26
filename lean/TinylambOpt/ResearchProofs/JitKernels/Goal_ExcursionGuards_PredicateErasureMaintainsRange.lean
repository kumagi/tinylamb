import TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

-- Refutation: filtering away a compensating term can break a certified prefix range.

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ExcursionGuards_PredicateErasureMaintainsRange

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards.PredicateErasureMaintainsRange.Goal := by
  intro h
  have c := h 0 10 0 ([(5 : Int), -5]) (fun x : Int => x < 0) (by native_decide)
  exact (by native_decide : ¬ (
    TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards.safe 0 10 0
      ([(5 : Int), -5].filter (fun x : Int => x < 0)))) c

end TinylambOpt.ResearchProofs.JitKernels.Goal_ExcursionGuards_PredicateErasureMaintainsRange

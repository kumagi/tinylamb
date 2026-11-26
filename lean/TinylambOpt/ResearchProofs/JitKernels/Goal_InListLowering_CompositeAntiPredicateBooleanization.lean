import TinylambOpt.ResearchGoals.JitKernels.InListLowering

/-!
Negating a three-valued disjunction is not the same as negating each `WHERE`
acceptance bit: two NULL-producing partitions make the SQL disjunction NULL,
which is not accepted but also is not FALSE. Both partitions `[NULL]` witness
the missing distinction.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.InListLowering.Goal_CompositeAntiPredicateBooleanization

open TinylambOpt.ResearchGoals.JitKernels.InListLowering

theorem refute : ¬ CompositeAntiPredicateBooleanization.Goal := by
  intro h
  have t := h [none] [none] (some (0 : Int))
  simp [test, disj, keep] at t

end TinylambOpt.ResearchProofs.JitKernels.InListLowering.Goal_CompositeAntiPredicateBooleanization

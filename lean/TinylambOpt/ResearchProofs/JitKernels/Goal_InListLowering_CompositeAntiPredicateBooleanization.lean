import TinylambOpt.ResearchGoals.JitKernels.InListLowering

/-!
SQL three-valued OR returns UNKNOWN when one side is UNKNOWN and the other is
FALSE, so negated WHERE acceptance is not the conjunction of the two negated
acceptance bits.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.Goal_InListLowering_CompositeAntiPredicateBooleanization

open TinylambOpt.ResearchGoals.JitKernels.InListLowering

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.InListLowering.CompositeAntiPredicateBooleanization.Goal := by
  intro h
  have h' := h [some 1] [none] (some 2)
  have hne : ¬ ((disj (test [some 1] (some 2)) (test [none] (some 2)) == some false) =
      (!(keep [some 1] (some 2)) && !(keep [none] (some 2)))) := by
    native_decide
  exact hne h'

end TinylambOpt.ResearchProofs.JitKernels.Goal_InListLowering_CompositeAntiPredicateBooleanization

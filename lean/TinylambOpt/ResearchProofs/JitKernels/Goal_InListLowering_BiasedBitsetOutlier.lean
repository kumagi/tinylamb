import TinylambOpt.ResearchGoals.JitKernels.InListLowering

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_InListLowering_BiasedBitsetOutlier

/-- `Int.toNat` truncates a negative bias to zero, so an out-of-domain negative
probe aliases the biased first bucket. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.InListLowering.BiasedBitsetOutlier.Goal := by
  intro h
  have c := h [some (0 : Int)] 0 5 (-1) (by omega) (by
    intro v hv
    simp at hv ⊢
    omega)
  have bad :
      TinylambOpt.ResearchGoals.JitKernels.InListLowering.keep [some (0 : Int)] (some (-1)) ≠
        TinylambOpt.ResearchGoals.JitKernels.InListLowering.keep [some (0 : Int)]
          (some (0 + ((-1 - 0 : Int).toNat % 5))) := by
    native_decide
  exact bad c

end TinylambOpt.ResearchProofs.JitKernels.Goal_InListLowering_BiasedBitsetOutlier

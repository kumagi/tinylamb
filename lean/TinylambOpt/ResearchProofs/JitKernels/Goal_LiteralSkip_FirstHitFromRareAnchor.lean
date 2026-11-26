import TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_LiteralSkip_FirstHitFromRareAnchor

/-- A unique anchor-matching candidate need not be a full match: needle `[1,2]`
in haystack `[1,3]` has one candidate at offset 0 but no real start. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.LiteralSkip.FirstHitFromRareAnchor.Goal := by
  intro h
  have c := h ([1, 2] : List UInt8) ([1, 3] : List UInt8) 0 (by decide) (by native_decide)
  have bad :
      (TinylambOpt.ResearchGoals.JitKernels.LiteralSkip.cand
          ([1, 2] : List UInt8) ([1, 3] : List UInt8) 0).head? ≠
        (TinylambOpt.ResearchGoals.JitKernels.LiteralSkip.starts
          ([1, 2] : List UInt8) ([1, 3] : List UInt8)).head? := by
    native_decide
  exact bad c

end TinylambOpt.ResearchProofs.JitKernels.Goal_LiteralSkip_FirstHitFromRareAnchor

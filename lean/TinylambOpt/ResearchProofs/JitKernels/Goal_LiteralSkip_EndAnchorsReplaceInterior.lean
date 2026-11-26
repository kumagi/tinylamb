import TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

-- Refutation: matching first and last bytes of a nodup needle does not verify interior bytes.

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_LiteralSkip_EndAnchorsReplaceInterior

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.LiteralSkip.EndAnchorsReplaceInterior.Goal := by
  intro h
  have c := h ([0, 1, 2] : List UInt8) ([0, 9, 2] : List UInt8)
    (by native_decide) (by native_decide)
  exact (by native_decide : ¬ (
    ((TinylambOpt.ResearchGoals.JitKernels.LiteralSkip.cand ([0, 1, 2] : List UInt8)
      ([0, 9, 2] : List UInt8) 0).filter
      (TinylambOpt.ResearchGoals.JitKernels.LiteralSkip.anchor
        ([0, 1, 2] : List UInt8) ([0, 9, 2] : List UInt8) (([0, 1, 2] : List UInt8).length - 1))) =
    TinylambOpt.ResearchGoals.JitKernels.LiteralSkip.starts
      ([0, 1, 2] : List UInt8) ([0, 9, 2] : List UInt8))) c

end TinylambOpt.ResearchProofs.JitKernels.Goal_LiteralSkip_EndAnchorsReplaceInterior

import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_GatherWordIndexNarrowing

/-- Lane indices starting near a word boundary can cross into the next null
word, so the first word alone cannot describe a 32-lane gather. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.NullWordFusion.GatherWordIndexNarrowing.Goal := by
  intro h
  let words : List TinylambOpt.ResearchGoals.JitKernels.NullWordFusion.W := [0, 1]
  have c := h words 60
  have h4 := congrArg (fun l => l[4]?) c
  have bad :
      (TinylambOpt.ResearchGoals.JitKernels.NullWordFusion.lanes words 92)[64]? ≠
        some ((words[60 / 64]?.getD 0).getLsbD ((60 + 4) % 64)) := by
    native_decide
  exact bad (by simpa using h4)

end TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_GatherWordIndexNarrowing

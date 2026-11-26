import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_DictionaryRuns_GreedyCut

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.DictionaryRuns

/-- The greedy hypothesis fails for dictionary+runs encoding.
    Take a = [0,1], b = [2], c = [2,2].  Then
      chosen (a++b) = 14 <= chosen a + chosen b = 10 + 6 = 16,
    so the locally cheaper prefix merge is selected.  But once c arrives the
    true optimum is the split a | (b++c):
      min (chosen (a++b++c)) (chosen (a++b) + chosen c)
        = min 21 (14 + 9) = 21,
      chosen a + chosen (b++c) = 10 + 9 = 19.
    The dictionary of the merged prefix already pays for both distinct values,
    which hides the cheaper boundary right after the first value. -/
theorem disproof_GreedyCut : ¬ DictionaryRuns.GreedyCut.Goal := by
  intro h
  have h' := h [0, 1] [2] [2, 2]
    (by decide) (by decide) (by decide) (by native_decide)
  have hc : ¬ (min (chosen write ([0, 1] ++ [2] ++ [2, 2]))
      (chosen write ([0, 1] ++ [2]) + chosen write [2, 2]) ≤
      chosen write [0, 1] + chosen write ([2] ++ [2, 2])) := by
    native_decide
  exact hc h'

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_DictionaryRuns_GreedyCut

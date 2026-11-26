import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SplitHigh_GreedyCut

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SplitHigh

/-- The greedy hypothesis is not valid for the split-high codec.

    Take a = [0,0], b = [4194304, 32768] (high bytes 16384 and 128) and
    c = [4194304, 32768, 4194304].  The framed encoding of the two-element
    prefix is cheaper than the two frames separately:

      chosen (a++b) = 15 <= chosen a + chosen b = 6 + 10 = 16,

    so the greedy merge is selected.  But the cheap two-element frame of b is
    the raw fallback (its high-byte stream has no repeated runs), and it hides
    the better boundary after a once c arrives:

      min (chosen (a++b++c)) (chosen (a++b) + chosen c)
        = min 29 (15 + 14) = 29,
      chosen a + chosen (b++c) = 6 + 22 = 28.

    Hence 29 <= 28 is required and fails. -/
theorem disproof_GreedyCut : ¬ SplitHigh.GreedyCut.Goal := by
  intro h
  have h' := h [0, 0] [4194304, 32768] [4194304, 32768, 4194304]
    (by decide) (by decide) (by decide) (by native_decide)
  have hc : ¬ (min (chosen write ([0, 0] ++ [4194304, 32768] ++ [4194304, 32768, 4194304]))
      (chosen write ([0, 0] ++ [4194304, 32768]) + chosen write [4194304, 32768, 4194304]) ≤
      chosen write [0, 0] + chosen write ([4194304, 32768] ++ [4194304, 32768, 4194304])) := by
    native_decide
  exact hc h'

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SplitHigh_GreedyCut

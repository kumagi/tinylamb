import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Refuted by counterexample

- `SeparatedLengths.GreedyCut` — Aggressive hypothesis: the cheapest two adjacent
  groups never need reconsideration when a third arrives.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SeparatedLengths_GreedyCut

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SeparatedLengths

/-- The greedy hypothesis fails for stream-VByte-style separated lengths.
    Take a = [0,0] (two one-byte lanes), b = three 2^24 lanes (four-byte each)
    and c = eight 2^24 lanes. Then
      chosen (a++b) = 18 <= chosen a + chosen b = 5 + 14 = 19,
    so the locally cheaper prefix merge is selected.  But once c arrives the
    true optimum is the split a | (b++c):
      min (chosen (a++b++c)) (chosen (a++b) + chosen c)
        = min 52 (18 + 34) = 52,
      chosen a + chosen (b++c) = 5 + 46 = 51.
    The framed encoding of all nine four-byte lanes stays just above the raw
    fallback, which hides the cheaper boundary right after the two small lanes. -/
theorem disproof_GreedyCut : ¬ SeparatedLengths.GreedyCut.Goal := by
  intro h
  have h' := h [0, 0] (List.replicate 3 16777216) (List.replicate 8 16777216)
    (by decide) (by decide) (by decide) (by native_decide)
  have hc : ¬ (min (chosen write ([0, 0] ++ List.replicate 3 16777216 ++ List.replicate 8 16777216))
      (chosen write ([0, 0] ++ List.replicate 3 16777216) + chosen write (List.replicate 8 16777216)) ≤
      chosen write [0, 0] + chosen write (List.replicate 3 16777216 ++ List.replicate 8 16777216)) := by
    native_decide
  exact hc h'

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SeparatedLengths_GreedyCut

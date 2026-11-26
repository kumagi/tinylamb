import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Refuted by counterexample

- `XorPredecessor.GreedyCut` — Aggressive hypothesis: the cheapest two adjacent groups
  never need reconsideration when a third arrives.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_XorPredecessor_GreedyCut

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.XorPredecessor

/-- Counterexample (all values nonnegative, so no admissibility issue):

    a = replicate 125 0 ++ [16385],  b = [1],  c = [1].

    write a = 125 one-byte ulebs for the zeros plus uleb 16385 (3 bytes):
      chosen a = 1 + 1 + 128 = 130.
    write (a++b) additionally has uleb (16385 ^^^ 1) = uleb 16384 (3 bytes),
      so chosen (a++b) = 1 + 1 + 131 = 133.
    Hence the greedy precondition holds with equality:
      chosen (a++b) = 133 <= chosen a + chosen b = 130 + 3 = 133.

    Adding c = [1] crosses the 127 -> 128 ULEB count boundary:
      chosen (a++b++c) = 1 + 2 + 132 = 135,
      chosen (a++b) + chosen c = 133 + 3 = 136,
      so min (chosen (a++b++c)) (chosen (a++b) + chosen c) = 135.
    The alternative split (a) | (b++c) has chosen (b++c) = 4, hence RHS = 130 + 4 = 134.

    Thus 135 <= 134 is false: keeping a separate and merging b with c is strictly
    better than the locally greedy prefix merge. -/
theorem disproof_GreedyCut : ¬ XorPredecessor.GreedyCut.Goal := by
  intro h
  have h' := h (List.replicate 125 0 ++ [16385]) [1] [1]
    (by native_decide) (by native_decide) (by native_decide) (by native_decide)
  have hc : ¬ (min (chosen write ((List.replicate 125 0 ++ [16385]) ++ [1] ++ [1]))
                    (chosen write ((List.replicate 125 0 ++ [16385]) ++ [1]) + chosen write [1]) ≤
                  chosen write (List.replicate 125 0 ++ [16385]) + chosen write ([1] ++ [1])) := by
    native_decide
  exact hc h'

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_XorPredecessor_GreedyCut

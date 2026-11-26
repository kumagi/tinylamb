import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Refuted by counterexample

- `SparsePlanes.GreedyCut` — Aggressive hypothesis: the cheapest two adjacent groups
  never need reconsideration when a third arrives.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SparsePlanes_GreedyCut

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SparsePlanes

/-- Counterexample: a = [0], b = [128,0], c = [127].

    chosen a = 3, chosen b = 10, so a + b costs 12 <= 3 + 10 = 13: the greedy
    precondition (the locally cheaper prefix merge) holds.

    The value 128 forces a second bitplane, and the high ordinal 2 of the trailing
    127 in that plane is expensive.  Concretely:
      chosen (a++b++c) = 18,  chosen (a++b) + chosen c = 12 + 6 = 18,
      so the greedy side of the conclusion has cost min 18 18 = 18.
    On the other side, merging b with c keeps the costly high ordinal local:
      chosen (b++c) = 14,  hence chosen a + chosen (b++c) = 3 + 14 = 17.

    Thus 18 <= 17 is false: splitting [0] | [128,0,127] beats both greedy options. -/
theorem disproof_GreedyCut : ¬ SparsePlanes.GreedyCut.Goal := by
  intro h
  have h' := h [0] [128, 0] [127] (by native_decide) (by native_decide) (by native_decide) (by native_decide)
  have hc : ¬ (min (chosen write ([0] ++ [128, 0] ++ [127])) (chosen write ([0] ++ [128, 0]) + chosen write [127]) ≤
      chosen write [0] + chosen write ([128, 0] ++ [127])) := by
    native_decide
  exact hc h'

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SparsePlanes_GreedyCut

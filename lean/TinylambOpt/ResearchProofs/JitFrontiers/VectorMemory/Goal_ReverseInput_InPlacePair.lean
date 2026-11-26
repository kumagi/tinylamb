import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `ReverseInput.InPlacePair` — `∀ j, j.src=j.dst → j.window=2 → ¬ rawFree kernel j
  [0,1]`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_ReverseInput_InPlacePair

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.ReverseInput
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : ReverseInput.InPlacePair.Goal := by
  exact rev_inplace

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_ReverseInput_InPlacePair

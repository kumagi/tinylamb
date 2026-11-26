import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `ReverseInput.Reflection` — `∀ j i, i<j.window → kernel.loads j
  (j.window-1-i)=[j.src+i]`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_ReverseInput_Reflection

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.ReverseInput
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : ReverseInput.Reflection.Goal := by
  exact rev_reflect

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_ReverseInput_Reflection

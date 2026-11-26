import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

/-!
A fixed-width SQL expression fast path depends on the integer range and representation
facts used to justify its machine operations.

Proved

- `SignedProduct.ZeroAnnihilator` — `∀ x, ir op (x,0)=some 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedProduct_ZeroAnnihilator

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises


theorem sound : SignedProduct.ZeroAnnihilator.Goal := by
  intro x
  unfold ir
  dsimp [SignedProduct.op]
  simp [signed]

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedProduct_ZeroAnnihilator

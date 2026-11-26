import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Refuted by counterexample

- `SwapReferences.TrueSwap` — `∀ s a b, a ≠ b → (finish model s a b).children.count
  (some a) = s.children.count (some b)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_SwapReferences_TrueSwap

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem refute : ¬ SwapReferences.TrueSwap.Goal := by
  intro h
  have hh := h ⟨[], [some 0, some 1]⟩ 0 1 (by decide)
  revert hh
  native_decide

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_SwapReferences_TrueSwap

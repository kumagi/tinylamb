import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `BitParity.TwinCancel` — `∀ xs x, model.result (x::x::xs) = model.result xs`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitParity_TwinCancel

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : BitParity.TwinCancel.Goal := by
  intro xs x
  change (x :: x :: xs).foldl (fun a b => a ^^^ b) 0 = xs.foldl (fun a b => a ^^^ b) 0
  simp only [List.foldl_cons, Nat.zero_xor]
  rw [foldl_xor_acc xs (x ^^^ x)]
  simp [Nat.xor_self]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitParity_TwinCancel

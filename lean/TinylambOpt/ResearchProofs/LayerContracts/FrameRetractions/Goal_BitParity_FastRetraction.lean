import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitParity_FastRetraction

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : BitParity.FastRetraction.Goal := by
  apply fastRetraction_of_remove
  intro xs x hx hgt
  have h := xor_erase_of_mem hx
  simp [BitParity.model]
  rw [h]
  rw [Nat.xor_assoc, Nat.xor_comm ((xs.erase x).foldl (fun a b => a ^^^ b) 0) x,
    ← Nat.xor_assoc, Nat.xor_self, Nat.zero_xor]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitParity_FastRetraction

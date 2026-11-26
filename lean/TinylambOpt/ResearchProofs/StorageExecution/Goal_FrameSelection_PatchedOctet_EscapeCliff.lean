import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `PatchedOctet.EscapeCliff` — `∀ v, (write [254,v]).length+1 = (write [255,v]).length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_PatchedOctet_EscapeCliff

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.PatchedOctet
open TinylambOpt.ResearchGoals.Compression.Wire

/-- Crossing the escape threshold costs exactly one extra byte for the
    leading sentinel: 254 is one inline byte, while 255 emits the sentinel
    plus a one-byte ULEB residual (`uleb 0`); the trailing lane is identical. -/
theorem sound_EscapeCliff : PatchedOctet.EscapeCliff.Goal := by
  intro v
  have h0 : (uleb 0).length = 1 := by native_decide
  simp only [PatchedOctet.write, List.flatMap_cons, List.flatMap_nil,
    List.length_append, List.length_cons, List.length_nil, h0,
    ↓reduceIte, Nat.reduceLT]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_PatchedOctet_EscapeCliff

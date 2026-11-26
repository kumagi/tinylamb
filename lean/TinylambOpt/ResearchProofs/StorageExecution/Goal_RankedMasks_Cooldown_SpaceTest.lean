import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `Cooldown.SpaceTest` — A shared language and row count can be amortized across
  equally sized bitmaps.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_Cooldown_SpaceTest

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.Cooldown
open TinylambOpt.ResearchGoals.Compression.Wire

theorem sound_SpaceTest : Cooldown.SpaceTest.Goal := by
  intro n q pages h hpages hcount
  unfold byteCount
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_Cooldown_SpaceTest

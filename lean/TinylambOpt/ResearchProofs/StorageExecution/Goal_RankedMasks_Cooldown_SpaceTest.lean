import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

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

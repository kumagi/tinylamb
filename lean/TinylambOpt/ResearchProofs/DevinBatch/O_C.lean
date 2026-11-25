import TinylambOpt.ResearchGoals.OverlapPairCount
import TinylambOpt.ResearchGoals.PageLoadTrace
import TinylambOpt.ResearchGoals.RadixReplayOwnership
import TinylambOpt.ResearchGoals.SkylineAntiJoin
import TinylambOpt.ResearchGoals.StreamingRunPage
import TinylambOpt.ResearchGoals.TwoLevelPartitionedAggMerge

import TinylambOpt.ResearchProofs.OverlapPairCount
import TinylambOpt.ResearchProofs.PageLoadTrace
import TinylambOpt.ResearchProofs.RadixReplayOwnership
import TinylambOpt.ResearchProofs.SkylineAntiJoin
import TinylambOpt.ResearchProofs.StreamingRunPage
import TinylambOpt.ResearchProofs.TwoLevelPartitionedAggMerge

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.O_C

/-!
# DevinBatch O_C: prior-art wrappers

All six assigned goals already have complete, verified proofs in the
corresponding `TinylambOpt.ResearchProofs.<Module>` modules (each proves
`theorem sound : Goal` and is re-exported by the root `TinylambOpt` module).
The theorems below re-emit each goal under the required `<Module>_sound`
name with the exact goal as its type; no statement is weakened or changed.
-/

theorem OverlapPairCount_sound :
    TinylambOpt.ResearchGoals.OverlapPairCount.Goal :=
  TinylambOpt.ResearchProofs.OverlapPairCount.sound

theorem PageLoadTrace_sound :
    TinylambOpt.ResearchGoals.PageLoadTrace.Goal :=
  TinylambOpt.ResearchProofs.PageLoadTrace.sound

theorem RadixReplayOwnership_sound :
    TinylambOpt.ResearchGoals.RadixReplayOwnership.Goal :=
  TinylambOpt.ResearchProofs.RadixReplayOwnership.sound

theorem SkylineAntiJoin_sound :
    TinylambOpt.ResearchGoals.SkylineAntiJoin.Goal :=
  TinylambOpt.ResearchProofs.SkylineAntiJoin.sound

theorem StreamingRunPage_sound :
    TinylambOpt.ResearchGoals.StreamingRunPage.Goal :=
  TinylambOpt.ResearchProofs.StreamingRunPage.sound

theorem TwoLevelPartitionedAggMerge_sound :
    TinylambOpt.ResearchGoals.TwoLevelPartitionedAggMerge.Goal :=
  TinylambOpt.ResearchProofs.TwoLevelPartitionedAggMerge.sound

end TinylambOpt.ResearchProofs.DevinBatch.O_C

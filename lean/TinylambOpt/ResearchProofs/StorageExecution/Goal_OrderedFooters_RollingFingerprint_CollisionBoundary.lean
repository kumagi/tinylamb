import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_RollingFingerprint_CollisionBoundary

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.RollingFingerprint

-- 真偽確認: 両者はともに (257, 0) に評価される (65521 % 65521 = 0)。
#eval (agg.reference [0], agg.reference [65521])

theorem sound_CollisionBoundary : RollingFingerprint.CollisionBoundary.Goal := by
  unfold CollisionBoundary.Goal agg Fold.reference
  rfl

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_RollingFingerprint_CollisionBoundary

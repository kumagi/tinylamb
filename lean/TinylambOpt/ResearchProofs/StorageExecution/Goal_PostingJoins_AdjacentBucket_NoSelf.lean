import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_AdjacentBucket_NoSelf

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.AdjacentBucket

/-! With x = y both bucket indices coincide (a = b = x / (t+1)), so the test
    reduces to `a + 1 == a || a + 1 == a`, which `simp` discharges using
    `Nat.succ_ne_self`. -/

theorem sound_NoSelf : AdjacentBucket.NoSelf.Goal := by
  intro t x
  dsimp [AdjacentBucket.search]
  simp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_AdjacentBucket_NoSelf

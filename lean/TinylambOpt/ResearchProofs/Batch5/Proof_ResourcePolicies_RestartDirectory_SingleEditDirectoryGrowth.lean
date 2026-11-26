import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.RestartDirectory.Goal_SingleEditDirectoryGrowth

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory


/-!
A single length edit can cross a varint threshold in several restart-directory
entries at once, because every later offset shifts by the same amount.

Refuted by changing the first length from 127 to 128: two directory offsets
move from 127 (one byte) to 128 (two bytes), so the stream grows by two bytes.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_SingleEditDirectoryGrowth

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
open TinylambOpt.ResearchGoals.Compression.Wire

theorem refute : ¬ SingleEditDirectoryGrowth.Goal := by
  intro h
  have hbad := h ([] : List Nat) [0, 0] 127 1 (by decide)
  have hcontra :
      ¬ streamBytes (directory (([127 + 1] : List Nat) ++ [0, 0]) 1) ≤
        streamBytes (directory (([127] : List Nat) ++ [0, 0]) 1) + 1 := by
    native_decide
  exact hcontra hbad

end TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_SingleEditDirectoryGrowth
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.RestartDirectory.Goal_SingleEditDirectoryGrowth

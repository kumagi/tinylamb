import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.RestartDirectory.Goal_AppendOnlyRestartDirectory

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory


/-!
Appending a frame keeps the restart directory prefix unchanged: the aligned row
indices in the old prefix are the same, and their offsets only sum old lengths.

The proof leaves all old length values and offsets intact, so an append can
share the previously written directory entries without repacking them.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_AppendOnlyRestartDirectory

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

theorem sound : AppendOnlyRestartDirectory.Goal := by
  intro a b s hs
  unfold directory
  rw [List.length_append, List.range_add, List.filter_append, List.map_append]
  have hmap :
      ((List.range a.length).filter (fun i => i % s == 0)).map (offset (a ++ b)) =
        ((List.range a.length).filter (fun i => i % s == 0)).map (offset a) := by
    apply List.map_congr_left
    intro i hi
    have hi_range : i ∈ List.range a.length := (List.mem_filter.mp hi).1
    have hi_le : i ≤ a.length := Nat.le_of_lt (List.mem_range.mp hi_range)
    unfold offset
    rw [List.take_append_of_le_length hi_le]
  rw [hmap]
  simp

end TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_AppendOnlyRestartDirectory
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.RestartDirectory.Goal_AppendOnlyRestartDirectory

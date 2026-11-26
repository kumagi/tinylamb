import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

namespace TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.AppendOnlyRestartDirectory

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

private theorem offset_append_left (a b : List Nat) (i : Nat) (hi : i ≤ a.length) :
    offset (a ++ b) i = offset a i := by
  simp only [offset]
  rw [List.take_append_of_le_length (by omega)]

theorem goal : AppendOnlyRestartDirectory.Goal := by
  intro a b s hs
  simp only [AppendOnlyRestartDirectory.Goal, directory, List.length_append]
  rw [List.range_add, List.filter_append, List.map_append]
  have hlen : (List.map (offset (a ++ b)) ((List.range a.length).filter (fun i => i % s == 0))).length =
      (List.map (offset a) ((List.range a.length).filter (fun i => i % s == 0))).length := by simp
  rw [← hlen, List.take_left]
  apply List.map_congr_left
  intro i hi
  have hmem : i ∈ List.range a.length := (List.mem_filter.mp hi).1
  have hlt : i < a.length := List.mem_range.mp hmem
  exact offset_append_left a b i (Nat.le_of_lt hlt)

end TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.AppendOnlyRestartDirectory

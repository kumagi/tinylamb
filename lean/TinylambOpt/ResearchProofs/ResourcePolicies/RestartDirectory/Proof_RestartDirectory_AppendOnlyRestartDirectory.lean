import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_RestartDirectory_AppendOnlyRestartDirectory
open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
theorem sound : AppendOnlyRestartDirectory.Goal := by
  intro a b s hs
  unfold directory offset
  rw [List.length_append, List.range_add, List.filter_append]
  simp only [List.filter_map, Function.comp]
  have hleft :
      List.map (fun row => (List.take row (a ++ b)).sum) (List.filter (fun i => i % s == 0) (List.range a.length)) =
      List.map (fun row => (List.take row a).sum) (List.filter (fun i => i % s == 0) (List.range a.length)) := by
    apply List.map_congr_left
    intro i hi
    have hi' : i < a.length := by
      have : i ∈ List.range a.length := (List.mem_filter.mp hi).1
      simpa [List.mem_range] using this
    simp [List.take_append_of_le_length (Nat.le_of_lt hi')]
  calc
    List.take
        (List.map (fun row => (List.take row a).sum) (List.filter (fun i => i % s == 0) (List.range a.length))).length
        (List.map (fun row => (List.take row (a ++ b)).sum)
          (List.filter (fun i => i % s == 0) (List.range a.length) ++
            List.map (fun x => a.length + x)
              (List.filter ((fun i => i % s == 0) ∘ fun x => a.length + x) (List.range b.length))))
      = List.take
          (List.map (fun row => (List.take row a).sum) (List.filter (fun i => i % s == 0) (List.range a.length))).length
          (List.map (fun row => (List.take row a).sum) (List.filter (fun i => i % s == 0) (List.range a.length)) ++
            List.map (fun row => (List.take row (a ++ b)).sum)
              (List.map (fun x => a.length + x)
                (List.filter ((fun i => i % s == 0) ∘ fun x => a.length + x) (List.range b.length)))) := by
        rw [List.map_append, hleft]
    _ = List.map (fun row => (List.take row a).sum) (List.filter (fun i => i % s == 0) (List.range a.length)) := by
        rw [List.take_left]
end Nov26.Proof_RestartDirectory_AppendOnlyRestartDirectory

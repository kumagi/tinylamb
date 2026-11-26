import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

/-!
If two waves touch disjoint slot sets, the merged per-slot delta equals the
contributing wave's delta: `delta (a ++ b) k` is `delta a k` when `k` is a
leader of `a` and `delta b k` otherwise, so both compiled waves may publish
their counters independently without losing any contribution.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ConflictScatter_DisjointWaveCommit
open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

private theorem delta_append (a b : Updates) (k : Nat) :
    delta (a ++ b) k = delta a k + delta b k := by
  unfold delta
  rw [List.filter_append, List.map_append, List.sum_append]

private theorem delta_eq_zero (us : Updates) (k : Nat) (h : k ∉ us.map Prod.fst) :
    delta us k = 0 := by
  unfold delta
  have hf : us.filter (fun u => u.1 == k) = [] := by
    rw [List.filter_eq_nil_iff]
    intro u hu hu2
    have h1 : u.1 = k := by simpa using hu2
    exact h (h1 ▸ List.mem_map_of_mem (f := Prod.fst) hu)
  rw [hf, List.map_nil, List.sum_nil]

theorem sound : DisjointWaveCommit.Goal := by
  intro a b k h
  rw [delta_append]
  by_cases hk : k ∈ leaders a
  · rw [if_pos hk,
      delta_eq_zero b k (fun h2 => h k hk (List.mem_eraseDups.mpr h2)),
      Int.add_zero]
  · rw [if_neg hk,
      delta_eq_zero a k (fun h2 => hk (List.mem_eraseDups.mpr h2)),
      Int.zero_add]

end TinylambOpt.ResearchProofs.JitKernels.ConflictScatter_DisjointWaveCommit

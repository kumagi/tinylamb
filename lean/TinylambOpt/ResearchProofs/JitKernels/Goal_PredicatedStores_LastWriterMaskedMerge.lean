import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-!
Two partial stores to the same base can be merged by taking the last active
writer in each lane. The proof inspects every byte address and the two lane
options at that address; `Option.orElse` exactly captures the last-writer rule.

Proved

- `LastWriterMaskedMerge` — merging masked stores into one masked store
  preserves memory, so the generated store count can be reduced.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_LastWriterMaskedMerge

open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

theorem sound : TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.LastWriterMaskedMerge.Goal := by
  intro m a xs ys hlen
  funext i
  by_cases ha : a ≤ i
  · by_cases hj : i - a < xs.length
    · have hjl : i - a < ys.length := by omega
      cases hx : xs[i - a]? with
      | none =>
          cases hy : ys[i - a]? with
          | none => simp [masked, ha, hj, hjl, hx, hy, List.getElem?_zipWith]
          | some w => simp [masked, ha, hj, hjl, hx, hy, List.getElem?_zipWith]
      | some x =>
          cases hy : ys[i - a]? with
          | none => simp [masked, ha, hj, hjl, hx, hy, List.getElem?_zipWith]
          | some w => simp [masked, ha, hj, hjl, hx, hy, List.getElem?_zipWith]
    · have hnx : xs[i - a]? = none := List.getElem?_eq_none (by omega)
      have hny : ys[i - a]? = none := List.getElem?_eq_none (by omega)
      simp [masked, ha, hj, hnx, hny, List.getElem?_zipWith]
  · simp [masked, ha]

end TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_LastWriterMaskedMerge

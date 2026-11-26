import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-!
A load may be hoisted across a masked store when every overlapping store lane
is inactive. The proof compares the two loads byte by byte: for each loaded
address covered by the store, the hypothesis says the corresponding lane has no
value, so the masked store leaves that byte unchanged.

Proved

- `ReusePreStoreLoadThroughAlias` — the cached pre-store load equals the
  post-store load, allowing the load to be reused across the masked store.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_ReusePreStoreLoadThroughAlias

open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

theorem sound : ReusePreStoreLoadThroughAlias.Goal := by
  intro m a b xs n h
  apply List.ext_getElem
  · simp [TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.read]
  · intro i hi1 hi2
    have hin : i < n := by
      simpa [TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.read] using hi1
    by_cases ha : a ≤ b + i
    · have hj := h i hin ha
      simp [TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.read, masked, ha, hj]
    · simp [TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.read, masked, ha]

end TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_ReusePreStoreLoadThroughAlias

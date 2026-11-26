import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-!
# SplitLoadPageCrossing: sound

A wide load may be assembled from two adjacent readable ranges: reading `k`
bytes at `a` followed by `n - k` bytes at `a + k` produces exactly the same
byte list as the single `n`-byte read at `a`, so SQL execution can satisfy a
crossing load with two page-local loads without speculating across the edge.
-/

set_option maxHeartbeats 4000000
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PredicatedStores_SplitLoadPageCrossing
open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

theorem sound : SplitLoadPageCrossing.Goal := by
  intro m a n k hk
  unfold TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.read
  refine List.ext_get ?len ?get
  · simp only [List.length_append, List.length_map, List.length_range]
    omega
  · intro i h1 h2
    simp only [List.get_eq_getElem, List.getElem_append, List.getElem_map, List.getElem_range,
      List.length_map, List.length_range] at *
    split
    · rfl
    · next h =>
      rw [show a + k + (i - k) = a + i from by omega]

end TinylambOpt.ResearchProofs.JitKernels.PredicatedStores_SplitLoadPageCrossing

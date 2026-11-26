import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-!
# ReusePreStoreLoadThroughAlias: sound

A load may be hoisted across a masked store when every store lane that
actually overlaps the loaded range is inactive: the masked store leaves all
those bytes untouched, so the load before and after the store agree byte for
byte and SQL execution may reuse the earlier vector register.
-/

set_option maxHeartbeats 4000000
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PredicatedStores_ReusePreStoreLoadThroughAlias
open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

private theorem masked_apply (m : Mem) (a : Nat) (xs : List (Option UInt8)) (i : Nat) :
    masked m a xs i = if a ≤ i then xs[i - a]?.join.getD (m i) else m i := rfl

theorem sound : ReusePreStoreLoadThroughAlias.Goal := by
  intro m a b xs n h
  unfold TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.read
  refine List.ext_get ?len ?get
  · simp only [List.length_map, List.length_range]
  · intro i h1 h2
    rw [List.length_map, List.length_range] at h1
    simp only [List.get_eq_getElem, List.getElem_map, List.getElem_range, masked_apply]
    by_cases hai : a ≤ b + i
    · rw [if_pos hai, h i h1 hai]
      rfl
    · rw [if_neg hai]

end TinylambOpt.ResearchProofs.JitKernels.PredicatedStores_ReusePreStoreLoadThroughAlias

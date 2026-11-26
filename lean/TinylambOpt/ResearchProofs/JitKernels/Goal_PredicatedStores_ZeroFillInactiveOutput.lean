import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-!
Replacing a masked store by an unconditional zero-filled store is safe at the
single byte that the next reader checks: when that lane is active, both forms
write the same value there. The proof inspects the lane option at index `i` and
uses the validity hypothesis to rule out inactive and out-of-range lanes.

Proved

- `ZeroFillInactiveOutput` — the checked byte agrees between a masked store and
  the corresponding zero-filled store, so a reader of that byte can be hoisted
  across the replacement without changing the modeled result.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_ZeroFillInactiveOutput

open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

theorem sound : ZeroFillInactiveOutput.Goal := by
  intro m a xs i h
  cases hxi : xs[i]? with
  | none => simp [hxi] at h
  | some o =>
      cases o with
      | none => simp [hxi] at h
      | some v => simp [masked, store, hxi]

end TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_ZeroFillInactiveOutput

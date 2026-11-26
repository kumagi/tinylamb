import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Refuted by counterexample

- `MatchValue.PayloadDedup` — Can DISTINCT on output payloads be dropped if offsets are
  distinct?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_MatchValue_PayloadDedup

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem refute : ¬ MatchValue.PayloadDedup.Goal := by
  exact Common.payloaddedup_MatchValue

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_MatchValue_PayloadDedup

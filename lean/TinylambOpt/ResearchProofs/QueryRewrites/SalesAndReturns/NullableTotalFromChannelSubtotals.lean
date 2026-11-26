import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns
import TinylambOpt.ResearchProofs.QueryRewrites.NullableSum

/-!
# NullableTotalFromChannelSubtotals

Combining nullable channel subtotals produces the same sum as aggregating all channel
rows, including empty and all-NULL inputs.

A rollup can update the grand total once per channel and preserve SQL SUM semantics.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.NullableTotalFromChannelSubtotals
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns
open TinylambOpt.ResearchProofs.QueryRewrites.NullableSum (nsum_append nsum_cons nsum_map_nsum)

theorem nsum_map_flatten : ∀ cs : List (List (List (Option Int))),
    nsum (cs.map (fun ids => nsum ids.flatten)) = nsum (cs.flatten.map nsum) := by
  intro cs
  induction cs with
  | nil => rfl
  | cons ids rest ih =>
    rw [List.map_cons, List.flatten_cons, List.map_append]
    rw [nsum_cons, nsum_append, nsum_map_nsum ids, ih]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.NullableTotalFromChannelSubtotals.Goal := by
  intro channels
  have hper : channels.map (fun ids => nsum (ids.map nsum)) =
      channels.map (fun ids => nsum ids.flatten) := by
    apply List.map_congr_left
    intro ids hids
    exact nsum_map_nsum ids
  rw [hper, nsum_map_flatten, nsum_map_nsum]
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.NullableTotalFromChannelSubtotals

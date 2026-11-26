import TinylambOpt.ResearchProofs.QueryRewrites.NullableSum
import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

namespace TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_TimeSliceRevenueRollupAcrossUnionAll

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-!
Union-all rollup of a nullable conditional SUM: each channel computes its own
NULL-preserving subtotal and the outer SUM merges those states. Since filtering
and projecting commute with flattening the channels, this is the same state as
one pass over the union-all input.
-/

set_option maxHeartbeats 0

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchProofs.QueryRewrites.NullableSum
open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-- Selecting and projecting each channel then flattening is one global pass. -/
theorem project_flatten (channels : List (List (Nat × Option Int))) (qualifies : Nat → Bool) :
    (channels.map (fun c => (c.filter (fun r => qualifies r.1)).map Prod.snd)).flatten =
      ((channels.flatten.filter (fun r => qualifies r.1)).map Prod.snd) := by
  induction channels with
  | nil => simp
  | cons c cs ih => simp [List.filter_append, List.map_append, ih]

theorem sound : TimeSliceRevenueRollupAcrossUnionAll.Goal := by
  intro channels qualifies
  dsimp
  rw [← project_flatten channels qualifies]
  simpa [List.map_map, Function.comp_def] using
    nsum_map_nsum (channels.map (fun c => (c.filter (fun r => qualifies r.1)).map Prod.snd))
end TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_TimeSliceRevenueRollupAcrossUnionAll

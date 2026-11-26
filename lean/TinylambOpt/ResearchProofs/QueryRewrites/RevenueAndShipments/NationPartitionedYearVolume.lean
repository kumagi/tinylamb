import TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments

/-!
# NationPartitionedYearVolume

For each year, the sum of the Peru and non-Peru states is the total volume. The
aggregate can obtain the denominator without rereading volumes.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.NationPartitionedYearVolume

open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments.NationPartitionedYearVolume.Goal := by
  intro rows
  induction rows with
  | nil => simp [total]
  | cons r rs ih =>
    rcases r with ⟨p,v⟩
    simp only [total, List.map_cons, List.sum_cons]
    have ih' : (List.map Prod.snd rs).sum = (List.map Prod.snd (List.filter Prod.fst rs)).sum +
        (List.map Prod.snd (List.filter (fun x => !x.fst) rs)).sum := by
      simpa [total] using ih
    by_cases hp : p <;> simp [hp] at * <;> omega

end TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.NationPartitionedYearVolume

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.NationPartitionedYearVolume.sound

import TinylambOpt.ResearchGoals.DatabaseCorrectness.GroupingOutput
import TinylambOpt.ResearchProofs.QueryRewrites.NullableSum

/-! Combining partial grand totals across spill partitions preserves both the
    exact row count and the nullable exact SUM (no synthesized zero). -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.GroupingOutput.Goal_GrandTotalAcrossSpillPartitions

open TinylambOpt.ResearchGoals.DatabaseCorrectness.GroupingOutput
open TinylambOpt.ResearchGoals.QueryRewrites (nsum)
open TinylambOpt.ResearchProofs.QueryRewrites.NullableSum (nsum_map_nsum)

theorem sound : GrandTotalAcrossSpillPartitions.Goal := by
  intro parts
  have hmap : (parts.flatten.map Prod.snd) = (parts.map (fun p => p.map Prod.snd)).flatten := by
    induction parts with
    | nil => rfl
    | cons p ps ih => simp [ih]
  simp [grand, List.length_flatten, hmap]
  rw [← nsum_map_nsum]
  have hcomp : List.map nsum (List.map (fun p => p.map Prod.snd) parts) =
      List.map (fun p => nsum (p.map Prod.snd)) parts := by
    rw [List.map_map]
    rfl
  rw [hcomp]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.GroupingOutput.Goal_GrandTotalAcrossSpillPartitions

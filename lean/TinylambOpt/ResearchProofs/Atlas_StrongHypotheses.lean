import TinylambOpt.ResearchGoals.Atlas.ExecutorResources
import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance
import TinylambOpt.ResearchGoals.Atlas.TextExecution

namespace TinylambOpt.ResearchProofs.Atlas.StrongHypotheses

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources
open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance
open TinylambOpt.ResearchGoals.Atlas.TextExecution

set_option maxHeartbeats 0

-- Two destinations holding one byte each need two packets at cap 4,
-- while the total of 2 bytes fits in a single packet.
theorem disproof_GlobalPacking :
    ¬ TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Exchanges.GlobalPacking.Goal := by
  intro h
  have hh := h 4 [(0, 1), (1, 1)] (by decide)
  have hneq : ¬ (Exchanges.traffic 4 [(0, 1), (1, 1)] =
      Exchanges.packets 4 ([(0, 1), (1, 1)].map Prod.snd).sum) := by
    native_decide
  exact hneq hh

-- A newer tombstone over the same interval wins the read: discarding it
-- resurrects the older payload.
theorem disproof_DropTombstones :
    ¬ TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.Versions.DropTombstones.Goal := by
  intro h
  let log : List Versions.Event :=
    [⟨3, 1, 0, 0, 10, some 7⟩, ⟨3, 2, 0, 0, 10, none⟩]
  have hh := h log 3 2 1
  have hneq : ¬ (Versions.readAt (log.filter (fun e => e.payload.isSome)) 3 2 1 =
      Versions.readAt log 3 2 1) := by
    native_decide
  exact hneq hh

-- Identical final stamps and identical final totals do not determine the
-- intermediate prefix sums: (0,4),(1,-4) and (0,0),(1,0) differ at t=0.
theorem disproof_FinalSummary :
    ¬ TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.BatchWatermarks.FinalSummary.Goal := by
  intro h
  let a : List BatchWatermarks.Row := [(0, 4), (1, -4)]
  let b : List BatchWatermarks.Row := [(0, 0), (1, 0)]
  have hh := h a b (by decide) (by decide) 0
  have hneq : ¬ (BatchWatermarks.score 0 a = BatchWatermarks.score 0 b) := by
    native_decide
  exact hneq hh

-- Equal lengths and equal interior hit counts need not agree on the number
-- of hits crossing into a shared tail: [1] and [0] both have 0 hits of
-- [1,2], but [1,2] and [0,2] have 1 and 0 hits respectively.
theorem disproof_InteriorCache :
    ¬ TinylambOpt.ResearchGoals.Atlas.TextExecution.Grammar.InteriorCache.Goal := by
  intro h
  have hh := h [1] [0] [1, 2] (by decide) (by decide) [2]
  have hneq : ¬ ((positions ([1] ++ [2] : Word) [1, 2]).length =
      (positions ([0] ++ [2] : Word) [1, 2]).length) := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.Atlas.StrongHypotheses

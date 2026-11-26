import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

/-!
Snapshot maintenance must preserve the rows visible to a SQL transaction across edits,
retention, and compaction.

Refuted by counterexample

- `BatchWatermarks.FinalSummary` — `∀ a b, stamp a = stamp b → (a.map Prod.snd).sum =
  (b.map Prod.snd).sum → ∀ t, score t a = score t b`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_BatchWatermarks_FinalSummary

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance
open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.BatchWatermarks

/-! Smallest counterexample: `a = [(0,4),(1,-4)]` and `b = [(0,0),(1,0)]`
have the same final stamp (1) and the same final total (0), yet their
`score` at `t = 0` is 4 vs. 0. -/
#eval let a : List BatchWatermarks.Row := [(0, 4), (1, -4)]
      let b : List BatchWatermarks.Row := [(0, 0), (1, 0)]
      (BatchWatermarks.stamp a, BatchWatermarks.stamp b,
       (a.map Prod.snd).sum, (b.map Prod.snd).sum,
       BatchWatermarks.score 0 a, BatchWatermarks.score 0 b)

theorem disproof_BatchWatermarks_FinalSummary : ¬ BatchWatermarks.FinalSummary.Goal := by
  intro h
  let a : List BatchWatermarks.Row := [(0, 4), (1, -4)]
  let b : List BatchWatermarks.Row := [(0, 0), (1, 0)]
  have hh := h a b (by decide) (by decide) 0
  have hneq : ¬ (BatchWatermarks.score 0 a = BatchWatermarks.score 0 b) := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.Atlas.Goal_BatchWatermarks_FinalSummary

import TinylambOpt.ResearchGoals.DatabaseCorrectness.WalReread

/-! A first LSN of zero still denotes a real WAL record.

The record at offset zero is located by the backward walk and stops it at the
first LSN, so its edit participates both in undo enumeration and in lookup.
SQL execution may place a transaction's first write at byte zero without
losing that write when switching to WAL re-read. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.WalReread_ZeroFirstLsnIsARealRecord

open TinylambOpt.ResearchGoals.DatabaseCorrectness.WalReread

theorem sound : ZeroFirstLsnIsARealRecord.Goal := by
  intro owner e tail
  dsimp
  simp [backward]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.WalReread_ZeroFirstLsnIsARealRecord

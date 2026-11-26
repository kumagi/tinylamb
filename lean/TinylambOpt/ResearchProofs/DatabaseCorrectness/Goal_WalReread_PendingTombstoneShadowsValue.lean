import TinylambOpt.ResearchGoals.DatabaseCorrectness.WalReread

/-! A staged tombstone is a distinct, shadowing observation.

Pending lookup scans edits newest-first, so a trailing delete for `(root, key)`
is found before any earlier after-image. SQL execution may represent a delete
as a present edit with no value while still hiding previously staged values. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.WalReread_PendingTombstoneShadowsValue

open TinylambOpt.ResearchGoals.DatabaseCorrectness.WalReread

theorem sound : PendingTombstoneShadowsValue.Goal := by
  intro es root key
  simp [pending]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.WalReread_PendingTombstoneShadowsValue

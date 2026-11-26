import TinylambOpt.ResearchGoals.DatabaseCorrectness.SimpleQuery

/-!
Compacting the reply buffer to the sent cursor and cancelling a withheld reply
commute: both keep exactly the earlier pending bytes after the cursor. This
lets the server reclaim buffer memory without changing the wire cut.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SimpleQuery_WireCompactionPreservesEarlierPending

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SimpleQuery

theorem sound : WireCompactionPreservesEarlierPending.Goal := by
  intro buf n earlier hn
  have hmin : min n buf.length = n := Nat.min_eq_left hn
  have hm : min (buf.length - n) earlier = min ((buf.drop n).length) earlier := by simp
  simp [TinylambOpt.ResearchGoals.DatabaseCorrectness.SimpleQuery.discard, hmin]
  have hsub : n + min (buf.length - n) earlier - n = min (buf.length - n) earlier := by omega
  rw [List.drop_take]
  rw [hsub]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SimpleQuery_WireCompactionPreservesEarlierPending

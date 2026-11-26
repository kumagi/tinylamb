import TinylambOpt.ResearchGoals.DatabaseCorrectness.SnapshotIndex

/-!
When a changed row's old and new keys both fail the range predicate, the
snapshot scan result is unaffected by the key move. Every other row keeps its
key, so the two heap scans select exactly the same positions.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SnapshotIndex_DisjointOldAndNewKeyFence

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SnapshotIndex

theorem sound : DisjointOldAndNewKeyFence.Goal := by
  intro ids
  induction ids with
  | nil => intro old now p h; simp [scan]
  | cons i is ih =>
      intro old now p h
      simp only [scan, List.filter_cons]
      have hi := h i (by simp)
      have hsame : matchesRange p old i = matchesRange p now i := by
        by_cases hneq : old i = now i
        · simp [matchesRange, hneq]
        · have hf := hi hneq
          rw [hf.1, hf.2]
      have htail := ih old now p (by intro j hj; exact h j (by simp [hj]))
      simp [scan] at htail
      simp [scan, hsame, htail]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SnapshotIndex_DisjointOldAndNewKeyFence

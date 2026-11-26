import TinylambOpt.ResearchGoals.DatabaseCorrectness.SnapshotIndex
import TinylambOpt.ResearchProofs.DatabaseCorrectness.SnapshotIndexCommon

/-!
Snapshot rechecking compares by row position, not by key value: two distinct
positions carrying the same matching key must both remain candidates exactly
once, even after the candidate list is deduplicated.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SnapshotIndex_DuplicateKeyDistinctPositions

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SnapshotIndex
open TinylambOpt.ResearchProofs.DatabaseCorrectness.SnapshotIndexCommon (count_eraseDups)

theorem sound : DuplicateKeyDistinctPositions.Goal := by
  intro a b k p old physical changed hne ha hb hoa hob hp
  let C := physical ++ changed
  have hcnta : (C.eraseDups).count a = 1 := by
    have h := count_eraseDups a C
    simp [C, ha] at h
    exact h
  have hcntb : (C.eraseDups).count b = 1 := by
    have h := count_eraseDups b C
    simp [C, hb] at h
    exact h
  have hpa : matchesRange p old a = true := by simp [matchesRange, hoa, hp]
  have hpb : matchesRange p old b = true := by simp [matchesRange, hob, hp]
  have hca : (checkedIndex physical changed old p).count a = 1 := by
    unfold checkedIndex scan
    rw [List.count_filter hpa]
    simpa [C] using hcnta
  have hcb : (checkedIndex physical changed old p).count b = 1 := by
    unfold checkedIndex scan
    rw [List.count_filter hpb]
    simpa [C] using hcntb
  omega

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SnapshotIndex_DuplicateKeyDistinctPositions

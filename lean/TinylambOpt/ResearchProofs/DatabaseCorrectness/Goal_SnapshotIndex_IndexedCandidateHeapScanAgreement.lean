import TinylambOpt.ResearchGoals.DatabaseCorrectness.SnapshotIndex
import TinylambOpt.ResearchProofs.DatabaseCorrectness.SnapshotIndexCommon

/-!
If every changed position is journalled and the physical index covers all
current matches, the deduplicated candidate positions agree as a bag with an
independent snapshot heap scan, including key moves and stale entries.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SnapshotIndex_IndexedCandidateHeapScanAgreement

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SnapshotIndex
open TinylambOpt.ResearchProofs.DatabaseCorrectness.SnapshotIndexCommon

theorem sound : IndexedCandidateHeapScanAgreement.Goal := by
  intro ids old now p physical changed hnodup hcover hnow hchanged
  let C := physical ++ changed
  change (C.eraseDups.filter (matchesRange p old)).Perm (ids.filter (matchesRange p old))
  apply (List.perm_ext_iff_of_nodup
    (nodup_filter C.eraseDups (matchesRange p old) (nodup_eraseDups C))
    (nodup_filter ids (matchesRange p old) hnodup)).mpr
  intro a
  simp only [List.mem_filter]
  constructor
  · intro hmem
    have haC : a ∈ C := List.mem_eraseDups.mp hmem.1
    exact ⟨by simpa [C] using hcover a haC, hmem.2⟩
  · intro hmem
    have haC : a ∈ C := by
      by_cases hmn : matchesRange p now a = true
      · exact List.mem_append_left changed (hnow a hmem.1 hmn)
      · have hne : old a ≠ now a := by
          intro heq
          have hold := hmem.2
          simp [matchesRange, heq] at hold
          exact hmn hold
        exact List.mem_append_right physical (hchanged a hmem.1 hne)
    exact ⟨List.mem_eraseDups.mpr (by simpa [C] using haC), hmem.2⟩

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SnapshotIndex_IndexedCandidateHeapScanAgreement

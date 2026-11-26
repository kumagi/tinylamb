import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ControlByteProbe_FirstEmptyMasksPackedHits

open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

theorem sound : TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe.FirstEmptyMasksPackedHits.Goal := by
  intro xs k f hcoh
  induction xs with
  | nil => simp [probe, untilEmpty]
  | cons s xs ih =>
      have htail : coherent f xs := by
        intro t kk v hm
        exact hcoh t kk v (by simp [hm])
      specialize ih htail
      cases s with
      | empty => simp [probe, untilEmpty]
      | tomb => simpa [probe, untilEmpty, value, tagHit, List.findSome?_cons] using ih
      | live t key val =>
          by_cases hkey : key == k
          · have hkeq : key = k := by simpa using hkey
            have htag : t = f k := by simpa [hkeq] using hcoh t key val (by simp)
            simp [probe, untilEmpty, value, tagHit, List.findSome?_cons, hkeq, htag]
          · have hneq : key ≠ k := by simpa using hkey
            have hneq' : k ≠ key := by intro h; exact hneq h.symm
            by_cases ht : t == f k
            · simpa [probe, untilEmpty, value, tagHit, List.findSome?_cons, hneq, hneq', ht] using ih
            · simpa [probe, untilEmpty, value, tagHit, List.findSome?_cons, hneq, hneq', ht] using ih

end TinylambOpt.ResearchProofs.JitKernels.Goal_ControlByteProbe_FirstEmptyMasksPackedHits

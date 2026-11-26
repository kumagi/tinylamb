import TinylambOpt.ResearchGoals.JitKernels.RadixCursors

/-!
A second vector can reuse the first vector's immutable bucket bases: the
histogram of the preceding vector is added to each lane's local same-bucket
rank. This keeps the SQL scatter cursors reusable across SIMD vectors.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace Nov26.Proof_RadixCursors_CrossVectorCursorCarry

open TinylambOpt.ResearchGoals.JitKernels.RadixCursors
open TinylambOpt.ResearchGoals.JitKernels.RadixCursors.CrossVectorCursorCarry

theorem sound : Goal := by
  intro a b j k
  have htake : (a ++ b).take (a.length + j) = a ++ b.take j := by
    rw [List.take_append]
    rw [List.take_of_length_le (by omega : a.length ≤ a.length + j)]
    simp
  simp only [rank, htake]
  simp [active, List.filterMap_append, List.count_append]

end Nov26.Proof_RadixCursors_CrossVectorCursorCarry

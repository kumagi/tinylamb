import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-!
A load fully covered by a preceding store can be forwarded from the stored byte
list instead of issuing another memory read. The proof compares the two lists
index by index: the store writes exactly the bytes whose offsets lie inside the
stored range, and the index bound guarantees every requested byte is present.

Proved

- `ForwardSubwordLoad` — the forwarded subword equals the original load, so
  store-to-load forwarding may replace the memory read.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_ForwardSubwordLoad

open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

theorem sound : ForwardSubwordLoad.Goal := by
  intro m base bs off n h
  apply List.ext_getElem
  · simp [TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.read, List.length_take]
    omega
  · intro i hi1 hi2
    have hlen : ((bs.drop off).take n).length ≤ n := by
      simp [List.length_take, Nat.min_le_left]
    have hin : i < n := by omega
    have hi : off + i < bs.length := by omega
    simp [TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.read, store,
      List.getElem?_take, List.getElem?_drop, hi, hin, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

end TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_ForwardSubwordLoad

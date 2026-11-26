import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-!
# ForwardSubwordLoad: sound

Forwarding a sub-word load from a wider preceding store: when the requested
byte range lies fully inside the stored byte range, reading the stored memory
at `base + off` for `n` bytes equals slicing the stored bytes at `off`.
This lets SQL execution elide a second load and read the just-written
register value instead, with no observable byte difference.
-/

set_option maxHeartbeats 4000000
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PredicatedStores_ForwardSubwordLoad
open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

theorem sound : ForwardSubwordLoad.Goal := by
  intro m base bs off n hlen
  unfold TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.read
  refine List.ext_get ?len ?get
  · rw [List.length_map, List.length_range, List.length_take, List.length_drop]
    omega
  · intro i h1 h2
    rw [List.length_map, List.length_range] at h1
    rw [List.length_take, List.length_drop] at h2
    have hin : off + i < bs.length := by omega
    rw [List.get_eq_getElem, List.get_eq_getElem, List.getElem_map, List.getElem_range,
      List.getElem_take, List.getElem_drop]
    show (if base ≤ base + off + i
      then bs[base + off + i - base]?.getD (m (base + off + i))
      else m (base + off + i)) = bs[off + i]
    rw [if_pos (by omega : base ≤ base + off + i), Nat.add_assoc, Nat.add_sub_cancel_left,
      List.getElem?_eq_getElem hin]
    simp

end TinylambOpt.ResearchProofs.JitKernels.PredicatedStores_ForwardSubwordLoad

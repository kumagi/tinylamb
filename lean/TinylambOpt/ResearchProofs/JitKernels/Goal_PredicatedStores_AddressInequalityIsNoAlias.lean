import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-!
Different aligned base addresses are not a no-alias proof for stores of up to
eight bytes: two bases four bytes apart have overlapping byte intervals. This
file refutes the reorder claim with adjacent 4-byte stores of length five.

Refuted by counterexample: bases `0` and `4`, with five-byte stores, overlap at
byte `4`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_AddressInequalityIsNoAlias

open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

theorem refute : ¬ AddressInequalityIsNoAlias.Goal := by
  intro h
  have heq := h (fun _ => (0 : UInt8)) 0 4 (List.replicate 5 1) (List.replicate 5 2)
    (by omega) (by omega) (by omega) (by native_decide) (by native_decide)
  exact (by native_decide :
    store (store (fun _ => (0 : UInt8)) 0 (List.replicate 5 1)) 4 (List.replicate 5 2) 4 ≠
      store (store (fun _ => (0 : UInt8)) 4 (List.replicate 5 2)) 0 (List.replicate 5 1) 4)
    (congrFun heq 4)

end TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_AddressInequalityIsNoAlias

import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-!
A dead-store-elimination claim about an observation between two stores fails
when the second store is not actually performed: as modeled, the first store is
still observed. This file refutes the claim that reading after a store of `xs`
returns the pre-store bytes merely because a longer store `ys` is available.

Refuted by counterexample: a single-byte store of `1` into a zero-filled memory
is observed as `1`, not `0`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_OverwrittenStoreAcrossObservation

open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

theorem refute : ¬ OverwrittenStoreAcrossObservation.Goal := by
  intro h
  have heq := h (fun _ => (0 : UInt8)) 0 [1] [0, 0] (by native_decide)
  exact (by native_decide :
    read (store (fun _ => (0 : UInt8)) 0 [1]) 0 [1].length ≠
      read (fun _ => (0 : UInt8)) 0 [1].length) heq

end TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_OverwrittenStoreAcrossObservation

import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-!
A NULL lane in a masked store leaves the original byte untouched, but a
zero-filled store writes zero to that byte unconditionally. This file refutes
the claim that a NULL validity lane protects an aliased live column from the
zero fill.

Refuted by counterexample: `xs[1] = none` but the memory byte at `a+1` is `9`,
so the zero-filled store changes it.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_SharedNullByteClobber

open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

theorem refute : ¬ SharedNullByteClobber.Goal := by
  intro h
  have heq := h (fun i => if i = 1 then (9 : UInt8) else 0) 0 [some 5, none] 1
    (by native_decide)
  exact (by native_decide :
    store (fun i => if i = 1 then (9 : UInt8) else 0) 0
      ([some 5, none].map (fun x => x.getD 0)) (0 + 1) ≠
        (fun i => if i = 1 then (9 : UInt8) else 0) (0 + 1)) heq

end TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_SharedNullByteClobber

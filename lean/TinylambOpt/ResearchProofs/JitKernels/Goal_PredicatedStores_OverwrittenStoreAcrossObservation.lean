import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-!
The stated identity claims that a store of nonmatching bytes is invisible to a
reader at the same base. The second store does not appear in the expression, so
the counterexample simply stores one byte over a different memory byte.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_OverwrittenStoreAcrossObservation

open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.OverwrittenStoreAcrossObservation.Goal := by
  intro h
  have h' := h (fun _ => 0) 10 [1] [2, 3] (by native_decide)
  have hne : ¬ (read (store (fun _ => 0) 10 [1]) 10 1 = read (fun _ => 0) 10 1) := by
    native_decide
  exact hne h'

end TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_OverwrittenStoreAcrossObservation

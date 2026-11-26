import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_ZeroFillInactiveOutput

open TinylambOpt.ResearchGoals.JitKernels

theorem sound : TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.ZeroFillInactiveOutput.Goal := by
  intro m a xs i hi
  have hai : a ≤ a + i := by omega
  simp [PredicatedStores.masked, PredicatedStores.store, hai]
  cases hxi : xs[i]? with
  | none => simp [hxi] at hi
  | some v =>
      cases v <;> simp [hxi] at hi ⊢

end TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_ZeroFillInactiveOutput

import TinylambOpt.ResearchGoals.JitKernels.CodeReachability

/-! An out-degree-one call chain can be longer than one callee per root, so
    the proposed stub-only space budget is refuted. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_StubOnlySpaceBudget

open TinylambOpt.ResearchGoals.JitKernels.CodeReachability

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.CodeReachability.StubOnlySpaceBudget.Goal := by
  intro h
  let k0 : Key := (0,0)
  let k1 : Key := (1,0)
  let k2 : Key := (2,0)
  let k3 : Key := (3,0)
  let heap : List Obj := [{ key := k0, calls := [k1], bytes := 1 },
    { key := k1, calls := [k2], bytes := 1 },
    { key := k2, calls := [k3], bytes := 1 },
    { key := k3, calls := [], bytes := 1 }]
  have hh := h heap [k0] (by native_decide) (by native_decide)
  have hne : ¬ (((retain heap (mark heap [k0] heap.length)).map Obj.bytes).sum ≤
      ((retain heap [k0]).map Obj.bytes).sum + [k0].length * ((heap.map Obj.bytes).max?.getD 0)) := by
    native_decide
  exact hne hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_StubOnlySpaceBudget

import TinylambOpt.ResearchGoals.JitKernels.CodeReachability

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_UnrootedRecursionRefcount

open TinylambOpt.ResearchGoals.JitKernels.CodeReachability

/-! A call target of an unreachable object is in the flat map of call lists,
but it is not reachable from the roots. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.CodeReachability.UnrootedRecursionRefcount.Goal := by
  rw [UnrootedRecursionRefcount.Goal]
  intro h
  let o : Obj := ⟨(1,0),[(2,0)],3⟩
  let heap := [⟨(0,0),[],0⟩, o, ⟨(2,0),[],0⟩]
  have hrhs : (2,0) ∈ [(0,0)] ∨ (2,0) ∈ heap.flatMap Obj.calls := by
    change (2,0) ∈ [(0,0)] ∨ (2,0) ∈ ([] ++ [(2,0)] ++ [])
    simp
  have hf : (2,0) ∈ mark heap [(0,0)] heap.length :=
    (h heap [(0,0)] ⟨(2,0),[],0⟩ (by simp [heap])).mpr hrhs
  have hnot : (2,0) ∉ mark heap [(0,0)] heap.length := by
    native_decide
  exact hnot hf

end TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_UnrootedRecursionRefcount

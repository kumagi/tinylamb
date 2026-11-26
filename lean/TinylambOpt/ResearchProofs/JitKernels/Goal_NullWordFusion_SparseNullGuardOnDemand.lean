import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

/-! A selection word is disjoint from a null word exactly when every selected
    ordinal has a clear null bit. SQL execution may guard the unchecked value
    path either by the word intersection or by visiting only the sparse
    selected ordinals, with the same result. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_SparseNullGuardOnDemand

open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

theorem sound : TinylambOpt.ResearchGoals.JitKernels.NullWordFusion.SparseNullGuardOnDemand.Goal := by
  intro selection nulls
  constructor
  · intro h i hi
    have hsel := (List.mem_filter.mp hi).2
    have hbit := congrArg (fun w : W => w.getLsbD i) h
    simp only [BitVec.getLsbD_and, hsel, Bool.true_and, BitVec.getLsbD_zero] at hbit
    simpa using hbit
  · intro hm
    apply BitVec.eq_of_getLsbD_eq_iff.mpr
    intro i hi
    simp only [BitVec.getLsbD_and, BitVec.getLsbD_zero]
    cases hsel : selection.getLsbD i with
    | false => simp
    | true =>
        have himem : i ∈ ords selection :=
          List.mem_filter.mpr ⟨List.mem_range.mpr hi, hsel⟩
        simp [hm i himem]

end TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_SparseNullGuardOnDemand

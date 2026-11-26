import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

/-!
**Proved: sparse on-demand null guard.** A selection word is disjoint from the
null word exactly when every selected ordinal has a clear null bit. SQL
execution may guard the unchecked value path either by the word intersection
or by visiting only the sparse selected ordinals, with the same result.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.NullWordFusion_SparseNullGuardOnDemand
open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

theorem sound : SparseNullGuardOnDemand.Goal := by
  intro selection nulls
  constructor
  · intro hdisjoint i hi
    have hsel := (List.mem_filter.mp hi).2
    have hbit := congrArg (fun w : W => w.getLsbD i) hdisjoint
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

end TinylambOpt.ResearchProofs.JitKernels.NullWordFusion_SparseNullGuardOnDemand

import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_SparseNullGuardOnDemand

open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

theorem sound : SparseNullGuardOnDemand.Goal := by
  intro selection nulls
  constructor
  · intro h i hi
    rcases (List.mem_filter.mp hi) with ⟨hirange, hsel⟩
    have hb := congrArg (fun (x : W) => x.getLsbD i) h
    simp [BitVec.getLsbD_and, hsel] at hb
    exact hb
  · intro hall
    apply BitVec.eq_of_getLsbD_eq
    intro i hi
    simp only [BitVec.getLsbD_and, BitVec.getLsbD_zero]
    by_cases hs : selection.getLsbD i = true
    · have himem : i ∈ ords selection := by
        simp [ords, hs, List.mem_range.mpr hi]
      have hn := hall i himem
      simp [hs, hn]
    · have hsf : selection.getLsbD i = false := by cases h : selection.getLsbD i <;> simp_all
      simp [hsf]

end TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_SparseNullGuardOnDemand

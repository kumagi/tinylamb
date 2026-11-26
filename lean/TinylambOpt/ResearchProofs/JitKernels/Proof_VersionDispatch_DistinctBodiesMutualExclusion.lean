import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false

/-! Permuting entries preserves the dispatched code when any two simultaneously accepting versions are required to name the same code. -/
namespace Nov26.Proof_VersionDispatch_DistinctBodiesMutualExclusion
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.DistinctBodiesMutualExclusion
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.DistinctBodiesMutualExclusion
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.DistinctBodiesMutualExclusion
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.DistinctBodiesMutualExclusion

private theorem acc_eq (e : Entry) (w : Word) :
    accepts e w = true ↔ (w &&& e.mask) = (e.value &&& e.mask) := by
  simp [accepts]

theorem sound : Goal := by
  intro a b w hp hcompat
  have noclash_of (e f : Entry) (he : accepts e w = true) (hf : accepts f w = true) :
      (!clash e f) = true := by
    have hee := (acc_eq e w).mp he
    have hff := (acc_eq f w).mp hf
    have hz : ((e.value ^^^ f.value) &&& e.mask &&& f.mask) = 0 := by
      bv_decide
    simpa [clash] using hz
  cases hfa : a.find? (accepts · w) with
  | none =>
      have hfb : b.find? (accepts · w) = none := by
        rw [List.find?_eq_none]
        intro x hxb
        have hxa : x ∈ a := hp.symm.mem_iff.mp hxb
        exact (List.find?_eq_none.mp hfa x hxa)
      simp [dispatch, hfa, hfb]
  | some ea =>
      have hea : accepts ea w = true := by exact List.find?_some (p := fun x => accepts x w) hfa
      have heam : ea ∈ a := List.mem_of_find?_eq_some hfa
      have hbsome : (b.find? (accepts · w)).isSome = true := by
        rw [List.find?_isSome]
        exact ⟨ea, hp.mem_iff.mp heam, hea⟩
      cases hfb : b.find? (accepts · w) with
      | none =>
          exfalso
          simpa [hfb] using hbsome
      | some fb =>
          have hfbacc : accepts fb w = true := by exact List.find?_some (p := fun x => accepts x w) hfb
          have hfbm : fb ∈ a := hp.symm.mem_iff.mp (List.mem_of_find?_eq_some hfb)
          have hver : ea.code = fb.code :=
            hcompat ea heam fb hfbm (noclash_of ea fb hea hfbacc)
          simp [dispatch, hfa, hfb, hver]

end Nov26.Proof_VersionDispatch_DistinctBodiesMutualExclusion
import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-! Permuting entries preserves the dispatched code when any two simultaneously accepting
versions are required to name the same code. -/
namespace TinylambOpt.ResearchProofs.JitKernels.VersionDispatch_DistinctBodiesMutualExclusion
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch DistinctBodiesMutualExclusion

/-- A guard accepts exactly the words whose masked bits equal the masked value. -/
private theorem acc_eq (e : Entry) (w : Word) :
    accepts e w = true ↔ (w &&& e.mask) = (e.value &&& e.mask) := by
  simp [accepts]

/-- Boolean core: a word satisfying both guard equations forces the two guard values to agree
on every bit both masks test, so the masked xor vanishes there. -/
private theorem agree_common_bool (A B W U V : Bool)
    (ha : (W && A) = (U && A)) (hb : (W && B) = (V && B)) :
    (((U ^^ V) && A) && B) = false := by
  revert A B W U V ha hb
  decide

/--
`DistinctBodiesMutualExclusion.Goal`: if every pair of versions in the chain that can accept
the same input row names the same code body, then reordering the cache (permuting the list)
never changes which code `dispatch` returns — so the optimizer may freely reorder the
specialization cache, e.g. by cost, without touching semantics. The first-match entry of one
order is present in the permuted list (permutation preserves membership), and the two
first-matches agree on the row, hence clash-free, hence equal by hypothesis.
-/
theorem sound : Goal := by
  intro a b w hp hcompat
  have noclash_of (e f : Entry) (he : accepts e w = true) (hf : accepts f w = true) :
      (!clash e f) = true := by
    have hee := (acc_eq e w).mp he
    have hff := (acc_eq f w).mp hf
    have hz : ((e.value ^^^ f.value) &&& e.mask &&& f.mask) = 0 := by
      apply BitVec.eq_of_getLsbD_eq_iff.mpr
      intro i hi
      have he' : (w.getLsbD i && e.mask.getLsbD i)
          = (e.value.getLsbD i && e.mask.getLsbD i) := by
        simpa [BitVec.getLsbD_and] using congrArg (fun v : Word => v.getLsbD i) hee
      have hf' : (w.getLsbD i && f.mask.getLsbD i)
          = (f.value.getLsbD i && f.mask.getLsbD i) := by
        simpa [BitVec.getLsbD_and] using congrArg (fun v : Word => v.getLsbD i) hff
      simp only [BitVec.getLsbD_and, BitVec.getLsbD_xor]
      rw [agree_common_bool _ _ _ _ _ he' hf']
      exact (BitVec.getLsbD_zero (w := 64) (i := i)).symm
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

end TinylambOpt.ResearchProofs.JitKernels.VersionDispatch_DistinctBodiesMutualExclusion

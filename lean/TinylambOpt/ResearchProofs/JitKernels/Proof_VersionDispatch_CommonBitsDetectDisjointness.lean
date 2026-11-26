import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.VersionDispatch_CommonBitsDetectDisjointness
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch CommonBitsDetectDisjointness

/-- A guard accepts exactly the words whose masked bits equal the masked value. -/
private theorem acc_eq (e : Entry) (w : Word) :
    accepts e w = true ↔ (w &&& e.mask) = (e.value &&& e.mask) := by
  simp [accepts]

/-- Boolean core: a word satisfying both guard equations forces the two guard values to agree on
every bit both masks test, so the masked xor vanishes there. -/
private theorem agree_common_bool (A B W U V : Bool)
    (ha : (W && A) = (U && A)) (hb : (W && B) = (V && B)) :
    (((U ^^ V) && A) && B) = false := by
  revert A B W U V ha hb
  decide

/-- Boolean core for the joint witness: when the masked xor vanishes, the word combining both
masked values still satisfies the first guard. -/
private theorem cover_bool (A B U V : Bool)
    (hz : (((U ^^ V) && A) && B) = false) :
    (((U && A) || (V && B)) && A) = (U && A) := by
  revert A B U V hz
  decide

/-- Boolean core for the joint witness, second guard. -/
private theorem cover_bool' (A B U V : Bool)
    (hz : (((U ^^ V) && A) && B) = false) :
    (((U && A) || (V && B)) && B) = (V && B) := by
  revert A B U V hz
  decide

/--
`CommonBitsDetectDisjointness.Goal`: two compiled versions clash on a commonly required bit
exactly when no input row can satisfy both guards at once, so a dispatch chain never has to
disambiguate them at run time. Forward: any jointly accepted word makes the masked xor vanish
bit-by-bit (`agree_common_bool`). Backward: without a clash the combined witness
`(a.value &&& a.mask) ||| (b.value &&& b.mask)` satisfies both guards bit-by-bit
(`cover_bool` / `cover_bool'`).
-/
theorem sound : Goal := by
  intro a b
  simp only [clash, bne_iff_ne]
  constructor
  · intro hc ⟨w, ha, hb⟩
    have hae := (acc_eq a w).mp ha
    have hbe := (acc_eq b w).mp hb
    have hzero : ((a.value ^^^ b.value) &&& a.mask &&& b.mask) = 0 := by
      apply BitVec.eq_of_getLsbD_eq_iff.mpr
      intro i hi
      have ha' : (w.getLsbD i && a.mask.getLsbD i) = (a.value.getLsbD i && a.mask.getLsbD i) := by
        simpa [BitVec.getLsbD_and] using congrArg (fun v : Word => v.getLsbD i) hae
      have hb' : (w.getLsbD i && b.mask.getLsbD i) = (b.value.getLsbD i && b.mask.getLsbD i) := by
        simpa [BitVec.getLsbD_and] using congrArg (fun v : Word => v.getLsbD i) hbe
      simp only [BitVec.getLsbD_and, BitVec.getLsbD_xor]
      rw [agree_common_bool _ _ _ _ _ ha' hb']
      exact (BitVec.getLsbD_zero (w := 64) (i := i)).symm
    exact hc hzero
  · intro hn hzero
    have hbit : ∀ i, i < 64 →
        (((a.value.getLsbD i ^^ b.value.getLsbD i) && a.mask.getLsbD i)
            && b.mask.getLsbD i) = false := by
      intro i hi
      have hz' := congrArg (fun v : Word => v.getLsbD i) hzero
      rw [BitVec.getLsbD_and, BitVec.getLsbD_and, BitVec.getLsbD_xor] at hz'
      rw [hz']
      exact BitVec.getLsbD_zero (w := 64) (i := i)
    apply hn
    refine ⟨(a.value &&& a.mask) ||| (b.value &&& b.mask), ?_, ?_⟩
    · apply (acc_eq a _).mpr
      apply BitVec.eq_of_getLsbD_eq_iff.mpr
      intro i hi
      simp only [BitVec.getLsbD_and, BitVec.getLsbD_or]
      exact cover_bool _ _ _ _ (hbit i hi)
    · apply (acc_eq b _).mpr
      apply BitVec.eq_of_getLsbD_eq_iff.mpr
      intro i hi
      simp only [BitVec.getLsbD_and, BitVec.getLsbD_or]
      exact cover_bool' _ _ _ _ (hbit i hi)

end TinylambOpt.ResearchProofs.JitKernels.VersionDispatch_CommonBitsDetectDisjointness

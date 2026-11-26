import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.VersionDispatch_ShadowChainPruning
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch ShadowChainPruning

/-- A guard accepts exactly the words whose masked bits equal the masked value. -/
private theorem acc_eq (e : Entry) (w : Word) :
    accepts e w = true ↔ (w &&& e.mask) = (e.value &&& e.mask) := by
  simp [accepts]

/-- Pure-Boolean core of guard subsumption (kernel-evaluated over all bit assignments). -/
private theorem mono_bool (A B W U V : Bool)
    (hinc : (A && B) = A)
    (hnc : (((V ^^ U) && A) && B) = false)
    (hb : (W && B) = (U && B)) : (W && A) = (V && A) := by
  revert A B W U V hnc
  decide

/--
`ShadowChainPruning.Goal`: if guard `a` is weaker than (dominates) guard `b`, then `b` sitting
later in an ordered dispatch chain can never be the first match — whenever `b` would fire, the
earlier `a` already fired — so the compiler may delete `b` from the chain (and everything its
removal exposes stays reachable). Per bit, mask containment plus value agreement on the
intersection converts `b`'s accept equation into `a`'s (`mono_bool`); the chain identity then
follows by case analysis on whether `a` fires at all.
-/
theorem sound : Goal := by
  intro a b rest tail w hw
  have himp : ∀ w, accepts b w = true → accepts a w = true := by
    intro w hb
    have hw' := hw
    simp only [weaker, Bool.and_eq_true] at hw'
    have hm : a.mask &&& b.mask = a.mask := by simpa using hw'.1
    have hnc : ((a.value ^^^ b.value) &&& a.mask &&& b.mask) = 0 := by simpa [clash] using hw'.2
    have hbe := (acc_eq b w).mp hb
    apply (acc_eq a w).mpr
    apply BitVec.eq_of_getLsbD_eq_iff.mpr
    intro i _
    have hincb : (a.mask.getLsbD i && b.mask.getLsbD i) = a.mask.getLsbD i := by
      simpa [BitVec.getLsbD_and] using congrArg (fun v : Word => v.getLsbD i) hm
    have hncb : (((a.value.getLsbD i ^^ b.value.getLsbD i) && a.mask.getLsbD i)
        && b.mask.getLsbD i) = false := by
      have h := congrArg (fun v : Word => v.getLsbD i) hnc
      rw [BitVec.getLsbD_and, BitVec.getLsbD_and, BitVec.getLsbD_xor] at h
      rw [h]
      exact BitVec.getLsbD_zero (w := 64) (i := i)
    have hbb : (w.getLsbD i && b.mask.getLsbD i) = (b.value.getLsbD i && b.mask.getLsbD i) := by
      simpa [BitVec.getLsbD_and] using congrArg (fun v : Word => v.getLsbD i) hbe
    rw [BitVec.getLsbD_and, BitVec.getLsbD_and]
    exact mono_bool _ _ _ _ _ hincb hncb hbb
  by_cases ha : accepts a w = true
  · simp [dispatch, ha]
  · have hbnot : ¬ accepts b w = true := by
      intro hb
      exact ha (himp w hb)
    have htail : (b :: tail).find? (accepts · w) = tail.find? (accepts · w) := by
      simp [hbnot]
    simp [dispatch, ha, htail]

end TinylambOpt.ResearchProofs.JitKernels.VersionDispatch_ShadowChainPruning

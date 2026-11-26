import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

/-! A dominated later guard is never selected: if it accepts, the earlier weaker guard already accepted. Removing it preserves dispatch. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_VersionDispatch_ShadowChainPruning

open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.ShadowChainPruning

theorem weaker_imp (a b : Entry) (h : weaker a b = true) : ∀ w, accepts b w → accepts a w := by
  intro w hb
  simp [weaker, accepts, clash] at h hb ⊢
  apply BitVec.eq_of_getLsbD_eq
  intro i hi
  have hm : a.mask.getLsbD i = true → b.mask.getLsbD i = true := by
    intro ham
    have hbit := congrArg (fun z : Word => z.getLsbD i) h.1
    simpa [BitVec.getLsbD_and, ham] using hbit
  have hc : a.mask.getLsbD i = true → b.mask.getLsbD i = true →
      a.value.getLsbD i = b.value.getLsbD i := by
    intro ham hbm
    have hbit := congrArg (fun z : Word => z.getLsbD i) h.2
    simpa [BitVec.getLsbD_and, BitVec.getLsbD_xor, ham, hbm] using hbit
  have hbw := congrArg (fun z : Word => z.getLsbD i) hb
  simp [BitVec.getLsbD_and] at hbw ⊢
  by_cases ham : a.mask.getLsbD i
  · have hbm := hm ham
    have hval := hc ham hbm
    simp [ham, hbm] at hbw ⊢
    exact hbw.trans hval.symm
  · simp [ham]

theorem bool_false_of_not {p : Bool} (h : ¬ p) : p = false := by
  cases p <;> simp_all

theorem find_cons_of_false {α : Type} (p : α → Bool) (a : α) (l : List α) (ha : ¬ p a) :
    (a :: l).find? p = l.find? p := by
  simp [List.find?, bool_false_of_not ha]

theorem dispatch_cons_of_false (e : Entry) (l : List Entry) (w : Word) (he : ¬ accepts e w) :
    dispatch (e :: l) w = dispatch l w := by
  rw [dispatch, dispatch, find_cons_of_false (accepts · w) e l he]

theorem dispatch_remove_second (b : Entry) (rest tail : List Entry) (w : Word) (hb : ¬ accepts b w) :
    dispatch (rest ++ b :: tail) w = dispatch (rest ++ tail) w := by
  simp [dispatch, List.find?_append, find_cons_of_false (accepts · w) b tail hb]

theorem dispatch_append_first_of_accepts (a : Entry) (rest tail : List Entry) (w : Word) (ha : accepts a w) :
    dispatch ((a :: rest) ++ tail) w = some a.code := by
  rw [dispatch, List.find?_append]
  simp [List.find?, ha]

theorem sound : TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.ShadowChainPruning.Goal := by
  intro a b rest tail w hw
  by_cases ha : accepts a w
  · rw [dispatch_append_first_of_accepts a rest (b :: tail) w ha]
    rw [dispatch_append_first_of_accepts a rest tail w ha]
  · have hb : ¬ accepts b w := by
      intro hb
      exact ha ((weaker_imp a b hw) w hb)
    exact dispatch_remove_second b (a :: rest) tail w hb

end TinylambOpt.ResearchProofs.JitKernels.Goal_VersionDispatch_ShadowChainPruning

import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! Rows agreeing on the union of all tested bits take the same branch in every guard, so dispatch can be hoisted out of a uniform batch. -/
namespace Nov26.Proof_VersionDispatch_PrecomputeGuardOnUniformRows
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.PrecomputeGuardOnUniformRows
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.PrecomputeGuardOnUniformRows
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.PrecomputeGuardOnUniformRows
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.PrecomputeGuardOnUniformRows

private theorem acc_eq (e : Entry) (w : Word) :
    accepts e w = true ↔ (w &&& e.mask) = (e.value &&& e.mask) := by
  simp [accepts]

theorem sound : Goal := by
  have foldl_or (ms : List Word) (acc : Word) :
      ms.foldl (· ||| ·) acc = acc ||| ms.foldl (· ||| ·) 0 := by
    induction ms generalizing acc with
    | nil => simp
    | cons m ms ih =>
        simp [List.foldl_cons]
        rw [ih (acc ||| m), ih m]
        bv_decide
  intro es
  induction es with
  | nil => intro a b m h; simp [dispatch]
  | cons e es ih =>
      intro a b m h
      have hm_eq : (List.map Entry.mask (e :: es)).foldl (· ||| ·) 0 =
          e.mask ||| (List.map Entry.mask es).foldl (· ||| ·) 0 := by
        simpa using foldl_or (List.map Entry.mask es) e.mask
      change a &&& (List.map Entry.mask (e :: es)).foldl (· ||| ·) 0 =
        b &&& (List.map Entry.mask (e :: es)).foldl (· ||| ·) 0 at h
      rw [hm_eq] at h
      have heq_e : (a &&& e.mask) = (b &&& e.mask) := by bv_decide
      have heq_es : a &&& (List.map Entry.mask es).foldl (· ||| ·) 0 =
          b &&& (List.map Entry.mask es).foldl (· ||| ·) 0 := by bv_decide
      by_cases hae : accepts e a = true
      · have hbe : accepts e b = true := by
          apply (acc_eq e b).mpr
          have hae' := (acc_eq e a).mp hae
          bv_decide
        simp [dispatch, hae, hbe]
      · have hbnot : ¬ accepts e b = true := by
          intro hbe
          apply hae
          apply (acc_eq e a).mpr
          have hbe' := (acc_eq e b).mp hbe
          bv_decide
        simp [dispatch, hae, hbnot]
        exact ih a b heq_es

end Nov26.Proof_VersionDispatch_PrecomputeGuardOnUniformRows
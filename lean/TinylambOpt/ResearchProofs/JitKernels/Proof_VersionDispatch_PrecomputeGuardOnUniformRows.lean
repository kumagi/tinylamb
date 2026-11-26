import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-! Rows agreeing on the union of all tested bits take the same branch in every guard, so
dispatch can be hoisted out of a uniform batch. -/
namespace TinylambOpt.ResearchProofs.JitKernels.VersionDispatch_PrecomputeGuardOnUniformRows
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch PrecomputeGuardOnUniformRows

/-- A guard accepts exactly the words whose masked bits equal the masked value. -/
private theorem acc_eq (e : Entry) (w : Word) :
    accepts e w = true ↔ (w &&& e.mask) = (e.value &&& e.mask) := by
  simp [accepts]

/-- Boolean core: if two words agree on a union of two masks, they agree on either mask
alone. -/
private theorem and_le_or_bool (A B E F : Bool)
    (h : (A && (E || F)) = (B && (E || F))) : (A && F) = (B && F) := by
  revert A B E F h
  decide

/--
`PrecomputeGuardOnUniformRows.Goal`: when every row of a batch agrees on all bits that any
guard in the dispatch chain tests (their union mask `m`), then the two representative row
words `a` and `b` trigger exactly the same guard, so the compiled loop may run `dispatch`
once per batch and reuse the chosen version for every row.
-/
theorem sound : Goal := by
  have foldl_or (ms : List Word) (acc : Word) :
      ms.foldl (· ||| ·) acc = acc ||| ms.foldl (· ||| ·) 0 := by
    induction ms generalizing acc with
    | nil => simp
    | cons m ms ih =>
        rw [List.foldl_cons, List.foldl_cons]
        have h0 : (0 : Word) ||| m = m := BitVec.zero_or
        rw [h0, ih (acc ||| m), ih m]
        exact BitVec.or_assoc acc m _
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
      have heq_e : (a &&& e.mask) = (b &&& e.mask) := by
        apply BitVec.eq_of_getLsbD_eq_iff.mpr
        intro k hk
        have h' : (a.getLsbD k && (e.mask.getLsbD k
              || ((List.map Entry.mask es).foldl (· ||| ·) 0).getLsbD k))
            = (b.getLsbD k && (e.mask.getLsbD k
              || ((List.map Entry.mask es).foldl (· ||| ·) 0).getLsbD k)) := by
          simpa [BitVec.getLsbD_and, BitVec.getLsbD_or] using
            congrArg (fun v : Word => v.getLsbD k) h
        have h2 : (a.getLsbD k && (((List.map Entry.mask es).foldl (· ||| ·) 0).getLsbD k
              || e.mask.getLsbD k))
            = (b.getLsbD k && (((List.map Entry.mask es).foldl (· ||| ·) 0).getLsbD k
              || e.mask.getLsbD k)) := by
          rw [Bool.or_comm]; exact h'
        simpa [BitVec.getLsbD_and] using and_le_or_bool _ _ _ _ h2
      have heq_es : a &&& (List.map Entry.mask es).foldl (· ||| ·) 0 =
          b &&& (List.map Entry.mask es).foldl (· ||| ·) 0 := by
        apply BitVec.eq_of_getLsbD_eq_iff.mpr
        intro k hk
        have h' : (a.getLsbD k && (e.mask.getLsbD k
              || ((List.map Entry.mask es).foldl (· ||| ·) 0).getLsbD k))
            = (b.getLsbD k && (e.mask.getLsbD k
              || ((List.map Entry.mask es).foldl (· ||| ·) 0).getLsbD k)) := by
          simpa [BitVec.getLsbD_and, BitVec.getLsbD_or] using
            congrArg (fun v : Word => v.getLsbD k) h
        simpa [BitVec.getLsbD_and] using and_le_or_bool _ _ _ _ h'
      by_cases hae : accepts e a = true
      · have hbe : accepts e b = true := by
          apply (acc_eq e b).mpr
          have hae' := (acc_eq e a).mp hae
          exact Eq.trans heq_e.symm hae'
        simp [dispatch, hae, hbe]
      · have hbnot : ¬ accepts e b = true := by
          intro hbe
          apply hae
          apply (acc_eq e a).mpr
          have hbe' := (acc_eq e b).mp hbe
          exact Eq.trans heq_e hbe'
        simp [dispatch, hae, hbnot]
        exact ih a b heq_es

end TinylambOpt.ResearchProofs.JitKernels.VersionDispatch_PrecomputeGuardOnUniformRows

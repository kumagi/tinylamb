import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! Two equal-bodied guards differing in exactly one masked bit can be merged by dropping that bit from the mask. -/
namespace Nov26.Proof_VersionDispatch_FoldNeighborVersions
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.FoldNeighborVersions
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.FoldNeighborVersions
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.FoldNeighborVersions
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.FoldNeighborVersions

private theorem acc_eq (e : Entry) (w : Word) :
    accepts e w = true ↔ (w &&& e.mask) = (e.value &&& e.mask) := by
  simp [accepts]

theorem sound : Goal := by
  intro a b i w hm hc hbit hxor
  let s : Entry := {a with mask := a.mask &&& ~~~((1 : Word) <<< i.val)}
  have hacc : accepts s w = true ↔ (accepts a w = true ∨ accepts b w = true) := by
    rw [acc_eq s w, acc_eq a w, acc_eq b w]
    dsimp [s]
    constructor <;> intro h <;> bv_decide
  by_cases ha : accepts a w = true
  · have hs : accepts s w = true := hacc.mpr (Or.inl ha)
    have hleft : dispatch [a, b] w = some a.code := by simp [dispatch, ha]
    have hright0 : dispatch [s] w = some s.code := by simp [dispatch, hs]
    have hcode : s.code = a.code := by simp [s]
    rw [hleft, hright0, hcode]
  · by_cases hb : accepts b w = true
    · have hs : accepts s w = true := hacc.mpr (Or.inr hb)
      have hleft : dispatch [a, b] w = some b.code := by simp [dispatch, ha, hb]
      have hright0 : dispatch [s] w = some s.code := by simp [dispatch, hs]
      have hcode : s.code = b.code := by simp [s, hc]
      rw [hleft, hright0, hcode]
    · have hs : ¬ accepts s w = true := by
        intro hs
        exact hb ((hacc.mp hs).resolve_left ha)
      have hleft : dispatch [a, b] w = none := by simp [dispatch, ha, hb]
      have hright : dispatch [s] w = none := by simp [dispatch, hs]
      rw [hleft, hright]

end Nov26.Proof_VersionDispatch_FoldNeighborVersions
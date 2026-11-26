import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

/-! The last matching entry for an old word may be outranked by an earlier entry on a fresh word. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.VersionDispatch.Goal_VersionDispatch_ReuseLastPassingEntry

open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.ReuseLastPassingEntry

theorem refute : ¬ ReuseLastPassingEntry.Goal := by
  intro h
  let hi : Entry := { mask := 2, value := 0, code := 1 }
  let e : Entry := { mask := 0, value := 0, code := 2 }
  let es : List Entry := [hi, e]
  have hfind : es.find? (accepts · 2) = some e := by
    simp [es, hi, e, accepts]
  have hfresh : accepts e 0 := by simp [e, accepts]
  have h' := h es 2 0 e hfind hfresh
  simp [dispatch, es, hi, e, accepts] at h'

end TinylambOpt.ResearchProofs.JitKernels.VersionDispatch.Goal_VersionDispatch_ReuseLastPassingEntry

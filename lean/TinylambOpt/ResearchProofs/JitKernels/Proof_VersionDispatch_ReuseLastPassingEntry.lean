import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! Refutation: the last entry accepted an old row, but a higher-priority
entry can also accept the new row, so replaying the cached code skips a guard
that would have won under ordered dispatch. -/
namespace Nov26.Proof_VersionDispatch_ReuseLastPassingEntry
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.ReuseLastPassingEntry
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.ReuseLastPassingEntry
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.ReuseLastPassingEntry
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.ReuseLastPassingEntry

theorem refute : ¬ Goal := by
  intro h
  let e1 : Entry := {mask := 1, value := 1, code := 1}
  let e2 : Entry := {mask := 0, value := 0, code := 2}
  let es : List Entry := [e1, e2]
  have hfind : es.find? (accepts · 0) = some e2 := by decide
  have hbad := h es 0 1 e2 hfind (by decide)
  have hne : dispatch es 1 ≠ some e2.code := by decide
  exact hne hbad

end Nov26.Proof_VersionDispatch_ReuseLastPassingEntry
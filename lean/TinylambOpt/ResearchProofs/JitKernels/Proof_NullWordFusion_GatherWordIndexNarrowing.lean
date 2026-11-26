import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

/-!
**Refuted: gather word-index narrowing across a word boundary.** Starting at
row 63 in words `[0, 1]`, the second gathered lane comes from word 1 and is
NULL, while the narrowed expression pins every lane to word `start/64` and
reports non-NULL. SQL execution may reuse the low-six-bit lane index with one
word only under an extra no-boundary-crossing certificate.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.NullWordFusion_GatherWordIndexNarrowing
open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

theorem refute : ¬ GatherWordIndexNarrowing.Goal := by
  intro h
  have hh := h ([0, 1] : List W) 63
  have hsecond := congrArg (fun xs : List (Option Bool) => xs[1]?) hh
  exact (by decide : ¬
    (((List.range 32).map (fun j =>
      (lanes ([0, 1] : List W) (63 + 32))[63 + j]?))[1]? =
      ((List.range 32).map (fun j =>
        some ((([0, 1] : List W)[63 / 64]?.getD 0).getLsbD ((63 + j) % 64))))[1]?)) hsecond

end TinylambOpt.ResearchProofs.JitKernels.NullWordFusion_GatherWordIndexNarrowing

import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-!
**Refuted: register renaming injective on destinations does not preserve all
observed transferred outputs when a renamed alias is live in the entry
file.**
The claim says that for a renaming `f` whose images of the destinations are
pairwise distinct, and which is a no-op on the sources' snapshots
(`∀ x ∈ ms, s (f x.2) = s x.2`), running the renamed bundle serially and
reading `f m.1` equals running the original bundle and reading `m.1`. It
fails: take `s r = 100 + r`, `ms = [(0, 1), (1, 2)]` and
`f r = if r == 0 then 2 else r`. Then `f` moves destination 0 onto register 2,
whose entry value 102 coincides with the value destination 1 must *not* see,
and the aliasing hypothesis holds vacuously (both sources are renamed onto
registers whose entry values are 101 = their originals read... concretely the
hypotheses check out), yet serial execution of the renamed bundle reaches
`(1, 2)` as reading source 2 — which the first renamed move overwrote with
the value of register 1 — yielding 102 where the original bundle yields 101
at `m.1 = 1`. Renaming passes in the JIT must keep a live alias for the
*source* registers, not only distinct destination images; allowing the
weaker condition would change query-visible values at deopt/OSR edges.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ParallelMoves_RegisterRenaming
open TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

theorem refute : ¬ RegisterRenamingWithLiveAlias.Goal := by
  intro h
  have hh := h (fun r => 100 + r) [(0, 1), (1, 2)] (fun r => if r == 0 then 2 else r)
    (by decide) (by decide) (1, 2) (by decide)
  exact absurd hh (by decide)

end TinylambOpt.ResearchProofs.JitKernels.ParallelMoves_RegisterRenaming

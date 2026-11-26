import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

/-! SOUND: For a CASE ladder compiled to a dense biased jump table over the profiled domain
`[base, base + n)`, the table slot addressed by the biased index `(x - base).toNat` holds
exactly the first-match ladder result `pick as x` (wrapped in `some`, since the slot always
exists). The `toNat` bias is safe because the two range guards force `0 ≤ x - base < n`, so
NULL-valued arm results are preserved separately from "no arm matched" inside the domain. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DispatchTables_BoundedBiasedJumpTable
open TinylambOpt.ResearchGoals.JitKernels.DispatchTables

/-- Soundness of the bounded biased jump table: inside the profiled domain, indexing the
dense table at `(x - base).toNat` reproduces the first-match ladder result. -/
theorem sound : BoundedBiasedJumpTable.Goal := by
  intro as base n x hlo hhi
  show (List.map (fun (i : Nat) => pick as (base + i)) (List.range n))[(x - base).toNat]?
    = some (pick as x)
  have hkn : (x - base).toNat < n := by omega
  rw [List.getElem?_map, List.getElem?_range]
  · show some (pick as (base + (x - base).toNat)) = some (pick as x)
    refine congrArg some ?_
    refine congrArg (pick as) ?_
    omega
  · omega

end TinylambOpt.ResearchProofs.JitKernels.DispatchTables_BoundedBiasedJumpTable

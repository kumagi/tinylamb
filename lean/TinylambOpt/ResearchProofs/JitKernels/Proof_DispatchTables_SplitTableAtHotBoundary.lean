import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

/-! SOUND: Splitting a jump table at a hot boundary. Emitting the two biased tables
`[base, base + a)` and `[base + a, base + a + b)` and concatenating them reproduces the
single table over `[base, base + a + b)`: `List.range` splits at the boundary and the biased
key `base + i` re-centers exactly, so a hot-boundary split needs no fix-up code. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DispatchTables_SplitTableAtHotBoundary
open TinylambOpt.ResearchGoals.JitKernels.DispatchTables

/-- Soundness of hot-boundary table splitting. -/
theorem sound : SplitTableAtHotBoundary.Goal := by
  intro as base a b
  show List.map (fun (i : Nat) => pick as (base + i)) (List.range a)
      ++ List.map (fun (i : Nat) => pick as (base + a + i)) (List.range b)
    = List.map (fun (i : Nat) => pick as (base + i)) (List.range (a + b))
  rw [List.range_add, List.map_append, List.map_map]
  have hf : (fun (i : Nat) => pick as (base + a + i))
      = (fun (i : Nat) => pick as (base + i)) ∘ (fun x => a + x) := by
    funext i
    show pick as (base + a + i) = pick as (base + (a + i))
    refine congrArg (pick as) ?_
    omega
  rw [hf]

end TinylambOpt.ResearchProofs.JitKernels.DispatchTables_SplitTableAtHotBoundary

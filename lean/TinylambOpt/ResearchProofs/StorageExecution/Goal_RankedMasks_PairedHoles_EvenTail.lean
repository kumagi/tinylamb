import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `PairedHoles.EvenTail` — `∀ n, legal model 0 (List.replicate n false) = decide
  (n%2=0)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PairedHoles_EvenTail

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PairedHoles

-- Starting from a state 0 or 1, each zero bit complements the state, so the
-- state after n zeros is (q + n) % 2.
private theorem run_replicate_false : ∀ n q, q < 2 →
    run model q (List.replicate n false) = some ((q + n) % 2)
  | 0, q, hq => by
      simp [run, Nat.mod_eq_of_lt hq]
  | n + 1, q, hq => by
      rw [List.replicate_succ]
      change run model (1 - q) (List.replicate n false) = some ((q + (n + 1)) % 2)
      rw [run_replicate_false n (1 - q) (by omega)]
      congr 1
      omega

theorem sound_EvenTail : PairedHoles.EvenTail.Goal := by
  intro n
  have hrun : run model 0 (List.replicate n false) = some (n % 2) := by
    simpa using run_replicate_false n 0 (by omega)
  simp [legal, hrun]
  simp [model]
  by_cases h : n % 2 = 0 <;> simp [h]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PairedHoles_EvenTail

import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `RunLimit.EraseSeparator` — `∀ a b, legal model 0 a → legal model 0 b → legal model 0
  (a++[false]++b)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_RunLimit_EraseSeparator

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.RunLimit

private theorem run_cons (m : Machine) (q : Nat) (b : Bool) (bs : List Bool) :
    run m q (b :: bs) = (m.step q b).bind (fun q' => run m q' bs) := by
  simp [run, List.foldlM]

private theorem run_append (m : Machine) (q : Nat) (a b : List Bool) :
    run m q (a ++ b) = (run m q a).bind (fun q' => run m q' b) := by
  induction a generalizing q with
  | nil => simp [run]
  | cons x xs ih => simp [run_cons, ih, Option.bind_assoc]

private theorem legal_model_run (xs : List Bool) (h : legal model 0 xs = true) :
    ∃ q, run model 0 xs = some q := by
  unfold legal at h
  generalize hr : run model 0 xs = o at h
  cases o with
  | none => simp [model] at h
  | some q => exact ⟨q, rfl⟩

theorem sound_EraseSeparator : RunLimit.EraseSeparator.Goal := by
  intro a b ha hb
  rcases legal_model_run a ha with ⟨qa, ha_run⟩
  rcases legal_model_run b hb with ⟨qb, hb_run⟩
  have hrun : run model 0 (a ++ [false] ++ b) = some qb := by
    rw [run_append model 0 (a ++ [false]) b]
    have hpref : run model 0 (a ++ [false]) = some 0 := by
      rw [run_append model 0 a [false]]
      rw [ha_run]
      simp only [Option.bind_some]
      simp [run, List.foldlM, model]
    rw [hpref]
    simp only [Option.bind_some]
    exact hb_run
  unfold legal
  rw [hrun]
  simp [model]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_RunLimit_EraseSeparator

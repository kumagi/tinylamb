import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `IndependentHits.Cofactor` — Compute a parallel shard count by conditioning on its
  prefix state.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_IndependentHits_Cofactor

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.IndependentHits

private abbrev hiCount (m : Machine) (n q : Nat) : Nat :=
  ((m.step q true).map (count m n)).getD 0

private theorem count_succ (m : Machine) (n q : Nat) :
    count m (n + 1) q = lowCount m n q + hiCount m n q := by
  simp [count, lowCount, hiCount]

private theorem run_cons (m : Machine) (q : Nat) (b : Bool) (bs : List Bool) :
    run m q (b :: bs) = (m.step q b).bind (fun q' => run m q' bs) := by
  simp [run, List.foldlM]

private theorem sum_map_bind_step (m : Machine) (q : Nat) (b : Bool) (n k : Nat) :
    (((words n).map (fun pre => (((m.step q b).bind (fun q' => run m q' pre)).map (count m k)).getD 0)).sum) =
    (((m.step q b).map (fun q' => ((words n).map (fun pre => ((run m q' pre).map (count m k)).getD 0)).sum)).getD 0) := by
  cases h : m.step q b with
  | none =>
    simp [h]
    induction (words n) with
    | nil => simp
    | cons x xs ih => simp [ih]
  | some q' => simp [h]

private theorem cofactor_generic (m : Machine) : cofactor m := by
  intro q a b
  induction a generalizing q with
  | zero =>
    simp [words, run, cofactor]
  | succ a ih =>
    have hsplit : count m (a + 1 + b) q = count m (a + b + 1) q := by
      have : a + 1 + b = a + b + 1 := by omega
      rw [this]
    rw [hsplit, count_succ]
    simp only [words, List.map_append, List.sum_append, List.map_map, Function.comp_def]
    have hl : ((words a).map (fun pre => ((run m q (false :: pre)).map (count m b)).getD 0)).sum = lowCount m (a + b) q := by
      have h1 : ∀ pre, ((run m q (false :: pre)).map (count m b)).getD 0 =
          (((m.step q false).bind (fun q' => run m q' pre)).map (count m b)).getD 0 := by
        intro pre
        rw [run_cons]
      simp only [h1]
      rw [sum_map_bind_step]
      simp [lowCount]
      cases h : m.step q false with
      | none => simp
      | some q' => simp [ih q']
    have hr : ((words a).map (fun pre => ((run m q (true :: pre)).map (count m b)).getD 0)).sum = hiCount m (a + b) q := by
      have h1 : ∀ pre, ((run m q (true :: pre)).map (count m b)).getD 0 =
          (((m.step q true).bind (fun q' => run m q' pre)).map (count m b)).getD 0 := by
        intro pre
        rw [run_cons]
      simp only [h1]
      rw [sum_map_bind_step]
      simp [hiCount]
      cases h : m.step q true with
      | none => simp
      | some q' => simp [ih q']
    rw [hl, hr]

theorem sound_Cofactor : IndependentHits.Cofactor.Goal := by
  exact cofactor_generic model

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_IndependentHits_Cofactor

import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Click_Goal_Q24DuckDB
open Std
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Click.Q24DuckDB.Goal := by
  intro ids key payload
  let cmp : Nat → Nat → Bool := fun a b => decide (key a ≤ key b)
  let pair : Nat → Nat × String := fun i => (i, payload i)
  have hsort : (ids.map pair).mergeSort (fun p q => cmp p.1 q.1) = (ids.mergeSort cmp).map pair := by
    symm
    exact List.map_mergeSort (r := cmp) (s := fun p q : Nat × String => cmp p.1 q.1)
      (f := pair) (l := ids) (by
        intro a ha b hb
        rfl)
  change ((ids.mergeSort cmp).take 10).map payload = (((ids.map pair).mergeSort (fun p q => cmp p.1 q.1)).take 10).map Prod.snd
  rw [hsort]
  rw [← List.map_take]
  simp [pair, Function.comp_def]
end TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Click_Goal_Q24DuckDB

import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q38ClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click
open TinylambOpt.ResearchGoals.BenchmarkRematch

private theorem filterMap_bifurcate :
    ∀ (xs : List (Bool × String)),
      (xs.filterMap fun (b, t) => if b then some t else none)
        = (xs.filter Prod.fst).map Prod.snd := by
  intro xs
  induction xs with
  | nil => rfl
  | cons r t ih =>
    obtain ⟨b, s⟩ := r
    cases b <;> simp [ih]

private theorem filter_snd :
    ∀ (xs : List (Bool × String)),
      (((xs.filter Prod.fst).map Prod.snd).filter (· != ""))
        = (xs.filter (fun x => x.1 && x.2 != "")).map Prod.snd := by
  intro xs
  induction xs with
  | nil => rfl
  | cons r t ih =>
    obtain ⟨b, s⟩ := r
    cases b <;> cases h : (s != "") <;> simp_all

theorem sound : Q38ClickHouse.Goal := by
  intro flags titles
  simp only [countWhere]
  rw [filterMap_bifurcate, filter_snd, List.length_map]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q38ClickHouse

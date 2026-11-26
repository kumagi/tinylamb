import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group
import TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_L5DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group
open TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum

theorem sound : L5DuckDB.Goal := by
  intro people edges
  dsimp only
  have F1 : ∀ ps : List (Nat × Option Int),
      (ps.flatMap (fun (k,a) => List.replicate (edges.count k) a)).length = (ps.map (fun r => edges.count r.1)).sum := by
    intro ps; induction ps with
    | nil => rfl
    | cons p ps ih => simp [List.flatMap_cons, ih]
  have F2 : ∀ ps : List (Nat × Option Int),
      ((ps.flatMap (fun (k,a) => List.replicate (edges.count k) a)).filterMap id).sum
        = total ps (fun (k,a) => (edges.count k : Int) * a.getD 0) := by
    intro ps; induction ps with
    | nil => rfl
    | cons p ps ih =>
      obtain ⟨k, a⟩ := p
      cases a with
      | none => simp [List.flatMap_cons, List.filterMap_append, fm_rep_none, total, ih] at *
      | some x => simp [List.flatMap_cons, List.filterMap_append, fm_rep_some, total, List.sum_append_int, ih] at *
  have F3 : ∀ ps : List (Nat × Option Int),
      ((ps.flatMap (fun (k,a) => List.replicate (edges.count k) a)).filterMap id).length
        = ((ps.filter (fun r => r.2.isSome)).map (fun r => edges.count r.1)).sum := by
    intro ps; induction ps with
    | nil => rfl
    | cons p ps ih =>
      obtain ⟨k, a⟩ := p
      cases a with
      | none => simp [List.flatMap_cons, List.filterMap_append, fm_rep_none, List.filter_cons, ih]
      | some x => simp [List.flatMap_cons, List.filterMap_append, fm_rep_some, List.filter_cons, ih]
  exact Prod.ext (F1 people) (Prod.ext (F2 people) (F3 people))

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_L5DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_L5DuckDB.sound

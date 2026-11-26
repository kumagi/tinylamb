import TinylambOpt.ResearchGoals.BenchmarkRematch.Click
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group
import TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum
import TinylambOpt.ResearchProofs.BenchmarkRematch.Topn
import TinylambOpt.ResearchProofs.BenchmarkRematch.Dedup

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Click_Q12DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group
open TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum
open TinylambOpt.ResearchProofs.BenchmarkRematch.Topn
open TinylambOpt.ResearchProofs.BenchmarkRematch.Dedup

theorem sound : Q12DuckDB.Goal := by
  intro parts
  induction parts with
  | nil => rfl
  | cons p ps ih =>
    rw [List.flatMap_cons, List.flatten_cons, List.eraseDups_append, List.eraseDups_append, ed_idem]
    congr 1
    have hq : ∀ x, (p.eraseDups.elem x) = (p.elem x) := by
      intro x
      rw [Bool.eq_iff_iff]
      simp [List.elem_iff]
    show ((List.flatMap List.eraseDups ps).filter (fun x => !p.eraseDups.elem x)).eraseDups
        = (ps.flatten.filter (fun x => !p.elem x)).eraseDups
    simp only [hq]
    rw [ed_filter, ed_filter, ih]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Click_Q12DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Click_Q12DuckDB.sound

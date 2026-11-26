import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q57DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

/-- `zipIdx start` tags each row with its own position, so looking a key up
there is exactly an index access. -/
private theorem zipIdx_lookup : ∀ (xs : List Int) (i start : Nat),
    ((xs.zipIdx start).find? (fun r => r.2 == start + i)).map Prod.fst = xs[i]? := by
  intro xs
  induction xs with
  | nil => intro i start; simp
  | cons x xs ih =>
    intro i start
    rw [List.zipIdx_cons]
    cases i with
    | zero =>
      rw [List.find?_cons_of_pos (by simp), Option.map_some, List.getElem?_cons_zero]
    | succ j =>
      rw [show (start + (j + 1)) = (start + 1) + j from by omega]
      rw [List.find?_cons_of_neg (by simp; omega), ih j (start + 1), List.getElem?_cons_succ]

private theorem lookup_eq (xs : List Int) (i : Nat) :
    ((xs.zipIdx.find? (fun r => r.2 == i)).map Prod.fst) = xs[i]? := by
  rw [show (fun r : Int × Nat => r.2 == i) = (fun r : Int × Nat => r.2 == 0 + i) from
    funext fun r => congrArg (fun t : Nat => r.2 == t) (Nat.zero_add i).symm]
  exact zipIdx_lookup xs i 0

theorem sound : Q57DuckDB.Goal := by
  intro xs i
  show ((xs.zipIdx.find? (fun r => r.2 == i)).map Prod.fst,
      (xs.zipIdx.find? (fun r => r.2 == i + 1)).map Prod.fst,
      (xs.zipIdx.find? (fun r => r.2 == i + 2)).map Prod.fst) =
      (xs[i]?, xs[i+1]?, xs[i+2]?)
  rw [lookup_eq xs i, lookup_eq xs (i+1), lookup_eq xs (i+2)]

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q57DuckDB.sound

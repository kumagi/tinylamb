import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_L5DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem sum_replicate_int (n : Nat) (v : Int) : (List.replicate n v).sum = (n : Int) * v := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.sum_cons, ih]
      rw [Int.natCast_add, Int.add_mul]
      simp [Int.add_comm]

theorem filterMap_replicate_sum (n : Nat) (a : Option Int) : ((List.replicate n a).filterMap id).sum = (n : Int) * a.getD 0 := by
  cases a <;> simp [sum_replicate_int]

theorem filterMap_replicate_length (n : Nat) (a : Option Int) : ((List.replicate n a).filterMap id).length = if a.isSome then n else 0 := by
  cases a <;> simp

theorem len_flatMap (people : List (Nat × Option Int)) (edges : List Nat) :
    (people.flatMap (fun (k,a) => List.replicate (edges.count k) a)).length =
      (people.map (fun r => edges.count r.1)).sum := by
  induction people with
  | nil => simp
  | cons p t ih =>
      rcases p with ⟨k,a⟩
      simp [ih]

theorem sum_flatMap (people : List (Nat × Option Int)) (edges : List Nat) :
    ((people.flatMap (fun (k,a) => List.replicate (edges.count k) a)).filterMap id).sum =
      total people (fun (k,a) => (edges.count k : Int) * a.getD 0) := by
  induction people with
  | nil => simp [total]
  | cons p t ih =>
      rcases p with ⟨k,a⟩
      simp [total, ih, filterMap_replicate_sum, List.sum_append]

theorem count_flatMap (people : List (Nat × Option Int)) (edges : List Nat) :
    ((people.flatMap (fun (k,a) => List.replicate (edges.count k) a)).filterMap id).length =
      ((people.filter (fun r => r.2.isSome)).map (fun r => edges.count r.1)).sum := by
  induction people with
  | nil => simp
  | cons p t ih =>
      rcases p with ⟨k,a⟩
      simp [ih, filterMap_replicate_length, List.filter_cons, List.length_append]
      by_cases ha : a.isSome = true <;> simp [ha]

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics.L5DuckDB.Goal := by
  intro people edges
  dsimp only
  apply Prod.ext
  · exact len_flatMap people edges
  · apply Prod.ext
    · exact sum_flatMap people edges
    · exact count_flatMap people edges

end TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_L5DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_L5DuckDB.sound

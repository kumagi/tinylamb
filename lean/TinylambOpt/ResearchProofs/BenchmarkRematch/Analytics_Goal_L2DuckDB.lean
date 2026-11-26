import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_L2DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem flatten_replicate_len (n : Nat) (scores : List Int) :
    (List.replicate n scores).flatten.length = n * scores.length := by
  induction n with
  | zero => simp
  | succ n ih => simp [Nat.add_mul, Nat.add_comm]

theorem flatten_replicate_sum (n : Nat) (scores : List Int) :
    (List.replicate n scores).flatten.sum = (n : Int) * scores.sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      simp only [List.replicate_succ, List.flatten_cons, List.sum_append, ih]
      rw [Int.natCast_add, Int.add_mul]
      simp [Int.add_comm]

theorem flatMap_replicate_const (n : Nat) (k : Nat) (scores : List Int) :
    (List.replicate n k).flatMap (fun _ => scores) = (List.replicate n scores).flatten := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.flatMap_cons, ih]
      rw [List.replicate_succ, List.flatten_cons]

theorem rep_flat (k : Nat) (people : List Nat) (scores : List Int) :
    (people.filter (· == k)).flatMap (fun _ => scores) =
      (List.replicate (people.count k) scores).flatten := by
  rw [List.filter_beq]
  exact flatMap_replicate_const (people.count k) k scores

theorem len_expanded (members people : List Nat) (scores : List Int) :
    (members.flatMap fun k => (people.filter (· == k)).flatMap (fun _ => scores)).length =
      (members.map people.count).sum * scores.length := by
  induction members with
  | nil => simp
  | cons k t ih =>
      simp only [List.flatMap_cons, List.length_append, List.map_cons, List.sum_cons]
      rw [rep_flat, flatten_replicate_len, ih]
      rw [Nat.add_mul]

theorem sum_expanded (members people : List Nat) (scores : List Int) :
    (members.flatMap fun k => (people.filter (· == k)).flatMap (fun _ => scores)).sum =
      ((members.map people.count).sum : Int) * scores.sum := by
  induction members with
  | nil => simp
  | cons k t ih =>
      simp only [List.flatMap_cons, List.sum_append, List.map_cons, List.sum_cons]
      rw [rep_flat, flatten_replicate_sum, ih]
      rw [Int.natCast_add, Int.add_mul]

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics.L2DuckDB.Goal := by
  intro members people scores
  dsimp only
  apply Prod.ext
  · exact len_expanded members people scores
  · exact sum_expanded members people scores

end TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_L2DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_L2DuckDB.sound

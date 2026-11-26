import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group
import TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_L2DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group
open TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum

theorem sound : L2DuckDB.Goal := by
  intro members people scores
  dsimp only
  have hl : ∀ (l : List Nat), (l.flatMap (fun _ => scores)).length = l.length * scores.length := by
    intro l; induction l with
    | nil => simp
    | cons a l ih => simp [List.flatMap_cons, ih, Nat.succ_mul, Nat.add_comm]
  have hs : ∀ (l : List Nat), (l.flatMap (fun _ => scores)).sum = (l.length : Int) * scores.sum := by
    intro l; induction l with
    | nil => simp
    | cons a l ih => simp [List.flatMap_cons, List.sum_append_int, ih, Int.add_mul, Int.add_comm]
  have hc : ∀ k, (people.filter (· == k)).length = people.count k := by
    intro k; rw [List.count_eq_length_filter]
  have hl2 : ∀ (ms : List Nat), (ms.flatMap fun k => (people.filter (· == k)).flatMap (fun _ => scores)).length
      = (ms.map people.count).sum * scores.length := by
    intro ms; induction ms with
    | nil => simp
    | cons k ms ih => simp [List.flatMap_cons, hl, hc, ih, Nat.add_mul]
  have hs2 : ∀ (ms : List Nat), (ms.flatMap fun k => (people.filter (· == k)).flatMap (fun _ => scores)).sum
      = (((ms.map people.count).sum : Nat) : Int) * scores.sum := by
    intro ms; induction ms with
    | nil => simp
    | cons k ms ih =>
      simp only [List.flatMap_cons, List.sum_append_int, hs, hc, ih, List.map_cons, List.sum_cons]
      push_cast
      simp only [Int.add_mul]
  exact Prod.ext (hl2 members) (hs2 members)

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_L2DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_L2DuckDB.sound

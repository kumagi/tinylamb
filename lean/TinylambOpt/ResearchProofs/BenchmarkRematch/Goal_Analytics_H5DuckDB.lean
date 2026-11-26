import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group
import TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_H5DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group
open TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum

theorem sound : H5DuckDB.Goal := by
  intro x y
  dsimp only
  have hm1 : ∀ (a : Option Int) (y : List (Option Int)), (y.map (fun b => (a,b))).map Prod.fst = List.replicate y.length a := by
    intro a y; induction y with
    | nil => rfl
    | cons b y ih => simp [List.replicate_succ, ih]
  have hm2 : ∀ (a : Option Int) (y : List (Option Int)), (y.map (fun b => (a,b))).map Prod.snd = y := by
    intro a y; induction y with
    | nil => rfl
    | cons b y ih => simp [ih]
  have h1 : ∀ x : List (Option Int), (x.flatMap fun a => y.map (fun b => (a,b))).map Prod.fst = x.flatMap (List.replicate y.length) := by
    intro x; induction x with
    | nil => rfl
    | cons a x ih => simp [List.flatMap_cons, List.map_append, ih, hm1]
  have h2 : ∀ x : List (Option Int), (x.flatMap fun a => y.map (fun b => (a,b))).map Prod.snd = (List.replicate x.length y).flatten := by
    intro x; induction x with
    | nil => rfl
    | cons a x ih => simp [List.flatMap_cons, List.map_append, ih, hm2, List.replicate_succ]
  refine Prod.ext ?_ ?_
  · show nsum _ = _
    rw [h1 x, nsum_rep_flat]
    by_cases hy : y.isEmpty = true
    · have : y.length = 0 := by simpa using hy
      simp [this, hy]
    · have : (y.length == 0) = false := by simpa using hy
      have hy' : y.isEmpty = false := by simpa using hy
      simp [this, hy', Int.mul_comm]
  · show nsum _ = _
    rw [h2 x, nsum_flat_rep]
    by_cases hx : x.isEmpty = true
    · have : x.length = 0 := by simpa using hx
      simp [this, hx]
    · have : (x.length == 0) = false := by simpa using hx
      have hx' : x.isEmpty = false := by simpa using hx
      simp [this, hx', Int.mul_comm]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_H5DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_H5DuckDB.sound

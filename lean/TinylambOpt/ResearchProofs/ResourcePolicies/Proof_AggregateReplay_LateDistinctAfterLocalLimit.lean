import TinylambOpt.ResearchGoals.QueryRewrites.Common
import TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

namespace TinylambOpt.ResearchProofs.ResourcePolicies.AggregateReplay.Goal_LateDistinctAfterLocalLimit

open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay


/-! Scratch proof: local top-k truncation before DISTINCT can crowd out a
    distinct value that appears in the global top-k of the deduplicated stream. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

/-- One run [0,0,1] with k=2: local best is [0,0] so the deduplicated local
    stream has only [0], while the deduplicated global stream has [0,1]. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay.LateDistinctAfterLocalLimit.Goal := by
  intro h
  have hbad := h 2 [[0, 0, 1]]
  have p3 : List.Pairwise (fun a b => decide (a ≤ b) = true) ([0, 0, 1] : List Nat) := by decide
  have p0 : List.Pairwise (fun a b => decide (a ≤ b) = true) ([0] : List Nat) := by decide
  have p01 : List.Pairwise (fun a b => decide (a ≤ b) = true) ([0, 1] : List Nat) := by decide
  have h1 : best 2 ([0, 0, 1] : List Nat) = [0, 0] := by
    show (List.mergeSort [0, 0, 1]).take 2 = [0, 0]
    rw [List.mergeSort_of_pairwise p3]
    rfl
  have h2 : ([[0, 0, 1]] : List (List Nat)).flatMap (best 2) = [0, 0] := by
    rw [List.flatMap_cons, List.flatMap_nil, h1]
    rfl
  have h3 : best 2 (([0, 0] : List Nat).eraseDups) = ([0] : List Nat) := by
    show (List.mergeSort [0]).take 2 = [0]
    rw [List.mergeSort_of_pairwise p0]
    rfl
  have h4 : best 2 (([0, 0, 1] : List Nat).eraseDups) = ([0, 1] : List Nat) := by
    show (List.mergeSort [0, 1]).take 2 = [0, 1]
    rw [List.mergeSort_of_pairwise p01]
    rfl
  have hfl : ([[0, 0, 1]] : List (List Nat)).flatten = ([0, 0, 1] : List Nat) := rfl
  rw [h2, h3, hfl, h4] at hbad
  exact absurd hbad (by simp)
end TinylambOpt.ResearchProofs.ResourcePolicies.AggregateReplay.Goal_LateDistinctAfterLocalLimit

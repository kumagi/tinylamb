import TinylambOpt.ResearchGoals.November26.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.November26.Analytics.Goal_L2
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem erase_contains_eq_decide_mem (forums : List Nat) :
    (fun r : Nat × Nat => forums.eraseDups.contains r.1) =
      (fun r : Nat × Nat => decide (r.1 ∈ forums)) := by
  funext r
  change List.elem r.1 forums.eraseDups = decide (r.1 ∈ forums)
  rw [List.elem_eq_mem]
  simp only [List.mem_eraseDups]

theorem count_eq_zero_of_not_mem (forums : List Nat) (k : Nat)
    (h : ¬ k ∈ forums) : forums.count k = 0 := by
  exact (List.count_eq_zero).2 h

theorem sound : TinylambOpt.ResearchGoals.November26.Analytics.L2.Goal := by
  intro members forums ageMatches
  let weight := fun (r : Nat × Nat) => forums.count r.1 * ageMatches r.2
  change (List.map weight (List.filter (fun r : Nat × Nat => forums.eraseDups.contains r.1) members)).sum =
    (List.map weight members).sum
  rw [erase_contains_eq_decide_mem forums]
  induction members with
  | nil => simp
  | cons r t ih =>
      by_cases hm : r.1 ∈ forums
      · have hp : decide (r.1 ∈ forums) = true := by simp [hm]
        rw [List.filter_cons_of_pos (p := fun r : Nat × Nat => decide (r.1 ∈ forums)) hp]
        simp [ih]
      · have hz : forums.count r.1 = 0 := count_eq_zero_of_not_mem forums r.1 hm
        have hp : ¬ decide (r.1 ∈ forums) = true := by simp [hm]
        rw [List.filter_cons_of_neg (p := fun r : Nat × Nat => decide (r.1 ∈ forums)) hp]
        simp [weight, hz, ih]
end TinylambOpt.ResearchProofs.November26.Analytics.Goal_L2

import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-!
# Goal_PartitionedAggregateStates_YearSelectionCommutesWithUnionDedup

Registers the proof of the YearSelectionCommutesWithUnionDedup goal in PartitionedAggregateStates: the modeled rewrite
preserves the exact result, so SQL execution may apply it.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_YearSelectionCommutesWithUnionDedup
open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.YearSelectionCommutesWithUnionDedup

private theorem filter_flatMap {α β : Type} (p : β → Bool) (f : α → List β) (xs : List α) :
    (xs.flatMap f).filter p = xs.flatMap (fun x => (f x).filter p) := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [List.flatMap_cons, List.filter_append, ih]

private theorem eraseDups_filter {α : Type} [BEq α] [LawfulBEq α] (p : α → Bool) :
    ∀ xs : List α, (xs.filter p).eraseDups = xs.eraseDups.filter p
  | [] => by simp
  | x :: xs => by
      by_cases hp : p x = true
      · have hcomm : ((xs.filter p).filter (fun b => !b == x)) =
            ((xs.filter (fun b => !b == x)).filter p) := by
          simp only [List.filter_filter]
          congr 1; funext y; cases p y <;> simp [Bool.and_comm]
        rw [List.filter_cons_of_pos hp, List.eraseDups_cons, hcomm]
        rw [eraseDups_filter p (xs.filter (fun b => !b == x))]
        rw [List.eraseDups_cons, List.filter_cons_of_pos hp]
      · have hp' : p x = false := by cases h : p x <;> simp_all
        have hcomm : (List.filter p (List.filter (fun b => !b == x) xs)) = List.filter p xs := by
          simp only [List.filter_filter]
          congr 1; funext y
          by_cases hyx : y == x <;> by_cases hpy : p y <;> simp [hyx, hpy, hp'] <;> try (cases (eq_of_beq hyx); simp_all)
        rw [List.filter_cons_of_neg (by simp [hp']), List.eraseDups_cons, List.filter_cons_of_neg (by simp [hp'])]
        rw [← hcomm]
        exact eraseDups_filter p (List.filter (fun b => !b == x) xs)
termination_by xs => xs.length
decreasing_by
  all_goals simp_wf
  all_goals exact Nat.lt_succ_of_le (List.length_filter_le _ _)

theorem sound : Goal := by
  intro channels
  dsimp [Goal]
  let keep := fun (p : Nat × Int × Int) => p.1 == 2001 || p.1 == 2002
  have hflat : channels.flatten = channels.flatMap id := by simp [List.flatten]
  rw [hflat]
  rw [show channels.flatMap (List.filter keep) = (channels.flatMap id).filter keep by
    symm; exact filter_flatMap keep id channels]
  exact eraseDups_filter keep (channels.flatMap id)
end TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_YearSelectionCommutesWithUnionDedup

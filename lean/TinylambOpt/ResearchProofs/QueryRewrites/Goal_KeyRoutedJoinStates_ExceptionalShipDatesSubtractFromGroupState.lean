import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
# Goal_KeyRoutedJoinStates_ExceptionalShipDatesSubtractFromGroupState

Registers the proof of the ExceptionalShipDatesSubtractFromGroupState goal in KeyRoutedJoinStates: the modeled rewrite
preserves the exact result, so SQL execution may apply it.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_ExceptionalShipDatesSubtractFromGroupState
open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.ExceptionalShipDatesSubtractFromGroupState

private theorem sum_filterMap_partition {α : Type} (xs : List α) (p : α → Bool) (f : α → Option Int) :
    (xs.filterMap f).sum = ((xs.filter p).filterMap f).sum + ((xs.filter (fun x => !p x)).filterMap f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases hp : p x = true
      · simp [hp, List.filterMap_cons, ih]
        cases hf : f x <;> (simp [hf, List.filterMap_cons, ih]; try omega)
      · have hp' : (!p x) = true := by simp [hp]
        simp [hp, hp', List.filterMap_cons, ih]
        cases hf : f x <;> (simp [hf, List.filterMap_cons, ih]; try omega)

private theorem length_filterMap_partition {α : Type} (xs : List α) (p : α → Bool) (f : α → Option Int) :
    (xs.filterMap f).length = ((xs.filter p).filterMap f).length + ((xs.filter (fun x => !p x)).filterMap f).length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases hp : p x = true
      · simp [hp, List.filterMap_cons, ih]
        cases f x <;> simp [Nat.add_assoc, Nat.add_left_comm, Nat.add_comm] <;> omega
      · have hp' : (!p x) = true := by simp [hp]
        simp [hp, hp', List.filterMap_cons, ih]
        cases f x <;> simp [Nat.add_assoc, Nat.add_left_comm, Nat.add_comm] <;> omega

theorem sound : Goal := by
  intro xs cutoff
  simp only [Goal]
  have hsum := sum_filterMap_partition xs (fun r : Nat × Option Int => decide (r.1 ≤ cutoff)) Prod.snd
  have hlen := length_filterMap_partition xs (fun r : Nat × Option Int => decide (r.1 ≤ cutoff)) Prod.snd
  have hnot : (fun r : Nat × Option Int => !(decide (r.1 ≤ cutoff))) = (fun r => decide (cutoff < r.1)) := by
    funext r
    by_cases h : r.1 ≤ cutoff <;> simp [h, Nat.not_le, Nat.lt_of_not_ge]
  rw [hnot] at hsum hlen
  apply Prod.ext <;> omega

end TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_ExceptionalShipDatesSubtractFromGroupState

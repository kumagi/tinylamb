import TinylambOpt.ResearchGoals.November26.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.November26.Click.Goal_Q14
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26.Click

theorem len_ed_le : ∀ l : List Nat, l.eraseDups.length ≤ l.length
  | [] => by simp
  | a :: l => by
      rw [List.eraseDups_cons]
      have h := len_ed_le (l.filter (fun b => !(b == a)))
      have hf := List.length_filter_le (fun b => !(b == a)) l
      simp at *
      omega
termination_by l => l.length
decreasing_by
  simp
  have := List.length_filter_le (fun b => !(b == a)) l
  omega

theorem len_removeAll_le : ∀ (a b : List Nat), (b.removeAll a).length ≤ b.length
  | [], b => by simp
  | y :: as, b => by
      rw [List.removeAll_cons]
      have ih := len_removeAll_le as (b.filter (fun x => !(x == y)))
      have hf := List.length_filter_le (fun x => !(x == y)) b
      omega

theorem len_ed_append_le (a b : List Nat) : (a ++ b).eraseDups.length ≤ a.eraseDups.length + b.length := by
  rw [List.eraseDups_append]
  rw [List.length_append]
  have h1 := len_ed_le (b.removeAll a)
  have h2 := len_removeAll_le a b
  omega

theorem countWhere_mono {α : Type} (xs : List α) (p q : α → Bool)
    (h : ∀ x, p x = true → q x = true) :
    countWhere xs p ≤ countWhere xs q := by
  unfold countWhere
  induction xs with
  | nil => simp
  | cons a xs ih =>
      by_cases hp : p a = true
      · have hq : q a = true := h a hp
        simp [hp, hq, ih]
      · by_cases hq : q a = true
        · simp [hp, hq]
          omega
        · simp [hp, hq, ih]

theorem sound : TinylambOpt.ResearchGoals.November26.Click.Q14.Goal := by
  intro seen rest otherFinal h
  have hmono := countWhere_mono otherFinal
    (fun x => decide (seen.eraseDups.length + rest.length < x))
    (fun x => decide ((seen ++ rest).eraseDups.length < x)) ?_
  · omega
  · intro x hx
    simp only [decide_eq_true_eq] at hx ⊢
    have hlen := len_ed_append_le seen rest
    omega
end TinylambOpt.ResearchProofs.November26.Click.Goal_Q14

import TinylambOpt.ResearchGoals.Atlas.PlanSearch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ConditionalSwap

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.PredicateOrder

private theorem filter_filter_comm {α : Type} (p q : α → Bool) : ∀ l : List α,
    (l.filter p).filter q = (l.filter q).filter p := by
  intro l
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases hp : p a = true <;> by_cases hq : q a = true <;> simp [hp, hq, ih]

private theorem countP_le_length {α : Type} (p : α → Bool) : ∀ l : List α,
    l.countP p ≤ l.length := by
  intro l
  induction l with
  | nil => simp
  | cons a l ih => by_cases hp : p a = true <;> simp [hp] <;> omega

private theorem countP_compl {α : Type} (p : α → Bool) : ∀ l : List α,
    l.countP (fun x => !p x) = l.length - l.countP p := by
  intro l
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases hp : p a = true
      · simp [hp, ih]
      · have hle := countP_le_length p l
        simp [hp, ih] at *
        omega

private theorem countP_eq_filter_length {α : Type} (p : α → Bool) (l : List α) :
    l.countP p = (l.filter p).length := by
  induction l with
  | nil => simp
  | cons a l ih => by_cases hp : p a = true <;> simp [hp, ih]

private theorem actual_two (ca cb : Nat) (pa pb : Nat → Bool)
    (rest : List (Nat × (Nat → Bool))) : ∀ rows : List Nat,
    actual ((ca, pa) :: (cb, pb) :: rest) rows =
      ca * rows.length + cb * (rows.filter pa).length +
        actual rest ((rows.filter pa).filter pb) := by
  intro rows
  simp [actual, Nat.add_assoc]

private theorem main : ∀ (ca cb : Nat) (pa pb : Nat → Bool)
    (rest : List (Nat × (Nat → Bool))) (rows : List Nat),
    (actual ((ca, pa) :: (cb, pb) :: rest) rows : Int) -
        actual ((cb, pb) :: (ca, pa) :: rest) rows =
      (ca:Int) * rows.countP (fun x => !pb x) - (cb:Int) * rows.countP (fun x => !pa x) := by
  intro ca cb pa pb rest rows
  rw [actual_two ca cb pa pb rest rows, actual_two cb ca pb pa rest rows]
  rw [filter_filter_comm pa pb rows]
  rw [countP_compl pb rows, countP_compl pa rows]
  rw [countP_eq_filter_length pb rows, countP_eq_filter_length pa rows]
  have hpb_le : (rows.filter pb).length ≤ rows.length := List.length_filter_le _ _
  have hpa_le : (rows.filter pa).length ≤ rows.length := List.length_filter_le _ _
  rw [Int.ofNat_sub hpb_le, Int.ofNat_sub hpa_le]
  rw [Int.mul_sub, Int.mul_sub]
  omega

theorem sound_ConditionalSwap : PredicateOrder.ConditionalSwap.Goal := by
  exact main

end TinylambOpt.ResearchProofs.Atlas.Goal_ConditionalSwap

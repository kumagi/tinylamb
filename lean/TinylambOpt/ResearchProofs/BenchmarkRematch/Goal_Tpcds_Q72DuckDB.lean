import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q72DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

/-- On a list sorted by `le`, a predicate that propagates to smaller elements
is decided by `takeWhile`: everything satisfying it forms a prefix. -/
private theorem ftw {α : Type} (p : α → Bool) (l : List α) (le : α → α → Prop)
    (hmono : ∀ x y, le x y → p y = true → p x = true) (h : List.Pairwise le l) :
    l.takeWhile p = l.filter p := by
  induction l with
  | nil => rfl
  | cons a as ih =>
    obtain ⟨h1, h2⟩ := List.pairwise_cons.mp h
    rw [List.takeWhile_cons, List.filter_cons]
    by_cases ha : p a
    · simp only [ha, ite_true]
      rw [ih h2]
    · simp only [ha, Bool.false_eq_true, ite_false]
      rw [eq_comm, List.filter_eq_nil_iff]
      intro b hb hpb
      exact absurd (hmono a b (h1 b hb) hpb) ha

theorem sound : Q72DuckDB.Goal := by
  intro stock sold
  have trans : ∀ a b c : Int, decide (a ≤ b) = true → decide (b ≤ c) = true →
      decide (a ≤ c) = true := by
    intro a b c h1 h2
    exact decide_eq_true (Int.le_trans (of_decide_eq_true h1) (of_decide_eq_true h2))
  have total : ∀ a b : Int, (decide (a ≤ b) || decide (b ≤ a)) = true := by
    intro a b
    rcases Int.le_total a b with h | h
    · rw [decide_eq_true h]; simp
    · rw [decide_eq_true h]; simp
  have hp : (stock.mergeSort).Pairwise (fun x y : Int => decide (x ≤ y) = true) :=
    List.pairwise_mergeSort trans total stock
  have hmono : ∀ x y : Int, decide (x ≤ y) = true → (decide (y < sold)) = true →
      (decide (x < sold)) = true := by
    intro x y hxy hy
    have hle : x ≤ y := of_decide_eq_true hxy
    have hlt : y < sold := of_decide_eq_true hy
    exact decide_eq_true (Int.lt_of_le_of_lt hle hlt)
  unfold countWhere
  rw [ftw (fun x => decide (x < sold)) _ (fun x y : Int => decide (x ≤ y) = true) hmono hp]
  exact ((List.mergeSort_perm stock (fun a b => decide (a ≤ b))).filter (fun x => decide (x < sold))).length_eq

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q72DuckDB.sound

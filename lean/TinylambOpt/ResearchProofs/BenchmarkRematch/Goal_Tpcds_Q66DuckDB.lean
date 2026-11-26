import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q66DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

private theorem bne_false (a b : Nat) (h : ¬(a == b)) : (a == b) = false := by
  cases e : a == b with
  | true => exact absurd e h
  | false => rfl

/-- `total` over an append splits into a sum of `total`s. -/
private theorem total_append (f : (Nat × Int) → Int) (l₁ l₂ : List (Nat × Int)) :
    total (l₁ ++ l₂) f = total l₁ f + total l₂ f := by
  unfold total
  rw [List.map_append, List.sum_append]

private theorem total_cons (f : α → Int) (a : α) (as : List α) :
    total (a :: as) f = f a + total as f := rfl

/-- Keeping only the matching month and summing equals the guarded sum. -/
private theorem month_total (month : Nat) : ∀ (p : List (Nat × Int)),
    total p (fun (m, v) => if m == month then v else 0) =
      total (p.filter (fun r => r.1 == month)) Prod.snd := by
  intro p
  induction p with
  | nil => rfl
  | cons r rest ih =>
    obtain ⟨m, v⟩ := r
    show (if (m == month) = true then v else (0 : Int)) +
        total rest (fun (m, v) => if m == month then v else 0) = _
    rw [show ((m, v) :: rest).filter (fun x : Nat × Int => x.1 == month) =
          if (m == month) = true then (m, v) :: rest.filter (fun x : Nat × Int => x.1 == month)
          else rest.filter (fun x : Nat × Int => x.1 == month) from by
        rw [List.filter_cons (p := fun x : Nat × Int => x.1 == month)]]
    by_cases h : m == month
    · rw [show (m == month) = true from h]
      show (v : Int) + total rest (fun (m, v) => if m == month then v else 0) =
          v + total (rest.filter (fun x : Nat × Int => x.1 == month)) Prod.snd
      rw [ih]
    · rw [show (m == month) = false from bne_false m month h]
      show (0 : Int) + total rest (fun (m, v) => if m == month then v else 0) =
          total (rest.filter (fun x : Nat × Int => x.1 == month)) Prod.snd
      rw [ih]
      omega

theorem sound : Q66DuckDB.Goal := by
  intro pages month
  induction pages with
  | nil => rfl
  | cons p rest ih =>
    rw [List.flatten_cons, total_append, month_total month p, total_cons, ← ih]

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q66DuckDB.sound

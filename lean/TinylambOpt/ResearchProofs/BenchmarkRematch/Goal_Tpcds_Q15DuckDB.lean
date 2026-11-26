import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q15DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

private theorem perm_split {α : Type} (p q : α → Bool) :
    ∀ (l : List α),
      (l.filter (fun x => p x || q x)).Perm (l.filter p ++ l.filter (fun x => !p x && q x))
  | [] => List.Perm.refl _
  | a :: l => by
    have ih := perm_split p q l
    by_cases hp : p a
    · rw [List.filter_cons_of_pos hp,
        List.filter_cons_of_pos (p := fun x => p x || q x) (by simp [hp]),
        List.filter_cons_of_neg (p := fun x => !p x && q x) (by simp [hp]),
        List.cons_append]
      exact ih.cons a
    · by_cases hq : q a
      · rw [List.filter_cons_of_pos (p := fun x => p x || q x) (by simp [hq]),
          List.filter_cons_of_pos (p := fun x => !p x && q x) (by simp [hp, hq]),
          List.filter_cons_of_neg hp]
        refine ((ih.cons a).trans (List.perm_append_comm.cons a)).trans ?_
        rw [← List.cons_append]
        exact List.perm_append_comm
      · rw [List.filter_cons_of_neg (p := fun x => p x || q x) (by simp [hp, hq]),
          List.filter_cons_of_neg hp,
          List.filter_cons_of_neg (p := fun x => !p x && q x) (by simp [hq])]
        exact ih

theorem sound : Q15DuckDB.Goal := by
  intro rows
  have hl : rows.filter (fun (address, price) => address || price > 500) =
      rows.filter (fun x => x.1 || (fun (a, p) => !a && p > (500 : Int)) x) := by
    refine List.filter_congr (fun x _ => ?_)
    cases x with
    | mk a p => by_cases hp : p > (500 : Int) <;> by_cases ha : a <;> simp [hp, ha]
  have hr : rows.filter (fun (a, p) => !a && p > (500 : Int)) =
      rows.filter (fun x => !x.1 && (fun (a, p) => !a && p > (500 : Int)) x) := by
    refine List.filter_congr (fun x _ => ?_)
    cases x with
    | mk a p => by_cases hp : p > (500 : Int) <;> by_cases ha : a <;> simp [hp, ha]
  rw [hl, hr]
  exact perm_split Prod.fst (fun (a, p) => !a && p > (500 : Int)) rows

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q15DuckDB

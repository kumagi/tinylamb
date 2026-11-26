import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q20DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

private theorem not_keep_nil (b : List Int) (k : Int)
    (hq : ¬((b.min?).any (· ≤ k) && (b.max?).any (k ≤ ·)) = true) : b.count k = 0 := by
  refine List.count_eq_zero_of_not_mem (fun hx => ?_)
  obtain ⟨m, hm⟩ : ∃ m, b.min? = some m :=
    Option.isSome_iff_exists.mp (List.isSome_min?_of_mem hx)
  obtain ⟨n, hn⟩ : ∃ n, b.max? = some n :=
    Option.isSome_iff_exists.mp (List.isSome_max?_of_mem hx)
  have h1 : m ≤ k := ((List.min?_eq_some_iff (xs := b)).mp hm).2 k hx
  have h2 : k ≤ n := ((List.max?_eq_some_iff (xs := b)).mp hn).2 k hx
  have hc : ((b.min?).any (· ≤ k) && (b.max?).any (k ≤ ·)) = true := by
    rw [hm, hn]
    simp [h1, h2]
  exact absurd hc hq

theorem sound : Q20DuckDB.Goal := by
  intro blocks k
  induction blocks with
  | nil => rfl
  | cons b t ih =>
    by_cases hq : ((b.min?).any (· ≤ k) && (b.max?).any (k ≤ ·)) = true
    · rw [List.filter_cons, if_pos hq, List.flatten_cons, List.count_append, ih,
        List.flatten_cons, List.count_append]
    · rw [List.filter_cons, if_neg hq, List.flatten_cons, List.count_append,
        not_keep_nil b k hq, Nat.zero_add, ih]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q20DuckDB

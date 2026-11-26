import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# RowCountBoundsDistinctOffset

The number of distinct output pairs cannot exceed the count of qualifying input rows.

When this count is at most the OFFSET, the executor can return an empty page without
decoding the resolution pairs.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.RowCountBoundsDistinctOffset
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

theorem eraseDups_length_le {α : Type} [BEq α] (xs : List α) : xs.eraseDups.length ≤ xs.length := by
  let motive : Nat → Prop := fun n => ∀ xs : List α, xs.length = n → xs.eraseDups.length ≤ xs.length
  exact (Nat.strongRecOn xs.length (motive := motive) (fun n ih => by
    intro xs hlen
    cases xs with
    | nil => simp
    | cons a as =>
        rw [List.eraseDups_cons]
        simp only [List.length_cons] at hlen
        have h2 := List.length_filter_le (fun b => !b == a) as
        have hsub : (as.filter (fun b => !b == a)).length < n := by omega
        have h1 := ih (as.filter (fun b => !b == a)).length hsub (as.filter (fun b => !b == a)) rfl
        simp only [List.length_cons]
        omega)) xs rfl

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.RowCountBoundsDistinctOffset.Goal := by
  intro hits resolution h
  have h' : (hits.map resolution).length ≤ 10000 := by simpa using h
  have hlen : (hits.map resolution).eraseDups.length ≤ 10000 := by
    exact Nat.le_trans (eraseDups_length_le (hits.map resolution)) h'
  have hdrop : (hits.map resolution).eraseDups.drop 10000 = [] :=
    List.drop_eq_nil_iff.mpr hlen
  rw [hdrop]
  simp
end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.RowCountBoundsDistinctOffset

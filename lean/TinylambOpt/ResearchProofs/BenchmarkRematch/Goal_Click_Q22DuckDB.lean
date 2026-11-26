import TinylambOpt.ResearchGoals.BenchmarkRematch.Click
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group
import TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum
import TinylambOpt.ResearchProofs.BenchmarkRematch.Topn
import TinylambOpt.ResearchProofs.BenchmarkRematch.Dedup
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group2

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Click_Q22DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group
open TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum
open TinylambOpt.ResearchProofs.BenchmarkRematch.Topn
open TinylambOpt.ResearchProofs.BenchmarkRematch.Dedup
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group2

theorem min_iff (xs : List String) (a : String) : xs.min? = some a ↔ a ∈ xs ∧ ∀ b ∈ xs, a ≤ b :=
  List.min?_eq_some_iff

theorem sound : Q22DuckDB.Goal := by
  intro L
  cases hf : L.flatten.min? with
  | none =>
    have hfl : L.flatten = [] := by
      cases h : L.flatten with
      | nil => rfl
      | cons x t => rw [h] at hf; simp [List.min?_cons'] at hf
    have hL : ∀ l ∈ L, l = [] := by
      intro l hl
      have : ∀ x ∈ l, x ∈ L.flatten := fun x hx => List.mem_flatten.mpr ⟨l, hl, hx⟩
      rw [hfl] at this
      cases l with
      | nil => rfl
      | cons y t => exact absurd (this y (by simp)) (by simp)
    have : L.filterMap List.min? = [] := by
      rw [List.filterMap_eq_nil_iff]
      intro l hl
      rw [hL l hl]; rfl
    rw [this]; rfl
  | some m =>
    rw [min_iff] at hf
    rw [min_iff]
    obtain ⟨hm, hle⟩ := hf
    obtain ⟨l, hl, hml⟩ := List.mem_flatten.mp hm
    refine ⟨?_, ?_⟩
    · cases hl' : l.min? with
      | none =>
        have h0 := List.min?_eq_none_iff.mp hl'
        rw [h0] at hml; simp at hml
      | some m' =>
        have := (min_iff l m').mp hl'
        have h1 : m' ∈ L.flatten := List.mem_flatten.mpr ⟨l, hl, this.1⟩
        have h2 := hle m' h1
        have h3 := this.2 m hml
        have : m' = m := Std.Antisymm.antisymm _ _ h3 h2
        subst this
        exact List.mem_filterMap.mpr ⟨l, hl, hl'⟩
    · intro b hb
      obtain ⟨l', hl', hb'⟩ := List.mem_filterMap.mp hb
      have := (min_iff l' b).mp hb'
      exact hle b (List.mem_flatten.mpr ⟨l', hl', this.1⟩)

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Click_Q22DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Click_Q22DuckDB.sound

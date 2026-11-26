import TinylambOpt.ResearchGoals.BenchmarkRematch.Cloud

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Cloud.Goal_Public108DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem mem_members (a b : List Nat) (i : Nat) : i ∈ members a b ↔ i ∈ a ∧ i ∈ b := by
  simp [members]

theorem contains_members (league pos yr : List Nat) (i : Nat) :
    (members (members league pos) yr).contains i = (league.contains i && pos.contains i && yr.contains i) := by
  simp [mem_members, Bool.and_assoc]

theorem filter_isEmpty_iff (p : Nat → Bool) (l : List Nat) :
    (l.filter p).isEmpty = true ↔ ¬ ∃ x ∈ l, p x = true := by
  induction l with
  | nil => simp
  | cons x xs ih =>
    by_cases h : p x = true
    · simp [h]
    · simp [h, ih]

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Cloud.Public108DuckDB.Goal := by
  intro rows league pos yr
  dsimp only
  have hf : rows.filter (members (members league pos) yr).contains =
      rows.filter (fun i => league.contains i && pos.contains i && yr.contains i) := by
    apply List.filter_congr
    intro i hi
    exact contains_members league pos yr i
  constructor
  · exact hf
  · rw [hf]
    rw [filter_isEmpty_iff]
    constructor
    · intro h
      rintro ⟨i, hi, hl, hp, hy⟩
      exact h ⟨i, hi, by simp [hl, hp, hy]⟩
    · intro h
      rintro ⟨i, hi, hb⟩
      exact h ⟨i, hi, by simpa [and_assoc] using hb⟩

end TinylambOpt.ResearchProofs.BenchmarkRematch.Cloud.Goal_Public108DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Cloud.Goal_Public108DuckDB.sound

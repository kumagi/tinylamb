import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace Proof_Q38DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] : (l : List α) → (l.eraseDups).Nodup
  | [] => by simp [List.Nodup]
  | a :: l => by
      rw [List.eraseDups_cons]
      rw [List.nodup_cons]
      constructor
      · intro hmem
        rw [List.mem_eraseDups] at hmem
        simp at hmem
      · exact nodup_eraseDups (l.filter (fun b => ! b == a))
termination_by l => l.length
decreasing_by
  simp only [List.length_cons]
  have hle := List.length_filter_le (p := fun b => ! b == a) (l := l)
  omega

theorem nodup_filter {α : Type} (p : α → Bool) : ∀ {l : List α}, l.Nodup → (l.filter p).Nodup := by
  intro l
  induction l with
  | nil => intro h; simp [List.Nodup]
  | cons a l ih =>
    intro h
    rw [List.nodup_cons] at h
    simp only [List.filter_cons]
    by_cases hp : p a = true
    · simp only [hp, ↓reduceIte]
      rw [List.nodup_cons]
      have haf : a ∉ l.filter p := by
        intro ha
        exact h.1 ((List.mem_filter.mp ha).1)
      exact ⟨haf, ih h.2⟩
    · simp only [hp, ↓reduceIte]
      exact ih h.2

theorem eraseDups_eq_self_of_nodup {α : Type} [BEq α] [LawfulBEq α] :
    ∀ {l : List α}, l.Nodup → l.eraseDups = l := by
  intro l
  induction l with
  | nil => intro h; rfl
  | cons a l ih =>
    intro h
    rw [List.nodup_cons] at h
    rw [List.eraseDups_cons]
    have hf : l.filter (fun b => !(b == a)) = l := by
      rw [List.filter_eq_self]
      intro b hb
      have hneq : b ≠ a := by
        intro heq
        apply h.1
        rw [heq] at hb
        exact hb
      simp [hneq]
    rw [hf]
    rw [ih h.2]

theorem eraseDups_filter_idem {α : Type} [BEq α] [LawfulBEq α] (l : List α) (p : α → Bool) :
    (l.eraseDups.filter p).eraseDups = l.eraseDups.filter p :=
  eraseDups_eq_self_of_nodup (nodup_filter p (nodup_eraseDups l))

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q38DuckDB.Goal := by
  intro a b c
  dsimp only [members, countWhere]
  rw [eraseDups_filter_idem a (fun k => b.contains k)]
  rw [List.filter_filter]
  simp [Bool.and_comm]

end Proof_Q38DuckDB

#print axioms Proof_Q38DuckDB.sound

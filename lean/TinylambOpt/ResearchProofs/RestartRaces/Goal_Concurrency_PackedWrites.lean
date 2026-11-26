import TinylambOpt.ResearchGoals.RestartRaces.Concurrency
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.RestartRaces.Concurrency_PackedWrites

open TinylambOpt.ResearchGoals.RestartRaces.Concurrency.PackedWrites

private theorem map_eq_pointwise {α β : Type} (l : List α) (f g : α → β)
    (h : l.map f = l.map g) : ∀ x ∈ l, f x = g x := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons, List.cons.injEq] at h
      intro x hx
      simp only [List.mem_cons] at hx
      rcases hx with rfl | hx
      · exact h.1
      · exact ih h.2 x hx

theorem sound : Goal := by
  intro a b
  constructor
  · intro hs s
    rcases hs with ⟨hA, hB, hC⟩
    constructor
    · apply List.map_congr_left
      intro k hk
      simp [run, hA k hk]
    constructor
    · apply List.map_congr_left
      intro k hk
      simp [run, hB k hk]
    · intro k
      cases ha : a.put k with
      | none =>
          cases hb : b.put k with
          | none => simp [run, ha, hb]
          | some y => simp [run, ha, hb]
      | some x =>
          cases hb : b.put k with
          | none => simp [run, ha, hb]
          | some y =>
              have hxy : x = y := hC k x y ha hb
              subst y
              simp [run, ha, hb]
  · intro H
    have hA : ∀ k ∈ a.readSet, b.put k = none := by
      intro k hk
      by_cases hb : b.put k = none
      · exact hb
      · exfalso
        obtain ⟨y, hy⟩ := Option.ne_none_iff_exists'.mp hb
        let s : Nat → Bool := fun j => if j == k then !y else false
        have hmap := (H s).1
        have hpt := map_eq_pointwise a.readSet (run b s) s hmap k hk
        have hrb : run b s k = y := by simp [run, hy]
        have hsk : s k = !y := by simp [s]
        rw [hrb, hsk] at hpt
        cases y <;> simp at hpt
    have hB : ∀ k ∈ b.readSet, a.put k = none := by
      intro k hk
      by_cases ha : a.put k = none
      · exact ha
      · exfalso
        obtain ⟨x, hx⟩ := Option.ne_none_iff_exists'.mp ha
        let s : Nat → Bool := fun j => if j == k then !x else false
        have hmap := (H s).2.1
        have hpt := map_eq_pointwise b.readSet (run a s) s hmap k hk
        have hra : run a s k = x := by simp [run, hx]
        have hsk : s k = !x := by simp [s]
        rw [hra, hsk] at hpt
        cases x <;> simp at hpt
    have hC : ∀ k x y, a.put k = some x → b.put k = some y → x = y := by
      intro k x y hax hby
      have hcomm := (H (fun _ => false)).2.2 k
      simp [run, hax, hby] at hcomm
      exact hcomm
    exact ⟨hA, hB, hC⟩

end TinylambOpt.ResearchProofs.RestartRaces.Concurrency_PackedWrites

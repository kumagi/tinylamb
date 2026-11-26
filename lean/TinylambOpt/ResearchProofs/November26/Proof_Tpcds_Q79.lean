import TinylambOpt.ResearchGoals.November26.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q79
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpcds

theorem eraseDups_eq_singleton {α : Type} [BEq α] [LawfulBEq α] (a : α) :
    ∀ xs : List α, (∀ x ∈ xs, x = a) →
      xs.eraseDups = if xs.isEmpty then [] else [a] := by
  intro xs
  induction xs with
  | nil => simp
  | cons x xs ih =>
    intro h
    have hx : x = a := h x (by simp)
    subst x
    rw [List.eraseDups_cons]
    have hf : xs.filter (fun b => !(b == a)) = [] := by
      rw [List.filter_eq_nil_iff]
      intro y hy
      have hya : y = a := h y (by simp [hy])
      subst y
      simp
    rw [hf]
    simp

theorem sound : TinylambOpt.ResearchGoals.November26.Tpcds.Q79.Goal := by
  intro rows addr hall
  dsimp only
  have hmapne : (rows.map Prod.fst).isEmpty = rows.isEmpty := by
    cases rows <;> simp
  have hfst : (rows.map Prod.fst).eraseDups = if rows.isEmpty then [] else [addr] := by
    rw [← hmapne]
    apply eraseDups_eq_singleton addr
    intro x hx
    obtain ⟨r, hr, hxr⟩ := List.mem_map.mp hx
    rw [← hxr]
    exact hall r hr
  have hfk : forKey rows addr = rows.map Prod.snd := by
    unfold forKey
    have hf : rows.filter (fun r => r.1 == addr) = rows := by
      rw [List.filter_eq_self]
      intro r hr
      rw [hall r hr]
      simp
    rw [hf]
  by_cases hne : rows.isEmpty
  · have hrows : rows = [] := List.nil_of_isEmpty hne
    subst rows
    simp
  · rw [hfst]
    simp [hne, hfk]
end TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q79

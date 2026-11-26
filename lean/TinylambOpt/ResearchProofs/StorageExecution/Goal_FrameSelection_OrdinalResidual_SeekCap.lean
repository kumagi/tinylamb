import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `OrdinalResidual.SeekCap` — Relaxing maximum rows per frame bounds the best fully
  framed size.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_OrdinalResidual_SeekCap

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.OrdinalResidual

/-- Specification of `List.min?` on natural numbers: `none` exactly for the
    empty list, and `some m` means `m` is a member and a lower bound. -/
private theorem minSpec : ∀ (l : List Nat),
    (l.min? = none ↔ l = []) ∧
    (∀ m, l.min? = some m → m ∈ l ∧ ∀ x ∈ l, m ≤ x) := by
  intro l
  induction l with
  | nil => simp [List.min?]
  | cons x xs ih =>
      constructor
      · constructor
        · intro h
          cases hxs : xs.min? with
          | none =>
              rw [List.min?_cons] at h
              simp [hxs] at h
          | some y =>
              rw [List.min?_cons] at h
              simp [hxs] at h
        · intro h
          simp at h
      · intro m hm
        cases hxs : xs.min? with
        | none =>
            rw [List.min?_cons] at hm
            simp [hxs] at hm
            subst m
            have hxsempty : xs = [] := ih.1.mp hxs
            subst xs
            simp
        | some y =>
            rw [List.min?_cons] at hm
            simp [hxs] at hm
            subst m
            have hymem : y ∈ xs := (ih.2 y hxs).1
            have hyle : ∀ z ∈ xs, y ≤ z := (ih.2 y hxs).2
            by_cases hxy : x ≤ y
            · rw [Nat.min_eq_left hxy]
              constructor
              · simp
              · intro z hz
                simp only [List.mem_cons] at hz
                rcases hz with rfl | hz
                · omega
                · exact Nat.le_trans hxy (hyle z hz)
            · have hyx : y ≤ x := by omega
              rw [Nat.min_eq_right hyx]
              constructor
              · exact List.mem_cons_of_mem x hymem
              · intro z hz
                simp only [List.mem_cons] at hz
                rcases hz with rfl | hz
                · exact hyx
                · exact hyle z hz

/-- Filtering by a weaker predicate gives a sublist. -/
private theorem filter_sublist_of_imp {α : Type} (p q : α → Bool)
    (h : ∀ x, p x = true → q x = true) (l : List α) :
    (l.filter p).Sublist (l.filter q) := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hp : p x = true
      · have hq : q x = true := h x hp
        simp [hp, hq]
        exact ih
      · by_cases hq : q x = true
        · simp [hp, hq]
          exact List.Sublist.cons x ih
        · simp [hp, hq]
          exact ih

/-- Taking the minimum over a larger list can only lower it. -/
private theorem minMonotoneSublist {s t : List Nat} (hst : s.Sublist t) {m : Nat}
    (hs : s.min? = some m) : ∃ u ≤ m, t.min? = some u := by
  have hm_mem : m ∈ s := ((minSpec s).2 m hs).1
  have hm_t : m ∈ t := List.Sublist.mem hm_mem hst
  let u := t.min?.getD 0
  have ht : t.min? = some u := by
    cases h : t.min? with
    | none =>
        have ht_empty : t = [] := (minSpec t).1.mp h
        simp [ht_empty] at hm_t
    | some v =>
        simp [h, u]
  refine ⟨u, ?_, ht⟩
  exact ((minSpec t).2 u ht).2 m hm_t

/-- All row lengths bounded by `a` are also bounded by `b` when `a ≤ b`. -/
private theorem all_le_of_all_le {a b : Nat} (hab : a ≤ b) (ps : List (List Nat)) :
    ps.all (fun p => p.length ≤ a) = true →
    ps.all (fun p => p.length ≤ b) = true := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp at ih ⊢
      intro hp hps
      exact ⟨Nat.le_trans hp hab, ih hps⟩

theorem sound_SeekCap : SeekCap.Goal := by
  intro xs a b s hab hbudget
  unfold budget at hbudget ⊢
  have hsub : ((parts xs).filter (fun ps => ps.all (fun p => p.length ≤ a))).Sublist
      ((parts xs).filter (fun ps => ps.all (fun p => p.length ≤ b))) := by
    exact filter_sublist_of_imp _ _ (all_le_of_all_le hab) (parts xs)
  exact minMonotoneSublist (hsub.map (charge write 0)) hbudget

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_OrdinalResidual_SeekCap

import TinylambOpt.ResearchGoals.DuckDBGaps.Slices

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DuckDBGaps.Slices

open TinylambOpt.ResearchGoals.DuckDBGaps.Slices
open TinylambOpt.ResearchGoals.CloudBI.PublicBI

/-! Common helpers for the DuckDB slice-gap goals. -/

private theorem map_congr' {α β : Type} (f g : α → β) (l : List α)
    (h : ∀ x ∈ l, f x = g x) : l.map f = l.map g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.map_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem bne_false (b : Bool) : (false != b) = b := by cases b <;> rfl
private theorem bne_true (b : Bool) : (true != b) = (!b) := by cases b <;> rfl

private theorem bool_not_not (b : Bool) : (!(!b)) = b := by cases b <;> rfl

/-- A list splits into the hits of `P` and of its complement. -/
private theorem filter_len_compl {α : Type} (l : List α) (P : α → Bool) :
    (l.filter P).length + (l.filter (fun x => ! P x)).length = l.length := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases h : P x <;> simp [h, ih] <;> omega

private theorem mem_filter_range {P : Nat → Bool} {i n : Nat} :
    i ∈ (List.range n).filter P ↔ i < n ∧ P i = true := by
  rw [List.mem_filter, List.mem_range]

private theorem contains_filter_range (P : Nat → Bool) (i n : Nat) (hlt : i < n) :
    ((List.range n).filter P).contains i = P i := by
  cases hP : P i with
  | true =>
      have hi : i ∈ (List.range n).filter P := by
        rw [mem_filter_range]
        exact ⟨hlt, hP⟩
      simpa [hP] using (List.contains_iff_mem.mpr hi)
  | false =>
      have hi : i ∉ (List.range n).filter P := by
        intro hm
        rw [mem_filter_range] at hm
        rw [hP] at hm
        contradiction
      have hc : ((List.range n).filter P).contains i = false := by
        cases hc : ((List.range n).filter P).contains i with
        | false => rfl
        | true => exact absurd (List.contains_iff_mem.mp hc) hi
      exact hc.trans hP.symm

/-- The minority-side code list never exceeds half the dictionary. -/
theorem sound_MinorityCodes :
    TinylambOpt.ResearchGoals.DuckDBGaps.Slices.MinorityCodes.Goal := by
  intro c hc
  dsimp only
  unfold exceptions
  dsimp only
  split
  · dsimp only [Prod.fst, Prod.snd]
    constructor
    · apply map_congr'
      intro i hi
      have hlt : i < c.dict.size := hc i hi
      rw [contains_filter_range (fun i => outfield c.dict[i]?.join) i c.dict.size hlt, bne_false]
      rfl
    · have hlen := filter_len_compl (List.range c.dict.size) (fun i => outfield c.dict[i]?.join)
      omega
  · constructor
    · apply map_congr'
      intro i hi
      have hlt : i < c.dict.size := hc i hi
      rw [contains_filter_range (fun i => !outfield c.dict[i]?.join) i c.dict.size hlt, bne_true]
      rfl
    · have hlen := filter_len_compl (List.range c.dict.size) (fun i => outfield c.dict[i]?.join)
      omega

end TinylambOpt.ResearchProofs.DuckDBGaps.Slices

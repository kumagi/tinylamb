import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.SparseSetAndWindowEvaluation.Goal_DisjointPostalAndItemFactSelections

open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation


open TinylambOpt.ResearchGoals.QueryRewrites

/-!
# DisjointPostalAndItemFactSelections

Appending the item-selected row positions that are not already postal-selected builds the
same set of positions as a single disjunctive filter, with no duplicate positions. The
resulting fact values are therefore a permutation, preserving bag multiplicity even when
different positions carry equal values. This permits two independent selection paths to be
merged without changing rows seen by the consumer.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace DisjointSelectionHelpers

theorem contains_true {α : Type} [BEq α] [LawfulBEq α] (l : List α) (x : α) :
    l.contains x = true ↔ x ∈ l := by
  induction l with
  | nil => simp
  | cons y l ih =>
      by_cases h : y == x
      · have hyx : y = x := beq_iff_eq.mp h
        subst hyx
        simp
      · simp

end DisjointSelectionHelpers

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.DisjointPostalAndItemFactSelections.Goal := by
  intro facts postal item
  dsimp only
  let ids := List.range facts.length
  let a := ids.filter postal
  let b := ids.filter item
  let left := a ++ b.filter (fun i => !a.contains i)
  let right := ids.filter (fun i => postal i || item i)
  have hnod_a : a.Nodup := by
    dsimp [a, ids]
    exact List.nodup_range.filter postal
  have hnod_bf : (b.filter (fun i => !a.contains i)).Nodup := by
    dsimp [b, ids]
    exact (List.nodup_range.filter item).filter (fun i => !a.contains i)
  have hdisj : ∀ x ∈ a, ∀ y ∈ b.filter (fun i => !a.contains i), x ≠ y := by
    intro x hx y hy hxy
    have hnot : !a.contains y = true := (List.mem_filter.mp hy).2
    have hc : a.contains y = true :=
      (DisjointSelectionHelpers.contains_true a y).mpr (by simpa [hxy] using hx)
    simp [hc] at hnot
    exact hnot (by simpa [hxy] using hx)
  have hnod_left : left.Nodup := by
    dsimp [left]
    exact List.nodup_append.mpr ⟨hnod_a, hnod_bf, hdisj⟩
  have hnod_right : right.Nodup := by
    dsimp [right, ids]
    exact List.nodup_range.filter (fun i => postal i || item i)
  have hmem : ∀ i, i ∈ left ↔ i ∈ right := by
    intro i
    constructor
    · intro hi
      rcases List.mem_append.mp hi with hi | hi
      · have hi' : i ∈ ids := by
          dsimp [a] at hi
          exact (List.mem_filter.mp hi).1
        have hpa : postal i = true := by
          dsimp [a] at hi
          exact (List.mem_filter.mp hi).2
        exact List.mem_filter.mpr ⟨hi', by simp [hpa]⟩
      · have hib : i ∈ b := (List.mem_filter.mp hi).1
        have hit : item i = true := (List.mem_filter.mp hib).2
        exact List.mem_filter.mpr ⟨(List.mem_filter.mp hib).1, by simp [hit]⟩
    · intro hi
      have hii : i ∈ ids := (List.mem_filter.mp hi).1
      have hor : (postal i || item i) = true := (List.mem_filter.mp hi).2
      by_cases hpa : postal i = true
      · exact List.mem_append.mpr (Or.inl (List.mem_filter.mpr ⟨hii, hpa⟩))
      · have hit : item i = true := by simpa [hpa] using hor
        have hnotmem : i ∉ a := by
          intro hia
          have hpa' : postal i = true := by
            dsimp [a] at hia
            exact (List.mem_filter.mp hia).2
          exact hpa hpa'
        have hnot : !a.contains i = true := by
          by_cases hc : a.contains i = true
          · exact False.elim (hnotmem ((DisjointSelectionHelpers.contains_true a i).mp hc))
          · have hcfalse : a.contains i = false := by simpa using hc
            simp [hcfalse]
            exact hnotmem
        exact List.mem_append.mpr (Or.inr (List.mem_filter.mpr ⟨List.mem_filter.mpr ⟨hii, hit⟩, hnot⟩))
  have hperm : left.Perm right := (List.perm_ext_iff_of_nodup hnod_left hnod_right).2 hmem
  have hvals : (left.filterMap (fun i => facts[i]?)).Perm (right.filterMap (fun i => facts[i]?)) :=
    hperm.filterMap (fun i => facts[i]?)
  change (left.filterMap (fun i => facts[i]?)).Perm (right.filterMap (fun i => facts[i]?))
  exact hvals
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.SparseSetAndWindowEvaluation.Goal_DisjointPostalAndItemFactSelections

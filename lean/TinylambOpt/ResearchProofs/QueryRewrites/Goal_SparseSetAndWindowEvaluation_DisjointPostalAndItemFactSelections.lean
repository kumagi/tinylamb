import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-!
# DisjointPostalAndItemFactSelections

The two selection arms produce disjoint sets of row positions from the same
snapshot range: the second arm keeps only positions not already selected by the
first. Their concatenation is therefore a permutation of the union of the two
position predicates, and gathering fact values preserves that permutation. SQL
may evaluate the two index lists separately and concatenate them without
losing repeated equal fact values or double counting rows satisfying both arms.
-/

namespace TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_DisjointPostalAndItemFactSelections

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

private theorem nodup_filter {α : Type} [BEq α] (p : α → Bool) {l : List α}
    (h : l.Nodup) : (l.filter p).Nodup := by
  change List.Pairwise (fun x y : α => x ≠ y) (l.filter p)
  apply List.Pairwise.filter
  change List.Pairwise (fun x y : α => x ≠ y) l
  exact h

theorem sound : DisjointPostalAndItemFactSelections.Goal := by
  intro facts postal item
  let ids := List.range facts.length
  let a := ids.filter postal
  let b := ids.filter item
  change (valuesAt facts (a ++ b.filter (fun i => !a.contains i))).Perm
    (valuesAt facts (ids.filter (fun i => postal i || item i)))
  have ha : a.Nodup := by
    exact nodup_filter postal (by simpa [a, ids] using
      (List.nodup_range : (List.range facts.length).Nodup))
  have hb : b.Nodup := by
    exact nodup_filter item (by simpa [b, ids] using
      (List.nodup_range : (List.range facts.length).Nodup))
  have hb' : (b.filter (fun i => !a.contains i)).Nodup :=
    nodup_filter (fun i => !a.contains i) hb
  have hleft : (a ++ b.filter (fun i => !a.contains i)).Nodup := by
    rw [List.nodup_append]
    refine ⟨ha, hb', ?_⟩
    intro x hx y hy hxy
    have hnot : ¬ a.contains y := by
      simpa using (List.mem_filter.mp hy).2
    subst x
    exact hnot (by simpa using hx)
  have hright : (ids.filter (fun i => postal i || item i)).Nodup := by
    exact nodup_filter (fun i => postal i || item i)
      (by simpa [ids] using (List.nodup_range : (List.range facts.length).Nodup))
  have hidx : (a ++ b.filter (fun i => !a.contains i)).Perm
      (ids.filter (fun i => postal i || item i)) := by
    apply (List.perm_ext_iff_of_nodup hleft hright).2
    intro i
    constructor
    · intro hi
      rcases List.mem_append.mp hi with hi | hi
      · have hi' := List.mem_filter.mp hi
        exact List.mem_filter.mpr ⟨hi'.1, (Bool.or_eq_true (postal i) (item i)).mpr
          (Or.inl hi'.2)⟩
      · have hi' := List.mem_filter.mp hi
        have hib := List.mem_filter.mp hi'.1
        exact List.mem_filter.mpr ⟨hib.1, (Bool.or_eq_true (postal i) (item i)).mpr
          (Or.inr hib.2)⟩
    · intro hi
      have hi' := List.mem_filter.mp hi
      by_cases hp : postal i = true
      · exact List.mem_append_left _ (List.mem_filter.mpr ⟨hi'.1, hp⟩)
      · have hitem : item i = true :=
          (Bool.or_eq_true (postal i) (item i)).mp hi'.2 |>.resolve_left hp
        have hnot : a.contains i = false := by
          simp [a, hi'.1, hp]
        have hnotmem : ¬ i ∈ a := by simpa using hnot
        exact List.mem_append_right a (List.mem_filter.mpr
          ⟨List.mem_filter.mpr ⟨hi'.1, hitem⟩, by simp [hnotmem]⟩)
  exact List.Perm.filterMap (fun i => facts[i]?) hidx

end TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_DisjointPostalAndItemFactSelections

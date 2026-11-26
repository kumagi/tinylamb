import TinylambOpt.AggBase
import TinylambOpt.ResearchGoals.QueryRewrites.Common

/-!
Grouping selected rows by distinct keys preserves every selected value and
its multiplicity. The permutation theorem lets SQL aggregates such as SUM and
COUNT reason about partial states grouped by those keys.
-/


namespace TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping

open TinylambOpt.ResearchGoals.QueryRewrites

theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] : ∀ l : List α, l.eraseDups.Nodup
  | [] => .nil
  | a :: l => by
      rw [List.eraseDups_cons]
      constructor
      · intro h hmem
        rw [List.mem_eraseDups, List.mem_filter] at hmem
        intro heq
        rw [← heq] at hmem
        simp at hmem
      · exact nodup_eraseDups (l.filter (fun b => !b == a))
termination_by l => l.length
decreasing_by
  have := List.length_filter_le (fun b => !b == a) l
  simp
  omega

private theorem perm_eraseDups_buckets {α β : Type} [BEq α] [LawfulBEq α]
    (r : List (α × β)) :
    r.Perm (((r.map Prod.fst).eraseDups).flatMap
      (fun k => r.filter (fun row => row.1 == k))) := by
  apply TinylambOpt.perm_buckets_of_nodup Prod.fst
  · exact nodup_eraseDups (r.map Prod.fst)
  · intro row hrow
    exact List.mem_eraseDups.mpr (List.mem_map.mpr ⟨row, hrow, rfl⟩)

private theorem filter_flatMap_buckets_of_key_pred {α β : Type} [BEq α] [LawfulBEq α]
    (keys : List α)
    (r : List (α × β)) (p : α → Bool) :
    (keys.flatMap (fun k => r.filter (fun row => row.1 == k))).filter (fun row => p row.1) =
      (keys.filter p).flatMap (fun k => r.filter (fun row => row.1 == k)) := by
  induction keys with
  | nil => rfl
  | cons k ks ih =>
      rw [List.flatMap_cons, List.filter_append, ih]
      by_cases hp : p k = true
      · rw [List.filter_cons_of_pos hp, List.flatMap_cons]
        congr 1
        apply List.filter_eq_self.mpr
        intro row hrow
        have hrow' := List.mem_filter.mp hrow
        have hkeq : row.1 = k := beq_iff_eq.mp hrow'.2
        simp [hkeq, hp]
      · have hp' : ¬ p k = true := by simpa using hp
        rw [List.filter_cons_of_neg hp']
        have hbucket : (r.filter (fun row => row.1 == k)).filter (fun row => p row.1) = [] := by
          apply List.eq_nil_iff_forall_not_mem.mpr
          intro row hrow
          have hrow' := List.mem_filter.mp hrow
          have hrow'' := List.mem_filter.mp hrow'.1
          have hkeq : row.1 = k := beq_iff_eq.mp hrow''.2
          have hpfalse : ¬ p row.1 = true := by simpa [hkeq] using hp'
          exact hpfalse hrow'.2
        simp [hbucket]

/-- Grouping by distinct selected keys preserves all selected values with their multiplicities. -/
theorem forKey_filter_perm {α β : Type} [BEq α] [LawfulBEq α]
    (rows : List (α × β)) (p : α → Bool) :
    ((rows.filter (fun row => p row.1)).map Prod.snd).Perm
      (((rows.map Prod.fst).eraseDups.filter p).flatMap (forKey rows)) := by
  have h := (perm_eraseDups_buckets rows).filter (fun row => p row.1)
  rw [filter_flatMap_buckets_of_key_pred] at h
  have hValues := h.map Prod.snd
  rw [List.map_flatMap] at hValues
  exact hValues

end TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping

import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-!
# Goal_PartitionedAggregateStates_StockRangeFullAcceptanceCounts

Min/max stock bounds let a page decide its strict-inequality contribution from
the page summary: all-below pages contribute their length, all-above pages
contribute zero, and mixed pages count their qualifying rows. Thus every page
can contribute directly to the join count.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_StockRangeFullAcceptanceCounts

open TinylambOpt.ResearchGoals.QueryRewrites

private theorem page_contribution (p : List Int) (sold : Int) :
    (if p.max?.any (fun x => x < sold) then p.length
     else if p.min?.any (fun x => sold ≤ x) then 0
     else countWhere p (fun x => x < sold)) = countWhere p (fun x => x < sold) := by
  unfold countWhere
  cases hmax : p.max? with
  | none =>
      rw [List.max?_eq_none_iff] at hmax
      subst p
      simp
  | some m =>
      have hm : m ∈ p ∧ ∀ b ∈ p, b ≤ m := List.max?_eq_some_iff.mp hmax
      by_cases hmall : m < sold
      · have hf : (List.filter (fun x : Int => decide (x < sold)) p) = p := by
          apply List.filter_eq_self.mpr
          intro b hb
          have hlt : b < sold := Int.lt_of_le_of_lt (hm.2 b hb) hmall
          simp [hlt]
        simp [hmall, hf]
      · simp [hmall]
        cases hmin : p.min? with
        | none =>
            rw [List.min?_eq_none_iff] at hmin
            subst p
            simp at hmax
        | some n =>
            have hn : n ∈ p ∧ ∀ b ∈ p, n ≤ b := List.min?_eq_some_iff.mp hmin
            by_cases hnall : sold ≤ n
            · have hf : (List.filter (fun x : Int => decide (x < sold)) p) = [] := by
                apply List.eq_nil_iff_forall_not_mem.mpr
                intro b hb
                have hb' := List.mem_filter.mp hb
                have hnb : n ≤ b := hn.2 b hb'.1
                have hblt : b < sold := by simpa using hb'.2
                omega
              simp [hnall, hf]
            · simp [hnall]

theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.StockRangeFullAcceptanceCounts.Goal := by
  intro pages sold
  dsimp only [TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.StockRangeFullAcceptanceCounts.Goal]
  induction pages with
  | nil => simp [countWhere]
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, List.flatten_cons]
      rw [page_contribution]
      simp only [countWhere] at ih ⊢
      simpa using ih

end TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_StockRangeFullAcceptanceCounts

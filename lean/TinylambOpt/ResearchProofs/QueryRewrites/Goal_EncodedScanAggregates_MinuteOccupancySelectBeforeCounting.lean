import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping
import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

set_option linter.unusedSimpArgs false
set_option linter.deprecated false



/-!
Selecting occupied minute slots in increasing timestamp order first yields
exactly the sorted distinct minutes. The later offset and limit therefore
select the same page as sorting the minute list directly; SQL may use the
sparse occupancy index before counting rows.
-/


namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_MinuteOccupancySelectBeforeCounting

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates



set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

open TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping

namespace EncodedScanAggregateMinuteOccupancy

private theorem perm_of_nodup_mem {α : Type} [BEq α] [LawfulBEq α] {l₁ l₂ : List α}
    (h₁ : l₁.Nodup) (h₂ : l₂.Nodup) (hm : ∀ x : α, x ∈ l₁ ↔ x ∈ l₂) : l₁.Perm l₂ := by
  apply List.perm_iff_count.mpr
  intro x
  rw [h₁.count, h₂.count]
  by_cases hx : x ∈ l₁
  · have hx₂ : x ∈ l₂ := (hm x).mp hx
    simp [hx, hx₂]
  · have hx₂ : x ∉ l₂ := by
      intro hx'
      exact hx ((hm x).mpr hx')
    simp [hx, hx₂]

private theorem range_pairwise : ∀ n : Nat, (List.range n).Pairwise (fun a b : Nat => a ≤ b)
  | 0 => by simp
  | n + 1 => by
      rw [List.range_succ, List.pairwise_append]
      refine ⟨range_pairwise n, by simp, ?_⟩
      intro a ha b hb
      simp only [List.mem_singleton] at hb
      subst b
      simp only [List.mem_range] at ha
      omega

private theorem sortedNatTrans : ∀ a b c : Nat, decide (a ≤ b) = true → decide (b ≤ c) = true → decide (a ≤ c) = true := by
  intro a b c hab hbc
  simp at hab hbc ⊢
  omega

private theorem sortedNatTotal : ∀ a b : Nat, (decide (a ≤ b) || decide (b ≤ a)) = true := by
  intro a b
  by_cases h : a ≤ b
  · simp [h]
  · have h' : b ≤ a := by omega
    simp [h, h']

end EncodedScanAggregateMinuteOccupancy

open TinylambOpt.ResearchGoals.QueryRewrites (ranges)
open EncodedScanAggregateMinuteOccupancy

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.MinuteOccupancySelectBeforeCounting.Goal := by
  intro ms bound hbound
  have hperm : ((List.range bound).filter ms.contains).Perm (ms.eraseDups.mergeSort) := by
    refine (perm_of_nodup_mem (l₂ := ms.eraseDups) (List.nodup_range.filter _) ?_ ?_).trans ?_
    · exact nodup_eraseDups ms
    · intro x
      simp only [List.mem_filter, List.mem_range]
      constructor
      · rintro ⟨hxb, hxms⟩
        exact List.mem_eraseDups.mpr (by simpa using hxms)
      · intro hx
        have hxms : x ∈ ms := List.mem_eraseDups.mp hx
        exact ⟨hbound x hxms, by simpa using hxms⟩
    · exact (List.mergeSort_perm ms.eraseDups _).symm
  have hfilter : (List.range bound).filter ms.contains = ms.eraseDups.mergeSort := by
    apply List.Perm.eq_of_pairwise (le := fun a b : Nat => a ≤ b)
    · intro a b _ _ hab hba
      exact Nat.le_antisymm hab hba
    · exact (range_pairwise bound).filter _
    · simpa using (List.pairwise_mergeSort sortedNatTrans sortedNatTotal ms.eraseDups)
    · exact hperm
  rw [hfilter]
  rfl
end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_MinuteOccupancySelectBeforeCounting

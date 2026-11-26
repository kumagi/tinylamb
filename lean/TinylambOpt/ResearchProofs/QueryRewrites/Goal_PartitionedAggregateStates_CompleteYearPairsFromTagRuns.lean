import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-!
# Goal_PartitionedAggregateStates_CompleteYearPairsFromTagRuns

Registers the proof of the CompleteYearPairsFromTagRuns goal in PartitionedAggregateStates: the modeled rewrite
preserves the exact result, so SQL execution may apply it.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_CompleteYearPairsFromTagRuns
open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.CompleteYearPairsFromTagRuns

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] : ∀ l : List α, l.eraseDups.Nodup
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

theorem sound : Goal := by
  intro tags
  constructor
  · intro h k
    by_cases hk : k ∈ tags
    · exact hk
    · exfalso
      have hsub : tags.eraseDups ⊆ ((List.finRange 4).filter (· != k)) := by
        intro x hx
        rw [List.mem_filter]
        constructor
        · simp [List.mem_finRange]
        · by_cases hxk : x == k
          · exfalso
            apply hk
            have : x = k := eq_of_beq hxk
            rw [List.mem_eraseDups] at hx
            simpa [this] using hx
          · simpa [bne_iff_ne] using hxk
      have hnodup : tags.eraseDups.Nodup := nodup_eraseDups tags
      have hle : tags.eraseDups.length ≤ ((List.finRange 4).filter (· != k)).length :=
        List.Nodup.length_le_of_subset hnodup hsub
      have hlt : ((List.finRange 4).filter (· != k)).length < (List.finRange 4).length := by
        rw [List.length_filter_lt_length_iff_exists]
        exact ⟨k, by simp [List.mem_finRange], by simp⟩
      simp [List.length_finRange] at hlt
      omega
  · intro hall
    have hsub : (List.finRange 4) ⊆ tags.eraseDups := by
      intro x hx
      rw [List.mem_eraseDups]
      exact hall x
    have hnodup : tags.eraseDups.Nodup := nodup_eraseDups tags
    have hnodup4 : (List.finRange 4).Nodup := by decide
    have hle1 : (List.finRange 4).length ≤ tags.eraseDups.length :=
      List.Nodup.length_le_of_subset hnodup4 hsub
    have hsub2 : tags.eraseDups ⊆ (List.finRange 4) := by
      intro x hx
      simp [List.mem_finRange]
    have hle2 : tags.eraseDups.length ≤ (List.finRange 4).length :=
      List.Nodup.length_le_of_subset hnodup hsub2
    have h4 : (List.finRange 4).length = 4 := by decide
    omega
end TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_CompleteYearPairsFromTagRuns

import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates
import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_CompleteYearPairsFromTagRuns

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-! Distinct tags of a four-way year partition determine whether all four
required partitions exist even when the source stream repeats tags: the distinct
tag list is a permutation of `List.finRange 4` exactly when every year occurs. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-- A nodup list contained in another nodup list cannot be longer than it. -/
theorem length_le_of_nodup_subset {α : Type} [BEq α] [LawfulBEq α] :
    ∀ (l₁ l₂ : List α), l₁.Nodup → (∀ x ∈ l₁, x ∈ l₂) → l₁.length ≤ l₂.length
  | [], _, _, _ => by simp
  | a :: t, l₂, hnd, hsub => by
      cases hnd with
      | cons ha htail =>
          have ha2 : a ∈ l₂ := hsub a (by simp)
          have hsubErase : ∀ x ∈ t, x ∈ l₂.erase a := by
            intro x hx
            have hx2 : x ∈ l₂ := hsub x (by simp [hx])
            have hxa : x ≠ a := by
              intro h
              exact ha x hx h.symm
            exact (List.mem_erase_of_ne hxa).2 hx2
          have ih := length_le_of_nodup_subset t (l₂.erase a) htail hsubErase
          rw [List.length_erase, if_pos ha2] at ih
          have hpos : 0 < l₂.length := List.length_pos_of_mem ha2
          simp only [List.length_cons] at ih ⊢
          omega

/-- A distinct-tag list is a permutation of the canonical four keys iff all
four keys occur in the original tag stream. -/
theorem perm_finRange_of_mem {tags : List (Fin 4)} :
    (List.finRange 4).Perm tags.eraseDups ↔ ∀ k : Fin 4, k ∈ tags := by
  constructor
  · intro hp k
    exact List.mem_eraseDups.mp (hp.mem_iff.mp (List.mem_finRange k))
  · intro hall
    apply (List.perm_ext_iff_of_nodup (List.nodup_finRange 4)
      (TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping.nodup_eraseDups tags)).2
    intro k
    constructor
    · intro _
      exact List.mem_eraseDups.mpr (hall k)
    · intro _
      exact List.mem_finRange k

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.CompleteYearPairsFromTagRuns.Goal := by
  intro tags
  constructor
  · intro hlen k
    by_cases hk : k ∈ tags
    · exact hk
    · have hsub : ∀ x ∈ tags.eraseDups, x ∈ (List.finRange 4).erase k := by
        intro x hx
        have hxk : x ≠ k := by
          intro h
          exact hk (List.mem_eraseDups.mp (by simpa [h] using hx))
        exact (List.mem_erase_of_ne hxk).2 (List.mem_finRange x)
      have hle := length_le_of_nodup_subset tags.eraseDups ((List.finRange 4).erase k)
        (TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping.nodup_eraseDups tags) hsub
      rw [List.length_erase, if_pos (List.mem_finRange k), List.length_finRange] at hle
      omega
  · intro hall
    have hp := perm_finRange_of_mem.mpr hall
    have hlen := hp.length_eq.symm
    rw [List.length_finRange] at hlen
    exact hlen
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_CompleteYearPairsFromTagRuns

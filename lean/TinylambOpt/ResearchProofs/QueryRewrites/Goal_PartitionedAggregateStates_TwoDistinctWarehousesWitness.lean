import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-!
# Goal_PartitionedAggregateStates_TwoDistinctWarehousesWitness

Registers the proof of the TwoDistinctWarehousesWitness goal in PartitionedAggregateStates: the modeled rewrite
preserves the exact result, so SQL execution may apply it.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_TwoDistinctWarehousesWitness
open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.TwoDistinctWarehousesWitness

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
  intro warehouses k
  dsimp [Goal]
  by_cases h : warehouses.any (fun x => x.any (· != k)) = true
  · rw [h]
    rw [List.any_eq_true] at h
    rcases h with ⟨x, hxmem, hx⟩
    cases x with
    | none => simp at hx
    | some v =>
        simp at hx
        have hvk : (v != k) = true := by simpa using hx
        have hv : v ∈ (warehouses.filterMap id).eraseDups := by
          rw [List.mem_eraseDups, List.mem_filterMap]
          exact ⟨some v, hxmem, rfl⟩
        have hany : ((warehouses.filterMap id).eraseDups.take 2).any (· != k) = true := by
          rw [List.any_eq_true]
          by_cases hv2 : v ∈ (warehouses.filterMap id).eraseDups.take 2
          · exact ⟨v, hv2, hvk⟩
          · -- take 2 must be length 2 and can't be [k,k]
            have hnd := nodup_eraseDups (warehouses.filterMap id)
            have hnd2 : ((warehouses.filterMap id).eraseDups.take 2).Nodup := (hnd.sublist (List.take_sublist 2 _))
            have hlen2 : ((warehouses.filterMap id).eraseDups.take 2).length = 2 := by
              rw [List.length_take]
              have hmem_drop : v ∈ (warehouses.filterMap id).eraseDups.drop 2 := by
                have hall := List.take_append_drop 2 (warehouses.filterMap id).eraseDups
                have hv' : v ∈ (warehouses.filterMap id).eraseDups.take 2 ++ (warehouses.filterMap id).eraseDups.drop 2 := by
                  rw [hall]; exact hv
                rcases List.mem_append.mp hv' with h1 | h2
                · exact absurd h1 hv2
                · exact h2
              have hpos : 0 < ((warehouses.filterMap id).eraseDups.drop 2).length :=
                List.length_pos_of_mem hmem_drop
              have h1 := List.length_take (l := (warehouses.filterMap id).eraseDups) (i := 2)
              have h2 := List.length_drop (i := 2) (l := (warehouses.filterMap id).eraseDups)
              omega
            -- if both elements == k, nodup contradicts
            by_cases hall : ((warehouses.filterMap id).eraseDups.take 2).all (· == k)
            · have hallmem : ∀ x ∈ (warehouses.filterMap id).eraseDups.take 2, x = k := by
                intro x hx
                have := List.all_eq_true.mp hall x hx
                exact eq_of_beq this
              -- a nodup list of length 2 where all elements = k is impossible
              have : ((warehouses.filterMap id).eraseDups.take 2).length ≤ 1 := by
                have hsub : ((warehouses.filterMap id).eraseDups.take 2) ⊆ [k] := by
                  intro x hx; simp [hallmem x hx]
                exact List.Nodup.length_le_of_subset hnd2 hsub
              omega
            · have hall' : ((warehouses.filterMap id).eraseDups.take 2).all (· == k) = false := by
                cases h : ((warehouses.filterMap id).eraseDups.take 2).all (· == k) <;> simp_all
              rw [List.all_eq_false] at hall'
              rcases hall' with ⟨x, hx, hxne⟩
              exact ⟨x, hx, by simpa using hxne⟩
        simp [hany]
  · have hf : warehouses.any (fun x => x.any (· != k)) = false := by
      cases h : warehouses.any (fun x => x.any (· != k)) <;> simp_all
    simp [hf, List.any_eq_false]
    intro x hx
    have hx' : x ∈ (warehouses.filterMap id).eraseDups := List.mem_of_mem_take hx
    rw [List.mem_eraseDups, List.mem_filterMap] at hx'
    rcases hx' with ⟨o, ho, ho'⟩
    cases o with
    | none => simp at ho'
    | some v =>
        simp at ho'; subst ho'
        rw [List.any_eq_false] at hf
        have := hf (some v) ho
        simpa using this
end TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_TwoDistinctWarehousesWitness

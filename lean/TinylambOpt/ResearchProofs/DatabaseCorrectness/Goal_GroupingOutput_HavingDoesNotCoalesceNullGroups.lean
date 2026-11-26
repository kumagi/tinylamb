import TinylambOpt.ResearchGoals.DatabaseCorrectness.GroupingOutput
import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping

/-! A HAVING predicate is applied after GROUPING: the real NULL detail row and
    the synthetic NULL subtotal row are counted separately, never coalesced. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.GroupingOutput.Goal_HavingDoesNotCoalesceNullGroups

open TinylambOpt.ResearchGoals.DatabaseCorrectness.GroupingOutput
open TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping (nodup_eraseDups)

private theorem detail_key_isNone (rs : Input) (k : Key) :
    (detail rs k).key.isNone = true ↔ k = none := by
  cases k <;> simp [detail]

private theorem filter_detail_keys_none (rs : Input) (keys : List Key)
    (hnd : keys.Nodup) (hmem : none ∈ keys) :
    (keys.map (fun k => detail rs k)).filter (fun r => r.key.isNone) = [detail rs none] := by
  induction keys with
  | nil => simp at hmem
  | cons k ks ih =>
      cases hnd with
      | cons hnot hndks =>
        by_cases hk : k = none
        · subst k
          have hnotmem : ∀ k' ∈ ks, k' ≠ none := by
            intro k' hk' hk'eq
            exact hnot k' hk' hk'eq.symm
          have htail : (ks.map (fun k => detail rs k)).filter (fun r => r.key.isNone) = [] := by
            apply List.eq_nil_iff_forall_not_mem.mpr
            intro r hr
            rcases List.mem_filter.mp hr with ⟨hr1, hr2⟩
            rcases List.mem_map.mp hr1 with ⟨k', hk'mem, rfl⟩
            have hk'eq : k' = none := (detail_key_isNone rs k').mp hr2
            exact (hnotmem k' hk'mem) hk'eq
          simp [detail]
          exact hnotmem
        · have hmemks : none ∈ ks := by
            rcases List.mem_cons.mp hmem with h | h
            · exact (hk h.symm).elim
            · exact h
          have hhead : (detail rs k).key.isNone = false := by
            cases k with
            | none => exact (hk rfl).elim
            | some n => simp [detail]
          simp [hhead, ih hndks hmemks]

private theorem filter_comm (rs : Input) (pass : Out → Bool) :
    ((rollup rs).filter pass).filter (fun r => r.key.isNone) =
        ((rollup rs).filter (fun r => r.key.isNone)).filter pass := by
  rw [List.filter_filter, List.filter_filter]
  congr 1
  funext r
  cases pass r <;> simp

theorem sound : HavingDoesNotCoalesceNullGroups.Goal := by
  intro rs pass
  by_cases hmem : none ∈ rs.map Prod.fst
  · have hkeysmem : none ∈ (rs.map Prod.fst).eraseDups := by simpa using hmem
    have hfilt := filter_detail_keys_none rs ((rs.map Prod.fst).eraseDups)
        (nodup_eraseDups _) hkeysmem
    have hfiltered : (rollup rs).filter (fun r => r.key.isNone) = [detail rs none, grand rs] := by
      simp [rollup, hfilt, grand, detail]
    rw [List.countP_eq_length_filter, filter_comm, hfiltered]
    by_cases hd : pass (detail rs none) <;> by_cases hg : pass (grand rs) <;> simp [hd, hg, hmem]
  · have hkeysmem : none ∉ (rs.map Prod.fst).eraseDups := by simpa using hmem
    have hfilt : ((rs.map Prod.fst).eraseDups.map (fun k => detail rs k)).filter
        (fun r => r.key.isNone) = [] := by
      apply List.eq_nil_iff_forall_not_mem.mpr
      intro r hr
      rcases List.mem_filter.mp hr with ⟨hr1, hr2⟩
      rcases List.mem_map.mp hr1 with ⟨k', hk'mem, rfl⟩
      have hk'eq : k' = none := (detail_key_isNone rs k').mp hr2
      exact hkeysmem (by simpa [hk'eq] using hk'mem)
    have hfiltered : (rollup rs).filter (fun r => r.key.isNone) = [grand rs] := by
      simp [rollup, hfilt, grand]
    rw [List.countP_eq_length_filter, filter_comm, hfiltered]
    by_cases hg : pass (grand rs) <;> simp [hg, hmem]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.GroupingOutput.Goal_HavingDoesNotCoalesceNullGroups

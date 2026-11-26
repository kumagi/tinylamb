import TinylambOpt.ResearchGoals.DatabaseCorrectness.GroupingOutput
import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping

/-! A real NULL key produces one detail row tagged false, while the synthetic
    NULL of the grand subtotal is tagged true; GROUPING can tell them apart. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.GroupingOutput.Goal_NullDetailAndSubtotalDistinct

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

theorem sound : NullDetailAndSubtotalDistinct.Goal := by
  intro rs hmem
  have hkeysmem : none ∈ (rs.map Prod.fst).eraseDups := by simpa using hmem
  have hfilt := filter_detail_keys_none rs ((rs.map Prod.fst).eraseDups)
      (nodup_eraseDups _) hkeysmem
  simp [rollup, hfilt, grand, detail]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.GroupingOutput.Goal_NullDetailAndSubtotalDistinct

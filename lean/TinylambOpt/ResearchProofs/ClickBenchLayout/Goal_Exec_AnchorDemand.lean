import TinylambOpt.ResearchGoals.ClickBenchLayout.Execution
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ClickBenchLayout.Exec_AnchorDemand

open TinylambOpt.ResearchGoals.ClickBenchLayout.Execution
open AnchorDemand

theorem sound : Goal := by
  intro tags frameOf ids available
  constructor
  · intro h x y hxy
    apply List.map_congr_left
    intro i hi
    unfold atRow
    rcases hti : tags[i]? with _ | t
    · simp [hti]
    · cases t with
      | literal v => simp [hti]
      | copyBase =>
        have htagbool : (tags[i]? == some .copyBase) = true := by
          rw [hti]
          decide
        have hneed : frameOf i ∈ needed tags frameOf ids := by
          unfold needed
          rw [List.mem_eraseDups]
          exact List.mem_map.mpr ⟨i, List.mem_filter.mpr ⟨hi, htagbool⟩, rfl⟩
        exact hxy i (h (frameOf i) hneed)
  · intro h f hf
    by_cases hnav : f ∈ available
    · exact hnav
    · have hf' : f ∈ (ids.filter (fun i => tags[i]? == some .copyBase)).map frameOf := by
        simpa [needed] using hf
      obtain ⟨i, hi_filter, hframe⟩ := List.mem_map.mp hf'
      have hi_ids : i ∈ ids := (List.mem_filter.mp hi_filter).1
      have htagbool : (tags[i]? == some .copyBase) = true := (List.mem_filter.mp hi_filter).2
      have htag : tags[i]? = some .copyBase := by
        rcases hti : tags[i]? with _ | t
        · rw [hti] at htagbool
          contradiction
        · cases t with
          | literal v =>
            rw [hti] at htagbool
            have hb : (some (Tag.literal v) == some Tag.copyBase) = false := by rfl
            rw [hb] at htagbool
            contradiction
          | copyBase => rfl
      let x : Nat → Option Word := fun _ => some []
      let y : Nat → Option Word := fun j => if j = i then some [0] else some []
      have hagree : ∀ j, frameOf j ∈ available → x j = y j := by
        intro j hj
        have hjne : j ≠ i := by
          intro hji
          subst hji
          rw [hframe] at hj
          exact hnav hj
        simp [x, y, hjne]
      have heq := h x y hagree
      obtain ⟨k, hklt, hk⟩ := List.mem_iff_getElem.mp hi_ids
      have hkmap : (ids.map (atRow tags x))[k]? = (ids.map (atRow tags y))[k]? := by
        rw [heq]
      rw [List.getElem?_map, List.getElem?_map] at hkmap
      rw [List.getElem?_eq_getElem hklt, hk] at hkmap
      simp [atRow, htag, x, y] at hkmap

end TinylambOpt.ResearchProofs.ClickBenchLayout.Exec_AnchorDemand

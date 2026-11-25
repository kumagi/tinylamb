import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Gather_CoarserPages

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Gather

/-- `eraseDups` of an image has at most as many distinct elements as the source. -/
private theorem nodup_eraseDups [BEq α] [LawfulBEq α] : ∀ l : List α, l.eraseDups.Nodup := by
  intro l
  induction h : l.length using Nat.strongRecOn generalizing l with
  | ind n ih =>
      cases l with
      | nil => simp
      | cons a l =>
          rw [List.eraseDups_cons]
          refine List.nodup_cons.mpr ⟨?_, ?_⟩
          · simp
          · apply ih ((l.filter (fun b => !b == a)).length)
            · have hle := List.length_filter_le (l := l) (p := fun b => !b == a)
              have hn : n = l.length + 1 := by simpa using h.symm
              omega
            · rfl

private theorem eraseDups_map_le [BEq α] [LawfulBEq α] [BEq β] [LawfulBEq β]
    (f : α → β) (l : List α) : (l.map f).eraseDups.length ≤ l.eraseDups.length := by
  calc
    (l.map f).eraseDups.length ≤ (l.eraseDups.map f).length :=
      List.Nodup.length_le_of_subset (nodup_eraseDups (l.map f)) (by
        intro y hy
        rw [List.mem_eraseDups] at hy
        rcases List.mem_map.mp hy with ⟨x, hx, rfl⟩
        exact List.mem_map.mpr ⟨x, List.mem_eraseDups.mpr hx, rfl⟩)
    _ = l.eraseDups.length := by simp

theorem sound_Gather_CoarserPages : CoarserPages.Goal := by
  intro ids cap n _hcap _hn
  unfold pages
  have hmap : ids.map (fun x => x / (cap * n)) =
      (ids.map (fun x => x / cap)).map (fun p => p / n) := by
    simp [List.map_map, Nat.div_div_eq_div_mul]
  rw [hmap]
  exact eraseDups_map_le (fun p : Nat => p / n) (ids.map (fun x => x / cap))

end TinylambOpt.ResearchProofs.Atlas.Goal_Gather_CoarserPages

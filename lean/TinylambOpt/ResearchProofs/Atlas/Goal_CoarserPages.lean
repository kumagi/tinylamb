import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

/-!
SQL operators may batch or defer work under the modeled memory and resource ownership
conditions.

Proved

- `Gather.CoarserPages` — `∀ ids cap n, 0 < cap → 0 < n → (pages (cap*n) ids).length ≤
  (pages cap ids).length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_CoarserPages

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Gather

private theorem nodup_eraseDups [BEq α] [LawfulBEq α] : ∀ xs : List α, xs.eraseDups.Nodup
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro h
        have hf : a ∈ List.filter (fun b => ! b == a) as := (List.mem_eraseDups.mp h)
        have hp := (List.mem_filter.mp hf).2
        simp at hp
      · exact nodup_eraseDups (List.filter (fun b => ! b == a) as)
termination_by xs => xs.length
decreasing_by
  exact Nat.lt_succ_of_le (List.length_filter_le _ _)

private theorem eraseDups_len_le [BEq α] : ∀ xs : List α, xs.eraseDups.length ≤ xs.length
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons]
      simp only [List.length_cons]
      exact Nat.succ_le_succ (Nat.le_trans (eraseDups_len_le (List.filter (fun b => ! b == a) as))
        (List.length_filter_le _ _))
termination_by xs => xs.length
decreasing_by
  exact Nat.lt_succ_of_le (List.length_filter_le _ _)

private theorem eraseDups_comp_map_le [BEq α] [LawfulBEq α] [BEq β] [LawfulBEq β]
    [BEq γ] [LawfulBEq γ] (f : α → β) (g : β → γ) (xs : List α) :
    (xs.map (fun x => g (f x))).eraseDups.length ≤ (xs.map f).eraseDups.length := by
  have hperm : (xs.map (fun x => g (f x))).eraseDups.Perm
      (((xs.map f).eraseDups.map g).eraseDups) := by
    apply (List.perm_ext_iff_of_nodup (nodup_eraseDups _) (nodup_eraseDups _)).mpr
    intro z
    simp only [List.mem_eraseDups, List.mem_map]
    constructor
    · rintro ⟨a, ha, rfl⟩
      exact ⟨f a, ⟨a, ha, rfl⟩, rfl⟩
    · rintro ⟨_, ⟨a, ha, rfl⟩, rfl⟩
      exact ⟨a, ha, rfl⟩
  have hlen := hperm.length_eq
  rw [hlen]
  simpa using (eraseDups_len_le ((xs.map f).eraseDups.map g))

theorem sound_CoarserPages : CoarserPages.Goal := by
  intro ids cap n hcap hn
  dsimp [pages]
  simpa [Nat.div_div_eq_div_mul] using
    (eraseDups_comp_map_le (fun id : Nat => id / cap) (fun p : Nat => p / n) ids)

end TinylambOpt.ResearchProofs.Atlas.Goal_CoarserPages

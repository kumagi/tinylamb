import TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FusionTraffic_CacheLineCoalesceCrossover

open TinylambOpt.ResearchGoals.JitKernels
open TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

theorem sound : TinylambOpt.ResearchGoals.JitKernels.FusionTraffic.CacheLineCoalesceCrossover.Goal := by
  intro base stride w sel n issue gatherFee line hsel
  dsimp only
  simp only [TinylambOpt.ResearchGoals.JitKernels.FusionTraffic.lazyCost]
  let dense := (footprint base stride w (List.range n)).length
  let sparse := (footprint base stride w sel).length
  change issue + line * dense ≤ gatherFee * sel.length + line * sparse ↔
    issue + line * (dense - sparse) ≤ gatherFee * sel.length
  have hfilterTrue : ∀ l : List Nat, l.filter (fun _ => true) = l := by
    intro l
    induction l with
    | nil => simp
    | cons x xs ih => simp [ih]
  have hnodupf : ∀ (l : List Nat) (p : Nat → Bool), (l.filter p).eraseDups.Nodup := by
    intro l
    induction l with
    | nil => intro p; simp
    | cons x xs ih =>
        intro p
        by_cases hpx : p x
        · rw [List.filter_cons_of_pos hpx, List.eraseDups_cons]
          constructor
          · intro a ha hax
            have haq : (! a == x) = true := (List.mem_filter.mp (List.mem_eraseDups.mp ha)).2
            have hane : a ≠ x := by
              intro h
              subst a
              simp at haq
            exact hane hax.symm
          · rw [List.filter_filter]
            exact ih (fun y => ! y == x && p y)
        · rw [List.filter_cons_of_neg hpx]
          exact ih p
  have hnodup : ∀ l : List Nat, l.eraseDups.Nodup := by
    intro l
    have h := hnodupf l (fun _ => true)
    rw [hfilterTrue] at h
    exact h
  have hmemdense : ∀ i, i ∈ sel → lineOf base stride w i ∈
      footprint base stride w (List.range n) := by
    intro i hi
    exact List.mem_eraseDups.mpr (List.mem_map.mpr ⟨i, List.mem_range.mpr (hsel i hi), rfl⟩)
  have hsubset : footprint base stride w sel ⊆ footprint base stride w (List.range n) := by
    intro l hl
    have hlmap : l ∈ List.map (lineOf base stride w) sel := List.mem_eraseDups.mp hl
    rcases List.mem_map.mp hlmap with ⟨i, hi, hli⟩
    simpa [hli] using hmemdense i hi
  have hle : sparse ≤ dense := by
    exact List.Nodup.length_le_of_subset (hnodup _) hsubset
  have hdecomp : dense = sparse + (dense - sparse) := by
    exact (Nat.add_sub_of_le hle).symm
  constructor
  · intro h
    rw [hdecomp, Nat.mul_add] at h
    omega
  · intro h
    rw [hdecomp, Nat.mul_add]
    omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_FusionTraffic_CacheLineCoalesceCrossover

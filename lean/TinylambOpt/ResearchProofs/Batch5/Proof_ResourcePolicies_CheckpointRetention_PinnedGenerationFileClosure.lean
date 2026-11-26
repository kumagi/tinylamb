import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CheckpointRetention.Goal_PinnedGenerationFileClosure

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention


set_option maxRecDepth 10000

/-! Sweeping with every reader root keeps exactly the disk files that any root
still references. For a pinned generation, each of its files is such a root
reference, so its readiness check sees the same file set as the raw disk. -/

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

private theorem sweep_contains_eq (disk : List Nat) (roots : List Generation)
    {g : Generation} (hg : g ∈ roots) {f : Nat} (hf : f ∈ g.files) :
    (sweep disk roots).contains f = disk.contains f := by
  have hprop : (f ∈ sweep disk roots) ↔ f ∈ disk := by
    constructor
    · intro hmem
      exact (List.mem_filter.mp hmem).1
    · intro hmem
      exact List.mem_filter.mpr ⟨hmem, List.any_eq_true.mpr ⟨g, hg, by
        change List.elem f g.files = true
        rw [List.elem_eq_mem]
        simp [hf]⟩⟩
  change List.elem f (sweep disk roots) = List.elem f disk
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    rw [List.elem_eq_mem] at h
    have hm : f ∈ disk := hprop.mp (of_decide_eq_true h)
    rw [List.elem_eq_mem]
    simp [hm]
  · intro h
    rw [List.elem_eq_mem] at h
    have hm : f ∈ sweep disk roots := hprop.mpr (of_decide_eq_true h)
    rw [List.elem_eq_mem]
    simp [hm]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention.PinnedGenerationFileClosure.Goal := by
  intro disk roots g w hg
  unfold ready
  have hcs : g.files.all (fun f => (sweep disk roots).contains f) =
      g.files.all (fun f => disk.contains f) := by
    apply Bool.eq_iff_iff.mpr
    constructor
    · intro h
      apply List.all_eq_true.mpr
      intro f hf
      have hfbool : (sweep disk roots).contains f = true :=
        (List.all_eq_true.mp h) f hf
      rwa [sweep_contains_eq disk roots hg hf] at hfbool
    · intro h
      apply List.all_eq_true.mpr
      intro f hf
      have hfbool : disk.contains f = true := (List.all_eq_true.mp h) f hf
      rwa [sweep_contains_eq disk roots hg hf]
  rw [hcs]
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CheckpointRetention.Goal_PinnedGenerationFileClosure

import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_PatchedOctet_SeekCap

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.PatchedOctet

/-- Relaxing the maximum frame length only enlarges the set of admissible
    partitions, so the minimum total charge cannot grow. This argument is
    independent of the particular writer. -/
theorem sound_SeekCap : PatchedOctet.SeekCap.Goal := by
  intro xs a b s hab hbudget
  let Qa : List (List Nat) → Bool := fun ps => ps.all (fun p => p.length ≤ a)
  let Qb : List (List Nat) → Bool := fun ps => ps.all (fun p => p.length ≤ b)
  let g : List (List Nat) → Nat := charge write 0
  let La : List Nat := ((parts xs).filter Qa).map g
  let Lb : List Nat := ((parts xs).filter Qb).map g
  have hLa : La.min? = some s := by
    simpa [La, Qa, g, budget] using hbudget
  have hs_mem : s ∈ La := (List.min?_eq_some_iff.mp hLa).1
  rcases List.mem_map.mp hs_mem with ⟨ps, hps_filt, hps_charge⟩
  have hps_mem : ps ∈ parts xs := (List.mem_filter.mp hps_filt).1
  have hps_Qa : Qa ps = true := (List.mem_filter.mp hps_filt).2
  have hps_Qb : Qb ps = true := by
    dsimp [Qa, Qb] at hps_Qa ⊢
    rw [List.all_eq_true] at hps_Qa ⊢
    intro p hp
    exact decide_eq_true (Nat.le_trans (of_decide_eq_true (hps_Qa p hp)) hab)
  have hps_filt_b : ps ∈ (parts xs).filter Qb :=
    List.mem_filter.mpr ⟨hps_mem, hps_Qb⟩
  have hs_mem_b : s ∈ Lb := by
    rw [← hps_charge]
    exact List.mem_map.mpr ⟨ps, hps_filt_b, rfl⟩
  cases hb : Lb.min? with
  | none =>
      have hnil : Lb = [] := List.min?_eq_none_iff.mp hb
      rw [hnil] at hs_mem_b
      simp at hs_mem_b
  | some t =>
      refine ⟨t, ?_, ?_⟩
      · exact (List.min?_eq_some_iff.mp hb).2 s hs_mem_b
      · simpa [Lb, Qb, g, budget] using hb

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_PatchedOctet_SeekCap

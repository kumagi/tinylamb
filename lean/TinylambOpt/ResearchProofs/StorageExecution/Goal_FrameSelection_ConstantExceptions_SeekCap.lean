import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_ConstantExceptions_SeekCap

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.ConstantExceptions

-- Relaxing the per-frame row cap only enlarges the set of admissible layouts,
-- so the minimum charge cannot increase. This holds for any writer.
theorem sound_SeekCap : ConstantExceptions.SeekCap.Goal := by
  intro xs a b s hab hb
  unfold budget at hb ⊢
  have hs_la : s ∈ (((parts xs).filter (fun ps => ps.all (fun p => p.length ≤ a))).map
      (charge ConstantExceptions.write 0)) := (List.min?_eq_some_iff.mp hb).1
  have hs_lb : s ∈ (((parts xs).filter (fun ps => ps.all (fun p => p.length ≤ b))).map
      (charge ConstantExceptions.write 0)) := by
    rcases List.mem_map.mp hs_la with ⟨ps, hps, rfl⟩
    apply List.mem_map.mpr
    refine ⟨ps, ?_, rfl⟩
    simp only [List.mem_filter] at hps ⊢
    refine ⟨hps.1, ?_⟩
    apply List.all_eq_true.mpr
    intro p hp
    have hp' := (List.all_eq_true.mp hps.2) p hp
    simp only [decide_eq_true_eq] at hp' ⊢
    exact Nat.le_trans hp' hab
  rcases hmin : (((parts xs).filter (fun ps => ps.all (fun p => p.length ≤ b))).map
      (charge ConstantExceptions.write 0)).min? with _ | t
  · have hempty : ((parts xs).filter (fun ps => ps.all (fun p => p.length ≤ b))).map
        (charge ConstantExceptions.write 0) = [] := List.min?_eq_none_iff.mp hmin
    rw [hempty] at hs_lb
    simp at hs_lb
  · refine ⟨t, ?_, hmin⟩
    exact (List.min?_eq_some_iff.mp hmin).2 s hs_lb

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_ConstantExceptions_SeekCap

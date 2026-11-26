import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SeparatedLengths_SeekCap

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SeparatedLengths

/-- A list containing `s` has a minimum, and that minimum is at most `s`. -/
private theorem min?_some_le_of_mem {l : List Nat} {s : Nat} (hs : s ∈ l) :
    ∃ t, l.min? = some t ∧ t ≤ s := by
  cases l with
  | nil => simp at hs
  | cons x xs =>
      let t := xs.min?.elim x (min x)
      have hmin : (x :: xs).min? = some t := by
        simp [t]
      exact ⟨t, hmin, (List.min?_eq_some_iff.mp hmin).2 s hs⟩

theorem sound_SeekCap : SeekCap.Goal := by
  unfold SeekCap.Goal seekCap
  intro xs a b s hab hbud
  unfold budget at hbud
  have hs_mem : s ∈
      (((parts xs).filter (fun ps => ps.all (fun p => p.length ≤ a))).map (charge write 0)) :=
    List.min?_mem hbud
  rcases List.mem_map.mp hs_mem with ⟨ps, hpsA, hcharge⟩
  have hpsA' : ps ∈ (parts xs).filter (fun ps => ps.all (fun p => p.length ≤ a)) := by
    simpa using hpsA
  rw [List.mem_filter] at hpsA'
  rcases hpsA' with ⟨hpsparts, hallA⟩
  have hpsB : ps ∈ (parts xs).filter (fun ps => ps.all (fun p => p.length ≤ b)) := by
    rw [List.mem_filter]
    refine ⟨hpsparts, ?_⟩
    simp only [List.all_eq_true] at hallA ⊢
    intro p hp
    have hple : p.length ≤ b := Nat.le_trans (of_decide_eq_true (hallA p hp)) hab
    simpa using hple
  have hs_mem_B : s ∈
      (((parts xs).filter (fun ps => ps.all (fun p => p.length ≤ b))).map (charge write 0)) := by
    exact List.mem_map.mpr ⟨ps, by simpa using hpsB, hcharge⟩
  rcases min?_some_le_of_mem hs_mem_B with ⟨t, htmin, hts⟩
  refine ⟨t, hts, ?_⟩
  simpa [budget] using htmin

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SeparatedLengths_SeekCap

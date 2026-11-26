import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_DictionaryRuns_SeekCap

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.DictionaryRuns

/-! The claim is generic in the writer: raising the cap from `a` to `b` only
enlarges the set of admissible partitions, so the minimum charge cannot grow.
No property of the dictionary codec is needed. -/

private theorem all_widen (ps : List (List Nat)) (a b : Nat) (hab : a ≤ b)
    (h : ps.all (fun p => p.length ≤ a) = true) :
    ps.all (fun p => p.length ≤ b) = true := by
  rw [List.all_eq_true] at h ⊢
  intro p hp
  have ha : p.length ≤ a := by simpa using h p hp
  have hb : p.length ≤ b := by omega
  simpa using hb

private theorem min?_some_of_mem {l : List Nat} {v : Nat} (hv : v ∈ l) :
    ∃ t, l.min? = some t := by
  cases l with
  | nil => simp at hv
  | cons x xs => exact ⟨(xs.min?.elim x (min x)), by simp⟩

theorem sound_SeekCap : DictionaryRuns.SeekCap.Goal := by
  intro xs a b s hab h
  unfold budget at h ⊢
  rw [List.min?_eq_some_iff] at h
  rcases h with ⟨hs_mem, _⟩
  rcases List.mem_map.mp hs_mem with ⟨ps, hps_a, hcharge⟩
  have hps_parts : ps ∈ parts xs := (List.mem_filter.mp hps_a).1
  have hps_all_a : ps.all (fun p => p.length ≤ a) = true :=
    (List.mem_filter.mp hps_a).2
  have hps_all_b : ps.all (fun p => p.length ≤ b) = true :=
    all_widen ps a b hab hps_all_a
  have hs_mem_b : s ∈ ((parts xs).filter (fun ps => ps.all (fun p => p.length ≤ b))).map
      (charge DictionaryRuns.write 0) := by
    exact List.mem_map.mpr ⟨ps, List.mem_filter.mpr ⟨hps_parts, hps_all_b⟩, hcharge⟩
  rcases min?_some_of_mem hs_mem_b with ⟨t, ht⟩
  refine ⟨t, ?_, ht⟩
  exact (List.min?_eq_some_iff.mp ht).2 s hs_mem_b

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_DictionaryRuns_SeekCap

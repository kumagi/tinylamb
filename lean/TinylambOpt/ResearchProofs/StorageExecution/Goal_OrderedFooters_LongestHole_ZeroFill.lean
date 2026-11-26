import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_LongestHole_ZeroFill

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.LongestHole
open TinylambOpt.ResearchGoals.Compression.Wire

private def probe (n : Nat) : Bool :=
  agg.reference (List.replicate n 0) == (n, n, n, n)

#eval (List.range 21).map probe

private theorem longest_zero_replicate (n : Nat) : longestZero (List.replicate n 0) = n := by
  unfold longestZero
  simp only [List.length_replicate]
  have hmap :
      (List.range (n + 1)).map
          (fun i => (((List.replicate n 0).drop i).takeWhile (fun x => x == 0)).length) =
        (List.range (n + 1)).map (fun i => n - i) := by
    simp [List.drop_replicate]
  rw [hmap]
  unfold peak
  have hmax : ((List.range (n + 1)).map (fun i => n - i)).max? = some n := by
    rw [List.max?_eq_some_iff]
    constructor
    · simp only [List.mem_map, List.mem_range]
      exact ⟨0, by omega, by simp⟩
    · intro b hb
      simp only [List.mem_map, List.mem_range] at hb
      obtain ⟨i, hi, rfl⟩ := hb
      omega
  rw [hmax]
  rfl

theorem sound_ZeroFill : ZeroFill.Goal := by
  intro n
  simp [agg, longest_zero_replicate]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_LongestHole_ZeroFill

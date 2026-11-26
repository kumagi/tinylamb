import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SplitNibble_MixedPointCost

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SplitNibble

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) : xs.eraseDups.Nodup := by
  match xs with
  | [] => simp
  | x :: xs =>
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro hx
        rw [List.mem_eraseDups, List.mem_filter] at hx
        simp at hx
      · exact nodup_eraseDups (xs.filter (fun y => !y == x))
  termination_by xs.length
  decreasing_by
    simp_wf
    exact Nat.lt_succ_of_le (List.length_filter_le _ _)

/-- With `4 ≤ w` the four low lanes of row `i` occupy the single byte
`i / 2` (their positions are `4*i+b`, `b < 4`).  Each remaining lane
`b ≥ 4` contributes at most one byte, so the footprint has at most
`1 + (w - 4)` bytes. -/
theorem sound_MixedPointCost : SplitNibble.MixedPointCost.Goal := by
  intro n w i hw
  let ys : List Nat := i / 2 :: ((List.range (w - 4)).map (fun k => ((k + 4) * n + i) / 8))
  have hsub : footprint layout n w i ⊆ ys := by
    intro x hx
    rw [footprint, lanes, List.mem_eraseDups, List.mem_map] at hx
    rcases hx with ⟨b, hbmem, rfl⟩
    rw [List.mem_range] at hbmem
    have hmin : min 4 w = 4 := Nat.min_eq_left hw
    rw [List.mem_cons, List.mem_map]
    by_cases hb4 : b < 4
    · left
      simp [layout, hmin, hb4]
      omega
    · right
      refine ⟨b - 4, ?_, ?_⟩
      · rw [List.mem_range]
        omega
      · simp [layout, hmin, hb4]
        congr 1
        rw [Nat.add_mul]
        ac_rfl
  calc
    (footprint layout n w i).length ≤ ys.length :=
      (nodup_eraseDups _).length_le_of_subset hsub
    _ = 1 + (w - 4) := by simp [ys, Nat.add_comm]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SplitNibble_MixedPointCost

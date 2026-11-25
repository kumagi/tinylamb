import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_BestFrame_NegativePage

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! Brute-force truth check: every series of length <= 6 whose entries are all
    nonpositive has measure 0. -/
#eval
  let rec gen : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => (gen n).flatMap (fun xs => [-3, -2, -1, 0].map (fun v => xs ++ [v]))
  (gen 6).all (fun xs => BestFrame.measure xs == 0)

private theorem sum_nonpos {xs : List Int} (h : ∀ a ∈ xs, a ≤ 0) : xs.sum ≤ 0 := by
  induction xs with
  | nil => simp
  | cons a as ih =>
      simp only [List.sum_cons]
      have ha := h a (by simp)
      have hi := ih (fun b hb => h b (by simp [hb]))
      omega

private theorem mem_of_window_mem {x w : Series} (hw : w ∈ windows x) :
    ∀ v ∈ w, v ∈ x := by
  unfold windows at hw
  rcases List.mem_flatMap.mp hw with ⟨i, hi, hw⟩
  rw [List.mem_range] at hi
  rcases List.mem_map.mp hw with ⟨j, hj, rfl⟩
  rw [List.mem_range] at hj
  intro v hv
  exact List.mem_of_mem_drop (List.mem_of_mem_take hv)

private theorem max0_le {xs : List Int} {c : Int} (hc : 0 ≤ c)
    (h : ∀ a ∈ xs, a ≤ c) : max0 xs ≤ c := by
  unfold max0
  apply Int.max_le.mpr
  constructor
  · exact hc
  · cases hm : xs.max? with
    | none => simpa [hm]
    | some m => exact (List.max?_le_iff hm).mpr h

theorem sound_BestFrame_NegativePage : BestFrame.NegativePage.Goal := by
  intro x hx
  unfold BestFrame.measure
  apply Int.le_antisymm
  · apply max0_le (c := 0) (by omega)
    intro v hv
    rcases List.mem_map.mp hv with ⟨w, hw, rfl⟩
    apply sum_nonpos
    intro a ha
    exact hx a (mem_of_window_mem hw a ha)
  · unfold max0
    exact Int.le_max_left 0 _

end TinylambOpt.ResearchProofs.Atlas.Goal_BestFrame_NegativePage

import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Minimum_FallbackNecessary

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : Minimum.FallbackNecessary.Goal := by
  intro xs x hx hd
  by_cases hlen : xs.length = 1
  · simp [dropOne, hlen] at hd
  have hgt : 1 < xs.length := by
    have hp : 0 < xs.length := List.length_pos_of_mem hx
    omega
  have hnolt : ¬ xs.min?.getD 0 < x := by
    simpa [dropOne, Minimum.model, hlen] using hd
  have hv : xs.min?.getD 0 = x := min_eq_of_mem_of_not_lt hx hnolt
  let l := xs.erase x
  have hllen : l.length = xs.length - 1 := by dsimp [l]; rw [List.length_erase_of_mem hx]
  have hlnonempty : l ≠ [] := by
    intro h
    have : l.length = 0 := by simp [h]
    omega
  let c := x + l.min?.getD 0 + 1
  have hclt : x < c := by dsimp [c]; omega
  let ys := x :: l.map (fun _ => c)
  refine ⟨ys, ?_, ?_, ?_, ?_⟩
  · simp [ys]
  · simp [ys, hllen]
    omega
  · have hminys : ys.min?.getD 0 = x := by
      have hysne : ys ≠ [] := by simp [ys]
      have hmems : x ∈ ys := by simp [ys]
      have hbound : ∀ b, b ∈ ys → x ≤ b := by
        intro b hb
        simp only [ys, List.mem_cons, List.mem_map] at hb
        rcases hb with hb | ⟨a, ha, hba⟩
        · subst b
          exact Nat.le_refl x
        · rw [← hba]
          exact Nat.le_of_lt hclt
      have hh : ys.min? = some x := (List.min?_eq_some_iff).2 ⟨hmems, hbound⟩
      simpa [hh]
    have hne_xs : xs ≠ [] := by intro h; subst xs; simp at hx
    have hvalxs : value Minimum.model xs = some x := by simp [value, Minimum.model, hne_xs, hv]
    have hvalys : value Minimum.model ys = some x := by simp [value, Minimum.model, hminys]
    rw [hvalxs, hvalys]
  · have hminl : l.min?.getD 0 = c - (x + 1) := by dsimp [c]; omega
    have hminerase : (ys.erase x).min?.getD 0 = c := by
      have hs : ys.erase x = l.map (fun _ => c) := by simp [ys]
      rw [hs]
      have hmapne : l.map (fun _ => c) ≠ [] := by simp [hlnonempty]
      have hmems2 : c ∈ l.map (fun _ => c) := by
        rcases List.exists_cons_of_ne_nil hlnonempty with ⟨a, as, hcons⟩
        rw [hcons]
        simp
      have hbound2 : ∀ b, b ∈ l.map (fun _ => c) → c ≤ b := by
        intro b hb
        simp only [List.mem_map] at hb
        rcases hb with ⟨a, ha, hba⟩
        rw [← hba]
        exact Nat.le_refl c
      have hm : (l.map (fun _ => c)).min? = some c :=
        (List.min?_eq_some_iff).2 ⟨hmems2, hbound2⟩
      simpa [hm]
    have hminorig : (xs.erase x).min?.getD 0 = l.min?.getD 0 := by rfl
    have hcne : c ≠ l.min?.getD 0 := by dsimp [c]; omega
    have hne_orig : xs.erase x ≠ [] := by
      intro h
      have : (xs.erase x).length = 0 := by simp [h]
      rw [List.length_erase_of_mem hx] at this
      omega
    have hne_ys : ys.erase x ≠ [] := by
      rw [show ys.erase x = l.map (fun _ => c) by simp [ys]]
      simp [hlnonempty]
    simp [value, Minimum.model, isEmpty_false_of_ne_nil hne_ys,
      isEmpty_false_of_ne_nil hne_orig, hminerase, hminorig, hcne]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Minimum_FallbackNecessary

import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `SeparatedLengths.StrictSavings` — Sum net page gains after counting the fixed-width
  alternative and each restart entry.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SeparatedLengths_StrictSavings

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SeparatedLengths

private theorem page_saving (p : List Nat) :
    chosen write p + saved write p = (raw p).length := by
  unfold chosen saved
  by_cases h : (frame write p).length ≤ (raw p).length
  · rw [Nat.min_eq_left h]
    omega
  · have h' : (raw p).length ≤ (frame write p).length := Nat.le_of_lt (Nat.lt_of_not_ge h)
    rw [Nat.min_eq_right h']
    omega

private theorem charge_add_saved (f : Writer) (fee : Nat) (ps : List (List Nat)) :
    charge f fee ps + (ps.map (saved f)).sum =
      (ps.map (fun p => fee + chosen f p + saved f p)).sum := by
  induction ps with
  | nil => simp [charge]
  | cons p ps ih =>
      have ih' := ih
      simp only [charge, List.map_cons, List.sum_cons] at ih' ⊢
      omega

theorem sound_StrictSavings : StrictSavings.Goal := by
  intro ps fee
  rw [charge_add_saved]
  have hpoint : ∀ p : List Nat, fee + chosen write p + saved write p = (raw p).length + fee := by
    intro p
    have hp := page_saving p
    omega
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [hpoint p, ih]
      simp [Nat.mul_succ, Nat.add_assoc, Nat.add_left_comm, Nat.add_comm]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SeparatedLengths_StrictSavings

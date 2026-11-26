import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_XorPredecessor_StrictSavings

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.XorPredecessor

/-- For every pair of natural numbers, the cheaper of the two plus the
    saturating shortfall of the first from the second is exactly the second. -/
private theorem min_add_sub (a b : Nat) : min a b + (b - a) = b := by
  by_cases h : a ≤ b
  · have hmin : min a b = a := Nat.min_eq_left h
    rw [hmin]
    omega
  · have hba : b ≤ a := by omega
    have hmin : min a b = b := Nat.min_eq_right hba
    rw [hmin]
    omega

/-- `chosen` plus `saved` recovers the raw length of a single frame. -/
private theorem chosen_add_saved (f : Writer) (p : List Nat) :
    chosen f p + saved f p = (raw p).length := by
  simpa [chosen, saved] using min_add_sub (frame f p).length (raw p).length

private theorem charge_add_saved (f : Writer) (fee : Nat) (p : List Nat) :
    fee + chosen f p + saved f p = fee + (raw p).length := by
  have h := chosen_add_saved f p
  omega

/-- The identity lifts to any list of frames; it is writer-independent. -/
private theorem charge_add_saved_frames (f : Writer) (fee : Nat) (ps : List (List Nat)) :
    charge f fee ps + (ps.map (saved f)).sum =
      (ps.map (fun p => (raw p).length)).sum + fee * ps.length := by
  induction ps with
  | nil => simp [charge]
  | cons p ps ih =>
      simp only [charge, List.map_cons, List.sum_cons, List.length_cons] at ih ⊢
      rw [Nat.mul_succ]
      have h := charge_add_saved f fee p
      omega

theorem sound_StrictSavings : StrictSavings.Goal := by
  intro ps fee
  exact charge_add_saved_frames write fee ps

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_XorPredecessor_StrictSavings

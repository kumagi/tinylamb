import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_SingleIsland_IntervalsAndEmpty

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.SingleIsland

/-! Truth check: the number of accepted bitmaps of length `n` is the number of
    intervals plus the empty bitmap, `1 + n*(n+1)/2`. -/

private def probe : Bool :=
  (List.range 17).all (fun n =>
    count model n 0 == 1 + n * (n + 1) / 2)

#eval probe

/-- From state 0 the first false keeps state 0, the first true enters the island. -/
private theorem count_succ_zero (n : Nat) :
    count model (n + 1) 0 = count model n 0 + count model n 1 := by
  simp [count, model]

/-- Inside an island, false closes it (state 2) and true stays inside. -/
private theorem count_succ_one (n : Nat) :
    count model (n + 1) 1 = count model n 2 + count model n 1 := by
  simp [count, model]

/-- After the island, only false stays legal; state 2 is absorbing. -/
private theorem count_succ_two (n : Nat) : count model (n + 1) 2 = count model n 2 := by
  simp [count, model]

private theorem count_two (n : Nat) : count model n 2 = 1 := by
  induction n with
  | zero => simp [count, model]
  | succ n ih => simp [count_succ_two n, ih]

private theorem count_one (n : Nat) : count model n 1 = n + 1 := by
  induction n with
  | zero => simp [count, model]
  | succ n ih =>
      rw [count_succ_one n, count_two n, ih]
      omega

private theorem even_consecutive (n : Nat) : (n * (n + 1)) % 2 = 0 := by
  rw [Nat.mul_mod]
  rcases Nat.mod_two_eq_zero_or_one n with h | h
  · simp [h]
  · have hs : (n + 1) % 2 = 0 := by omega
    simp [h, hs]

private theorem div_two_step (n : Nat) :
    n * (n + 1) / 2 + (n + 1) = (n + 1) * (n + 2) / 2 := by
  have h1 : 2 * (n * (n + 1) / 2) = n * (n + 1) := by
    rw [Nat.mul_comm]
    exact Nat.div_mul_cancel (Nat.dvd_of_mod_eq_zero (even_consecutive n))
  have h2 : 2 * ((n + 1) * (n + 2) / 2) = (n + 1) * (n + 2) := by
    rw [Nat.mul_comm]
    exact Nat.div_mul_cancel (Nat.dvd_of_mod_eq_zero (even_consecutive (n + 1)))
  have h3 : (n + 1) * (n + 2) = n * (n + 1) + 2 * (n + 1) := by
    simp [Nat.succ_mul, Nat.mul_succ]
    omega
  omega

theorem sound_IntervalsAndEmpty : SingleIsland.IntervalsAndEmpty.Goal := by
  intro n
  induction n with
  | zero => simp [count, model]
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, count_succ_zero n, count_one n, ih]
      change 1 + n * (n + 1) / 2 + (n + 1) = 1 + (n + 1) * (n + 2) / 2
      have h := div_two_step n
      omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_SingleIsland_IntervalsAndEmpty

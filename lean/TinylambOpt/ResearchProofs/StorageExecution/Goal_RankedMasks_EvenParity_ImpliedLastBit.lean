import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `EvenParity.ImpliedLastBit` — `∀ n q, q<2 → count model (n+1) q = 2^n`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_EvenParity_ImpliedLastBit

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.EvenParity

-- Truth check on small inputs.
private def check : Bool :=
  (List.range 12).all fun n =>
  (List.range 3).all fun q =>
    decide (q < 2 → count model (n + 1) q = 2 ^ n)

#eval check

/-- One further bit flips the parity, so the count at length `n+2` is the sum
of the two successor-state counts at length `n+1`. -/
private theorem count_plus_two (n q : Nat) :
    count model (n + 2) q = count model (n + 1) (q % 2) + count model (n + 1) ((q + 1) % 2) := by
  change count model (Nat.succ (Nat.succ n)) q = _
  rw [count.eq_2 model q (Nat.succ n)]
  simp [model]

theorem sound_ImpliedLastBit : ImpliedLastBit.Goal := by
  intro n
  induction n with
  | zero =>
      intro q hq
      have hq' : q = 0 ∨ q = 1 := by omega
      rcases hq' with rfl | rfl <;> native_decide
  | succ n ih =>
      intro q hq
      have h0 : count model (n + 1) 0 = 2 ^ n := ih 0 (by omega)
      have h1 : count model (n + 1) 1 = 2 ^ n := ih 1 (by omega)
      have hq' : q = 0 ∨ q = 1 := by omega
      rcases hq' with rfl | rfl
      · rw [count_plus_two]
        simp [h0, h1, Nat.pow_succ, Nat.mul_comm, Nat.two_mul]
      · rw [count_plus_two]
        simp [h0, h1, Nat.pow_succ, Nat.mul_comm, Nat.two_mul]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_EvenParity_ImpliedLastBit

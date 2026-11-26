import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
One row growing by a single byte can cost more than one byte of directory.

Refutation (`SingleEditDirectoryGrowth.Goal`): changing a single length `n` to
`n+1` was conjectured to grow the varint-encoded restart directory by at most
one byte. With stride 1 every row is a directory boundary, so all *later*
entries shift by the delta of the edited row. Take lengths `[127] → [128]`
inside `[127, 0, 0]`: the directory offsets become `[0, 127, 127]` (three
one-byte varints, 3 bytes) versus `[0, 128, 128]` (one plus two two-byte
varints, 5 bytes) — a jump of 2 bytes from a 1-byte edit. A page writer that
budgets directory growth per edit must charge the delta against the tail of the
directory, not just one entry.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory_SingleEditDirectoryGrowth
open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
open TinylambOpt.ResearchGoals.Compression.Wire

/-- Bounds characterization: `2^k ≤ n < 2^(k+1)` pins `log2 n = k`. -/
private theorem log2_eq_of_bounds (n k : Nat) (hn : n ≠ 0)
    (hlo : ¬ (n < 2 ^ k)) (hhi : n < 2 ^ (k + 1)) : Nat.log2 n = k := by
  rw [Nat.log2_eq_iff hn]
  exact ⟨by omega, hhi⟩

private theorem log2_127 : Nat.log2 127 = 6 :=
  log2_eq_of_bounds 127 6 (by decide) (by decide) (by decide)

private theorem log2_128 : Nat.log2 128 = 7 :=
  log2_eq_of_bounds 128 7 (by decide) (by decide) (by decide)

private theorem ub0 : ubytes 0 = 1 := by unfold ubytes width; decide

private theorem ub127 : ubytes 127 = 1 := by unfold ubytes width; rw [log2_127]; decide

private theorem ub128 : ubytes 128 = 2 := by unfold ubytes width; rw [log2_128]; decide

theorem refute : ¬ SingleEditDirectoryGrowth.Goal := by
  intro h
  have h128 : directory (([] : List Nat) ++ [128] ++ [0, 0]) 1 = [0, 128, 128] := by decide
  have h127 : directory (([] : List Nat) ++ [127] ++ [0, 0]) 1 = [0, 127, 127] := by decide
  have hb := h [] [0, 0] 127 1 (by decide)
  rw [h128, h127] at hb
  unfold streamBytes at hb
  simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil] at hb
  rw [ub0, ub127, ub128] at hb
  exact absurd hb (by decide)

end TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory_SingleEditDirectoryGrowth


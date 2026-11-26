import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
A patched-frame width can look locally optimal yet lose to a much smaller one.

Refutation (`PatchedWidthLocalMinimum.Goal`): comparing only the neighbouring
widths `w-1` and `w+1` was conjectured to certify that `w` is the global
optimum for `patchedBytes`. On the frame `[0, 0, 129]` the width-8 patched
frame costs 4 bytes while its neighbours cost 6 (w=7, exception list
`[129]` = 2 varint bytes) and 5 (w=9, no exceptions but 4 packed bytes) — a
strict local minimum. But width 0, which flags everything `≥ 2^0 = 1` and
stores nothing inline, costs only 3 bytes. Local width search must scan
further (or at least consider the degenerate widths) before a page commits to
a patched frame layout.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory_PatchedWidthLocalMinimum
open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
open TinylambOpt.ResearchGoals.Compression.Wire

/-- Bounds characterization: `2^k ≤ n < 2^(k+1)` pins `log2 n = k`. -/
private theorem log2_eq_of_bounds (n k : Nat) (hn : n ≠ 0)
    (hlo : ¬ (n < 2 ^ k)) (hhi : n < 2 ^ (k + 1)) : Nat.log2 n = k := by
  rw [Nat.log2_eq_iff hn]
  exact ⟨by omega, hhi⟩

private theorem log2_129 : Nat.log2 129 = 7 :=
  log2_eq_of_bounds 129 7 (by decide) (by decide) (by decide)

private theorem sb_nil : streamBytes [] = 0 := rfl

private theorem sb_129 : streamBytes [129] = 2 := by
  unfold streamBytes
  rw [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
  unfold ubytes width
  rw [log2_129]
  decide

private theorem fl7 : List.filter (flagged 7) [0, 0, 129] = [129] := by decide
private theorem fl8 : List.filter (flagged 8) [0, 0, 129] = [] := by decide
private theorem fl9 : List.filter (flagged 9) [0, 0, 129] = [] := by decide
private theorem fl0 : List.filter (flagged 0) [0, 0, 129] = [129] := by decide

theorem refute : ¬ PatchedWidthLocalMinimum.Goal := by
  intro h
  have h87 : patchedBytes 8 [0, 0, 129] < patchedBytes 7 [0, 0, 129] := by
    unfold patchedBytes
    rw [fl8, fl7, sb_nil, sb_129]
    decide
  have h89 : patchedBytes 8 [0, 0, 129] < patchedBytes 9 [0, 0, 129] := by
    unfold patchedBytes
    rw [fl8, fl9, sb_nil]
    decide
  have h80 : ¬ (patchedBytes 8 [0, 0, 129] ≤ patchedBytes 0 [0, 0, 129]) := by
    intro hle
    unfold patchedBytes at hle
    rw [fl8, fl0, sb_nil, sb_129] at hle
    exact absurd hle (by decide)
  exact h80 (h [0, 0, 129] 8 (by decide) h87 h89 0)

end TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory_PatchedWidthLocalMinimum


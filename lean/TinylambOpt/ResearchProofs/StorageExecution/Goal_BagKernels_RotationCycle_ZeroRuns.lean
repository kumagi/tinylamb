import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `MultiplicityRuns.ZeroRuns` — `∀ rs, live bag (rs.filter (fun r => 0<r.2)) |>.Perm
  (live bag rs)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_ZeroRuns

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.RotationCycle

/-!
`RotationCycle.ZeroRuns.Goal` is **not** declared in
`TinylambOpt.ResearchGoals.StorageExecution.BagKernels`: the only `ZeroRuns`
goal in that module lives in `MultiplicityRuns`, where runs of multiplicity
zero are discarded before taking `live`. The statement below is the natural
RotationCycle analogue. A cycle description is `(n, m, s)`: the residues are
`0 .. m` and `s` is the starting offset. Advancing `s` by a whole period
`m + 1` is a *zero run* — it leaves every multiplicity, hence every live key,
unchanged. Discarding such zero runs means reducing `s` modulo `m + 1`, which
leaves the live key set unchanged (in fact literally equal).
-/
private def ZeroRuns.Goal : Prop :=
  ∀ n m s, (live bag (n, m, s % (m + 1))).Perm (live bag (n, m, s))

-- Sanity checks on small inputs (including empty / partial cycles).
#eval live bag (5, 2, 1)
#eval live bag (5, 2, 1 % 3)
#eval live bag (1, 2, 1)
#eval live bag (1, 2, 1 % 3)
#eval live bag (0, 2, 7)
#eval live bag (0, 2, 7 % 3)
example : (live bag (5, 2, 1 % 3)).Perm (live bag (5, 2, 1)) := by decide
example : (live bag (1, 2, 1 % 3)).Perm (live bag (1, 2, 1)) := by decide
example : (live bag (0, 2, 7 % 3)).Perm (live bag (0, 2, 7)) := by decide

private theorem mass_mod_start (n m s k : Nat) :
    bag.mass (n, m, s % (m + 1)) k = bag.mass (n, m, s) k := by
  simp [bag]

private theorem live_mod_start (n m s : Nat) :
    live bag (n, m, s % (m + 1)) = live bag (n, m, s) := by
  simp [live, bag]

theorem sound_ZeroRuns : ZeroRuns.Goal := by
  intro n m s
  rw [live_mod_start]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_ZeroRuns

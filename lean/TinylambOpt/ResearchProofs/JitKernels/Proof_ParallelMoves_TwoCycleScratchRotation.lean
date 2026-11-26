import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-!
**Proved: the standard three-move scratch rotation breaks a two-register PHI
cycle exactly where it is observable.**
For distinct registers `a`, `b` and a scratch `tmp` different from both, the
serial bundle `[(tmp, a), (a, b), (b, tmp)]` agrees with the ideal parallel
swap `[(a, b), (b, a)]` at every register other than `tmp`: the first move
parks `a`'s entry value in `tmp`, the second overwrites `a` with `b`'s entry
value (it reads `b`, which is untouched so far because `tmp ≠ b`), and the
third writes the parked value into `b` (reading `tmp` gives `s a` because the
accumulator still has the parked value since `tmp ≠ a`). Only `tmp` itself
ends up holding `s a` instead of its entry value — exactly the dead-on-exit
scratch assumption. A PHI lowering or OSR entry may therefore emit straight
`mov` instructions for a swap cycle without changing any register the
continuation observes.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ParallelMoves_TwoCycle
open TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

private theorem put_apply (s : File) (d v r : Nat) :
    put s d v r = if r == d then v else s r := rfl

private theorem put_same (s : File) (d v : Nat) : put s d v d = v := by
  simp [put_apply]

private theorem put_other (s : File) (d v r : Nat) (h : r ≠ d) : put s d v r = s r := by
  simp [put_apply, h]

private def pcopy (s file : File) (u : List Move) : File :=
  u.foldl (fun acc (d, r) => put acc d (s r)) file

private theorem pcopy_cons (s file : File) (m : Move) (u : List Move) :
    pcopy s file (m :: u) = pcopy s (put file m.1 (s m.2)) u := rfl

private theorem pcopy_nil (s file : File) : pcopy s file [] = file := rfl

private theorem pcopy_eq_parallelCopy (s : File) (u : List Move) :
    parallelCopy s u = pcopy s s u := rfl

private theorem serial_nil (s : File) : serial s [] = s := rfl

private theorem serial_cons (s : File) (m : Move) (u : List Move) :
    serial s (m :: u) = serial (put s m.1 (s m.2)) u := rfl

theorem sound : TwoCycleScratchRotation.Goal := by
  intro s a b tmp hta htb r hr
  have h1 : serial s [(tmp, a), (a, b), (b, tmp)] =
      put (put (put s tmp (s a)) a (s b)) b (s a) := by
    rw [serial_cons, serial_cons, serial_cons, serial_nil,
      show ((tmp, a).1 : Nat) = tmp from rfl, show ((tmp, a).2 : Nat) = a from rfl,
      show ((a, b).1 : Nat) = a from rfl, show ((a, b).2 : Nat) = b from rfl,
      show ((b, tmp).1 : Nat) = b from rfl, show ((b, tmp).2 : Nat) = tmp from rfl,
      put_other s tmp (s a) b (Ne.symm htb),
      put_other (put s tmp (s a)) a (s b) tmp hta,
      put_same]
  have h2 : parallelCopy s [(a, b), (b, a)] = put (put s a (s b)) b (s a) := by
    rw [pcopy_eq_parallelCopy, pcopy_cons, pcopy_cons, pcopy_nil,
      show ((a, b).1 : Nat) = a from rfl, show ((a, b).2 : Nat) = b from rfl,
      show ((b, a).1 : Nat) = b from rfl, show ((b, a).2 : Nat) = a from rfl]
  rw [h1, h2]
  by_cases hrb : r = b
  · rw [hrb, put_same, put_same]
  · rw [put_other _ _ _ _ hrb, put_other _ _ _ _ hrb]
    by_cases hra : r = a
    · rw [hra, put_same, put_same]
    · rw [put_other _ _ _ _ hra, put_other _ _ _ _ hra, put_other s tmp (s a) r hr]

end TinylambOpt.ResearchProofs.JitKernels.ParallelMoves_TwoCycle

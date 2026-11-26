import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `EvenParity.SplitCode` — Encode a suffix from a restart state and shift it by the
  prefix interval.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_EvenParity_SplitCode

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.EvenParity

private theorem rank_cons (m : Machine) (q : Nat) (b : Bool) (bs : List Bool) :
    rank m q (b :: bs) = (m.step q b).bind
      (fun q' => (rank m q' bs).map
        (fun r => r + if b then lowCount m bs.length q else 0)) := by
  cases hs : m.step q b with
  | none => simp [rank, hs]
  | some q' =>
    cases hr : rank m q' bs with
    | none => simp [rank, hs, hr]
    | some r => simp [rank, hs, hr]

/-- Both `descend` and `rank` charge `lowCount` with the length of the whole
    remaining suffix, so the rank of a concatenation is the suffix rank shifted
    by the offset accumulated across the prefix for every machine. -/
private theorem splitCode_of (m : Machine) : splitCode m := by
  intro q a b q' off hdesc
  induction a generalizing q q' off with
  | nil =>
    simp [descend] at hdesc
    rcases hdesc with ⟨rfl, rfl⟩
    simp
  | cons x xs ih =>
    have hlen : (x :: xs).length + b.length = (xs.length + b.length) + 1 := by
      simp
      omega
    rw [hlen] at hdesc
    simp only [descend] at hdesc
    cases hs : m.step q x with
    | none => simp [hs] at hdesc
    | some q1 =>
      simp only [hs] at hdesc
      cases hd : descend m (xs.length + b.length) q1 xs with
      | none => simp [hd] at hdesc
      | some p =>
        rcases p with ⟨last, off1⟩
        simp [hd] at hdesc
        rcases hdesc with ⟨rfl, rfl⟩
        have ih' := ih q1 last off1 hd
        simp only [List.cons_append, rank_cons, hs, Option.bind_some,
          List.length_append, ih']
        cases hr : rank m last b with
        | none => simp
        | some rb =>
          simp only [Option.map_some]
          cases x <;>
            simp only [Bool.false_eq_true, ite_false, ite_true, Nat.add_zero,
              Option.some.injEq] <;> omega

theorem sound_SplitCode : EvenParity.SplitCode.Goal := by
  exact splitCode_of model

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_EvenParity_SplitCode

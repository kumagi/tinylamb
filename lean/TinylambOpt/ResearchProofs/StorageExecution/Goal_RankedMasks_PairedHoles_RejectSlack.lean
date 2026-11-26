import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `PairedHoles.RejectSlack` — Reject unused bit patterns in the final code word.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PairedHoles_RejectSlack

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PairedHoles

-- Truth check on small inputs (all ranks, including ranks past the code space).
private def check : Bool :=
  (List.range 8).all fun n =>
  (List.range 6).all fun q =>
  (List.range 40).all fun r =>
    decide (((unrank model n q r).isNone) = decide (count model n q ≤ r))

#eval check

private theorem bind_cons_isNone (o : Option (List Bool)) (b : Bool) :
    (o.bind fun bs => some (b :: bs)).isNone = o.isNone := by
  cases o <;> simp

private theorem count_succ (m : Machine) (n q : Nat) :
    count m (n + 1) q = lowCount m n q + ((m.step q true).map (count m n)).getD 0 := by
  rfl

private theorem unrank_succ_low (m : Machine) (n q r : Nat) (hlow : lowCount m n q ≤ r) :
    (unrank m (n + 1) q r).isNone =
      ((m.step q true).bind fun next =>
        (unrank m n next (r - lowCount m n q)).bind fun bs => some (true :: bs)).isNone := by
  simp [unrank, hlow]

private theorem unrank_succ_high (m : Machine) (n q r : Nat) (hlow : ¬ lowCount m n q ≤ r) :
    (unrank m (n + 1) q r).isNone =
      ((m.step q false).bind fun next =>
        (unrank m n next r).bind fun bs => some (false :: bs)).isNone := by
  simp [unrank, hlow]

-- The rejection property holds for *every* machine: `unrank` walks the same
-- binary trie that `count` counts, taking the high branch exactly when the
-- low branch cannot contain the requested rank.
private theorem rejectSlack_all (m : Machine) : rejectSlack m := by
  intro n
  induction n with
  | zero =>
      intro q r
      cases hacc : m.accept q <;> by_cases hr : r = 0 <;>
        simp [unrank, count, hacc, hr] <;> omega
  | succ n ih =>
      intro q r
      by_cases hlow : lowCount m n q ≤ r
      · rw [unrank_succ_low m n q r hlow, count_succ]
        cases hst : m.step q true with
        | none =>
            simp only [Option.bind_none, Option.isNone_none, Option.map_none, Option.getD_none]
            simpa using (decide_eq_true hlow)
        | some next =>
            simp [bind_cons_isNone]
            rw [ih next (r - lowCount m n q)]
            by_cases htail : count m n next ≤ r - lowCount m n q
            · have htot : lowCount m n q + count m n next ≤ r := by omega
              simp [htail, htot]
            · have htot : ¬ lowCount m n q + count m n next ≤ r := by omega
              simp [htail, htot]
      · rw [unrank_succ_high m n q r hlow, count_succ]
        cases hst : m.step q false with
        | none =>
            simp [lowCount, hst] at hlow
        | some next =>
            simp [bind_cons_isNone]
            rw [ih next r]
            have hlt : r < count m n next := by
              simpa [lowCount, hst] using Nat.lt_of_not_ge hlow
            have hleft : ¬ count m n next ≤ r := by omega
            have hloweq : lowCount m n q = count m n next := by simp [lowCount, hst]
            simp [hleft]
            omega

theorem sound_RejectSlack : PairedHoles.RejectSlack.Goal := by
  exact rejectSlack_all model

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PairedHoles_RejectSlack

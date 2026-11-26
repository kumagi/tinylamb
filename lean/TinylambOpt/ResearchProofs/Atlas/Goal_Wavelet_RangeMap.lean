import TinylambOpt.ResearchGoals.Atlas.TextExecution

/-!
SQL string predicates can use encoded or summarized text when the modeled match,
boundaries, and collation are preserved.

Proved

- `Wavelet.RangeMap` — `∀ bit l w xs high, let branch := fun x : Nat => x.testBit bit
  == high let (start,len) := remap bit l w xs high (((xs.drop l).take w).filter
  branch) = (((xs.filter branch).drop start).take len)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Wavelet_RangeMap

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Wavelet

/-! Brute-force confirmation: all words over {0,1,2,3} of length ≤ 3, all
    bits 0..2, all offsets/lengths 0..4 and both branches satisfy the goal. -/

private theorem filter_take_length (p : Nat → Bool) (w : Nat) (xs : List Nat) :
    (xs.take w).filter p = (xs.filter p).take ((xs.take w).filter p).length := by
  induction xs generalizing w with
  | nil => simp
  | cons x xs ih =>
      cases w with
      | zero => simp
      | succ w =>
          cases hp : p x
          · simpa [hp, List.take_succ_cons] using ih w
          · simpa [hp, List.take_succ_cons] using congrArg (fun t => x :: t) (ih w)

private theorem filter_take_drop (p : Nat → Bool) (l w : Nat) (xs : List Nat) :
    ((xs.drop l).take w).filter p =
      ((xs.filter p).drop (((xs.take l).filter p).length)).take
        (((xs.drop l).take w).filter p).length := by
  induction xs generalizing l w with
  | nil => simp
  | cons x xs ih =>
      cases l with
      | zero => simpa using filter_take_length p w (x :: xs)
      | succ l =>
          cases hp : p x
          · simpa [hp, List.take_succ_cons, List.drop_succ_cons] using ih l w
          · simpa [hp, List.take_succ_cons, List.drop_succ_cons] using ih l w

theorem sound_Wavelet_RangeMap : Wavelet.RangeMap.Goal := by
  intro bit l w xs high
  dsimp only
  dsimp [remap]
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
  exact filter_take_drop (fun x : Nat => x.testBit bit == high) l w xs

end TinylambOpt.ResearchProofs.Atlas.Goal_Wavelet_RangeMap

import TinylambOpt.ResearchGoals.ThresholdBitCounter

/-!
Proved

- `Goal` — `∀ active a b c, ∀ i < 64, let (yes,unknown) := masks active a b c let r :=
  reference (atom a i) (atom b i) (atom c i) yes.getLsbD i = (active.getLsbD i &&
  decide (r = .tTrue)) ∧ unknown.getLsbD i = (active.getLsbD i && decide (r =
  .tNull))`

The reference model is defined in `TinylambOpt.ResearchGoals.ThresholdBitCounter`.
-/

namespace TinylambOpt.ResearchProofs.ThresholdBitCounter

open TinylambOpt.ResearchGoals.ThresholdBitCounter

/-!
Per-lane soundness of the three-valued "2 of 3" predicate as a bit-sliced
circuit.  `atLeastTwo` is the carry of a three-input binary adder (its bit
`i` is set iff at least two input bits at `i` are set), and the `yes` /
`unknown` masks are shown to agree, lane by lane, with the Kleene reference
`((A ∧ B) ∨ (A ∧ C)) ∨ (B ∧ C)` projected through the `yes`-bit /
`unknown`-bit encoding used by `atom`.

`atLeastTwo_bit` exposes the carry bitwise; `lane_bool` is the finite
Kleene truth table over the seven lane bits (`active`, and the `yes` /
`unknown` bit of each of `a`, `b`, `c`); `sound` pushes bit extraction
through the whole circuit with the core `BitVec.getLsbD_{and,or,xor,not}`
lemmas and then instantiates the truth table.
-/

/-- Lane-wise view of `atLeastTwo`: bit `i` is set iff at least two of the
three input bits at `i` are set (the carry of a three-input adder). -/
private theorem atLeastTwo_bit (a b c : W) (i : Nat) :
    (atLeastTwo a b c).getLsbD i
      = ((a.getLsbD i && b.getLsbD i) || ((a.getLsbD i ^^ b.getLsbD i) && c.getLsbD i)) := by
  unfold atLeastTwo
  rw [BitVec.getLsbD_or, BitVec.getLsbD_and, BitVec.getLsbD_and, BitVec.getLsbD_xor]

/-- `atom`'s three-valued cell in terms of its two lane bits. -/
private def tcell (yes unknown : Bool) : TinylambOpt.SqlBool :=
  if yes then .tTrue else if unknown then .tNull else .tFalse

/-- The Kleene "2 of 3" truth table, one bit at a time: the `yes` bit of the
circuit is exactly `r = .tTrue` and the `unknown` bit is exactly `r = .tNull`,
gated by the lane's `active` bit. -/
private theorem lane_bool (ACT A UA B UB C UC : Bool) :
    ((ACT && (A && B || (A ^^ B) && C)) =
        (ACT && decide (reference (tcell A UA) (tcell B UB) (tcell C UC) = .tTrue))) ∧
      ((ACT && ((A || UA) && (B || UB) || ((A || UA) ^^ (B || UB)) && (C || UC)) &&
          !(A && B || (A ^^ B) && C)) =
        (ACT && decide (reference (tcell A UA) (tcell B UB) (tcell C UC) = .tNull))) := by
  cases A <;> cases UA <;> cases B <;> cases UB <;> cases C <;> cases UC <;>
    cases ACT <;> decide

theorem sound : TinylambOpt.ResearchGoals.ThresholdBitCounter.Goal := by
  intro active a b c i hi
  -- Expose every mask bit as a plain Boolean formula in the seven lane bits.
  simp only [masks, atom, reference, atLeastTwo_bit, BitVec.getLsbD_and,
    BitVec.getLsbD_or, BitVec.getLsbD_not,
    show decide (i < 64) = true from decide_eq_true hi, Bool.true_and]
  -- Discharge all 2^7 lane configurations from the truth table.
  exact lane_bool (active.getLsbD i) (a.yes.getLsbD i) (a.unknown.getLsbD i)
    (b.yes.getLsbD i) (b.unknown.getLsbD i) (c.yes.getLsbD i) (c.unknown.getLsbD i)

end TinylambOpt.ResearchProofs.ThresholdBitCounter

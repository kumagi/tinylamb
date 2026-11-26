import Std

/-!
These claims compare reciprocal-based arithmetic with division and remainder. A SQL
expression fast path can replace division only on the proven operand range.

Modeled claims

- `Goal` — `∀ x d, run (prepare d) x = reference x d`
-/

namespace TinylambOpt.ResearchGoals.ReciprocalDivMod

inductive Result where
  | null | zeroDivisor | overflow | pair (q r : Int)
  deriving DecidableEq

def reference (x d : Option Int64) : Result :=
  match x, d with
  | some a, some b =>
    if b.toInt = 0 then .zeroDivisor
    else if a.toInt = -(2^63) ∧ b.toInt = -1 then .overflow
    else .pair (a.toInt.tdiv b.toInt) (a.toInt.tmod b.toInt)
  | _, _ => .null

inductive Plan where
  | null | zero | unit (negative : Bool)
  | multiply (d : Int64) (reciprocal : BitVec 64)

def prepare : Option Int64 → Plan
  | none => .null
  | some d =>
    let m := d.toInt.natAbs
    if m = 0 then .zero else if m = 1 then .unit (d.toInt < 0)
    else .multiply d (BitVec.ofNat 64 ((2^64 + m - 1) / m))

def run (p : Plan) (x : Option Int64) : Result :=
  match x, p with
  | none, _ | _, .null => .null
  | some _, .zero => .zeroDivisor
  | some a, .unit neg =>
    if neg && a.toInt == -(2^63) then .overflow
    else .pair (if neg then -a.toInt else a.toInt) 0
  | some a, .multiply d multiplier =>
    let dividend := BitVec.ofNat 128 a.toInt.natAbs
    let wide := dividend * multiplier.zeroExtend 128
    let estimate := wide.extractLsb' 64 64
    let product := estimate.zeroExtend 128 * BitVec.ofNat 128 d.toInt.natAbs
    let corrected := if product > dividend then estimate - 1 else estimate
    let magnitude : Int := corrected.toNat
    let q := if (a.toInt < 0) != (d.toInt < 0) then -magnitude else magnitude
    .pair q (a.toInt - q * d.toInt)

def Goal : Prop := ∀ x d, run (prepare d) x = reference x d

example : run (prepare (some 3)) (some (-7)) = .pair (-2) (-1) := by decide
example : run (prepare (some (-1))) (some (-9223372036854775808)) = .overflow := by decide
example : run (prepare (some 0)) none = .null := by decide
example : reference (some (-7)) (some 3) = .pair (-2) (-1) := by decide

private def correctionCase := some (9223372036854775804 : Int64)
example : run (prepare (some 5)) correctionCase = .pair 1844674407370955160 4 := by decide
example : reference correctionCase (some 5) = .pair 1844674407370955160 4 := by decide

end TinylambOpt.ResearchGoals.ReciprocalDivMod

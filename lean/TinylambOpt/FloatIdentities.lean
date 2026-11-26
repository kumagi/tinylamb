import Init.Data.Rat.Lemmas

/-!
These identities and counterexamples characterize the effect of floating-point values on
scalar rewrites. An optimizer must preserve the modeled behavior at rounding and
exceptional-value boundaries.
-/

namespace TinylambOpt.FloatIdentities

/-- Values after numeric promotion to FLOAT64. A separate sign distinguishes
    the two zeros. SQL observes the NaN class, not its hardware payload. -/
inductive FpValue where
  | finite (negative : Bool) (magnitude : Rat)
  | infinity (negative : Bool)
  | nan

/-- The operand may already have failed or produced SQL NULL. Rounding an
    exact result can also fail (overflow). -/
abbrev Result (ε : Type) := Except ε (Option FpValue)

private def liftNumeric (op : FpValue → Except ε FpValue) (x : Result ε) : Result ε :=
  match x with
  | .error e => .error e
  | .ok none => .ok none
  | .ok (some v) => (op v).map some

private def divideByPositive (round : Rat → Except ε Rat) (x : FpValue) (d : Rat) :
    Except ε FpValue := do
  match x with
  | .finite sign m => return .finite sign (← round (m / d))
  | .infinity sign => return .infinity sign
  | .nan => return .nan

private def positiveIntegerPower (round : Rat → Except ε Rat) (x : FpValue)
    (exponentMinusOne : Nat) : Except ε FpValue := do
  let n := exponentMinusOne + 1
  match x with
  | .finite sign m => return .finite (sign && (n % 2 == 1)) (← round (m ^ n))
  | .infinity sign => return .infinity (sign && (n % 2 == 1))
  | .nan => return .nan

/-- An already-representable finite magnitude rounds back to itself. -/
def Representable (round : Rat → Except ε Rat) (x : Result ε) : Prop :=
  ∀ sign m, x = .ok (some (.finite sign m)) → round m = .ok m

/-- The C++ rewrite accepts a statically DOUBLE dividend. -/
theorem identity_divide_one_sound (round : Rat → Except ε Rat) (x : Result ε)
    (h : Representable round x) :
    liftNumeric (fun v => divideByPositive round v 1) x = x := by
  cases x with
  | error => rfl
  | ok x =>
    cases x with
    | none => rfl
    | some v =>
      cases v with
      | finite sign m =>
        have one : (1 : Rat)⁻¹ = 1 := Rat.inv_eq_of_mul_eq_one (Rat.mul_one 1)
        simp [liftNumeric, divideByPositive, Rat.div_def, one, h sign m rfl,
          Bind.bind, Except.bind, Except.map, pure, Except.pure]
      | infinity => rfl
      | nan => rfl

/-- Both POW and CAST use the same promotion of a statically numeric base.
    This theorem starts at its result, including NULL and incoming errors. -/
theorem pow_one_after_promotion_sound (round : Rat → Except ε Rat) (promoted : Result ε)
    (h : Representable round promoted) :
    liftNumeric (fun v => positiveIntegerPower round v 0) promoted = promoted := by
  cases promoted with
  | error => rfl
  | ok x =>
    cases x with
    | none => rfl
    | some v =>
      cases v with
      | finite sign m =>
        simp [liftNumeric, positiveIntegerPower, h sign m rfl, Bind.bind, Except.bind,
          Except.map, pure, Except.pure]
      | infinity sign => cases sign <;> rfl
      | nan => rfl

end TinylambOpt.FloatIdentities

-- C++ references to this model or its proof.
-- expression/rewrite.cpp

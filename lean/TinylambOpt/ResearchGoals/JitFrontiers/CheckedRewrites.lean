import Std

/-!
The expression compiler must preserve both the checked integer result and observable
overflow behavior.
-/

namespace TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

/-! Signed intermediate values are checked left-to-right. The site number
identifies the first observable arithmetic fault. The fast expression keeps
only one final check. Width 64 models INT64; small widths expose boundaries. -/
def limit (w : Nat) : Int := 2^(w-1)
def fits (w : Nat) (v : Int) := -limit w ≤ v && v < limit w
def check (w pc : Nat) (v : Int) : Except Nat Int :=
  if fits w v then .ok v else .error pc
structure Rule where
  intermediate : Int → Int → Int
  finish : Int → Int → Int → Int
  folded : Int → Int → Int
  guard : Nat → Int → Int → Bool
def scalar (r : Rule) (w : Nat) (x y : Int) := do
  let v ← check w 0 (r.intermediate x y)
  check w 1 (r.finish x y v)
def fast (r : Rule) (w : Nat) (x y : Int) := check w 1 (r.folded x y)
def dispatch (r : Rule) (w : Nat) (x y : Int) :=
  if r.guard w x y then fast r w x y else scalar r w x y
def safeInputs (w : Nat) (p : Int × Int) := fits w p.1 && fits w p.2
def badRow (f : Int × Int → Except Nat Int) (rs : List (Int × Int)) :=
  rs.zipIdx.findSome? (fun (p,i) => match f p with | .error pc => some (i,pc) | .ok _ => none)

/-- Algebraic replacement remains exact in the widened IR domain. -/
def foldValue (r : Rule) : Prop := ∀ x y, r.finish x y (r.intermediate x y) = r.folded x y

/-- The proposed strength-reduced guard excludes every fault of the removed instruction. -/
def sufficient (r : Rule) : Prop := ∀ w x y, 1<w → safeInputs w (x,y) → r.guard w x y → fits w (r.intermediate x y)

/-- Aggressive converse: no safe input is unnecessarily sent to the slow path. -/
def necessary (r : Rule) : Prop := ∀ w x y, 1<w → safeInputs w (x,y) → fits w (r.intermediate x y) → r.guard w x y

/-- The guarded scalar kernel returns the original site as well as the same value. -/
def firstFault (r : Rule) : Prop := ∀ w x y, 1<w → safeInputs w (x,y) → dispatch r w x y = scalar r w x y

/-- A once-per-block guard allows an entirely folded loop without changing its first failing lane. -/
def hoistGuard (r : Rule) : Prop := ∀ w rows, 1<w → rows.all (safeInputs w) →
  rows.all (fun (x,y) => r.guard w x y) →
  badRow (fun (x,y) => fast r w x y) rows = badRow (fun (x,y) => scalar r w x y) rows

/-- Four corner guard checks suffice to hoist a rectangular range check. Interior extrema stress this claim. -/
def endpointCertificate (r : Rule) : Prop := ∀ w a b c d x y, 1<w → a≤x → x≤b → c≤y → y≤d →
  safeInputs w (a,c) → safeInputs w (b,d) →
  ([a,b].all (fun u => [c,d].all (fun v => r.guard w u v))) → fits w (r.intermediate x y)

/-- A narrow version may widen its result only when both the internal and final operations are safe. -/
def narrowVersion (r : Rule) : Prop := ∀ n w x y, 1<n → n≤w → safeInputs n (x,y) → r.guard n x y →
  fits n (r.folded x y) → fast r n x y = scalar r w x y

/-- Aggressive speculation: success of the fused expression alone licenses deletion of the internal check. -/
def finalOnly (r : Rule) : Prop := ∀ w x y, 1<w → safeInputs w (x,y) → fits w (r.folded x y) →
  scalar r w x y = .ok (r.folded x y)

namespace CancelAdd

/-- Cancel (x+y)-y while preserving the first overflow site. -/
def rule : Rule where
  intermediate := fun x y => x+y
  finish := fun _ y t => t-y
  folded := fun x _ => x
  guard := fun w x y => -limit w-y≤x && x<limit w-y

def FoldValue.Goal : Prop := foldValue rule
def Sufficient.Goal : Prop := sufficient rule
def Necessary.Goal : Prop := necessary rule
def FirstFault.Goal : Prop := firstFault rule
def HoistGuard.Goal : Prop := hoistGuard rule
def EndpointCertificate.Goal : Prop := endpointCertificate rule
def NarrowVersion.Goal : Prop := narrowVersion rule
def FinalOnly.Goal : Prop := finalOnly rule

def PositiveRoom.Goal : Prop := ∀ w x y, 0≤y → fits w x → (rule.guard w x y = true ↔ y < limit w-x)

def UpperCliff.Goal : Prop := ∀ w, 1<w → scalar rule w (limit w-1) 1=.error 0

end CancelAdd

namespace CancelSubtract

/-- Simplify (x-y)+y after a signed subtraction. -/
def rule : Rule where
  intermediate := fun x y => x-y
  finish := fun _ y t => t+y
  folded := fun x _ => x
  guard := fun w x y => -limit w+y≤x && x<limit w+y

def FoldValue.Goal : Prop := foldValue rule
def Sufficient.Goal : Prop := sufficient rule
def Necessary.Goal : Prop := necessary rule
def FirstFault.Goal : Prop := firstFault rule
def HoistGuard.Goal : Prop := hoistGuard rule
def EndpointCertificate.Goal : Prop := endpointCertificate rule
def NarrowVersion.Goal : Prop := narrowVersion rule
def FinalOnly.Goal : Prop := finalOnly rule

def NegativeRoom.Goal : Prop := ∀ w x y, 0≤y → fits w x → (rule.guard w x y = true ↔ y≤x+limit w)

def LowerCliff.Goal : Prop := ∀ w, 1<w → scalar rule w (-limit w) 1=.error 0

end CancelSubtract

namespace DoubleHalf

/-- Recover a key by halving its checked double. -/
def rule : Rule where
  intermediate := fun x _ => 2*x
  finish := fun _ _ t => t/2
  folded := fun x _ => x
  guard := fun w x _ => -(limit w/2)≤x && x<limit w/2

def FoldValue.Goal : Prop := foldValue rule
def Sufficient.Goal : Prop := sufficient rule
def Necessary.Goal : Prop := necessary rule
def FirstFault.Goal : Prop := firstFault rule
def HoistGuard.Goal : Prop := hoistGuard rule
def EndpointCertificate.Goal : Prop := endpointCertificate rule
def NarrowVersion.Goal : Prop := narrowVersion rule
def FinalOnly.Goal : Prop := finalOnly rule

def SafeMiddle.Goal : Prop := ∀ w x, 1<w → (rule.guard w x 0 = true ↔ fits (w-1) x)

def LastPositive.Goal : Prop := ∀ w, 1<w → scalar rule w (limit w/2) 0=.error 0

end DoubleHalf

namespace NegateTwice

/-- Eliminate both negations except at the signed minimum. -/
def rule : Rule where
  intermediate := fun x _ => -x
  finish := fun _ _ t => -t
  folded := fun x _ => x
  guard := fun w x _ => x != -limit w

def FoldValue.Goal : Prop := foldValue rule
def Sufficient.Goal : Prop := sufficient rule
def Necessary.Goal : Prop := necessary rule
def FirstFault.Goal : Prop := firstFault rule
def HoistGuard.Goal : Prop := hoistGuard rule
def EndpointCertificate.Goal : Prop := endpointCertificate rule
def NarrowVersion.Goal : Prop := narrowVersion rule
def FinalOnly.Goal : Prop := finalOnly rule

def SingleHole.Goal : Prop := ∀ w x, fits w x → (scalar rule w x 0=.error 0 ↔ x= -limit w)

def IdentityElse.Goal : Prop := ∀ w x, fits w x → x≠ -limit w → scalar rule w x 0=.ok x

end NegateTwice

namespace DistributeScale

/-- Expand (x+y)*3 using a widened accumulator. -/
def rule : Rule where
  intermediate := fun x y => x+y
  finish := fun _ _ t => 3*t
  folded := fun x y => 3*x+3*y
  guard := fun w x y => if 0 ≤ y then x < limit w - y else -limit w - y ≤ x

def FoldValue.Goal : Prop := foldValue rule
def Sufficient.Goal : Prop := sufficient rule
def Necessary.Goal : Prop := necessary rule
def FirstFault.Goal : Prop := firstFault rule
def HoistGuard.Goal : Prop := hoistGuard rule
def EndpointCertificate.Goal : Prop := endpointCertificate rule
def NarrowVersion.Goal : Prop := narrowVersion rule
def FinalOnly.Goal : Prop := finalOnly rule

def FinalImpliesInner.Goal : Prop := ∀ w x y, fits w (3*x+3*y) → fits w (x+y)

def FinalSite.Goal : Prop := ∀ w x y, fits w (x+y) → ¬ fits w (3*(x+y)) → scalar rule w x y=.error 1

end DistributeScale

namespace ProductMinusInput

/-- Factor x*y-x as x*(y-1) while guarding the old multiply. -/
def rule : Rule where
  intermediate := fun x y => x*y
  finish := fun x _ t => t-x
  folded := fun x y => x*(y-1)
  guard := fun w x y =>
    if y == 0 then true else
      let capacity := if (x < 0) != (y < 0) then limit w else limit w - 1
      x.natAbs ≤ capacity.toNat / y.natAbs

def FoldValue.Goal : Prop := foldValue rule
def Sufficient.Goal : Prop := sufficient rule
def Necessary.Goal : Prop := necessary rule
def FirstFault.Goal : Prop := firstFault rule
def HoistGuard.Goal : Prop := hoistGuard rule
def EndpointCertificate.Goal : Prop := endpointCertificate rule
def NarrowVersion.Goal : Prop := narrowVersion rule
def FinalOnly.Goal : Prop := finalOnly rule

def UnitMultiplier.Goal : Prop := ∀ w x, fits w x → scalar rule w x 1=.ok 0

def MinusOneCliff.Goal : Prop := ∀ w, 1<w → scalar rule w (-limit w) (-1)=.error 0

end ProductMinusInput

namespace SquareDistance

/-- Square a checked difference in a widened register. -/
def rule : Rule where
  intermediate := fun x y => x-y
  finish := fun _ _ t => t*t
  folded := fun x y => x*x-2*x*y+y*y
  guard := fun w x y => if 0 ≤ y then y - limit w ≤ x else x < limit w + y

def FoldValue.Goal : Prop := foldValue rule
def Sufficient.Goal : Prop := sufficient rule
def Necessary.Goal : Prop := necessary rule
def FirstFault.Goal : Prop := firstFault rule
def HoistGuard.Goal : Prop := hoistGuard rule
def EndpointCertificate.Goal : Prop := endpointCertificate rule
def NarrowVersion.Goal : Prop := narrowVersion rule
def FinalOnly.Goal : Prop := finalOnly rule

def SymmetricSuccess.Goal : Prop := ∀ w x y v, scalar rule w x y=.ok v → scalar rule w y x=.ok v

def Diagonal.Goal : Prop := ∀ w x, scalar rule w x x=.ok 0

end SquareDistance

namespace LinearRebias

/-- Move a constant bias out of a running expression. -/
def rule : Rule where
  intermediate := fun x _ => x+7
  finish := fun _ y t => t-y
  folded := fun x y => (x-y)+7
  guard := fun w x _ => x<limit w-7

def FoldValue.Goal : Prop := foldValue rule
def Sufficient.Goal : Prop := sufficient rule
def Necessary.Goal : Prop := necessary rule
def FirstFault.Goal : Prop := firstFault rule
def HoistGuard.Goal : Prop := hoistGuard rule
def EndpointCertificate.Goal : Prop := endpointCertificate rule
def NarrowVersion.Goal : Prop := narrowVersion rule
def FinalOnly.Goal : Prop := finalOnly rule

def SevenTail.Goal : Prop := ∀ w x, fits w x → (rule.guard w x 0=false ↔ limit w-7≤x)

def CancelledBias.Goal : Prop := ∀ w, 4≤w → scalar rule w (limit w-1) 7=.error 0

end LinearRebias

namespace ScaleZero

/-- Multiplication by zero may not discard an earlier failing product. -/
def rule : Rule where
  intermediate := fun x y => x*y
  finish := fun _ _ _ => 0
  folded := fun _ _ => 0
  guard := fun w x y => x.natAbs*y.natAbs < (limit w).toNat

def FoldValue.Goal : Prop := foldValue rule
def Sufficient.Goal : Prop := sufficient rule
def Necessary.Goal : Prop := necessary rule
def FirstFault.Goal : Prop := firstFault rule
def HoistGuard.Goal : Prop := hoistGuard rule
def EndpointCertificate.Goal : Prop := endpointCertificate rule
def NarrowVersion.Goal : Prop := narrowVersion rule
def FinalOnly.Goal : Prop := finalOnly rule

def NegativeMinimum.Goal : Prop := ∀ w, 1<w → scalar rule w (-limit w) 1=.ok 0 ∧ rule.guard w (-limit w) 1=false

def WidthBound.Goal : Prop := ∀ w x y, x.natAbs*y.natAbs < (limit w).toNat → scalar rule w x y=.ok 0

end ScaleZero

namespace RecenterAbs

/-- Lower ABS(x)-y to a sign-select and a widened subtraction. -/
def rule : Rule where
  intermediate := fun x _ => x.natAbs
  finish := fun _ y t => t-y
  folded := fun x y => (if x<0 then -x else x)-y
  guard := fun w x _ => x != -limit w

def FoldValue.Goal : Prop := foldValue rule
def Sufficient.Goal : Prop := sufficient rule
def Necessary.Goal : Prop := necessary rule
def FirstFault.Goal : Prop := firstFault rule
def HoistGuard.Goal : Prop := hoistGuard rule
def EndpointCertificate.Goal : Prop := endpointCertificate rule
def NarrowVersion.Goal : Prop := narrowVersion rule
def FinalOnly.Goal : Prop := finalOnly rule

def MinFault.Goal : Prop := ∀ w y, scalar rule w (-limit w) y=.error 0

def SignPair.Goal : Prop := ∀ w x y, fits w x → fits w (-x) → scalar rule w x y=scalar rule w (-x) y

end RecenterAbs

#guard (scalar CancelAdd.rule 8 127 1 matches .error 0)
#guard (fast CancelAdd.rule 8 127 1 matches .ok 127)

end TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

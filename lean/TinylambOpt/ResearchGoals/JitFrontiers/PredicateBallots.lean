import Std

namespace TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

/-! 64-lane predicate words. The scalar reference decides whether the RHS is
demanded. The candidate circuit emits true, NULL, and fault bitmaps; false
is implicit. Inactive lanes may contain arbitrary input flags. -/
abbrev W := BitVec 64
inductive T where | yes | no | null | fault
deriving DecidableEq, BEq, Repr, Inhabited
structure Bits where
  yes : W
  nul : W
  err : W
deriving BEq, Repr
def zeros (p : Bits) := ~~~(p.yes ||| p.nul ||| p.err)
def well (p : Bits) := p.yes &&& p.nul=0 ∧ p.yes &&& p.err=0 ∧ p.nul &&& p.err=0
def gate (m : W) (p : Bits) : Bits := ⟨m &&& p.yes,m &&& p.nul,m &&& p.err⟩
def lane (p : Bits) (i : Nat) : T := if p.err.getLsbD i then .fault else
  if p.nul.getLsbD i then .null else if p.yes.getLsbD i then .yes else .no
structure Test where
  scalar : T → T → T
  emit : Bits → Bits → Bits
  demand : Bits → W
def ballot (f : Nat → Bool) : W := (List.range 64).foldl (fun w i =>
  if f i then w ||| ((1:W) <<< i) else w) 0
def rows (m : W) := (List.range 64).filter m.getLsbD
def observe (k : Test) (a b : Bits) (i : Nat) := k.scalar (lane a i) (lane b i)
def firstEvent (t : T) := t==.yes || t==.fault
def strict (op : Bool → Bool → Bool) (a b : T) := if a==.fault || b==.fault then T.fault
  else if a==.null || b==.null then .null else if op (a==.yes) (b==.yes) then .yes else .no
def strictBits (v : W) (a b : Bits) : Bits :=
  let e:=a.err ||| b.err
  let n:=(a.nul ||| b.nul) &&& ~~~e
  ⟨v &&& ~~~(n ||| e),n,e⟩
def negate (x : T) := match x with | .yes => T.no | .no => .yes | v => v
def negBits (p : Bits) : Bits := ⟨zeros p,p.nul,p.err⟩
def sqlAnd (x y : T) : T := match x with
  | .no => T.no | .fault => .fault | .yes => y
  | .null => if y==.fault then .fault else if y==.no then .no else .null
def andBits (a b : Bits) : Bits :=
  let need:=a.yes ||| a.nul
  ⟨a.yes &&& b.yes,(a.yes &&& b.nul) ||| (a.nul &&& (b.yes ||| b.nul)),a.err ||| (need &&& b.err)⟩

/-- A ballot for TRUE equals scalar three-valued execution on just the active ordinals. -/
def maskedTruth (k : Test) : Prop := ∀ m a b, well a → well b →
  (gate m (k.emit a b)).yes=ballot (fun i => m.getLsbD i && observe k a b i == .yes)

/-- A projection must preserve UNKNOWN, even when WHERE would discard it. -/
def nullOutput (k : Test) : Prop := ∀ m a b, well a → well b →
  (gate m (k.emit a b)).nul=ballot (fun i => m.getLsbD i && observe k a b i == .null)

/-- Mask exceptional RHS lanes by demand before selecting the first error. -/
def trapBallot (k : Test) : Prop := ∀ m a b, well a → well b →
  (rows (gate m (k.emit a b)).err).head?=(rows m).find? (fun i => observe k a b i == .fault)

/-- Every well-formed output lane has at most one tag. -/
def disjointOutputs (k : Test) : Prop := ∀ a b, well a → well b → well (k.emit a b)

/-- Replacing arbitrary RHS flags outside the generated demand mask leaves all live outputs unchanged. -/
def skipRhs (k : Test) : Prop := ∀ m a b c, well a → well b → well c → gate (m &&& k.demand a) b=gate (m &&& k.demand a) c →
  gate m (k.emit a b)=gate m (k.emit a c)

/-- Every requested RHS lane has two possible RHS tags that change its observable result. -/
def minimalDemand (k : Test) : Prop := ∀ a i, well a → i<64 → (k.demand a).getLsbD i=true ↔
  ∃ u v, k.scalar (lane a i) u ≠ k.scalar (lane a i) v

/-- LIMIT 1 must choose the earliest of a qualifying lane and a failing lane. -/
def limitBeforeFault (k : Test) : Prop := ∀ m a b, well a → well b → let out:=gate m (k.emit a b)
  (rows (out.yes ||| out.err)).head?=(rows m).find? (fun i => firstEvent (observe k a b i))

/-- Split active rows between two kernel calls, then combine the selection and fault flags. -/
def splitActive (k : Test) : Prop := ∀ m n a b, well a → well b →
  let u:=gate m (k.emit (gate m a) (gate m b))
  let v:=gate n (k.emit (gate n a) (gate n b))
  gate (m ||| n) (k.emit a b)=⟨u.yes ||| v.yes,u.nul ||| v.nul,u.err ||| v.err⟩

namespace Conjunction

/-- Short-circuit AND. -/
def test : Test where
  scalar := sqlAnd
  emit := andBits
  demand := fun a => a.yes ||| a.nul

def MaskedTruth.Goal : Prop := maskedTruth test
def NullOutput.Goal : Prop := nullOutput test
def TrapBallot.Goal : Prop := trapBallot test
def DisjointOutputs.Goal : Prop := disjointOutputs test
def SkipRhs.Goal : Prop := skipRhs test
def MinimalDemand.Goal : Prop := minimalDemand test
def LimitBeforeFault.Goal : Prop := limitBeforeFault test
def SplitActive.Goal : Prop := splitActive test

def ZeroLhs.Goal : Prop := ∀ b, well b → (test.emit ⟨0,0,0⟩ b).err=0

def UnknownNeedsFault.Goal : Prop := test.scalar .null .fault=.fault

end Conjunction

namespace Disjunction

/-- Implement short-circuit OR by a dual negated circuit. -/
def test : Test where
  scalar := fun x y => negate (sqlAnd (negate x) (negate y))
  emit := fun a b => negBits (andBits (negBits a) (negBits b))
  demand := fun a => zeros a ||| a.nul

def MaskedTruth.Goal : Prop := maskedTruth test
def NullOutput.Goal : Prop := nullOutput test
def TrapBallot.Goal : Prop := trapBallot test
def DisjointOutputs.Goal : Prop := disjointOutputs test
def SkipRhs.Goal : Prop := skipRhs test
def MinimalDemand.Goal : Prop := minimalDemand test
def LimitBeforeFault.Goal : Prop := limitBeforeFault test
def SplitActive.Goal : Prop := splitActive test

def TrueSuppresses.Goal : Prop := ∀ b, well b → (test.emit ⟨-1,0,0⟩ b).err=0

def UnknownHit.Goal : Prop := test.scalar .null .yes=.yes

end Disjunction

namespace ExclusiveOr

/-- Strict three-valued XOR; even NULL cannot mask RHS failure. -/
def test : Test where
  scalar := strict xor
  emit := fun a b => strictBits (a.yes ^^^ b.yes) a b
  demand := fun a => ~~~a.err

def MaskedTruth.Goal : Prop := maskedTruth test
def NullOutput.Goal : Prop := nullOutput test
def TrapBallot.Goal : Prop := trapBallot test
def DisjointOutputs.Goal : Prop := disjointOutputs test
def SkipRhs.Goal : Prop := skipRhs test
def MinimalDemand.Goal : Prop := minimalDemand test
def LimitBeforeFault.Goal : Prop := limitBeforeFault test
def SplitActive.Goal : Prop := splitActive test

def Parity.Goal : Prop := ∀ a b, a.nul=0 → b.nul=0 → a.err=0 → b.err=0 → (test.emit a b).yes=a.yes ^^^ b.yes

def EagerNull.Goal : Prop := test.scalar .null .fault=.fault

end ExclusiveOr

namespace BoolEqual

/-- Nullable equality via the complement of XOR. -/
def test : Test where
  scalar := strict (· == ·)
  emit := fun a b => strictBits (~~~(a.yes ^^^ b.yes)) a b
  demand := fun a => ~~~a.err

def MaskedTruth.Goal : Prop := maskedTruth test
def NullOutput.Goal : Prop := nullOutput test
def TrapBallot.Goal : Prop := trapBallot test
def DisjointOutputs.Goal : Prop := disjointOutputs test
def SkipRhs.Goal : Prop := skipRhs test
def MinimalDemand.Goal : Prop := minimalDemand test
def LimitBeforeFault.Goal : Prop := limitBeforeFault test
def SplitActive.Goal : Prop := splitActive test

def SelfCompare.Goal : Prop := ∀ a, well a → (test.emit a a).yes = ~~~(a.nul ||| a.err)

def NullEqualsNull.Goal : Prop := test.scalar .null .null=.null

end BoolEqual

namespace Coalesce

/-- Nullable COALESCE only reads missing left values. -/
def test : Test where
  scalar := fun x y => if x==.null then y else x
  emit := fun a b => ⟨a.yes ||| (a.nul &&& b.yes),a.nul &&& b.nul,a.err ||| (a.nul &&& b.err)⟩
  demand := Bits.nul

def MaskedTruth.Goal : Prop := maskedTruth test
def NullOutput.Goal : Prop := nullOutput test
def TrapBallot.Goal : Prop := trapBallot test
def DisjointOutputs.Goal : Prop := disjointOutputs test
def SkipRhs.Goal : Prop := skipRhs test
def MinimalDemand.Goal : Prop := minimalDemand test
def LimitBeforeFault.Goal : Prop := limitBeforeFault test
def SplitActive.Goal : Prop := splitActive test

def TotalDefault.Goal : Prop := ∀ a b, b.nul=0 → (test.emit a b).nul=0

def StickyFault.Goal : Prop := ∀ y, test.scalar .fault y=.fault

end Coalesce

namespace NullSafeEqual

/-- Equality treating two NULL values as a match. -/
def test : Test where
  scalar := fun x y => if x==.fault || y==.fault then .fault else if x==y then .yes else .no
  emit := fun a b => ⟨(a.yes &&& b.yes) ||| (zeros a &&& zeros b) ||| (a.nul &&& b.nul),0,a.err ||| b.err⟩
  demand := fun a => ~~~a.err

def MaskedTruth.Goal : Prop := maskedTruth test
def NullOutput.Goal : Prop := nullOutput test
def TrapBallot.Goal : Prop := trapBallot test
def DisjointOutputs.Goal : Prop := disjointOutputs test
def SkipRhs.Goal : Prop := skipRhs test
def MinimalDemand.Goal : Prop := minimalDemand test
def LimitBeforeFault.Goal : Prop := limitBeforeFault test
def SplitActive.Goal : Prop := splitActive test

def NoUnknown.Goal : Prop := ∀ a b, (test.emit a b).nul=0

def EqualNull.Goal : Prop := test.scalar .null .null=.yes

end NullSafeEqual

namespace ConditionalNull

/-- CASE WHEN lhs THEN rhs ELSE NULL. -/
def test : Test where
  scalar := fun x y => if x==.fault then .fault else if x==.yes then y else .null
  emit := fun a b => ⟨a.yes &&& b.yes,zeros a ||| a.nul ||| (a.yes &&& b.nul),a.err ||| (a.yes &&& b.err)⟩
  demand := Bits.yes

def MaskedTruth.Goal : Prop := maskedTruth test
def NullOutput.Goal : Prop := nullOutput test
def TrapBallot.Goal : Prop := trapBallot test
def DisjointOutputs.Goal : Prop := disjointOutputs test
def SkipRhs.Goal : Prop := skipRhs test
def MinimalDemand.Goal : Prop := minimalDemand test
def LimitBeforeFault.Goal : Prop := limitBeforeFault test
def SplitActive.Goal : Prop := splitActive test

def UnknownSuppresses.Goal : Prop := test.scalar .null .fault=.null

def AllRejected.Goal : Prop := ∀ b, (test.emit ⟨0,0,0⟩ b).nul= -1

end ConditionalNull

namespace AntiPredicate

/-- Left condition with negated or failing exclusion predicate. -/
def test : Test where
  scalar := fun x y => sqlAnd x (negate y)
  emit := fun a b => andBits a (negBits b)
  demand := fun a => a.yes ||| a.nul

def MaskedTruth.Goal : Prop := maskedTruth test
def NullOutput.Goal : Prop := nullOutput test
def TrapBallot.Goal : Prop := trapBallot test
def DisjointOutputs.Goal : Prop := disjointOutputs test
def SkipRhs.Goal : Prop := skipRhs test
def MinimalDemand.Goal : Prop := minimalDemand test
def LimitBeforeFault.Goal : Prop := limitBeforeFault test
def SplitActive.Goal : Prop := splitActive test

def MaskDifference.Goal : Prop := ∀ a b, well a → well b → (test.emit a b).yes=a.yes &&& zeros b

def RejectHit.Goal : Prop := test.scalar .null .yes=.no

end AntiPredicate

namespace Implication

/-- Compile NOT a OR b with short-circuited RHS. -/
def test : Test where
  scalar := fun x y => negate (sqlAnd x (negate y))
  emit := fun a b => negBits (andBits a (negBits b))
  demand := fun a => a.yes ||| a.nul

def MaskedTruth.Goal : Prop := maskedTruth test
def NullOutput.Goal : Prop := nullOutput test
def TrapBallot.Goal : Prop := trapBallot test
def DisjointOutputs.Goal : Prop := disjointOutputs test
def SkipRhs.Goal : Prop := skipRhs test
def MinimalDemand.Goal : Prop := minimalDemand test
def LimitBeforeFault.Goal : Prop := limitBeforeFault test
def SplitActive.Goal : Prop := splitActive test

def FalsePremise.Goal : Prop := ∀ rhs, test.scalar .no rhs=.yes

def FaultPremise.Goal : Prop := ∀ rhs, test.scalar .fault rhs=.fault

end Implication

namespace IfMissingTest

/-- If the left slot is missing, test whether the RHS is present. -/
def test : Test where
  scalar := fun x y => if x==.fault then .fault else if x != .null then .no else if y==.fault then .fault else if y==.null then .no else .yes
  emit := fun a b => ⟨a.nul &&& ~~~(b.nul ||| b.err),0,a.err ||| (a.nul &&& b.err)⟩
  demand := Bits.nul

def MaskedTruth.Goal : Prop := maskedTruth test
def NullOutput.Goal : Prop := nullOutput test
def TrapBallot.Goal : Prop := trapBallot test
def DisjointOutputs.Goal : Prop := disjointOutputs test
def SkipRhs.Goal : Prop := skipRhs test
def MinimalDemand.Goal : Prop := minimalDemand test
def LimitBeforeFault.Goal : Prop := limitBeforeFault test
def SplitActive.Goal : Prop := splitActive test

def MaskNonNull.Goal : Prop := ∀ a b, (test.emit a b).yes &&& b.nul=0

def TwoMissing.Goal : Prop := test.scalar .null .null=.no

end IfMissingTest

#guard sqlAnd .null .fault == .fault
#guard (Conjunction.test.emit ⟨1,2,0⟩ ⟨0,0,7⟩).err == 3

end TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

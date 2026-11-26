import TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

/-!
A JIT can use certified operand and difference bounds to choose guards, fold
comparisons, or eliminate unreachable paths for active rows.
-/

namespace TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards

open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)
/-! A relational JIT guard records x, y and x-y ranges on an active CFG edge.
Candidate extrema use polygon vertices and optional transition diagonals. The
finite point scan is a specification for tightness, not a run-time guard. -/
structure Zone where
  xl : Int
  xh : Int
  yl : Int
  yh : Int
  dl : Int
  dh : Int
deriving Repr
abbrev Point := Int × Int
def inZone (z : Zone) (p : Point) :=
  z.xl≤p.1 && p.1≤z.xh && z.yl≤p.2 && p.2≤z.yh && z.dl≤p.1-p.2 && p.1-p.2≤z.dh
def interval (a b : Int) := (List.range (b-a+1).toNat).map (fun (i : Nat) => a+(i:Int))
def points (z : Zone) := ((interval z.xl z.xh).flatMap (fun x => (interval z.yl z.yh).map (x,·))).filter (inZone z)
def vertices (z : Zone) (ds : List Int) :=
  let xs := [z.xl,z.xh]
  let ys := [z.yl,z.yh]
  let diffs := z.dl::z.dh::ds
  ((xs.flatMap (fun x => ys.map (x,·))) ++
   (xs.flatMap (fun x => diffs.map (fun d => (x,x-d)))) ++
   (ys.flatMap (fun y => diffs.map (fun d => (y+d,y))))).filter (inZone z)
structure Transfer where
  score : Int → Point → Int
  cuts : Int → List Int
def bounds (vs : List Int) : Option (Int × Int) := do return (← vs.min?, ← vs.max?)
def estimate (f : Transfer) (t : Int) (z : Zone) := bounds ((vertices z (f.cuts t)).map (f.score t))
def extrema (f : Transfer) (t : Int) (z : Zone) := bounds ((points z).map (f.score t))
def split (z : Zone) (d : Int) := ({z with dh:=min z.dh d},{z with dl:=max z.dl (d+1)})
def join (z v : Zone) : Zone := ⟨min z.xl v.xl,max z.xh v.xh,min z.yl v.yl,max z.yh v.yh,min z.dl v.dl,max z.dh v.dh⟩

/-- Can the JIT compute the exact extrema without enumerating rows? -/
def minimalHull (f : Transfer) : Prop := ∀ t z, estimate f t z=extrema f t z

/-- Use only extrema to omit an overflow intrinsic on active rows. -/
def omitOverflow (f : Transfer) : Prop := ∀ t z w lo hi p, estimate f t z=some (lo,hi) → fits w lo → fits w hi →
  inZone z p → fits w (f.score t p)

/-- An absent candidate eliminates a bytecode edge only if it is actually unreachable. -/
def deadEdge (f : Transfer) : Prop := ∀ t z, estimate f t z=none ↔ ∀ p, inZone z p=false

/-- A certified upper bound removes a generated comparison against a parameter. -/
def foldCompare (f : Transfer) : Prop := ∀ t z lo hi lit p, estimate f t z=some (lo,hi) → hi<lit →
  inZone z p → f.score t p<lit

/-- Refining the two sides of a difference predicate must cover the original extrema exactly. -/
def branchSplit (f : Transfer) : Prop := ∀ z t cut,
  let (a,b):=split z cut
  let es:=[estimate f t a,estimate f t b].filterMap id
  estimate f t z=bounds (es.flatMap (fun (l,h) => [l,h]))

/-- The merged CFG fact must cover every value from the hot predecessor. -/
def phiHull (f : Transfer) : Prop := ∀ t a b l h x y, estimate f t (join a b)=some (l,h) →
  inZone a (x,y) → l≤f.score t (x,y) ∧ f.score t (x,y)≤h

/-- Strong hypothesis: checking just the upper extremum also licenses a signed no-wrap claim. -/
def removeLower (f : Transfer) : Prop := ∀ z t l h w p, estimate f t z=some (l,h) → fits w h →
  inZone z p → fits w (f.score t p)

/-- Failure of the extrema guard has an actual row witness in the modeled domain. -/
def guardNecessity (f : Transfer) : Prop := ∀ z t l h w, estimate f t z=some (l,h) → (!(fits w l) || !(fits w h)) →
  ∃ p, inZone z p=true ∧ fits w (f.score t p)=false

namespace CorrelatedSub

/-- Subtract columns tied by a delta guard. -/
def expr : Transfer := ⟨fun _ (x,y) => x-y, fun _ => []⟩

def MinimalHull.Goal : Prop := minimalHull expr
def OmitOverflow.Goal : Prop := omitOverflow expr
def DeadEdge.Goal : Prop := deadEdge expr
def FoldCompare.Goal : Prop := foldCompare expr
def BranchSplit.Goal : Prop := branchSplit expr
def PhiHull.Goal : Prop := phiHull expr
def RemoveLower.Goal : Prop := removeLower expr
def GuardNecessity.Goal : Prop := guardNecessity expr

def ExactDelta.Goal : Prop := ∀ z l h, estimate expr 0 z=some (l,h) → l=max z.dl (z.xl-z.yh) ∧ h=min z.dh (z.xh-z.yl)

def CancelEqual.Goal : Prop := ∀ z p, inZone z p → z.dl=0 → z.dh=0 → expr.score 0 p=0

end CorrelatedSub

namespace ScaledDifference

/-- A possibly negative bound parameter changes the monotonic direction. -/
def expr : Transfer := ⟨fun t (x,y) => t*(x-y), fun _ => []⟩

def MinimalHull.Goal : Prop := minimalHull expr
def OmitOverflow.Goal : Prop := omitOverflow expr
def DeadEdge.Goal : Prop := deadEdge expr
def FoldCompare.Goal : Prop := foldCompare expr
def BranchSplit.Goal : Prop := branchSplit expr
def PhiHull.Goal : Prop := phiHull expr
def RemoveLower.Goal : Prop := removeLower expr
def GuardNecessity.Goal : Prop := guardNecessity expr

def NegativeScale.Goal : Prop := ∀ z t l h, t<0 → estimate CorrelatedSub.expr 0 z=some (l,h) → estimate expr t z=some (t*h,t*l)

def ConstantScale.Goal : Prop := ∀ p, expr.score 0 p=0

end ScaledDifference

namespace DoubleMinus

/-- The fused arithmetic x+(x-y) shares correlated operands. -/
def expr : Transfer := ⟨fun t (x,y) => 2*x-y+t, fun _ => []⟩

def MinimalHull.Goal : Prop := minimalHull expr
def OmitOverflow.Goal : Prop := omitOverflow expr
def DeadEdge.Goal : Prop := deadEdge expr
def FoldCompare.Goal : Prop := foldCompare expr
def BranchSplit.Goal : Prop := branchSplit expr
def PhiHull.Goal : Prop := phiHull expr
def RemoveLower.Goal : Prop := removeLower expr
def GuardNecessity.Goal : Prop := guardNecessity expr

def Translation.Goal : Prop := ∀ t x y d, expr.score t (x+d,y+d)=expr.score t (x,y)+d

def BiasFold.Goal : Prop := ∀ z t l h, estimate expr 0 z=some (l,h) → estimate expr t z=some (l+t,h+t)

end DoubleMinus

namespace WeightedMix

/-- Affine parameter times x, residual weight times y. -/
def expr : Transfer := ⟨fun t (x,y) => t*x+(1-t)*y, fun _ => []⟩

def MinimalHull.Goal : Prop := minimalHull expr
def OmitOverflow.Goal : Prop := omitOverflow expr
def DeadEdge.Goal : Prop := deadEdge expr
def FoldCompare.Goal : Prop := foldCompare expr
def BranchSplit.Goal : Prop := branchSplit expr
def PhiHull.Goal : Prop := phiHull expr
def RemoveLower.Goal : Prop := removeLower expr
def GuardNecessity.Goal : Prop := guardNecessity expr

def DiagonalValue.Goal : Prop := ∀ t x, expr.score t (x,x)=x

def SwapWeight.Goal : Prop := ∀ t x y, expr.score t (x,y)=expr.score (1-t) (y,x)

end WeightedMix

namespace AbsLag

/-- Interior minimum on x-y=t; insert that diagonal. -/
def expr : Transfer := ⟨fun t (x,y) => (x-y-t).natAbs, fun t => [t]⟩

def MinimalHull.Goal : Prop := minimalHull expr
def OmitOverflow.Goal : Prop := omitOverflow expr
def DeadEdge.Goal : Prop := deadEdge expr
def FoldCompare.Goal : Prop := foldCompare expr
def BranchSplit.Goal : Prop := branchSplit expr
def PhiHull.Goal : Prop := phiHull expr
def RemoveLower.Goal : Prop := removeLower expr
def GuardNecessity.Goal : Prop := guardNecessity expr

def ZeroPossible.Goal : Prop := ∀ t z hi, estimate expr t z=some (0,hi) ↔ (∃ p, inZone z p=true ∧ p.1-p.2=t) ∧ (estimate expr t z).map Prod.snd=some hi

def NoNegative.Goal : Prop := ∀ t p, 0≤expr.score t p

end AbsLag

namespace SquareLag

/-- The square can have an interior minimum and a boundary maximum. -/
def expr : Transfer := ⟨fun t (x,y) => (x-y-t)^2, fun t => [t]⟩

def MinimalHull.Goal : Prop := minimalHull expr
def OmitOverflow.Goal : Prop := omitOverflow expr
def DeadEdge.Goal : Prop := deadEdge expr
def FoldCompare.Goal : Prop := foldCompare expr
def BranchSplit.Goal : Prop := branchSplit expr
def PhiHull.Goal : Prop := phiHull expr
def RemoveLower.Goal : Prop := removeLower expr
def GuardNecessity.Goal : Prop := guardNecessity expr

def SquareHull.Goal : Prop := ∀ z t l h, estimate AbsLag.expr t z=some (l,h) → estimate expr t z=some (l*l,h*h)

def EqualDistance.Goal : Prop := ∀ t x y k, expr.score t (x+k,y+k)=expr.score t (x,y)

end SquareLag

namespace PositiveExcess

/-- Branchless max(0,x-y-t) after min/max propagation. -/
def expr : Transfer := ⟨fun t (x,y) => max 0 (x-y-t), fun t => [t]⟩

def MinimalHull.Goal : Prop := minimalHull expr
def OmitOverflow.Goal : Prop := omitOverflow expr
def DeadEdge.Goal : Prop := deadEdge expr
def FoldCompare.Goal : Prop := foldCompare expr
def BranchSplit.Goal : Prop := branchSplit expr
def PhiHull.Goal : Prop := phiHull expr
def RemoveLower.Goal : Prop := removeLower expr
def GuardNecessity.Goal : Prop := guardNecessity expr

def InactiveZone.Goal : Prop := ∀ z t l h, z.dh≤t → estimate expr t z=some (l,h) → l=0 ∧ h=0

def UpperOnlyExact.Goal : Prop := ∀ t w p, fits w (expr.score t p)=true ↔ expr.score t p<limit w

end PositiveExcess

namespace LeastShift

/-- Model LEAST(x+t,y) with its diagonal switch. -/
def expr : Transfer := ⟨fun t (x,y) => min (x+t) y, fun t => [-t]⟩

def MinimalHull.Goal : Prop := minimalHull expr
def OmitOverflow.Goal : Prop := omitOverflow expr
def DeadEdge.Goal : Prop := deadEdge expr
def FoldCompare.Goal : Prop := foldCompare expr
def BranchSplit.Goal : Prop := branchSplit expr
def PhiHull.Goal : Prop := phiHull expr
def RemoveLower.Goal : Prop := removeLower expr
def GuardNecessity.Goal : Prop := guardNecessity expr

def LeftBranch.Goal : Prop := ∀ z t p, z.dh≤ -t → inZone z p → expr.score t p=p.1+t

def RightBranch.Goal : Prop := ∀ z t p, -t≤z.dl → inZone z p → expr.score t p=p.2

end LeastShift

namespace ClampedDiff

/-- Two transition diagonals for clamping a difference. -/
def expr : Transfer := ⟨fun t (x,y) => max (-t) (min t (x-y)), fun t => [-t,t]⟩

def MinimalHull.Goal : Prop := minimalHull expr
def OmitOverflow.Goal : Prop := omitOverflow expr
def DeadEdge.Goal : Prop := deadEdge expr
def FoldCompare.Goal : Prop := foldCompare expr
def BranchSplit.Goal : Prop := branchSplit expr
def PhiHull.Goal : Prop := phiHull expr
def RemoveLower.Goal : Prop := removeLower expr
def GuardNecessity.Goal : Prop := guardNecessity expr

def Saturation.Goal : Prop := ∀ z t p, 0≤t → t≤z.dl → inZone z p → expr.score t p=t

def FixedWidth.Goal : Prop := ∀ t w p, 0≤t → fits w t → fits w (expr.score t p)

end ClampedDiff

namespace ProductCorners

/-- Speculative extension from piecewise-affine to bilinear products on diagonal edges. -/
def expr : Transfer := ⟨fun _ (x,y) => x*y, fun _ => [0]⟩

def MinimalHull.Goal : Prop := minimalHull expr
def OmitOverflow.Goal : Prop := omitOverflow expr
def DeadEdge.Goal : Prop := deadEdge expr
def FoldCompare.Goal : Prop := foldCompare expr
def BranchSplit.Goal : Prop := branchSplit expr
def PhiHull.Goal : Prop := phiHull expr
def RemoveLower.Goal : Prop := removeLower expr
def GuardNecessity.Goal : Prop := guardNecessity expr

def AxisRectangle.Goal : Prop := ∀ z, z.dl≤z.xl-z.yh → z.xh-z.yl≤z.dh → estimate expr 0 z=extrema expr 0 z

def InteriorWitness.Goal : Prop := ∃ z, estimate expr 0 z ≠ extrema expr 0 z

end ProductCorners

private def diagonal : Zone := ⟨0,4,-4,0,4,4⟩
#guard estimate ProductCorners.expr 0 diagonal == some (0,0)
#guard extrema ProductCorners.expr 0 diagonal == some (-4,0)

end TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards

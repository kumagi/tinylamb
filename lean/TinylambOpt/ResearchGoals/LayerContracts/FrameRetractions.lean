import Std

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.
-/

namespace TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

/-! Retracting one *present* non-NULL input from a window aggregate. The fast
path sees only the previous finalized scalar, the non-NULL population and the
outgoing value. An outer none requests a rescan; the inner none is SQL NULL.
Numbers are exact nonnegative integers in this model. -/
structure Model where
  empty : Option Nat
  result : List Nat → Nat
  remove : Nat → Nat → Nat → Option Nat
  combine : Nat → Nat → Nat → Nat → Option Nat
def value (m : Model) (xs : List Nat) := if xs.isEmpty then m.empty else some (m.result xs)
def dropOne (m : Model) (xs : List Nat) (x : Nat) : Option (Option Nat) :=
  if xs.length == 1 then some m.empty else (m.remove xs.length (m.result xs) x).map some
def merged (m : Model) (a b : List Nat) : Option (Option Nat) :=
  if a.isEmpty then some (value m b) else if b.isEmpty then some (value m a)
  else (m.combine a.length (m.result a) b.length (m.result b)).map some
def repaired (m : Model) (xs : List Nat) (x : Nat) :=
  match dropOne m xs x with
  | some out => out
  | none => value m (xs.erase x)
def lowerMedian (xs : List Nat) := xs.mergeSort[(xs.length-1)/2]?.getD 0

/-- Every accepted constant-state deletion agrees with recomputing the remaining bag. -/
def fastRetraction (m : Model) : Prop := ∀ xs x out, x ∈ xs → dropOne m xs x = some out → value m (xs.erase x) = out

/-- A rejected deletion has indistinguishable old summaries requiring different results. -/
def fallbackNecessary (m : Model) : Prop := ∀ xs x, x ∈ xs → dropOne m xs x = none → ∃ ys, x ∈ ys ∧ ys.length=xs.length ∧
  value m ys=value m xs ∧ value m (ys.erase x) ≠ value m (xs.erase x)

/-- Can a nullable frame pass physical length instead of non-NULL population to the inverse? -/
def physicalCount (m : Model) : Prop := ∀ (rows : List (Option Nat)) x out, some x ∈ rows → 1 < rows.length →
  m.remove rows.length (m.result (rows.filterMap id)) x = some out →
  value m ((rows.erase (some x)).filterMap id) = some out

/-- An admitted merge reconstructs the final scalar without retaining input rows. -/
def combineFrames (m : Model) : Prop := ∀ a b out, merged m a b = some out → value m (a++b) = out

/-- Repaired deletions in either order agree, including repeated outgoing values. -/
def twoEvictions (m : Model) : Prop := ∀ (xs : List Nat) x y, x ∈ xs → y ∈ xs.erase x →
  repaired m (xs.erase x) y = repaired m (xs.erase y) x

/-- Changing input order cannot change the output or the choice to rescan. -/
def orderIndependent (m : Model) : Prop := ∀ a b x, a.Perm b → value m a = value m b ∧ dropOne m a x = dropOne m b x

/-- Does a finalized scalar and non-NULL count determine every single-row retraction? -/
def alwaysInvertible (m : Model) : Prop := ∀ xs ys x, x ∈ xs → x ∈ ys → xs.length=ys.length → value m xs=value m ys →
  value m (xs.erase x) = value m (ys.erase x)

/-- Can an executor collapse a duplicated whole peer frame before finalizing? -/
def duplicateFrame (m : Model) : Prop := ∀ xs, value m (xs++xs) = value m xs

namespace Sum

/-- Checked-width concerns are excluded; count disambiguates NULL and zero. -/
def model : Model :=
  ⟨none, List.sum,
   fun _ v x => some (v-x),
   fun _ v _ w => some (v+w)⟩

def FastRetraction.Goal : Prop := fastRetraction model
def FallbackNecessary.Goal : Prop := fallbackNecessary model
def PhysicalCount.Goal : Prop := physicalCount model
def CombineFrames.Goal : Prop := combineFrames model
def TwoEvictions.Goal : Prop := twoEvictions model
def OrderIndependent.Goal : Prop := orderIndependent model
def AlwaysInvertible.Goal : Prop := alwaysInvertible model
def DuplicateFrame.Goal : Prop := duplicateFrame model

def RemoveZero.Goal : Prop := ∀ xs, 0 ∈ xs → 1 < xs.length → repaired model xs 0 = value model xs

def PairSum.Goal : Prop := ∀ x y, value model [x,y] = some (x+y)

end Sum

namespace Product

/-- Nonzero outgoing factors can be divided out exactly. -/
def model : Model :=
  ⟨none, fun xs => xs.foldl (·*·) 1,
   fun _ v x => if x == 0 then none else some (v/x),
   fun _ v _ w => some (v*w)⟩

def FastRetraction.Goal : Prop := fastRetraction model
def FallbackNecessary.Goal : Prop := fallbackNecessary model
def PhysicalCount.Goal : Prop := physicalCount model
def CombineFrames.Goal : Prop := combineFrames model
def TwoEvictions.Goal : Prop := twoEvictions model
def OrderIndependent.Goal : Prop := orderIndependent model
def AlwaysInvertible.Goal : Prop := alwaysInvertible model
def DuplicateFrame.Goal : Prop := duplicateFrame model

def OtherZero.Goal : Prop := ∀ xs x, 0 ∈ xs → 0 < x → x ∈ xs → repaired model xs x = some 0

def LastZero.Goal : Prop := ∀ n, repaired model [0,n] 0 = some n

end Product

namespace Minimum

/-- Keep the minimum if the expired value was greater. -/
def model : Model :=
  ⟨none, fun xs => xs.min?.getD 0,
   fun _ v x => if v<x then some v else none,
   fun _ v _ w => some (min v w)⟩

def FastRetraction.Goal : Prop := fastRetraction model
def FallbackNecessary.Goal : Prop := fallbackNecessary model
def PhysicalCount.Goal : Prop := physicalCount model
def CombineFrames.Goal : Prop := combineFrames model
def TwoEvictions.Goal : Prop := twoEvictions model
def OrderIndependent.Goal : Prop := orderIndependent model
def AlwaysInvertible.Goal : Prop := alwaysInvertible model
def DuplicateFrame.Goal : Prop := duplicateFrame model

def RepeatedMin.Goal : Prop := ∀ x y, x ≤ y → repaired model [x,x,y] x = some x

def DropUniqueMin.Goal : Prop := ∀ x y, x < y → repaired model [x,y] x = some y

end Minimum

namespace Maximum

/-- Keep the maximum if the expired value was smaller. -/
def model : Model :=
  ⟨none, fun xs => xs.max?.getD 0,
   fun _ v x => if x<v then some v else none,
   fun _ v _ w => some (max v w)⟩

def FastRetraction.Goal : Prop := fastRetraction model
def FallbackNecessary.Goal : Prop := fallbackNecessary model
def PhysicalCount.Goal : Prop := physicalCount model
def CombineFrames.Goal : Prop := combineFrames model
def TwoEvictions.Goal : Prop := twoEvictions model
def OrderIndependent.Goal : Prop := orderIndependent model
def AlwaysInvertible.Goal : Prop := alwaysInvertible model
def DuplicateFrame.Goal : Prop := duplicateFrame model

def RepeatedMax.Goal : Prop := ∀ x y, x ≤ y → repaired model [x,y,y] y = some y

def DropUniqueMax.Goal : Prop := ∀ x y, x < y → repaired model [x,y] y = some x

end Maximum

namespace BitUnion

/-- OR cannot identify which row supplied the last set bit. -/
def model : Model :=
  ⟨none, fun xs => xs.foldl (· ||| ·) 0,
   fun _ v x => if x == 0 || v == 0 then some v else none,
   fun _ v _ w => some (v ||| w)⟩

def FastRetraction.Goal : Prop := fastRetraction model
def FallbackNecessary.Goal : Prop := fallbackNecessary model
def PhysicalCount.Goal : Prop := physicalCount model
def CombineFrames.Goal : Prop := combineFrames model
def TwoEvictions.Goal : Prop := twoEvictions model
def OrderIndependent.Goal : Prop := orderIndependent model
def AlwaysInvertible.Goal : Prop := alwaysInvertible model
def DuplicateFrame.Goal : Prop := duplicateFrame model

def UnsupportedBit.Goal : Prop := ∀ xs x i, x ∈ xs → ((xs.erase x).all (fun y => !y.testBit i)) → ((repaired model xs x).getD 0).testBit i = false

def SingleSource.Goal : Prop := ∀ x, 0 < x → repaired model [x,0] x = some 0

end BitUnion

namespace BitParity

/-- XOR removes an element by applying the same mask. -/
def model : Model :=
  ⟨none, fun xs => xs.foldl (· ^^^ ·) 0,
   fun _ v x => some (v ^^^ x),
   fun _ v _ w => some (v ^^^ w)⟩

def FastRetraction.Goal : Prop := fastRetraction model
def FallbackNecessary.Goal : Prop := fallbackNecessary model
def PhysicalCount.Goal : Prop := physicalCount model
def CombineFrames.Goal : Prop := combineFrames model
def TwoEvictions.Goal : Prop := twoEvictions model
def OrderIndependent.Goal : Prop := orderIndependent model
def AlwaysInvertible.Goal : Prop := alwaysInvertible model
def DuplicateFrame.Goal : Prop := duplicateFrame model

def TwinCancel.Goal : Prop := ∀ xs x, model.result (x::x::xs) = model.result xs

def DoubleFrame.Goal : Prop := ∀ x xs, value model ((x::xs)++(x::xs)) = some 0

end BitParity

namespace DistinctCount

/-- A single distinct group survives eviction unless the whole frame expires. -/
def model : Model :=
  ⟨some 0, fun xs => xs.eraseDups.length,
   fun n v _ => if v == 1 then some 1 else if v == n then some (n-1) else none,
   fun _ v _ w => some (v+w)⟩

def FastRetraction.Goal : Prop := fastRetraction model
def FallbackNecessary.Goal : Prop := fallbackNecessary model
def PhysicalCount.Goal : Prop := physicalCount model
def CombineFrames.Goal : Prop := combineFrames model
def TwoEvictions.Goal : Prop := twoEvictions model
def OrderIndependent.Goal : Prop := orderIndependent model
def AlwaysInvertible.Goal : Prop := alwaysInvertible model
def DuplicateFrame.Goal : Prop := duplicateFrame model

def LastOccurrence.Goal : Prop := ∀ xs x, xs.count x = 1 → (repaired model xs x).getD 0 + 1 = model.result xs

def Duplicate.Goal : Prop := ∀ xs x, 1 < xs.count x → repaired model xs x = value model xs

end DistinctCount

namespace FloorMean

/-- The missing integer remainder makes arithmetic on finalized means speculative. -/
def model : Model :=
  ⟨none, fun xs => xs.sum/xs.length,
   fun n v x => some ((n*v-x)/(n-1)),
   fun n v k w => some ((n*v+k*w)/(n+k))⟩

def FastRetraction.Goal : Prop := fastRetraction model
def FallbackNecessary.Goal : Prop := fallbackNecessary model
def PhysicalCount.Goal : Prop := physicalCount model
def CombineFrames.Goal : Prop := combineFrames model
def TwoEvictions.Goal : Prop := twoEvictions model
def OrderIndependent.Goal : Prop := orderIndependent model
def AlwaysInvertible.Goal : Prop := alwaysInvertible model
def DuplicateFrame.Goal : Prop := duplicateFrame model

def ExactDividend.Goal : Prop := ∀ xs x, x ∈ xs → xs.sum%xs.length=0 → repaired model xs x = value model (xs.erase x)

def Singleton.Goal : Prop := ∀ x, value model [x] = some x

end FloorMean

namespace RangeWidth

/-- Keep the old width only if the population is known constant. -/
def model : Model :=
  ⟨none, fun xs => xs.max?.getD 0-xs.min?.getD 0,
   fun _ v _ => if v==0 then some 0 else none,
   fun _ v _ w => if v==0 && w==0 then some 0 else none⟩

def FastRetraction.Goal : Prop := fastRetraction model
def FallbackNecessary.Goal : Prop := fallbackNecessary model
def PhysicalCount.Goal : Prop := physicalCount model
def CombineFrames.Goal : Prop := combineFrames model
def TwoEvictions.Goal : Prop := twoEvictions model
def OrderIndependent.Goal : Prop := orderIndependent model
def AlwaysInvertible.Goal : Prop := alwaysInvertible model
def DuplicateFrame.Goal : Prop := duplicateFrame model

def Translate.Goal : Prop := ∀ xs k, value model (xs.map (·+k)) = value model xs

def DifferentConstants.Goal : Prop := ∀ x y, x<y → value model [x,y] = some (y-x)

end RangeWidth

namespace LowerMedian

/-- Removing the lower median may need its successor or predecessor. -/
def model : Model :=
  ⟨none, lowerMedian,
   fun n v x => if n%2==0 && x>v then some v else if n%2==1 && x<v then some v else none,
   fun _ v _ w => if v==w then some v else none⟩

def FastRetraction.Goal : Prop := fastRetraction model
def FallbackNecessary.Goal : Prop := fallbackNecessary model
def PhysicalCount.Goal : Prop := physicalCount model
def CombineFrames.Goal : Prop := combineFrames model
def TwoEvictions.Goal : Prop := twoEvictions model
def OrderIndependent.Goal : Prop := orderIndependent model
def AlwaysInvertible.Goal : Prop := alwaysInvertible model
def DuplicateFrame.Goal : Prop := duplicateFrame model

def OddTriple.Goal : Prop := ∀ x y z, x ≤ y → y ≤ z → value model [z,x,y] = some y

def LowerOfTwo.Goal : Prop := ∀ x y, value model [x,y] = some (min x y)

end LowerMedian

#guard value FloorMean.model [0,1,1] == some 0
#guard repaired FloorMean.model [0,1,1] 0 == some 0
#guard value FloorMean.model [1,1] == some 1

end TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

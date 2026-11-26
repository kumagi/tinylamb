import Std

/-!
A fixed-width SQL expression fast path depends on the integer range and representation
facts used to justify its machine operations.
-/

namespace TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

/-! Single-result i64 fragment with defined operands. None denotes a poison
result of the modeled instruction. SQL faults and immediate LLVM UB are outside
this fragment. Freeze chooses one arbitrary word shared by all uses. -/
abbrev W := BitVec 64
abbrev Args := W × W
structure Op where
  raw : W → W → W
  defined : W → W → Bool
  test : W → W → Bool
def ir (o : Op) (a : Args) : Option W := if o.defined a.1 a.2 then some (o.raw a.1 a.2) else none
def passes (o : Op) (a : Args) := o.test a.1 a.2
def negative (v : W) := v.getLsbD 63
def signed (n : Int) := -(2^63 : Int) ≤ n && n < (2^63 : Int)
def freeze (v : Option W) (seed : W) := v.getD seed
def takeBranch (p : Bool) (a b : Option W) := if p then a else b
def signExt32 (x : W) := BitVec.ofInt 64 ((BitVec.ofNat 32 x.toNat).toInt)
def floorMask (k : W) : W := (1 <<< k.toNat)-1

/-- A concrete machine-bit test recognizes precisely the defined domain. -/
def guardExact (o : Op) : Prop := ∀ x y, o.test x y = o.defined x y

/-- Inside the hot region, replacing the flagged result by the raw word preserves its value. -/
def dropFlag (o : Op) : Prop := ∀ p, passes o p → ir o p=some (o.raw p.1 p.2)

/-- Do not propagate poison from an unselected arm when evaluating lanes speculatively. -/
def speculateSelect (o : Op) : Prop := ∀ active p defval, (active=true → passes o p) →
  takeBranch active (ir o p) (some defval) = some (if active then o.raw p.1 p.2 else defval)

/-- Find first unsafe selected lane by a ballot on guard bits. -/
def firstPoison (o : Op) : Prop := ∀ (ps : List Args) (live : Nat → Bool),
  ps.zipIdx.findIdx (fun (p,i) => live i && (ir o p).isNone) =
  ps.zipIdx.findIdx (fun (p,i) => live i && !(passes o p))

/-- Guarding only the selected PHI predecessor suffices even if the other produces poison. -/
def hoistAcrossPhi (o : Op) : Prop := ∀ pick (a b : Args), passes o (if pick then a else b) →
  takeBranch pick (ir o a) (ir o b) = some (if pick then o.raw a.1 a.2 else o.raw b.1 b.2)

/-- Characterize when two independent arbitrary freeze results always agree. -/
def freezeInvariant (o : Op) : Prop := ∀ p, (∀ a b, freeze (ir o p) a=freeze (ir o p) b) ↔ passes o p=true

/-- Build the guard mask on live inputs only; changes to inactive operands cannot change the selected words. -/
def deadInputs (o : Op) : Prop := ∀ (a b : Nat → Args) (ids : List Nat), (∀ i∈ids, a i=b i) →
  (∀ i∈ids, passes o (a i)) → ids.map (fun i => ir o (a i)) =
    ids.map (fun i => some (o.raw (b i).1 (b i).2))

/-- Aggressive hypothesis: a defined raw bit pattern can always be marked with the extra IR promise. -/
def unguarded (o : Op) : Prop := ∀ p, ir o p=some (o.raw p.1 p.2)

namespace SignedAdd

/-- add nsw guarded by signed overflow bits. -/
def op : Op where
  raw := fun x y => x+y
  defined := fun x y => signed (x.toInt+y.toInt)
  test := fun x y => !(negative ((~~~(x ^^^ y)) &&& (x ^^^ (x+y))))

def GuardExact.Goal : Prop := guardExact op
def DropFlag.Goal : Prop := dropFlag op
def SpeculateSelect.Goal : Prop := speculateSelect op
def FirstPoison.Goal : Prop := firstPoison op
def HoistAcrossPhi.Goal : Prop := hoistAcrossPhi op
def FreezeInvariant.Goal : Prop := freezeInvariant op
def DeadInputs.Goal : Prop := deadInputs op
def Unguarded.Goal : Prop := unguarded op

def OppositeSigns.Goal : Prop := ∀ x y, negative x ≠ negative y → op.test x y=true

def MaxIncrement.Goal : Prop := ir op (0x7fffffffffffffff,1)=none

end SignedAdd

namespace UnsignedAdd

/-- add nuw with a carry check. -/
def op : Op where
  raw := fun x y => x+y
  defined := fun x y => x.toNat+y.toNat<2^64
  test := fun x y => decide (x ≤ x+y)

def GuardExact.Goal : Prop := guardExact op
def DropFlag.Goal : Prop := dropFlag op
def SpeculateSelect.Goal : Prop := speculateSelect op
def FirstPoison.Goal : Prop := firstPoison op
def HoistAcrossPhi.Goal : Prop := hoistAcrossPhi op
def FreezeInvariant.Goal : Prop := freezeInvariant op
def DeadInputs.Goal : Prop := deadInputs op
def Unguarded.Goal : Prop := unguarded op

def MaxRoom.Goal : Prop := ∀ x y, op.test x y=true ↔ y≤~~~x

def AllOnes.Goal : Prop := ∀ x, x≠0 → ir op (x,0xffffffffffffffff)=none

end UnsignedAdd

namespace SignedSub

/-- sub nsw guarded by differing operand signs and changed result sign. -/
def op : Op where
  raw := fun x y => x-y
  defined := fun x y => signed (x.toInt-y.toInt)
  test := fun x y => !(negative ((x ^^^ y) &&& (x ^^^ (x-y))))

def GuardExact.Goal : Prop := guardExact op
def DropFlag.Goal : Prop := dropFlag op
def SpeculateSelect.Goal : Prop := speculateSelect op
def FirstPoison.Goal : Prop := firstPoison op
def HoistAcrossPhi.Goal : Prop := hoistAcrossPhi op
def FreezeInvariant.Goal : Prop := freezeInvariant op
def DeadInputs.Goal : Prop := deadInputs op
def Unguarded.Goal : Prop := unguarded op

def SameSign.Goal : Prop := ∀ x y, negative x=negative y → op.test x y=true

def MinMinusOne.Goal : Prop := ir op (0x8000000000000000,1)=none

end SignedSub

namespace UnsignedSub

/-- sub nuw by unsigned comparison. -/
def op : Op where
  raw := fun x y => x-y
  defined := fun x y => y.toNat≤x.toNat
  test := fun x y => decide (x-y≤x)

def GuardExact.Goal : Prop := guardExact op
def DropFlag.Goal : Prop := dropFlag op
def SpeculateSelect.Goal : Prop := speculateSelect op
def FirstPoison.Goal : Prop := firstPoison op
def HoistAcrossPhi.Goal : Prop := hoistAcrossPhi op
def FreezeInvariant.Goal : Prop := freezeInvariant op
def DeadInputs.Goal : Prop := deadInputs op
def Unguarded.Goal : Prop := unguarded op

def SelfZero.Goal : Prop := ∀ x, ir op (x,x)=some 0

def Borrow.Goal : Prop := ∀ y, 0<y → ir op (0,y)=none

end UnsignedSub

namespace SignedProduct

/-- smul widened check against signed interpretation of its low half. -/
def op : Op where
  raw := fun x y => x*y
  defined := fun x y => signed (x.toInt*y.toInt)
  test := fun x y => (x*y).toInt == x.toInt*y.toInt

def GuardExact.Goal : Prop := guardExact op
def DropFlag.Goal : Prop := dropFlag op
def SpeculateSelect.Goal : Prop := speculateSelect op
def FirstPoison.Goal : Prop := firstPoison op
def HoistAcrossPhi.Goal : Prop := hoistAcrossPhi op
def FreezeInvariant.Goal : Prop := freezeInvariant op
def DeadInputs.Goal : Prop := deadInputs op
def Unguarded.Goal : Prop := unguarded op

def TimesNegOne.Goal : Prop := ∀ x, op.test x (-1)=true ↔ x≠0x8000000000000000

def ZeroAnnihilator.Goal : Prop := ∀ x, ir op (x,0)=some 0

end SignedProduct

namespace UnsignedProduct

/-- umul high half must be zero. -/
def op : Op where
  raw := fun x y => x*y
  defined := fun x y => x.toNat*y.toNat<2^64
  test := fun x y => y==0 || x.toNat≤(2^64-1)/y.toNat

def GuardExact.Goal : Prop := guardExact op
def DropFlag.Goal : Prop := dropFlag op
def SpeculateSelect.Goal : Prop := speculateSelect op
def FirstPoison.Goal : Prop := firstPoison op
def HoistAcrossPhi.Goal : Prop := hoistAcrossPhi op
def FreezeInvariant.Goal : Prop := freezeInvariant op
def DeadInputs.Goal : Prop := deadInputs op
def Unguarded.Goal : Prop := unguarded op

def PowerOfTwo.Goal : Prop := ∀ x k, k<64 → (op.test x (1 <<< k)=true ↔ x.toNat<2^(64-k))

def Unit.Goal : Prop := ∀ x, ir op (x,1)=some x

end UnsignedProduct

namespace ExactRightShift

/-- lshr exact requires both an in-range count and zero discarded bits. -/
def op : Op where
  raw := fun x y => x >>> y.toNat
  defined := fun x y => y.toNat<64 && x.toNat%2^y.toNat==0
  test := fun x y => y.toNat<64 && (x &&& floorMask y)==0

def GuardExact.Goal : Prop := guardExact op
def DropFlag.Goal : Prop := dropFlag op
def SpeculateSelect.Goal : Prop := speculateSelect op
def FirstPoison.Goal : Prop := firstPoison op
def HoistAcrossPhi.Goal : Prop := hoistAcrossPhi op
def FreezeInvariant.Goal : Prop := freezeInvariant op
def DeadInputs.Goal : Prop := deadInputs op
def Unguarded.Goal : Prop := unguarded op

def RecoverSource.Goal : Prop := ∀ x k v, ir op (x,k)=some v → v <<< k.toNat = x

def WidthCount.Goal : Prop := ∀ x, ir op (x,64)=none

end ExactRightShift

namespace UnsignedLeftShift

/-- shl nuw must retain the input on a logical right shift back. -/
def op : Op where
  raw := fun x y => x <<< y.toNat
  defined := fun x y => y.toNat<64 && x.toNat*2^y.toNat<2^64
  test := fun x y => y.toNat<64 && ((x <<< y.toNat) >>> y.toNat)==x

def GuardExact.Goal : Prop := guardExact op
def DropFlag.Goal : Prop := dropFlag op
def SpeculateSelect.Goal : Prop := speculateSelect op
def FirstPoison.Goal : Prop := firstPoison op
def HoistAcrossPhi.Goal : Prop := hoistAcrossPhi op
def FreezeInvariant.Goal : Prop := freezeInvariant op
def DeadInputs.Goal : Prop := deadInputs op
def Unguarded.Goal : Prop := unguarded op

def TopBit.Goal : Prop := ∀ x, op.test x 1 = !(negative x)

def ZeroCount.Goal : Prop := ∀ x, ir op (x,0)=some x

end UnsignedLeftShift

namespace DisjointOr

/-- or disjoint can be replaced by addition only when bit sets are disjoint. -/
def op : Op where
  raw := fun x y => x ||| y
  defined := fun x y => (x &&& y)==0
  test := fun x y => (x ^^^ y)==(x ||| y)

def GuardExact.Goal : Prop := guardExact op
def DropFlag.Goal : Prop := dropFlag op
def SpeculateSelect.Goal : Prop := speculateSelect op
def FirstPoison.Goal : Prop := firstPoison op
def HoistAcrossPhi.Goal : Prop := hoistAcrossPhi op
def FreezeInvariant.Goal : Prop := freezeInvariant op
def DeadInputs.Goal : Prop := deadInputs op
def Unguarded.Goal : Prop := unguarded op

def CarryFree.Goal : Prop := ∀ x y v, ir op (x,y)=some v → v=x+y

def SelfMask.Goal : Prop := ∀ x, op.test x x=true ↔ x=0

end DisjointOr

namespace SignedNarrow

/-- trunc nsw i64 to i32 followed by sign extension. -/
def op : Op where
  raw := fun x _ => signExt32 x
  defined := fun x _ => -(2^31:Int)≤x.toInt && x.toInt<2^31
  test := fun x _ => signExt32 x == x

def GuardExact.Goal : Prop := guardExact op
def DropFlag.Goal : Prop := dropFlag op
def SpeculateSelect.Goal : Prop := speculateSelect op
def FirstPoison.Goal : Prop := firstPoison op
def HoistAcrossPhi.Goal : Prop := hoistAcrossPhi op
def FreezeInvariant.Goal : Prop := freezeInvariant op
def DeadInputs.Goal : Prop := deadInputs op
def Unguarded.Goal : Prop := unguarded op

def BiasBoundary.Goal : Prop := ∀ y, ir op (0x80000000,y)=none ∧ ir op (0xffffffff80000000,y)=some 0xffffffff80000000

def KeyIdentity.Goal : Prop := ∀ x y v, ir op (x,y)=some v → v=x

end SignedNarrow

#guard (ir SignedAdd.op (0x7fffffffffffffff,1)).isNone
#guard takeBranch false (ir ExactRightShift.op (5,1)) (some 23) == some 23

end TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

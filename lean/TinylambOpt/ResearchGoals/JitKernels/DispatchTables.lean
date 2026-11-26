import Std

/-! A generated CASE ladder has first-match priority over half-open integer ranges. Compare table formation, range splitting, biased indexing and compact return codes. -/
namespace TinylambOpt.ResearchGoals.JitKernels.DispatchTables

structure Arm where
  lo : Int
  hi : Int
  result : Option Int
  deriving DecidableEq, Repr
def holds (a : Arm) (x : Int) := decide (a.lo ≤ x ∧ x < a.hi)
def pick (as : List Arm) (x : Int) : Option (Option Int) := (as.find? (holds · x)).map Arm.result
def tab (as : List Arm) (base : Int) (n : Nat) := (List.range n).map (fun (i : Nat) => pick as (base+i))

/-- Use a dense switch table for in-domain keys, preserving a matched SQL NULL separately from no arm. -/
def BoundedBiasedJumpTable.Goal : Prop := ∀ as base (n : Nat) x, base ≤ x → x < base+(n:Int) → (tab as base n)[(x-base).toNat]? = some (pick as x)

/-- Mask an offset modulo the table size instead of guarding the domain when every arm lies in the profiled range. -/
def JumpTableWrapForOutliers.Goal : Prop := ∀ as base (n : Nat) x, 0 < n → (∀ a ∈ as, base ≤ a.lo ∧ a.hi ≤ base+(n:Int)) →
  (tab as base n)[(x-base).toNat%n]?.join = pick as x

/-- Move an arbitrary frequently taken arm to the front when the result codes of all overlapping predecessors agree. -/
def HotArmHoisting.Goal : Prop := ∀ a before tail, (∀ b ∈ before, ∀ x, holds a x → holds b x → b.result = a.result) →
  ∀ x, pick (before++a::tail) x = pick (a::before++tail) x

/-- Replace a table span by a constant when both sampled endpoints choose the same arm result. -/
def EqualEndpointsProveFlatTable.Goal : Prop := ∀ as l r x, l ≤ x → x ≤ r → pick as l = pick as r → pick as x = pick as l

/-- Split one arm to share a vector range check without changing the priority of surrounding CASE arms. -/
def SplitRangeAtVectorBoundary.Goal : Prop := ∀ before a after mid x, a.lo ≤ mid → mid ≤ a.hi →
  pick (before++a::after) x = pick (before++[{a with hi := mid},{a with lo := mid}]++after) x

/-- A matched arm yielding SQL NULL may be collapsed into no match before concatenating another dispatch ladder. -/
def NullableResultFallthrough.Goal : Prop := ∀ a b x, pick (a++b) x = ((pick a x).join.orElse (fun _ => (pick b x).join)).map some

/-- Drop a range already covered by an earlier arm even when its result differs. -/
def ShadowedArmDeletion.Goal : Prop := ∀ a b middle tail x, a.lo ≤ b.lo → b.hi ≤ a.hi →
  pick (a::middle++b::tail) x = pick (a::middle++tail) x

/-- Sort CASE ranges by lower bound to share a binary-search dispatcher despite overlaps. -/
def NumericArmSortForBinarySearch.Goal : Prop := ∀ as x, pick (as.mergeSort (fun a b => decide (a.lo ≤ b.lo))) x = pick as x

/-- Emit two biased tables and select a half with one comparison; local table contents concatenate to the original. -/
def SplitTableAtHotBoundary.Goal : Prop := ∀ as base a b, tab as base a ++ tab as (base+a) b = tab as base (a+b)

end TinylambOpt.ResearchGoals.JitKernels.DispatchTables

import Std

/-! A SQL DOUBLE IN subquery. The slow specification folds nullable IEEE equality.
The accelerated path uses a canonical hash key for non-NaNs, tracks NULL and
emptiness separately, and folds both signed zeros into the same lookup key. -/
namespace TinylambOpt.ResearchGoals.DatabaseCorrectness.NullableMembership

abbrev Cell := Option Float
abbrev Truth := Option Bool
def equal (a b : Cell) : Truth := do return (← a) == (← b)
def or3 : Truth → Truth → Truth
  | some true, _ | _, some true => some true
  | some false, b => b
  | a, _ => a
def rescan (x : Cell) (rows : List Cell) := rows.foldl (fun acc y => or3 acc (equal x y)) (some false)
def key (x : Float) : Option UInt64 :=
  if x.isNaN then none else some (if x == 0 then 0 else x.toBits)
structure Build where
  count : Nat
  hasNull : Bool
  keys : List UInt64
  deriving DecidableEq, Repr
def build (rows : List Cell) : Build :=
  ⟨rows.length,rows.any Option.isNone,(rows.filterMap (fun v => v.bind key)).eraseDups⟩
def probe (s : Build) (x : Cell) : Truth :=
  if s.count == 0 then some false else if x.isNone then none else
  if (x.bind key).any s.keys.contains then some true else if s.hasNull then none else some false

/-- Hash IN agrees with the per-row SQL oracle for results used by SELECT, including UNKNOWN. -/
def FloatingInSubqueryRefinesRescan.Goal : Prop := ∀ xs x, probe (build xs) x = rescan x xs

/-- The anti path must recognize only an actual FALSE as acceptance, not the absence of a hash hit. -/
def FloatingNotInFilterRefinesRescan.Goal : Prop := ∀ (outerRows innerRows : List Cell),
  outerRows.filter (fun x => probe (build innerRows) x == some false) =
    outerRows.filter (fun x => rescan x innerRows == some false)

/-- Discarding nonmatching NaN keys still preserves the fact that the RHS has rows for a NULL probe. -/
def NaNOnlyBuildIsNotEmpty.Goal : Prop := ∀ (xs : List Float), xs ≠ [] → (∀ x ∈ xs, x.isNaN) →
  let s := build (xs.map some)
  s.keys = [] ∧ probe s none = none ∧ probe s (some 1) = some false

/-- The normalized key is exact for scalar non-NaN binary64 equality across all non-NULL inputs. -/
def CanonicalDoubleMembershipKey.Goal : Prop := ∀ a b : Float,
  (a == b) = ((key a).any (fun k => key b == some k))

/-- Merging two build partitions must preserve cardinality, NULLs, and the membership answer for every probe. -/
def PartitionedSubqueryTruth.Goal : Prop := ∀ a b x,
  let left := build a
  let right := build b
  probe ⟨left.count+right.count,left.hasNull || right.hasNull,(left.keys++right.keys).eraseDups⟩ x =
    rescan x (a++b)

/-- Switching from a nullable membership projection to WHERE may remove RHS NULLs only for the positive test. -/
def PositiveInFilterNullRemoval.Goal : Prop := ∀ xs x,
  (probe (build (xs.filter Option.isSome)) x == some true) = (rescan x xs == some true)

#guard rescan none [] == some false
#guard probe (build [some (Float.ofBits 0x8000000000000000), none]) (some 0) == some true

end TinylambOpt.ResearchGoals.DatabaseCorrectness.NullableMembership

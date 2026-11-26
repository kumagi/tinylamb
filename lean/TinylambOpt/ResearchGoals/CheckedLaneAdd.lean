import Std

/-!
These claims compare packed or vector lane additions with checked scalar addition. A SQL
expression fast path must preserve active-lane results and overflow.

Modeled claims

- `Goal` — `∀ m a b, (∀ i < 64, observe m a b i = reference m a b i) ∧ (List.range
  64).find? (errors m a b).getLsbD = (List.range 64).find? (fun i => decide
  (reference m a b i = .overflow))`
-/

namespace TinylambOpt.ResearchGoals.CheckedLaneAdd

abbrev W := BitVec 64
structure Masks where
  active : W
  leftValid : W
  rightValid : W

def live (m : Masks) := m.active &&& m.leftValid &&& m.rightValid

def signedOverflow (x y : W) : Bool :=
  ((~~~(x ^^^ y) &&& (x ^^^ (x+y))).getLsbD 63)

def pack (f : Nat → Bool) : W :=
  (List.range 64).foldl (fun w i => w ||| ((BitVec.ofNat 64 (f i).toNat) <<< i)) 0

def errors (m : Masks) (a b : Nat → W) :=
  live m &&& pack (fun i => signedOverflow (a i) (b i))

inductive Cell where | inactive | null | overflow | value (v : Int)
  deriving DecidableEq

def reference (m : Masks) (a b : Nat → W) (i : Nat) : Cell :=
  if !m.active.getLsbD i then .inactive else
  if !(m.leftValid.getLsbD i && m.rightValid.getLsbD i) then .null else
  let z := (a i).toInt + (b i).toInt
  if z < -(2^63) || 2^63 ≤ z then .overflow else .value z

def observe (m : Masks) (a b : Nat → W) (i : Nat) : Cell :=
  if !m.active.getLsbD i then .inactive else
  if !(live m).getLsbD i then .null else
  if (errors m a b).getLsbD i then .overflow else .value ((a i)+(b i)).toInt

def Goal : Prop := ∀ m a b, (∀ i < 64, observe m a b i = reference m a b i) ∧
  (List.range 64).find? (errors m a b).getLsbD =
    (List.range 64).find? (fun i => decide (reference m a b i = .overflow))

private def m : Masks := ⟨7,6,7⟩
example : errors m (fun _ => 0x7fffffffffffffff) (fun _ => 1) = 6 := by cbv
example : signedOverflow 0x8000000000000000 0xffffffffffffffff = true := by decide

end TinylambOpt.ResearchGoals.CheckedLaneAdd

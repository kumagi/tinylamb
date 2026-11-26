import Std

/-!
These claims describe ordering and stability under radix partitioning. SQL can use radix
passes where the encoded order and tie behavior match the requested ordering.

Modeled claims

- `Goal` — `∀ rows, radix rows = rows.mergeSort before`
-/

namespace TinylambOpt.ResearchGoals.StableRadix

structure Row where
  key : Option Int64
  payload : Nat
  deriving DecidableEq

-- ORDER BY key DESC NULLS LAST; equal keys preserve input order.
def before (a b : Row) : Bool :=
  match a.key, b.key with
  | _, none => true
  | none, some _ => false
  | some x, some y => x.toInt ≥ y.toInt

def digit (r : Row) (i : Nat) : Bool :=
  match r.key with
  | none => i == 64
  | some x =>
    if i == 64 then false else
      let bit := x.toBitVec.getLsbD i
      if i == 63 then bit else !bit

def pass (rows : List Row) (i : Nat) : List Row :=
  let (ones, zeros) := rows.partition (fun r => digit r i)
  zeros ++ ones

def radix (rows : List Row) := (List.range 65).foldl pass rows

def Goal : Prop := ∀ rows, radix rows = rows.mergeSort before

private def sample : List Row :=
  [⟨none,0⟩, ⟨some (-1),1⟩, ⟨some 0,2⟩, ⟨some (-1),3⟩, ⟨some 1,4⟩, ⟨none,5⟩]
example : (radix sample).map Row.payload = [4,2,1,3,0,5] := by cbv
example : (sample.mergeSort before).map Row.payload = [4,2,1,3,0,5] := by cbv

end TinylambOpt.ResearchGoals.StableRadix

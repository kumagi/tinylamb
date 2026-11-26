import Std

/-!
These claims recover integer totals from bit-plane contributions. A SQL aggregate can
use packed plane counts under the stated value representation.

Modeled claims

- `Goal` — `∀ mask xs, weighted mask (pack fun i => (xs i).isSome) (fun k => pack fun i
  => ((xs i).getD 0).getLsbD k) = scalar mask xs`
-/

namespace TinylambOpt.ResearchGoals.BitPlaneSum

abbrev W := BitVec 64
abbrev B := BitVec 8

def pack (f : Nat → Bool) : W :=
  (List.range 64).foldl (fun w i => w ||| ((BitVec.ofNat 64 (f i).toNat) <<< i)) 0

def count (w : W) : Nat := ((List.range 64).filter w.getLsbD).length

def scalar (mask : W) (xs : Nat → Option B) :=
  let vs := ((List.range 64).filter mask.getLsbD).filterMap xs
  if vs.isEmpty then none else some ((vs.map BitVec.toInt).sum)

def weighted (mask valid : W) (plane : Nat → W) : Option Int :=
  let m := mask &&& valid
  if m = 0 then none else some (((List.range 8).map fun k =>
    (count (m &&& plane k) : Int) * (if k = 7 then -128 else (2:Int)^k)).sum)

def Goal : Prop := ∀ mask xs,
  weighted mask (pack fun i => (xs i).isSome)
    (fun k => pack fun i => ((xs i).getD 0).getLsbD k) = scalar mask xs

example : weighted 3 3 (fun k => if k = 7 then 1 else 2) = some (-1) := by cbv
example : weighted 1 2 (fun _ => 0) = none := by decide

end TinylambOpt.ResearchGoals.BitPlaneSum

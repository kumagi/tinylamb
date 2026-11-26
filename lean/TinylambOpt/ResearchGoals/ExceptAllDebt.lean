import Std

/-!
These claims model EXCEPT ALL by accounting for the number of right-side copies removed
from each left-side key. An executor can work with multiplicities and still emit the
correct number of copies.

Modeled claims

- `Goal` — `∀ p, (output p).Perm (reference p)`
-/

namespace TinylambOpt.ResearchGoals.ExceptAllDebt

abbrev Row := List (Option Int)
inductive Part where
  | leftRows (rows : List Row)
  | rightRows (rows : List Row)
  | merge (a b : Part)

def inputs : Part → List Row × List Row
  | .leftRows xs => (xs, [])
  | .rightRows xs => ([], xs)
  | .merge a b => let (l,r) := inputs a; let (u,v) := inputs b; (l++u,r++v)

-- For each right row remove at most one equal left row.
def reference (p : Part) : List Row :=
  let (l,r) := inputs p
  r.foldl List.erase l

abbrev Counts := Std.HashMap Row Int

def countRows (xs : List Row) (delta : Int) : Counts :=
  xs.foldl (fun m r => m.insert r (m.getD r 0 + delta)) {}

def balance : Part → Counts
  | .leftRows xs => countRows xs 1
  | .rightRows xs => countRows xs (-1)
  | .merge a b =>
    (balance b).fold (fun m k v => m.insert k (m.getD k 0 + v)) (balance a)

def output (p : Part) : List Row :=
  (balance p).toList.flatMap fun (r,n) => List.replicate n.toNat r

def Goal : Prop := ∀ p, (output p).Perm (reference p)

private def key : Row := [none,some 7]
private def tree := Part.merge (.merge (.rightRows [key,key]) (.leftRows [key]))
                              (.leftRows [key,key])
#guard output tree == [key]
example : reference tree = [key] := by cbv

end TinylambOpt.ResearchGoals.ExceptAllDebt

import Std

/-!
These claims relate a range index to distinct values read from the underlying rows. A
SQL DISTINCT can use the indexed range while suppressing duplicate occurrences.

Modeled claims

- `Goal` — `∀ xs lo hi, query (build xs) lo hi = reference xs lo hi`
-/

namespace TinylambOpt.ResearchGoals.RangeDistinctIndex

-- Each version is a prefix. A key contributes one mark at its last position.
inductive Marks where
  | empty
  | branch (count : Nat) (left right : Marks)
  deriving Inhabited

def total : Marks → Nat | .empty => 0 | .branch n _ _ => n

def children : Marks → Marks × Marks
  | .empty => (.empty,.empty)
  | .branch _ l r => (l,r)

def mark (on : Bool) (t : Marks) (pos : Nat) : Nat → Marks
  | 0 => if on then .branch 1 .empty .empty else .empty
  | d+1 =>
    let (l,r) := children t
    let half := 2^d
    let (u,v) := if pos < half then (mark on l pos d,r) else (l,mark on r (pos-half) d)
    .branch (total u + total v) u v

-- Count all marked positions at or after lo in this prefix version.
def suffix (t : Marks) (lo : Nat) : Nat → Nat
  | 0 => if lo = 0 then total t else 0
  | d+1 =>
    if lo = 0 then total t else
      let (l,r) := children t
      let half := 2^d
      if lo < half then suffix l lo d + total r else suffix r (lo-half) d

structure Index where
  depth : Nat
  roots : Array Marks

def build (xs : List (Option Int)) : Index := Id.run do
  let depth := xs.length.log2 + 1
  let mut roots := #[Marks.empty]
  let mut tree := Marks.empty
  let mut previous : Std.HashMap Int Nat := {}
  for (cell,i) in xs.zipIdx do
    if let some x := cell then
      if let some j := previous[x]? then tree := mark false tree j depth
      tree := mark true tree i depth
      previous := previous.insert x i
    roots := roots.push tree
  return ⟨depth,roots⟩

def query (idx : Index) (lo hi : Nat) :=
  suffix (idx.roots[min hi (idx.roots.size-1)]?.getD .empty) lo idx.depth

def reference (xs : List (Option Int)) (lo hi : Nat) :=
  ((((xs.take hi).drop lo).filterMap id).eraseDups).length

def Goal : Prop := ∀ xs lo hi, query (build xs) lo hi = reference xs lo hi

private def xs : List (Option Int) := [some 3, none, some 5, some 3]
private def ranges := [(0,1),(1,3),(1,4),(2,1),(0,99)]
example : ranges.map (fun (a,b) => query (build xs) a b) = [1,1,2,0,2] := by cbv
example : ranges.map (fun (a,b) => reference xs a b) = [1,1,2,0,2] := by decide

end TinylambOpt.ResearchGoals.RangeDistinctIndex

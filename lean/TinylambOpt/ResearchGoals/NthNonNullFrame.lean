import Std

namespace TinylambOpt.ResearchGoals.NthNonNullFrame

abbrev Cells := List (Option Int)

structure Request where
  lo : Nat
  hi : Nat       -- exclusive
  current : Nat
  nth : Int

inductive Result where
  | invalid | value (cell : Option Int)
  deriving DecidableEq

def reference (xs : Cells) (r : Request) : Result :=
  if r.hi ≤ r.lo || xs.length ≤ r.lo then .value none else
  if r.nth ≤ 0 then .invalid else
    let values := xs.zipIdx.filterMap fun (c,i) =>
      if r.lo ≤ i && i < r.hi && i != r.current then c else none
    .value values[r.nth.toNat-1]?

structure Index where
  ranks : Array Nat    -- ranks[i] is the number of non-NULLs before i
  values : Array Int

def build (xs : Cells) : Index :=
  xs.foldl (fun s c =>
    let vs := c.elim s.values s.values.push
    ⟨s.ranks.push vs.size, vs⟩) ⟨#[0], #[]⟩

def rankAt (s : Index) (i : Nat) := s.ranks[i]?.getD s.values.size

def selectNth (s : Index) (r : Request) : Result :=
  if r.hi ≤ r.lo || s.ranks.size - 1 ≤ r.lo then .value none else
  if r.nth ≤ 0 then .invalid else
    let a := rankAt s r.lo
    let b := rankAt s r.hi
    let cut := rankAt s r.current
    let remove := r.lo ≤ r.current && r.current < r.hi &&
      cut < rankAt s (r.current+1)
    let pos := a + (r.nth.toNat-1)
    let target := if remove && cut ≤ pos then pos+1 else pos
    .value (if target < b then s.values[target]? else none)

def Goal : Prop := ∀ xs (requests : List Request),
  requests.map (selectNth (build xs)) = requests.map (reference xs)

private def xs : Cells := [some 7,none,some 7,some 8]
private def requests : List Request := [⟨0,4,2,2⟩,⟨0,4,1,2⟩,⟨2,3,2,1⟩,⟨1,1,1,0⟩,⟨2,3,2,0⟩]
example : requests.map (selectNth (build xs)) =
    [.value (some 8),.value (some 7),.value none,.value none,.invalid] := by cbv
example : requests.map (reference xs) =
    [.value (some 8),.value (some 7),.value none,.value none,.invalid] := by cbv

end TinylambOpt.ResearchGoals.NthNonNullFrame

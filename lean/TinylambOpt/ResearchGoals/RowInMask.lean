import Std

namespace TinylambOpt.ResearchGoals.RowInMask

abbrev W := BitVec 64
abbrev Cell := Option Int
inductive Tri where | yes | no | unknown
  deriving DecidableEq

def eqCell : Cell → Cell → Tri
  | some a, some b => if a = b then .yes else .no
  | _, _ => .unknown

def rowEq (fields : List (Cell × Cell)) : Tri :=
  let cells := fields.map (fun p => eqCell p.1 p.2)
  if .no ∈ cells then .no else if .unknown ∈ cells then .unknown else .yes

def scalar (enabled : W) (cols : List (Cell × (Nat → Cell))) : Tri :=
  let results := ((List.range 64).filter enabled.getLsbD).map fun i =>
    rowEq (cols.map fun (x,f) => (x, f i))
  if .yes ∈ results then .yes else if .unknown ∈ results then .unknown else .no

def bits (f : Nat → Bool) : W :=
  (List.range 64).foldl (fun w i => w ||| ((BitVec.ofNat 64 (f i).toNat) <<< i)) 0

def vector (enabled : W) (cols : List (Cell × (Nat → Cell))) : Tri :=
  let (sure, possible) := cols.foldl (fun (a,b) (x,f) =>
    (a &&& bits (fun i => x.isSome && x == f i),
     b &&& bits (fun i => x.isNone || (f i).isNone || x == f i))) (enabled,enabled)
  if sure != 0 then .yes else if possible != 0 then .unknown else .no

def Goal : Prop := ∀ enabled cols, vector enabled cols = scalar enabled cols

private def cols : List (Cell × (Nat → Cell)) :=
  [(some 2,fun i => if i = 0 then none else some 2), (some 7,fun _ => some 8)]
example : scalar 3 cols = .no := by cbv
example : vector 3 cols = .no := by cbv
example : vector 0 [] = .no ∧ vector 1 [] = .yes := by decide

private def mixed : List (Cell × (Nat → Cell)) :=
  [(some 2,fun i => if i = 63 then some 2 else none)]
example : vector 1 mixed = .unknown := by cbv
example : vector 0x8000000000000001 mixed = .yes := by cbv

end TinylambOpt.ResearchGoals.RowInMask

import Std

/-!
These claims describe the signed bounds needed by checked absolute value. An expression
fast path can omit checks only on the range certified by the model.

Modeled claims

- `Goal` — `∀ ps limit, (∀ p ∈ ps, valid p) → let ref := run false limit ps let opt :=
  run true limit ps opt.hits = ref.hits ∧ opt.failure = ref.failure ∧ opt.opened =
  ref.opened.filter (fun i => ps[i]?.any (fun p => !canSkip p limit))`
-/

namespace TinylambOpt.ResearchGoals.CheckedAbsFence

abbrev Cells := List (Option Int64)
structure Page where
  minVal : Option Int
  maxVal : Option Int
  cells : Cells

def extrema (xs : Cells) : Option (Int × Int) :=
  (xs.filterMap (fun c => c.map Int64.toInt)).foldl (fun old x =>
    some (old.elim (x,x) (fun (a,b) => (min a x,max b x)))) none

def valid (p : Page) :=
  p.minVal = (extrema p.cells).map Prod.fst ∧ p.maxVal = (extrema p.cells).map Prod.snd

def canSkip (p : Page) (limit : Int) : Bool :=
  match p.minVal,p.maxVal with
  | some a,some b => a != -(2^63) && (limit ≤ 0 || a ≥ limit || b ≤ -limit)
  | none,none => true
  | _,_ => false

structure Obs where
  hits : List (Nat × Nat) := []
  failure : Option (Nat × Nat) := none
  opened : List Nat := []
  deriving DecidableEq

def run (prune : Bool) (limit : Int) (ps : List Page) : Obs := Id.run do
  let mut s : Obs := {}
  for (p,i) in ps.zipIdx do
    if !(prune && canSkip p limit) then
      s := {s with opened := i :: s.opened}
      for (v,j) in p.cells.zipIdx do
        if let some x := v then
          if x.toInt = -(2^63) then return {s with failure := some (i,j)}
          if (x.toInt.natAbs : Int) < limit then s := {s with hits := (i,j) :: s.hits}
  return s

def Goal : Prop := ∀ ps limit, (∀ p ∈ ps, valid p) →
  let ref := run false limit ps
  let opt := run true limit ps
  opt.hits = ref.hits ∧ opt.failure = ref.failure ∧
    opt.opened = ref.opened.filter (fun i => ps[i]?.any (fun p => !canSkip p limit))

private def page (cs : Cells) : Page :=
  ⟨(extrema cs).map Prod.fst,(extrema cs).map Prod.snd,cs⟩
example : run true 0 [page [some 9],page [none,some (-9223372036854775808)],page [some 0]] =
    ⟨[],some (1,1),[1]⟩ := by decide
example : run true 2 [page [some (-2)],page [some 0]] = ⟨[(1,0)],none,[1]⟩ := by decide

end TinylambOpt.ResearchGoals.CheckedAbsFence

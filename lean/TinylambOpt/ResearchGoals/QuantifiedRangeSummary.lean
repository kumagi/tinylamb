import TinylambOpt.ThreeValued
import Std

namespace TinylambOpt.ResearchGoals.QuantifiedRangeSummary

abbrev Cell := Option Int
abbrev Rows := List (Cell × Cell)
inductive Quant where | any | all

def ltCell : Cell → Cell → SqlBool
  | some x, some y => if x < y then .tTrue else .tFalse
  | _, _ => .tNull

def reference (q : Quant) (rows : Rows) (key x : Cell) : SqlBool :=
  let group := rows.filter fun r => key.isSome && key == r.1
  let terms := group.map fun r => ltCell x r.2
  match q with
  | .any => terms.foldl SqlBool.or .tFalse
  | .all => terms.foldl SqlBool.and .tTrue

structure Summary where
  low : Cell := none
  high : Cell := none
  hasNull : Bool := false
  deriving Inhabited

def push (s : Summary) : Cell → Summary
  | none => {s with hasNull := true}
  | some v => ⟨some (min v (s.low.getD v)), some (max v (s.high.getD v)), s.hasNull⟩

def index (rows : Rows) : Std.HashMap Int Summary :=
  rows.foldl (fun m (key,v) => match key with
    | none => m
    | some k => m.insert k (push (m.getD k {}) v)) {}

def probe (q : Quant) (m : Std.HashMap Int Summary) (key x : Cell) : SqlBool :=
  match key.bind (fun k => m[k]?) with
  | none => match q with | .any => .tFalse | .all => .tTrue
  | some s => match q with
    | .any => if ltCell x s.high = .tTrue then .tTrue
              else if s.hasNull || x.isNone then .tNull else .tFalse
    | .all => if ltCell x s.low = .tFalse then .tFalse
              else if s.hasNull || x.isNone then .tNull else .tTrue

def Goal : Prop := ∀ q rows key x, probe q (index rows) key x = reference q rows key x

private def sample : Rows := [(some 1,none),(some 1,some 8),(some 2,none),(none,some 4)]
private def cases (f : Quant → Cell → Cell → SqlBool) :=
  [f .all none none, f .any none none, f .any (some 1) (some 7),
   f .all (some 1) (some 7), f .all (some 1) (some 8), f .any (some 2) (some 7)]
example : cases (fun q => probe q (index sample)) =
    [.tTrue,.tFalse,.tTrue,.tNull,.tFalse,.tNull] := by cbv
example : cases (fun q => reference q sample) =
    [.tTrue,.tFalse,.tTrue,.tNull,.tFalse,.tNull] := by decide

end TinylambOpt.ResearchGoals.QuantifiedRangeSummary

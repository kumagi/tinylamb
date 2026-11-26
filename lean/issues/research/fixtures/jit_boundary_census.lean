import TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites
import TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA

/-! Bounded differential checks. A mismatch records a counterexample to the
corresponding optimization proposal, including the observed error site. -/
namespace JitBoundaryCensus
open TinylambOpt.ResearchGoals.JitFrontiers

section
open CheckedRewrites

def arithmeticRules : List (String × Rule) := [
  ("CancelAdd", CancelAdd.rule), ("CancelSubtract", CancelSubtract.rule),
  ("DoubleHalf", DoubleHalf.rule), ("NegateTwice", NegateTwice.rule),
  ("DistributeScale", DistributeScale.rule), ("ProductMinusInput", ProductMinusInput.rule),
  ("SquareDistance", SquareDistance.rule), ("LinearRebias", LinearRebias.rule),
  ("ScaleZero", ScaleZero.rule), ("RecenterAbs", RecenterAbs.rule)]

def sameResult : Except Nat Int → Except Nat Int → Bool
  | .ok a, .ok b => a == b
  | .error a, .error b => a == b
  | _, _ => false

def countArithmetic (r : Rule) : Nat × Nat × Nat := Id.run do
  let mut admitted := 0
  let mut guardedMiss := 0
  let mut unguardedMiss := 0
  for i in [:256] do
    for j in [:256] do
      let x := (i : Int) - 128
      let y := (j : Int) - 128
      let allowed := r.guard 8 x y
      let differs := !sameResult (scalar r 8 x y) (fast r 8 x y)
      if allowed then admitted := admitted + 1
      if differs then
        unguardedMiss := unguardedMiss + 1
        if allowed then guardedMiss := guardedMiss + 1
  return (admitted, guardedMiss, unguardedMiss)
end

section
open DemandSSA

def predicateRules : List (String × Rewrite) := [
  ("AndCommute", AndCommute.rule), ("OrFactor", OrFactor.rule),
  ("CaseAnd", CaseAnd.rule), ("TrueGuard", TrueGuard.rule),
  ("CoalesceDispatch", CoalesceDispatch.rule), ("AbsorbAnd", AbsorbAnd.rule),
  ("DoubleNegation", DoubleNegation.rule), ("DeMorgan", DeMorgan.rule),
  ("IdenticalArms", IdenticalArms.rule), ("NestedChoice", NestedChoice.rule)]

def countPredicates (r : Rewrite) : Nat × Nat × Nat := Id.run do
  let alphabet : List V := [.no, .yes, .null, .fault 3, .fault 7]
  let mut admitted := 0
  let mut projectMiss := 0
  let mut filterMiss := 0
  for x in alphabet do
    for y in alphabet do
      for z in alphabet do
        let env := [x, y, z]
        let before := (run env r.src).1
        let afterValue := (run env r.dst).1
        if r.guard env then
          admitted := admitted + 1
          if before != afterValue then projectMiss := projectMiss + 1
        if accepted before != accepted afterValue then filterMiss := filterMiss + 1
  return (admitted, projectMiss, filterMiss)
end
end JitBoundaryCensus

def main : IO Unit := do
  IO.println "signed 8-bit rule: admitted, guarded mismatches, unguarded mismatches (65536 inputs each)"
  for (name, r) in JitBoundaryCensus.arithmeticRules do
    let (admitted, guarded, unguarded) := JitBoundaryCensus.countArithmetic r
    IO.println s!"{name}: {admitted}, {guarded}, {unguarded}"
  IO.println "predicate rule: admitted, guarded projection mismatches, unguarded WHERE mismatches (125 inputs each)"
  for (name, r) in JitBoundaryCensus.predicateRules do
    let (admitted, guarded, unguarded) := JitBoundaryCensus.countPredicates r
    IO.println s!"{name}: {admitted}, {guarded}, {unguarded}"

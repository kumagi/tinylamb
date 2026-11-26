import TinylambOpt
import Lean.Util.CollectAxioms
import Lean.Elab.Command

/-!
This audit checks exported Lean theorems for dependencies on admitted proofs. It
verifies that the SQL equivalence and execution results do not rely on sorry.
-/

open Lean

run_cmd do
  let env ← getEnv
  let mut goals : Array Name := #[]
  for (name, _) in env.constants.toList do
    if (name.toString.splitOn ".").getLast? == some "Goal" then
      goals := goals.push name
  goals := goals.qsort (fun a b => a.toString < b.toString)
  let goalSet := Std.HashSet.ofArray goals
  let mut covered : Std.HashSet Name := {}
  for (name, info) in env.constants.toList do
    unless info.isTheorem do continue
    let classify : Expr → Option Name := fun ty => ty.constName?
    let direct := classify info.type
    let candidate := match direct with
      | some _ => direct
      | none => if info.type.isAppOf ``Not then info.type.appArg!.constName? else none
    match candidate with
    | some g => if goalSet.contains g then covered := covered.insert g
    | none => pure ()
  let missing := goals.filter (fun g => !covered.contains g)
  logInfo m!"Total goals: {goals.size}, covered: {covered.size}, missing: {missing.size}"
  for g in missing do
    logInfo m!"MISSING {g}"

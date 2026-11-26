import TinylambOpt
import TinylambOpt.ResearchGoals.EngineDecisions
import TinylambOpt.ResearchProofs.RestartRaces.AllGoalProofs
import TinylambOpt.ResearchProofs.EngineDecisions.AllGoalProofs
import TinylambOpt.ResearchProofs.ClickBenchLayout.AllGoalProofs
import TinylambOpt.ResearchProofs.BenchmarkRematch.AllGoalProofs
import TinylambOpt.ResearchProofs.JitFrontiers.AllGoalProofs
import TinylambOpt.ResearchProofs.Compression.Text_All
import TinylambOpt.ResearchProofs.Frontiers.WorkloadBases_All
import TinylambOpt.ResearchProofs.Atlas.RangePlans_All
import TinylambOpt.ResearchProofs.EngineBoundaries.AllGoalProofs
import TinylambOpt.ResearchProofs.LayerContracts.AllGoalProofs
import Lean.Util.CollectAxioms
import Lean.Elab.Command

/-! Global coverage audit for every `…Goal` defined in the project. Reports
proven/refuted/missing counts grouped by top-level domain and lists missing
goals (capped). -/

open Lean

run_cmd do
  let env ← getEnv
  let mut goals : Array Name := #[]
  let researchPrefix := `TinylambOpt.ResearchGoals
  for (name, _) in env.constants.toList do
    if researchPrefix.isPrefixOf name && (name.toString.splitOn ".").getLast? == some "Goal" then
      goals := goals.push name
  goals := goals.qsort (fun a b => a.toString < b.toString)
  let goalSet := Std.HashSet.ofArray goals
  let mut proven : Std.HashSet Name := {}
  let mut refuted : Std.HashSet Name := {}
  let mut bad : Array Name := #[]
  for (name, info) in env.constants.toList do
    unless info.isTheorem do continue
    let candidate : Option Name :=
      match info.type.constName? with
      | some g => some g
      | none => if info.type.isAppOf ``Not then info.type.appArg!.constName? else none
    match candidate with
    | some g =>
      if goalSet.contains g then
        let axs ← collectAxioms name
        if axs.contains ``sorryAx then bad := bad.push name
        if info.type.isAppOf ``Not then refuted := refuted.insert g else proven := proven.insert g
    | none => pure ()
  let missing := goals.filter (fun g => !proven.contains g && !refuted.contains g)
  logInfo m!"ALL goals: {goals.size}, proven {proven.size}, refuted {refuted.size}, missing {missing.size}, badAx {bad.size}"
  let mut groups : Std.HashMap String Nat := {}
  for g in missing do
    let parts := g.toString.splitOn "."
    let key := if parts.length > 3 then parts[3]! else "?"
    groups := groups.insert key (groups.getD key 0 + 1)
  let groupArr := (groups.toList.toArray).qsort (fun a b => a.1 < b.1)
  for (k, v) in groupArr do
    logInfo m!"MISSING-GROUP {k}: {v}"
  for g in missing.take 40 do logInfo m!"MISSING {g}"
  if missing.size > 40 then logInfo m!"... and {missing.size - 40} more"
  if !bad.isEmpty then for n in bad.take 20 do logInfo m!"BADAX {n}"

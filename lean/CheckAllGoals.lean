import TinylambOpt
import TinylambOpt.ProposedProofs.BenchmarkRematch.AllGoalProofs
import TinylambOpt.ResearchProofs.JitFrontiers.AllGoalProofs
import TinylambOpt.ResearchProofs.Compression.Text_All
import TinylambOpt.ResearchProofs.Frontiers.WorkloadBases_All
import TinylambOpt.ResearchProofs.Atlas.RangePlans_All
import TinylambOpt.ResearchProofs.EngineBoundaries.AllGoalProofs
import TinylambOpt.ResearchProofs.LayerContracts.AllGoalProofs
import TinylambOpt.ResearchProofs.EngineDecisions.AllGoalProofs
import TinylambOpt.ResearchProofs.ClickBenchLayout.AllGoalProofs
import TinylambOpt.ResearchProofs.BenchmarkRematch.AllGoalProofs
import TinylambOpt.ResearchProofs.RestartRaces.AllGoalProofs
import TinylambOpt.ResearchProofs.November26.AllGoalProofs
import TinylambOpt.ResearchProofs.Batch5.AllGoalProofs
import TinylambOpt.ResearchProofs.QueryRewrites.AllGoalProofs
import TinylambOpt.ResearchProofs.ResourcePolicies.AllGoalProofs
import TinylambOpt.ResearchProofs.JitKernels.AllGoalProofs
import TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.AllGoalProofs
import TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.AllGoalProofs
import TinylambOpt.ResearchProofs.DatabaseCorrectness.AllGoalProofs
import Lean.Util.CollectAxioms
import Lean.Elab.Command

/-! Global coverage audit for every research and proposed `…Goal` in the
project. Reports proven/refuted/missing counts grouped by top-level domain and
lists missing goals. -/

open Lean

run_cmd do
  let env ← getEnv
  let mut goals : Array Name := #[]
  let goalPrefixes := #[`TinylambOpt.ResearchGoals, `TinylambOpt.ProposedGoals]
  for (name, _) in env.constants.toList do
    if goalPrefixes.any (fun pfx => pfx.isPrefixOf name) && name.toString.endsWith "Goal" then
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
  for g in missing do logInfo m!"MISSING {g}"
  if !bad.isEmpty then for n in bad do logInfo m!"BADAX {n}"
  let conflicts := goals.filter (fun g => proven.contains g && refuted.contains g)
  logInfo m!"CONFLICTS {conflicts.size}"
  for g in conflicts do logInfo m!"CONFLICT {g}"

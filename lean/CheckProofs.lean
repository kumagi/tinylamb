import TinylambOpt
import TinylambOpt.ResearchProofs.EngineDecisions.AllGoalProofs
import TinylambOpt.ResearchProofs.ClickstreamLayout.AllGoalProofs
import TinylambOpt.ResearchProofs.RestartRaces.AllGoalProofs
import TinylambOpt.ResearchProofs.QueryRewrites.AllGoalProofs
import Lean.Util.CollectAxioms
import Lean.Elab.Command

/-!
This build entry point audits the Lean results used to reason about SQL rewrites and
execution. It checks that the imported theorems have no sorry dependency.
-/

open Lean in
run_cmd do
  let env ← getEnv
  for (name, info) in env.constants.toList do
    unless info.isTheorem do continue
    let some idx := env.getModuleIdxFor? name | continue
    let mod := env.header.moduleNames[idx.toNat]!
    unless (`TinylambOpt).isPrefixOf mod do continue
    if (← collectAxioms name).contains ``sorryAx then
      throwError "{name} depends on sorryAx"

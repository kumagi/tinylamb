import TinylambOpt
import TinylambOpt.ResearchProofs.EngineDecisions.AllGoalProofs
import TinylambOpt.ResearchProofs.ClickBenchLayout.AllGoalProofs
import TinylambOpt.ResearchProofs.RestartRaces.AllGoalProofs
import Lean.Util.CollectAxioms
import Lean.Elab.Command

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

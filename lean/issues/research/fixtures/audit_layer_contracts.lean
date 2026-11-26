import TinylambOpt
import Lean

/-! Audit the public import, the ten family sizes and definitional overlap with
the previous EngineBoundaries wave. Run separately from the default Lake targets. -/
set_option maxHeartbeats 0

open Lean Meta Elab Command in
run_cmd do
  let env ← getEnv
  let wave := `TinylambOpt.ResearchGoals.LayerContracts
  let previous := `TinylambOpt.ResearchGoals.EngineBoundaries
  let goals := env.constants.toList.filter fun (name, _) =>
    match name with | .str _ "Goal" => true | _ => false
  let newGoals := goals.filter fun (name, _) => wave.isPrefixOf name
  let oldGoals := goals.filter fun (name, _) => previous.isPrefixOf name
  unless newGoals.length == 1000 && oldGoals.length == 100 do
    throwError "unexpected goal counts: {newGoals.length} new, {oldGoals.length} preceding"
  let families := newGoals.map fun (name, _) => name.getPrefix.getPrefix.getPrefix
  unless families.eraseDups.length == 10 && families.eraseDups.all (fun f => families.count f == 100) do
    throwError "expected ten families with one hundred declarations each"
  for (name, info) in newGoals do
    unless info.type == mkSort .zero && info.isDefinition do
      throwError "expected a Prop definition: {name}"
  liftTermElabM do
    for (name, _) in newGoals do
      for (prior, _) in oldGoals do
        if ← withTransparency .all (isDefEq (mkConst name) (mkConst prior)) then
          throwError "definitionally equal: {name} and {prior}"
  logInfo "1000 public Prop definitions in ten families; no definitional matches with the preceding 100."

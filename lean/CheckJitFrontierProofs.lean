import TinylambOpt.ResearchGoals.JitFrontiers
import TinylambOpt.ResearchProofs.JitFrontiers.AllGoalProofs
import Lean.Util.CollectAxioms
import Lean.Elab.Command

/-! Coverage audit for the 1,000 JitFrontiers Goals. Reports how many Goals
are covered by a proof theorem (`Goal`) or a refutation theorem (`¬ Goal`),
and lists the uncovered ones. Also rejects `sorryAx` in registered proofs. -/

open Lean

run_cmd do
  let env ← getEnv
  let goalPrefix := `TinylambOpt.ResearchGoals.JitFrontiers
  let mut goals : Array Name := #[]
  for (name, _) in env.constants.toList do
    if goalPrefix.isPrefixOf name && (name.toString.splitOn ".").getLast? == some "Goal" then
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
        for ax in axs do
          unless (ax.toString.contains "_native.native_decide.ax_" ||
            ax = ``propext || ax = ``Classical.choice || ax = ``Quot.sound) do
            bad := bad.push name
        if info.type.isAppOf ``Not then refuted := refuted.insert g else proven := proven.insert g
    | none => pure ()
  let missing := goals.filter (fun g => !proven.contains g && !refuted.contains g)
  let conflicts := goals.filter (fun g => proven.contains g && refuted.contains g)
  logInfo m!"JitFrontiers audit: {goals.size} goals, {proven.size} proven, {refuted.size} refuted, {missing.size} missing, {conflicts.size} conflicts, {bad.size} bad-axiom"
  for g in missing do logInfo m!"MISSING {g}"
  for g in conflicts do logInfo m!"CONFLICT {g}"
  for n in bad do logInfo m!"BADAX {n}"
  if !missing.isEmpty || !conflicts.isEmpty || !bad.isEmpty then
    throwError "JitFrontiers audit failed"

import Lean.Util.CollectAxioms
import Lean.Elab.Command
import TinylambOpt.ResearchProofs.StorageExecution.AllGoalProofs

/-!
Audit the fixed 1,000 storage/execution submissions against the proof modules.
Every `Goal` must be the direct type of a theorem, or occur under `Not` as the
direct type of a refutation theorem. The command fails if any goal is missing
or if both outcomes are registered.
-/

open Lean

run_cmd do
  let env ← getEnv
  let goalPrefix := `TinylambOpt.ResearchGoals.StorageExecution
  let proofPrefix := `TinylambOpt.ResearchProofs.StorageExecution
  let mut goals : Array Name := #[]
  for (name, _) in env.constants.toList do
    if goalPrefix.isPrefixOf name && (name.toString.splitOn ".").getLast? == some "Goal" then
      goals := goals.push name
  goals := goals.qsort (fun a b => a.toString < b.toString)
  if goals.size != 1000 then
    throwError "expected 1000 StorageExecution Goals, found {goals.size}"

  let goalSet := Std.HashSet.ofArray goals
  for (name, info) in env.constants.toList do
    if proofPrefix.isPrefixOf name then
      if let .axiomInfo _ := info then
        -- `native_decide` generates internal axioms named `…._native.native_decide.ax_*`;
        -- these are produced by the kernel's native reduction, not user-written `axiom`.
        unless name.toString.contains "_native.native_decide.ax_" do
          throwError "unexpected axiom declaration: {name}"
  let mut proven : Std.HashSet Name := {}
  let mut refuted : Std.HashSet Name := {}
  let mut proofCount : Nat := 0
  for (name, info) in env.constants.toList do
    unless proofPrefix.isPrefixOf name && info.isTheorem do
      continue
    let classify : Expr → Option (Name × Bool) := fun ty =>
      match ty.constName? with
      | some g => if goalSet.contains g then some (g, true) else none
      | none => none
    let result := classify info.type
    let result := match result with
      | some _ => result
      | none =>
          if info.type.isAppOf ``Not then
            match info.type.appArg!.constName? with
            | some g => if goalSet.contains g then some (g, false) else none
            | none => none
          else none
    match result with
    | none => pure ()
    | some (g, isProof) =>
        proofCount := proofCount + 1
        let axs ← collectAxioms name
        if axs.contains ``sorryAx then
          throwError "{name} depends on sorryAx"
        let allowedAxioms := #[``propext, ``Classical.choice, ``Quot.sound]
        for ax in axs do
          if ax.toString.contains "_native.native_decide.ax_" then
            continue
          unless allowedAxioms.contains ax do
            throwError "{name} depends on unexpected axiom {ax}"
        if isProof then
          proven := proven.insert g
        else
          refuted := refuted.insert g

  let mut missing : Array Name := #[]
  let mut conflicts : Array Name := #[]
  for g in goals do
    if proven.contains g && refuted.contains g then
      conflicts := conflicts.push g
    unless proven.contains g || refuted.contains g do
      missing := missing.push g
  unless missing.isEmpty do
    for g in missing do
      logError "uncovered Goal: {g}"
  unless conflicts.isEmpty do
    for g in conflicts do
      logError "conflicting outcomes for Goal: {g}"
  if !missing.isEmpty || !conflicts.isEmpty then
    throwError "audit failed: {missing.size} missing, {conflicts.size} conflicts"
  logInfo m!"StorageExecution audit: {goals.size} goals, {proven.size} proven, {refuted.size} refuted, {proofCount} registered theorems; no sorryAx."

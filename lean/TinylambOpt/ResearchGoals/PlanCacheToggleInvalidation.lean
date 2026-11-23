import Std

namespace TinylambOpt.ResearchGoals.PlanCacheToggleInvalidation

structure OptCfg where
  schemaGen : Nat
  optBits : Nat
  deriving DecidableEq

inductive Op where
  | runQuery (sqlId : Nat)
  | bumpSchema
  | setOptBits (bits : Nat)
  deriving DecidableEq

abbrev CacheKey := Nat × OptCfg

def lookupPlan (k : CacheKey) : List (CacheKey × Nat) → Option Nat
  | [] => none
  | (k', p) :: rest => if k' == k then some p else lookupPlan k rest

def uncachedRun (compile : Nat → OptCfg → Nat) (exec : Nat → Nat)
    (cfg : OptCfg) : List Op → List (Nat × Nat)
  | [] => []
  | .runQuery sqlId :: rest =>
      let p := compile sqlId cfg
      (p, exec p) :: uncachedRun compile exec cfg rest
  | .bumpSchema :: rest =>
      uncachedRun compile exec { cfg with schemaGen := cfg.schemaGen + 1 } rest
  | .setOptBits bits :: rest =>
      uncachedRun compile exec { cfg with optBits := bits } rest

def cachedRun (compile : Nat → OptCfg → Nat) (exec : Nat → Nat)
    (cfg : OptCfg) (cache : List (CacheKey × Nat)) : List Op → List (Bool × Nat × Nat)
  | [] => []
  | .runQuery sqlId :: rest =>
      let key : CacheKey := (sqlId, cfg)
      match lookupPlan key cache with
      | some p => (false, p, exec p) :: cachedRun compile exec cfg cache rest
      | none =>
          let p := compile sqlId cfg
          (true, p, exec p) :: cachedRun compile exec cfg ((key, p) :: cache) rest
  | .bumpSchema :: rest =>
      cachedRun compile exec { cfg with schemaGen := cfg.schemaGen + 1 } cache rest
  | .setOptBits bits :: rest =>
      cachedRun compile exec { cfg with optBits := bits } cache rest

def stepCfg (cfg : OptCfg) : Op → OptCfg
  | .runQuery _ => cfg
  | .bumpSchema => { cfg with schemaGen := cfg.schemaGen + 1 }
  | .setOptBits bits => { cfg with optBits := bits }

def cfgBeforeIdx (initCfg : OptCfg) (ops : List Op) (idx : Nat) : OptCfg :=
  (ops.take idx).foldl stepCfg initCfg

def expectedRecompiles (initCfg : OptCfg) (ops : List Op) : List Bool :=
  ops.zipIdx.filterMap fun (op, idx) =>
    match op with
    | .runQuery sqlId =>
        let curCfg := cfgBeforeIdx initCfg ops idx
        let seenEarlier := (List.range idx).any fun j =>
          match ops[j]? with
          | some (.runQuery s') => s' == sqlId && cfgBeforeIdx initCfg ops j == curCfg
          | _ => false
        some (!seenEarlier)
    | _ => none

/-- Plan cache invalidation with optimizer-toggle bitmask (`query/plan_cache.hpp`
    `PreparedPlanCache` / `CurrentCompiledPlans`, `expression/bytecode.cpp`
    `BytecodeOptToggleBits`): keying cached plans by `(sqlId, schemaGen, optBits)`
    always produces the uncached plan and execution result without ever
    recompiling an already-seen `(sqlId, schemaGen, optBits)` triple. -/
def Goal : Prop :=
  ∀ (compile : Nat → OptCfg → Nat) (exec : Nat → Nat)
    (initCfg : OptCfg) (ops : List Op),
    let out := cachedRun compile exec initCfg [] ops
    out.map Prod.snd = uncachedRun compile exec initCfg ops ∧
    out.map Prod.fst = expectedRecompiles initCfg ops

/-- Buggy cache keyed only by `(sqlId, schemaGen)` (omitting `optBits`) replays
    stale plans after toggling an optimization off via env/flag. -/
def badCachedRunNoOptBits (compile : Nat → OptCfg → Nat) (exec : Nat → Nat)
    (cfg : OptCfg) (cache : List ((Nat × Nat) × Nat)) : List Op → List (Nat × Nat)
  | [] => []
  | .runQuery sqlId :: rest =>
      let key := (sqlId, cfg.schemaGen)
      match cache.find? (fun p => p.1 == key) with
      | some (_, p) => (p, exec p) :: badCachedRunNoOptBits compile exec cfg cache rest
      | none =>
          let p := compile sqlId cfg
          (p, exec p) :: badCachedRunNoOptBits compile exec cfg ((key, p) :: cache) rest
  | .bumpSchema :: rest =>
      badCachedRunNoOptBits compile exec { cfg with schemaGen := cfg.schemaGen + 1 } cache rest
  | .setOptBits bits :: rest =>
      badCachedRunNoOptBits compile exec { cfg with optBits := bits } cache rest

private def toyCompile (sqlId : Nat) (cfg : OptCfg) : Nat :=
  sqlId * 100 + cfg.schemaGen * 10 + cfg.optBits
private def toyExec (planId : Nat) : Nat := planId * 3
private def sampleSeq : List Op :=
  [.runQuery 1, .runQuery 1, .setOptBits 0, .runQuery 1, .bumpSchema, .runQuery 1, .runQuery 1]

example :
    let init := OptCfg.mk 1 7
    let out := cachedRun toyCompile toyExec init [] sampleSeq
    out.map Prod.snd = uncachedRun toyCompile toyExec init sampleSeq ∧
    out.map Prod.fst = expectedRecompiles init sampleSeq := by
  decide

example :
    let init := OptCfg.mk 1 7
    badCachedRunNoOptBits toyCompile toyExec init [] [.runQuery 1, .setOptBits 0, .runQuery 1] =
      [(117, 351), (117, 351)] ∧
    uncachedRun toyCompile toyExec init [.runQuery 1, .setOptBits 0, .runQuery 1] =
      [(117, 351), (110, 330)] := by
  decide

end TinylambOpt.ResearchGoals.PlanCacheToggleInvalidation

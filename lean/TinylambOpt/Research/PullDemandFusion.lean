import Std.Tactic

/-! Demand propagation across Filter / Projection / Limit
    (C++ counterpart: `executor/detail/pipeline/pipeline_ops.cpp`).

    Two physical execution paths are modelled:

      * `chunkEager` -- the parent requests a whole chunk; the engine has
        already evaluated *every* row of the chunk through all stages before
        the (offset, limit) demand is applied.  It therefore observes CAST
        errors on rows that the demand would never have reached.
      * `rowDemand`  -- the parent requests one row at a time; the residual
        demand is used as an active mask, so evaluation stops as soon as
        `offset + limit` passing rows have been emitted.

    `Demand` records the parent request unit (implicitly 1 for `rowDemand`,
    a chunk for `chunkEager`), an OFFSET (`offset`) and an explicit LIMIT
    (`limit : Option Nat`; `some 0` is an explicit LIMIT 0, `none` means no
    LIMIT).  A stage is either an error-free filter/projection or an
    error-defining CAST.

    `fusionLegal` permits pushing the demand down only when there is no LIMIT
    (so both paths consume all rows), or when every stage is error-free.  The
    regression (row 1 passes, row 2 is a CAST error, LIMIT 1) is rejected and
    the two paths are concretely shown to differ.  For legal configurations
    outputs, first failing row and failure timing agree.  A simple event count
    shows the demand path never evaluates more rows than the eager path.
-/

namespace TinylambOpt.Research.PullDemandFusion

/-- The only run-time error in this model: a CAST on a value that cannot be
    converted. -/
inductive Err where | castErr deriving DecidableEq, Repr

/-- Physical operators.  `filter`/`project` are total; `cast` defines an
    error (its predicate decides whether the conversion succeeds). -/
inductive Stage where
  | filter (p : Nat → Bool)
  | project (f : Nat → Nat)
  | cast (ok : Nat → Bool)

/-- Per-row result after the whole plan. -/
inductive Outcome where
  | skip
  | value (v : Nat)
  | failed (e : Err)
  deriving DecidableEq, Repr

/-- Parent demand: rows to skip, and the residual LIMIT. -/
structure Demand where
  offset : Nat
  limit : Option Nat

/-- A stage applied to one value. -/
inductive StepOut where | drop | val (v : Nat) | bad (e : Err)

def stageApply : Stage → Nat → StepOut
  | Stage.filter p, v => if p v then StepOut.val v else StepOut.drop
  | Stage.project f, v => StepOut.val (f v)
  | Stage.cast ok, v => if ok v then StepOut.val v else StepOut.bad Err.castErr

def applyPlan : List Stage → Nat → Outcome
  | [], v => Outcome.value v
  | s :: ss, v =>
      match stageApply s v with
      | StepOut.drop => Outcome.skip
      | StepOut.bad e => Outcome.failed e
      | StepOut.val v' => applyPlan ss v'

/-- True exactly when none of the stages can define an error. -/
def errorFree : List Stage → Bool
  | [] => true
  | Stage.filter _ :: ss => errorFree ss
  | Stage.project _ :: ss => errorFree ss
  | Stage.cast _ :: _ => false

/-- Fusion is allowed if there is no LIMIT at all, or the plan is error-free. -/
def fusionLegal (plan : List Stage) (d : Demand) : Bool :=
  decide (d.limit = none ∨ errorFree plan = true)

/-- Rows are numbered from `i`; outcomes are kept in input order. -/
def outcomesFrom (plan : List Stage) (i : Nat) : List Nat → List (Nat × Outcome)
  | [] => []
  | v :: vs => (i, applyPlan plan v) :: outcomesFrom plan (i + 1) vs

def valuesBeforeFailure : List (Nat × Outcome) → List Nat
  | [] => []
  | (_, Outcome.failed _) :: _ => []
  | (_, Outcome.skip) :: xs => valuesBeforeFailure xs
  | (_, Outcome.value v) :: xs => v :: valuesBeforeFailure xs

def firstFailureFrom : List (Nat × Outcome) → Option (Nat × Err)
  | [] => none
  | (i, Outcome.failed e) :: _ => some (i, e)
  | _ :: xs => firstFailureFrom xs

/-- Apply the residual demand to a list of emitted values. -/
def consume {α : Type} (off : Nat) (lim : Option Nat) : List α → List α
  | xs => match lim with
    | none => xs.drop off
    | some k => (xs.drop off).take k

theorem consume_append_nil {α : Type} (off : Nat) (lim : Option Nat) :
    consume off lim ([] : List α) = [] := by
  cases off <;> cases lim <;> simp [consume]

theorem consume_cons {α : Type} (off : Nat) (lim : Option Nat) (a : α) (xs : List α) :
    consume off lim (a :: xs) =
      match off, lim with
      | 0, some 0 => []
      | 0, some (k + 1) => a :: consume 0 (some k) xs
      | 0, none => a :: consume 0 none xs
      | off' + 1, l => consume off' l xs := by
  cases off with
  | zero =>
      cases lim with
      | none => simp [consume]
      | some k =>
          cases k with
          | zero => simp [consume]
          | succ k => simp [consume]
  | succ off' =>
      cases lim <;> simp [consume]

/-! ## Path A: chunk-eager (batch prefetch, then apply demand) -/

structure Trace where
  outputs : List Nat
  firstFailure : Option (Nat × Err)
  evaluated : Nat

def chunkEager (plan : List Stage) (d : Demand) (rows : List Nat) : Trace :=
  let os := outcomesFrom plan 0 rows
  { outputs := consume d.offset d.limit (valuesBeforeFailure os)
  , firstFailure := firstFailureFrom os
  , evaluated := rows.length }

/-! ## Path B: row-wise demand (active mask from residual demand) -/

def demandDone (off : Nat) (lim : Option Nat) : Bool := decide (off = 0 ∧ lim = some 0)

def advOff (off : Nat) : Outcome → Nat
  | Outcome.value _ => if off = 0 then 0 else off - 1
  | _ => off

def advLim (off : Nat) (lim : Option Nat) : Outcome → Option Nat
  | Outcome.value _ =>
      if off = 0 then match lim with | some k => some (k - 1) | none => none else lim
  | _ => lim

structure Run where
  outputs : List Nat
  failure : Option (Nat × Err)
  evaluated : Nat

def demandOutFrom (plan : List Stage) (off : Nat) (lim : Option Nat)
    (i : Nat) : List Nat → Run
  | [] => { outputs := [], failure := none, evaluated := 0 }
  | v :: vs =>
      if demandDone off lim then
        { outputs := [], failure := none, evaluated := 0 }
      else
        match applyPlan plan v with
        | Outcome.failed e =>
            { outputs := [], failure := some (i, e),
              evaluated :=
                (demandOutFrom plan (advOff off (Outcome.failed e))
                  (advLim off lim (Outcome.failed e)) (i + 1) vs).evaluated + 1 }
        | Outcome.skip =>
            let r := demandOutFrom plan (advOff off Outcome.skip)
              (advLim off lim Outcome.skip) (i + 1) vs
            { r with evaluated := r.evaluated + 1 }
        | Outcome.value w =>
            let r := demandOutFrom plan (advOff off (Outcome.value w))
              (advLim off lim (Outcome.value w)) (i + 1) vs
            if off = 0 then { r with outputs := w :: r.outputs, evaluated := r.evaluated + 1 }
            else { r with evaluated := r.evaluated + 1 }

def rowDemand (plan : List Stage) (d : Demand) (rows : List Nat) : Trace :=
  let r := demandOutFrom plan d.offset d.limit 0 rows
  { outputs := r.outputs, firstFailure := r.failure, evaluated := r.evaluated }

/-! ## Output agreement (unconditional) -/

theorem demand_outputs (plan : List Stage) (off : Nat) (lim : Option Nat)
    (i : Nat) (rows : List Nat) :
    (demandOutFrom plan off lim i rows).outputs =
      consume off lim (valuesBeforeFailure (outcomesFrom plan i rows)) := by
  induction rows generalizing off lim i with
  | nil => simp [demandOutFrom, outcomesFrom, valuesBeforeFailure, consume_append_nil]
  | cons v vs ih =>
      by_cases hd : demandDone off lim
      · have hoff : off = 0 := (of_decide_eq_true hd).1
        have hlim : lim = some 0 := (of_decide_eq_true hd).2
        subst off
        subst lim
        have hdd : demandDone 0 (some 0) = true := by native_decide
        simp [demandOutFrom, hdd, consume]
      · simp [demandOutFrom, hd]
        cases ho : applyPlan plan v with
        | failed e =>
            simpa [outcomesFrom, valuesBeforeFailure, ho, consume_append_nil]
        | skip =>
            simpa [outcomesFrom, valuesBeforeFailure, ho, advOff, advLim] using
              ih (off := off) (lim := lim) (i := i + 1)
        | value w =>
            by_cases hoff : off = 0
            · subst off
              cases hlim : lim with
              | none =>
                  simpa [outcomesFrom, valuesBeforeFailure, ho, advOff, advLim, consume_cons] using
                    ih (off := 0) (lim := none) (i := i + 1)
              | some k =>
                  cases k with
                  | zero =>
                      have hdd : demandDone 0 (some 0) = true := by native_decide
                      rw [hlim] at hd
                      exact (hd hdd).elim
                  | succ k' =>
                      simpa [outcomesFrom, valuesBeforeFailure, ho, advOff, advLim, consume_cons] using
                        ih (off := 0) (lim := some k') (i := i + 1)
            · rcases Nat.exists_eq_succ_of_ne_zero hoff with ⟨o', rfl⟩
              simpa [outcomesFrom, valuesBeforeFailure, ho, advOff, advLim, consume_cons] using
                ih (off := o') (lim := lim) (i := i + 1)

theorem eager_fused_outputs (plan : List Stage) (d : Demand) (rows : List Nat) :
    (chunkEager plan d rows).outputs = (rowDemand plan d rows).outputs := by
  symm
  exact demand_outputs plan d.offset d.limit 0 rows

/-! ## First-failure agreement -/

theorem demand_failure_unlimited (plan : List Stage) (off : Nat) (i : Nat)
    (rows : List Nat) :
    (demandOutFrom plan off none i rows).failure =
      firstFailureFrom (outcomesFrom plan i rows) := by
  induction rows generalizing off i with
  | nil => simp [demandOutFrom, outcomesFrom, firstFailureFrom]
  | cons v vs ih =>
      have hd : demandDone off none = false := by simp [demandDone]
      simp [demandOutFrom, hd]
      cases ho : applyPlan plan v with
      | failed e => simp [outcomesFrom, firstFailureFrom, ho]
      | skip =>
          simpa [outcomesFrom, firstFailureFrom, ho, advOff, advLim] using
            ih (off := off) (i := i + 1)
      | value w =>
          by_cases hoff : off = 0
          · subst off
            simpa [outcomesFrom, firstFailureFrom, ho, advOff, advLim] using
              ih (off := 0) (i := i + 1)
          · rcases Nat.exists_eq_succ_of_ne_zero hoff with ⟨o', rfl⟩
            simpa [outcomesFrom, firstFailureFrom, ho, advOff, advLim] using
              ih (off := o') (i := i + 1)

theorem applyPlan_cons_of_val (s : Stage) (ss : List Stage) (v v' : Nat)
    (h : stageApply s v = StepOut.val v') :
    applyPlan (s :: ss) v = applyPlan ss v' := by
  simp [applyPlan, h]

theorem errorFree_no_failure (plan : List Stage) (hEF : errorFree plan = true)
    (v : Nat) : ∀ e : Err, applyPlan plan v ≠ Outcome.failed e := by
  induction plan generalizing v with
  | nil => intro e h; simp [applyPlan] at h
  | cons s ss ih =>
      cases s with
      | cast ok => simp [errorFree] at hEF
      | filter p =>
          simp [errorFree] at hEF
          intro e h
          cases hs : stageApply (Stage.filter p) v with
          | drop =>
              have : applyPlan (Stage.filter p :: ss) v = Outcome.skip := by
                simp [applyPlan, hs]
              rw [this] at h
              cases h
          | bad er =>
              have hneq : (if p v = true then StepOut.val v else StepOut.drop) ≠ StepOut.bad er := by
                by_cases hp : p v = true <;> simp [hp]
              exact (hneq hs).elim
          | val v' =>
              rw [applyPlan_cons_of_val (Stage.filter p) ss v v' hs] at h
              exact ih hEF v' e h
      | project f =>
          simp [errorFree] at hEF
          intro e h
          cases hs : stageApply (Stage.project f) v with
          | drop => simp [stageApply] at hs
          | bad er => simp [stageApply] at hs
          | val v' =>
              rw [applyPlan_cons_of_val (Stage.project f) ss v v' hs] at h
              exact ih hEF v' e h

theorem errorFree_firstFailure_none (plan : List Stage) (hEF : errorFree plan = true)
    (i : Nat) (rows : List Nat) :
    firstFailureFrom (outcomesFrom plan i rows) = none := by
  induction rows generalizing i with
  | nil => simp [outcomesFrom, firstFailureFrom]
  | cons v vs ih =>
      have hne := errorFree_no_failure plan hEF v
      cases ho : applyPlan plan v with
      | failed e =>
          have : e = Err.castErr := by cases e; rfl
          subst e
          exact (hne Err.castErr ho).elim
      | skip => simpa [outcomesFrom, firstFailureFrom, ho] using ih (i := i + 1)
      | value w => simpa [outcomesFrom, firstFailureFrom, ho] using ih (i := i + 1)

/-- An error-free plan can never record a failure on the demand path either. -/
theorem demand_failure_errorFree (plan : List Stage) (hEF : errorFree plan = true)
    (off : Nat) (lim : Option Nat) (i : Nat) (rows : List Nat) :
    (demandOutFrom plan off lim i rows).failure = none := by
  induction rows generalizing off lim i with
  | nil => simp [demandOutFrom]
  | cons v vs ih =>
      by_cases hd : demandDone off lim
      · simp [demandOutFrom, hd]
      · simp [demandOutFrom, hd]
        cases ho : applyPlan plan v with
        | failed e =>
            have hne := errorFree_no_failure plan hEF v e
            exact (hne ho).elim
        | skip =>
            simpa [advOff, advLim, ho] using ih (off := off) (lim := lim) (i := i + 1)
        | value w =>
            by_cases hoff : off = 0
            · subst off
              simpa [advOff, advLim, ho] using
                ih (off := 0) (lim := advLim 0 lim (Outcome.value w)) (i := i + 1)
            · rcases Nat.exists_eq_succ_of_ne_zero hoff with ⟨o', rfl⟩
              simpa [advOff, advLim, ho] using
                ih (off := o') (lim := lim) (i := i + 1)

/-- Number of rows whose stages the demand path actually evaluates. -/
theorem demand_evaluated_le (plan : List Stage) (off : Nat) (lim : Option Nat)
    (i : Nat) (rows : List Nat) :
    (demandOutFrom plan off lim i rows).evaluated ≤ rows.length := by
  induction rows generalizing off lim i with
  | nil => simp [demandOutFrom]
  | cons v vs ih =>
      by_cases hd : demandDone off lim
      · simp [demandOutFrom, hd]
      · cases ho : applyPlan plan v with
        | failed e =>
            simpa [demandOutFrom, hd, ho] using
              Nat.succ_le_succ (ih (off := advOff off (Outcome.failed e))
                (lim := advLim off lim (Outcome.failed e)) (i := i + 1))
        | skip =>
            simpa [demandOutFrom, hd, ho, advOff, advLim] using
              Nat.succ_le_succ (ih (off := advOff off Outcome.skip)
                (lim := advLim off lim Outcome.skip) (i := i + 1))
        | value w =>
            by_cases hoff : off = 0
            · subst off
              simpa [demandOutFrom, hd, ho, advOff, advLim] using
                Nat.succ_le_succ (ih (off := 0)
                  (lim := advLim 0 lim (Outcome.value w)) (i := i + 1))
            · rcases Nat.exists_eq_succ_of_ne_zero hoff with ⟨o', rfl⟩
              simpa [demandOutFrom, hd, ho, advOff, advLim] using
                Nat.succ_le_succ (ih (off := o') (lim := lim) (i := i + 1))

/-! ## Main theorem -/

/-- **MAIN THEOREM.** For a legal placement, eager and demand-driven execution
    agree on output rows, first failing row and failure timing; the demand
    path never evaluates more rows than the eager path. -/
theorem pull_demand_fusion_sound (plan : List Stage) (d : Demand) (rows : List Nat)
    (hlegal : fusionLegal plan d = true) :
    let e := chunkEager plan d rows
    let f := rowDemand plan d rows
    e.outputs = f.outputs ∧
    e.firstFailure = f.firstFailure ∧
    f.evaluated ≤ e.evaluated := by
  have hcond : d.limit = none ∨ errorFree plan = true := of_decide_eq_true hlegal
  dsimp
  constructor
  · exact eager_fused_outputs plan d rows
  constructor
  · unfold chunkEager rowDemand
    dsimp
    rcases hcond with hnone | hEF
    · rw [hnone]
      rw [demand_failure_unlimited plan d.offset 0 rows]
    · rw [errorFree_firstFailure_none plan hEF 0 rows]
      exact (demand_failure_errorFree plan hEF d.offset d.limit 0 rows).symm
  · unfold chunkEager rowDemand
    dsimp
    exact demand_evaluated_le plan d.offset d.limit 0 rows

/-! ## Rejected case: pushing demand through an error-defining CAST -/

def badPlan : List Stage := [Stage.cast (fun v => v ≠ 2)]

def regressionRows : List Nat := [1, 2]

def regressionDemand : Demand := { offset := 0, limit := some 1 }

/-- Row 1 passes and row 2 is a CAST error.  The eager (whole-chunk) path has
    already evaluated row 2 and records its failure; the row-wise demand path
    stops after row 1 and never sees it. -/
theorem demand_through_cast_counterexample :
    fusionLegal badPlan regressionDemand = false ∧
    (chunkEager badPlan regressionDemand regressionRows).firstFailure =
      some (1, Err.castErr) ∧
    (rowDemand badPlan regressionDemand regressionRows).firstFailure = none := by
  native_decide

/-- An error-free plan with a LIMIT is legal for fusion. -/
theorem legal_error_free_plan :
    fusionLegal [Stage.filter (fun v => v % 2 = 0), Stage.project (fun v => v + 1)]
      regressionDemand = true := by
  native_decide

/-- Explicit LIMIT 0 evaluates nothing on the demand path, while the chunk
    path has already prefetched the whole chunk and observes its error. -/
theorem limit_zero_counterexample :
    fusionLegal badPlan { offset := 0, limit := some 0 } = false ∧
    (chunkEager badPlan { offset := 0, limit := some 0 } regressionRows).firstFailure =
      some (1, Err.castErr) ∧
    (rowDemand badPlan { offset := 0, limit := some 0 } regressionRows).firstFailure = none := by
  native_decide

/-- OFFSET with no LIMIT is legal even with a CAST: both paths consume the
    whole input and record the same first failure. -/
theorem offset_without_limit_legal :
    fusionLegal badPlan { offset := 1, limit := none } = true ∧
    (chunkEager badPlan { offset := 1, limit := none } regressionRows).firstFailure =
      (rowDemand badPlan { offset := 1, limit := none } regressionRows).firstFailure := by
  native_decide

end TinylambOpt.Research.PullDemandFusion

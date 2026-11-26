import Std.Tactic

/-!
One lane of the batch.

What the reference reports for one batch. `carry r` means "no output, no error, r
passing lanes still needed in the next batch".

Named results include `firstErrorFrom_ge`, `kthPassFrom_ge`,
`refIdx_eq_simdDecisionFrom`.
-/
-- C++ counterpart: expression/jit.cpp (see issues/README.md).

/-! LIMIT error frontier: when a row-ordered reference stops after K passing
    lanes, a batched decision can be taken from two lane masks alone.

    Each lane is one of `.skip` (no effect), `.pass v` (emits output `v`), or
    `.error e` (raises error `e`).  The reference scans lanes in row order and
    stops immediately either at the first error lane or at the K-th passing
    lane; if the batch ends before either, the remaining demand is carried to
    the next batch.  K = 0 evaluates nothing.

    The SIMD decision computes two indices from the batch: the index of the
    *first error* lane (the error mask) and the index of the *K-th pass* lane
    (the pass mask).  Whichever index is smaller determines the result: an
    error before the K-th pass is reported, otherwise the K-th pass is emitted.
    If neither exists, the remaining demand is carried.  The masks are pure
    predicates over lane values — lanes after the stopping point are inspected
    but never evaluated for output or side effects, which is the caller's
    "later lanes are side-effect free" condition. -/

namespace TinylambOpt.LimitErrorFrontier

/-- One lane of the batch. -/
inductive Lane (out err : Type) where
  | skip
  | pass (value : out)
  | error (reason : err)
  deriving DecidableEq, Repr

/-- What the reference reports for one batch.  `carry r` means "no output,
    no error, r passing lanes still needed in the next batch". -/
inductive Result (out err : Type) where
  | done (value : out)
  | failed (reason : err)
  | carry (remaining : Nat)
  deriving DecidableEq, Repr

/-! ## Reference: ordered scan with carried demand -/

/-- `budget` is the number of passing lanes still needed.  Stops on the first
    error, or emits the last needed pass.  A budget of 0 evaluates no lane. -/
def reference {out err : Type} (budget : Nat) (lanes : List (Lane out err)) : Result out err :=
  match budget with
  | 0 => Result.carry 0
  | b + 1 =>
      match lanes with
      | [] => Result.carry (b + 1)
      | Lane.error e :: _ => Result.failed e
      | Lane.skip :: rest => reference (b + 1) rest
      | Lane.pass v :: rest => if b = 0 then Result.done v else reference b rest

/-! ## Masks and the SIMD decision -/

/-- Index (from `i`) and reason of the first error lane, or `none`. -/
def firstErrorFrom {out err : Type} (i : Nat) : List (Lane out err) → Option (Nat × err)
  | [] => none
  | Lane.error e :: _ => some (i, e)
  | _ :: rest => firstErrorFrom (i + 1) rest

/-- Index (from `i`) and value of the `k+1`-st passing lane, or `none`. -/
def kthPassFrom {out err : Type} (k i : Nat) : List (Lane out err) → Option (Nat × out)
  | [] => none
  | Lane.pass v :: rest => if k = 0 then some (i, v) else kthPassFrom (k - 1) (i + 1) rest
  | _ :: rest => kthPassFrom k (i + 1) rest

def countPass {out err : Type} : List (Lane out err) → Nat
  | [] => 0
  | Lane.pass _ :: rest => countPass rest + 1
  | _ :: rest => countPass rest

def firstError {out err : Type} (lanes : List (Lane out err)) : Option (Nat × err) :=
  firstErrorFrom 0 lanes

def kthPass {out err : Type} (k : Nat) (lanes : List (Lane out err)) : Option (Nat × out) :=
  kthPassFrom k 0 lanes

/-- Indexed version of the SIMD decision used in the induction.  `b + 1` is
    the budget; `i` is the absolute index of the current lane. -/
def simdDecisionFrom {out err : Type} (b i : Nat) (lanes : List (Lane out err)) : Result out err :=
  match firstErrorFrom i lanes, kthPassFrom b i lanes with
  | some (ie, e), some (ik, v) => if ie < ik then Result.failed e else Result.done v
  | some (_, e), none => Result.failed e
  | none, some (_, v) => Result.done v
  | none, none => Result.carry ((b + 1) - countPass lanes)

/-- SIMD decision for one batch from the two mask indices. -/
def simdDecision {out err : Type} (budget : Nat) (lanes : List (Lane out err)) : Result out err :=
  if budget = 0 then Result.carry 0
  else
    match firstError lanes, kthPass (budget - 1) lanes with
    | some (ie, e), some (ik, v) => if ie < ik then Result.failed e else Result.done v
    | some (_, e), none => Result.failed e
    | none, some (_, v) => Result.done v
    | none, none => Result.carry (budget - countPass lanes)

/-! ## Index bounds of the two masks -/

theorem firstErrorFrom_ge {out err : Type} (i : Nat) (lanes : List (Lane out err)) :
    ∀ j e, firstErrorFrom i lanes = some (j, e) → i ≤ j := by
  induction lanes generalizing i with
  | nil => simp [firstErrorFrom]
  | cons lane rest ih =>
      intro j e h
      cases lane with
      | error er =>
          simp [firstErrorFrom] at h
          rcases h with ⟨rfl, rfl⟩
          omega
      | skip =>
          simp [firstErrorFrom] at h
          have := ih (i + 1) j e h
          omega
      | pass v =>
          simp [firstErrorFrom] at h
          have := ih (i + 1) j e h
          omega

theorem kthPassFrom_ge {out err : Type} (k i : Nat) (lanes : List (Lane out err)) :
    ∀ j v, kthPassFrom k i lanes = some (j, v) → i ≤ j := by
  induction lanes generalizing i k with
  | nil => simp [kthPassFrom]
  | cons lane rest ih =>
      intro j v' h
      cases lane with
      | pass v =>
          by_cases hk : k = 0
          · subst k
            simp [kthPassFrom] at h
            rcases h with ⟨rfl, rfl⟩
            omega
          · simp [kthPassFrom, hk] at h
            have := ih (k - 1) (i + 1) j v' h
            omega
      | skip =>
          simp [kthPassFrom] at h
          have := ih k (i + 1) j v' h
          omega
      | error e =>
          simp [kthPassFrom] at h
          have := ih k (i + 1) j v' h
          omega

/-! ## Reference = SIMD decision -/

theorem refIdx_eq_simdDecisionFrom {out err : Type}
    (b i : Nat) (lanes : List (Lane out err)) :
    reference (b + 1) lanes = simdDecisionFrom b i lanes := by
  induction lanes generalizing i b with
  | nil =>
      simp [reference, simdDecisionFrom, firstErrorFrom, kthPassFrom, countPass]
  | cons lane rest ih =>
      cases lane with
      | skip =>
          simp [reference, simdDecisionFrom, firstErrorFrom, kthPassFrom, countPass]
          exact ih b (i + 1)
      | error e =>
          simp [reference, simdDecisionFrom, firstErrorFrom, kthPassFrom]
          cases hkp : kthPassFrom b (i + 1) rest with
          | none => simp
          | some kv =>
              obtain ⟨ik, v⟩ := kv
              have hge : i + 1 ≤ ik := kthPassFrom_ge b (i + 1) rest ik v hkp
              have hlt : i < ik := by omega
              simp [hlt]
      | pass v =>
          cases b with
          | zero =>
              simp [reference, simdDecisionFrom, firstErrorFrom, kthPassFrom]
              cases hfe : firstErrorFrom (i + 1) rest with
              | none => simp
              | some fe =>
                  obtain ⟨ie, er⟩ := fe
                  have hge : i + 1 ≤ ie := firstErrorFrom_ge (i + 1) rest ie er hfe
                  have hnlt : ¬ ie < i := by omega
                  simp [hnlt]
          | succ b' =>
              simp [reference, simdDecisionFrom, firstErrorFrom, kthPassFrom, countPass]
              exact ih b' (i + 1)

/-- **Reference = SIMD decision**, including `K = 0` (no lane evaluated). -/
theorem limit_error_frontier_sound {out err : Type}
    (budget : Nat) (lanes : List (Lane out err)) :
    reference budget lanes = simdDecision budget lanes := by
  cases budget with
  | zero => simp [reference, simdDecision]
  | succ b =>
      simpa [simdDecision, simdDecisionFrom, firstError, kthPass] using
        (refIdx_eq_simdDecisionFrom b 0 lanes)

/-! ## Multiple batches with carried remaining demand -/

/-- Process a sequence of batches.  `done`/`failed` terminate; `carry r`
    continues with `r` as the next batch's budget. -/
def runBatchesRef {out err : Type} : Nat → List (List (Lane out err)) → Result out err
  | _, [] => Result.carry 0
  | budget, batch :: rest =>
      match reference budget batch with
      | Result.carry r => runBatchesRef r rest
      | other => other

def runBatchesSimd {out err : Type} : Nat → List (List (Lane out err)) → Result out err
  | _, [] => Result.carry 0
  | budget, batch :: rest =>
      match simdDecision budget batch with
      | Result.carry r => runBatchesSimd r rest
      | other => other

/-- The frontier decision composes across batches: carried demand is passed
    unchanged, so the multi-batch results agree too. -/
theorem limit_error_frontier_batches_sound {out err : Type}
    (budget : Nat) (batches : List (List (Lane out err))) :
    runBatchesRef budget batches = runBatchesSimd budget batches := by
  induction batches generalizing budget with
  | nil => rfl
  | cons batch rest ih =>
      rw [runBatchesRef, runBatchesSimd]
      rw [← limit_error_frontier_sound budget batch]
      cases hres : reference budget batch with
      | done v => simp
      | failed e => simp
      | carry r => simpa [hres] using ih r

/-! ## Boundary examples -/

theorem limit_zero_evaluates_nothing :
    reference 0 [Lane.pass (7 : Nat), Lane.error (9 : Nat)] = Result.carry 0 ∧
    simdDecision 0 [Lane.pass (7 : Nat), Lane.error (9 : Nat)] = Result.carry 0 := by
  decide

/-- K = 1, lane 0 passes and lane 1 errors: the pass precedes the error. -/
theorem pass_then_error_at_k1 :
    reference 1 [Lane.pass (7 : Nat), Lane.error (9 : Nat)] = Result.done 7 ∧
    simdDecision 1 [Lane.pass (7 : Nat), Lane.error (9 : Nat)] = Result.done 7 := by
  decide

/-- K = 1 with the order reversed: the error precedes the pass. -/
theorem error_then_pass_at_k1 :
    reference 1 [Lane.error (9 : Nat), Lane.pass (7 : Nat)] = Result.failed 9 ∧
    simdDecision 1 [Lane.error (9 : Nat), Lane.pass (7 : Nat)] = Result.failed 9 := by
  decide

/-- First batch leaves one unit of demand; the next batch errors before its
    single pass, so the error is reported in the second batch. -/
theorem carried_demand_batches :
    runBatchesRef 2 [[Lane.pass (1 : Nat)],
      [Lane.error (5 : Nat), Lane.pass (2 : Nat)]] = Result.failed 5 ∧
    runBatchesSimd 2 [[Lane.pass (1 : Nat)],
      [Lane.error (5 : Nat), Lane.pass (2 : Nat)]] = Result.failed 5 := by
  decide

def emitted : Nat → List (Lane α ε) → List α
  | 0, _ | _, [] => []
  | _+1, .error _ :: _ => []
  | k+1, .skip :: xs => emitted (k+1) xs
  | k+1, .pass v :: xs => v :: emitted k xs

def passing : Lane α ε → Option α
  | .pass v => some v
  | _ => none

def beforeError : Lane α ε → Bool
  | .error _ => false
  | _ => true

/-- Gather all passing lanes before the first error, capped by remaining
    demand. The observation includes rows delivered before failure. -/
theorem limit_rows_and_error (k : Nat) (xs : List (Lane α ε)) :
    (emitted k xs, reference k xs) =
      ((((xs.takeWhile beforeError).filterMap passing).take k), simdDecision k xs) := by
  rw [limit_error_frontier_sound]
  congr 1
  induction xs generalizing k with
  | nil => cases k <;> rfl
  | cons x xs ih =>
    cases k <;> cases x <;> simp [emitted, List.takeWhile, List.filterMap, passing, beforeError, ih]

/-- Each batch contributes its delivered rows before the consumer either
    stops (also on error) or requests the next batch with residual demand. -/
def collectBatches (step : Nat → List (Lane α ε) → List α × Result α ε)
    (k : Nat) : List (List (Lane α ε)) → List α × Result α ε
  | [] => ([], .carry k)
  | batch :: rest =>
    let (out, status) := step k batch
    match status with
    | .carry (n+1) =>
      let (more, last) := collectBatches step (n+1) rest
      (out ++ more,last)
    | _ => (out,status)

theorem limit_batch_trace (k : Nat) (batches : List (List (Lane α ε))) :
    collectBatches (fun n xs => (emitted n xs, reference n xs)) k batches =
    collectBatches (fun n xs =>
      (((xs.takeWhile beforeError).filterMap passing).take n, simdDecision n xs)) k batches := by
  have eq (n : Nat) (xs : List (Lane α ε)) := limit_rows_and_error n xs
  exact congrArg (fun step => collectBatches step k batches) (funext fun n => funext (eq n))

example :
    let batches : List (List (Lane Nat Nat)) := [[.pass 4,.skip],[.pass 8,.error 9,.pass 12]]
    collectBatches (fun n xs => (emitted n xs,reference n xs)) 3 batches = ([4,8],.failed 9) ∧
    collectBatches (fun n xs => (emitted n xs,reference n xs)) 2 batches = ([4,8],.done 8) := by
  decide

end TinylambOpt.LimitErrorFrontier

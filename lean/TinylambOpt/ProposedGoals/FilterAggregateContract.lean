import Std

namespace TinylambOpt.ProposedGoals.FilterAggregateContract

inductive TV where | t | f | u deriving DecidableEq

inductive Result where
  | ok (sum : Option Int)
  | failed (ordinal : Nat)
  deriving DecidableEq

abbrev Pred := Option Int → Option TV -- none = SQL error at that row
abbrev Rows := List (Option Int)

def addVal (acc : Option Int) (v : Option Int) : Option Int :=
  match v with | none => acc | some x => some (acc.getD 0 + x)

/-- Reference: evaluate the FILTER predicate in row order, stop at the first
    error, and add only rows classified TRUE. -/
def scan : Pred → Rows → Nat → Result
  | _, [], _ => .ok none
  | p, v :: vs, i =>
    match p v with
    | none => .failed i
    | some TV.t =>
      match scan p vs (i + 1) with
      | .ok s => .ok (addVal s v)
      | e => e
    | _ => scan p vs (i + 1)

/-- Staged path: first materialize the predicate outcomes for the whole batch,
    then fold values. Speculative evaluation after the first error is harmless
    for deterministic predicates (it only must not change the observation). -/
def staged (p : Pred) (rows : Rows) : Result :=
  let outcomes := rows.map (fun v => (v, p v))
  match outcomes.findIdx? (fun x => x.2 == none) with
  | some i => .failed i
  | none =>
    .ok (List.foldl (fun s v => addVal s v) none
      ((outcomes.filter (fun x => x.2 == some TV.t)).map Prod.fst))

def shift : Result → Nat → Result
  | .ok s, _ => .ok s
  | .failed i, k => .failed (i + k)

/-- Parallel plan: evaluate a chunk, merge with the suffix result by shifting
    the suffix failure ordinal by the chunk length. -/
def merged (p : Pred) (pre suf : Rows) : Result :=
  match scan p pre 0 with
  | .failed i => .failed i
  | .ok s =>
    match scan p suf 0 with
    | .failed j => .failed (j + pre.length)
    | .ok t => .ok (addVal t s)

/-- Staged evaluation preserves value, NULL and first error; the FILTER
    aggregate is chunk-mergeable under the row-order contract. -/
def Goal : Prop :=
  ∀ (p : Pred) (rows : Rows),
    staged p rows = scan p rows 0 ∧
    ∀ (pre suf : Rows), pre ++ suf = rows →
      merged p pre suf = scan p rows 0

example : scan (fun v => if v == some 0 then none else some TV.t)
    [some 2, some 0, some 3] 0 = .failed 1 := by decide

example : staged (fun v => if v == some 0 then none else some TV.t)
    [some 2, some 0, some 3] = .failed 1 := by decide

example : scan (fun v => if v == some 3 then some TV.u else some TV.t)
    [some 1, some 3, some 5] 0 = .ok (some 6) := by decide

end TinylambOpt.ProposedGoals.FilterAggregateContract

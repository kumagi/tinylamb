import Std

/-!
Initializing `SUM` accumulators to `some 0` instead of `none` breaks SQL semantics on
empty branch matches.

Modeled claims

- `Goal` — Single-pass split-filter CTE scan (`query/tpch_select_rewrites.cpp`
  `__merged_scalar_agg`, TPC-DS Q90/Q9/Q28/Q88): pre-filtering by Kleene 3VL `base
  AND (p_1 OR ... OR p_k)` and conditionally accumulating all branches in one pass
  equals `k` separate scans (even when `ps` overlap or contain NULL predicates).
-/

namespace TinylambOpt.ResearchGoals.CteSinglePassSplitFilter

def isTrue3 : Option Bool → Bool
  | some true => true
  | _ => false

def sqlAnd (a b : Option Bool) : Option Bool :=
  match a, b with
  | some false, _ => some false
  | _, some false => some false
  | some true, some true => some true
  | _, _ => none

def sqlOr (a b : Option Bool) : Option Bool :=
  match a, b with
  | some true, _ => some true
  | _, some true => some true
  | some false, some false => some false
  | _, _ => none

def combineSumOpt (cur v : Option Int) : Option Int :=
  match cur, v with
  | none, x => x
  | some s, none => some s
  | some s, some x => some (s + x)

def multiPassBranch {α : Type} (rows : List α) (base p : α → Option Bool)
    (measure : α → Option Int) : Nat × Option Int :=
  rows.foldl
    (fun (cnt, sumAcc) r =>
      if isTrue3 (sqlAnd (base r) (p r)) then
        (cnt + 1, combineSumOpt sumAcc (measure r))
      else
        (cnt, sumAcc))
    (0, none)

def multiPassRef {α : Type} (rows : List α) (base : α → Option Bool)
    (ps : List (α → Option Bool)) (measure : α → Option Int) : List (Nat × Option Int) :=
  ps.map (fun p => multiPassBranch rows base p measure)

def disjPred {α : Type} (ps : List (α → Option Bool)) (r : α) : Option Bool :=
  ps.foldl (fun acc p => sqlOr acc (p r)) (some false)

def updateBranches {α : Type} (r : α) (m : α → Option Int)
    (ps : List (α → Option Bool)) (accs : List (Nat × Option Int)) :
    List (Nat × Option Int) :=
  (ps.zip accs).map fun (p, (cnt, sumAcc)) =>
    if isTrue3 (p r) then (cnt + 1, combineSumOpt sumAcc (m r)) else (cnt, sumAcc)

def singlePassSplit {α : Type} (rows : List α) (base : α → Option Bool)
    (ps : List (α → Option Bool)) (measure : α → Option Int) : List (Nat × Option Int) :=
  let init := List.replicate ps.length (0, (none : Option Int))
  let surviving := rows.filter (fun r => isTrue3 (sqlAnd (base r) (disjPred ps r)))
  surviving.foldl (fun accs r => updateBranches r measure ps accs) init

/-- Single-pass split-filter CTE scan (`query/tpch_select_rewrites.cpp`
    `__merged_scalar_agg`, TPC-DS Q90/Q9/Q28/Q88): pre-filtering by Kleene 3VL
    `base AND (p_1 OR ... OR p_k)` and conditionally accumulating all branches in
    one pass equals `k` separate scans (even when `ps` overlap or contain
    NULL predicates). -/
def Goal : Prop :=
  ∀ {α : Type} (rows : List α) (base : α → Option Bool)
    (ps : List (α → Option Bool)) (measure : α → Option Int),
    singlePassSplit rows base ps measure = multiPassRef rows base ps measure

/-- Initializing `SUM` accumulators to `some 0` instead of `none` breaks SQL
    semantics on empty branch matches. -/
def badSinglePassZeroSum {α : Type} (rows : List α) (base : α → Option Bool)
    (ps : List (α → Option Bool)) (measure : α → Option Int) : List (Nat × Option Int) :=
  let init := List.replicate ps.length (0, some (0 : Int))
  let surviving := rows.filter (fun r => isTrue3 (sqlAnd (base r) (disjPred ps r)))
  surviving.foldl (fun accs r => updateBranches r measure ps accs) init

private def sampleRows : List (Option Bool × Int × Option Int) :=
  [ (some true, 8, some 10)
  , (some true, 9, none)
  , (none, 8, some 99)
  , (some true, 25, some 50)
  ]
private def bPred (r : Option Bool × Int × Option Int) : Option Bool := r.1
private def mVal (r : Option Bool × Int × Option Int) : Option Int := r.2.2
private def branchPs : List ((Option Bool × Int × Option Int) → Option Bool) :=
  [ fun r => some (8 ≤ r.2.1 && r.2.1 ≤ 9)
  , fun r => if r.2.1 == 9 then some true else none
  , fun r => some (r.2.1 == 999)
  ]

example :
    singlePassSplit sampleRows bPred branchPs mVal = [(2, some 10), (1, none), (0, none)] ∧
    multiPassRef sampleRows bPred branchPs mVal = [(2, some 10), (1, none), (0, none)] := by
  decide

example :
    badSinglePassZeroSum sampleRows bPred branchPs mVal =
      [(2, some 10), (1, some 0), (0, some 0)] := by
  decide

end TinylambOpt.ResearchGoals.CteSinglePassSplitFilter

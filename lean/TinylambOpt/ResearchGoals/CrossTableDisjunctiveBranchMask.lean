import Std

/-!
Optimized build-time bitmask pruning + single bitwise `&` probe test.

Modeled claims

- `Goal` — Cross-table disjunctive branch bitmask reduction (TPC-DS Q48/Q15/Q79): under
  Kleene 3VL `(pD_0 ∧ pF_0) ∨ ... ∨ (pD_{k-1} ∧ pF_{k-1})`, precomputing bit `j` on
  dimension `D` (`pD_j = some true`), dropping zero-mask dimension rows before
  joining, and probing fact rows via `(maskD &&& maskF) ≠ 0` reproduces exact join
  output even when branch predicates evaluate to `NULL` (`none`).
-/

namespace TinylambOpt.ResearchGoals.CrossTableDisjunctiveBranchMask

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

def isTrue3 : Option Bool → Bool
  | some true => true
  | _ => false

def evalBranches {D F : Type} (branches : List ((D → Option Bool) × (F → Option Bool)))
    (d : D) (f : F) : Option Bool :=
  branches.foldl (fun acc (pD, pF) => sqlOr acc (sqlAnd (pD d) (pF f))) (some false)

def refJoin {D F : Type} (ds : List D) (fs : List F)
    (branches : List ((D → Option Bool) × (F → Option Bool))) : List (D × F) :=
  ds.flatMap fun d =>
    fs.filterMap fun f =>
      if isTrue3 (evalBranches branches d f) then some (d, f) else none

def maskOfPreds {α : Type} (preds : List (α → Option Bool)) (a : α) : Nat :=
  preds.zipIdx.foldl
    (fun m (p, idx) => if p a == some true then m ||| (1 <<< idx) else m)
    0

/-- Optimized build-time bitmask pruning + single bitwise `&` probe test. -/
def optMaskedJoin {D F : Type} (ds : List D) (fs : List F)
    (branches : List ((D → Option Bool) × (F → Option Bool))) : List (D × F) :=
  let dPreds := branches.map Prod.fst
  let fPreds := branches.map Prod.snd
  let dsMasked := (ds.map (fun d => (d, maskOfPreds dPreds d))).filter (fun p => p.2 != 0)
  let fsMasked := (fs.map (fun f => (f, maskOfPreds fPreds f))).filter (fun p => p.2 != 0)
  dsMasked.flatMap fun (d, md) =>
    fsMasked.filterMap fun (f, mf) =>
      if (md &&& mf) != 0 then some (d, f) else none

/-- Cross-table disjunctive branch bitmask reduction (TPC-DS Q48/Q15/Q79):
    under Kleene 3VL `(pD_0 ∧ pF_0) ∨ ... ∨ (pD_{k-1} ∧ pF_{k-1})`, precomputing
    bit `j` on dimension `D` (`pD_j = some true`), dropping zero-mask dimension
    rows before joining, and probing fact rows via `(maskD &&& maskF) ≠ 0`
    reproduces exact join output even when branch predicates evaluate to
    `NULL` (`none`). -/
def Goal : Prop :=
  ∀ {D F : Type} (ds : List D) (fs : List F)
    (branches : List ((D → Option Bool) × (F → Option Bool))),
    optMaskedJoin ds fs branches = refJoin ds fs branches

private def sampleBranches :
    List ((Option Bool × Option Bool → Option Bool) × (Option Bool × Option Bool → Option Bool)) :=
  [ (Prod.fst, Prod.fst), (Prod.snd, Prod.snd) ]

example :
    let ds : List (Option Bool × Option Bool) :=
      [(some true, some false), (none, none), (some false, some true)]
    let fs : List (Option Bool × Option Bool) :=
      [(some true, none), (none, some true), (some false, some false)]
    optMaskedJoin ds fs sampleBranches = refJoin ds fs sampleBranches := by
  decide

example :
    let ds : List (Option Bool × Option Bool) := [(none, some false)]
    let fs : List (Option Bool × Option Bool) := [(some true, some true)]
    optMaskedJoin ds fs sampleBranches = [] ∧ refJoin ds fs sampleBranches = [] := by
  decide

end TinylambOpt.ResearchGoals.CrossTableDisjunctiveBranchMask

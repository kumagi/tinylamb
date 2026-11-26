import Std

/-!
Arbitrary-depth, arbitrary-fanout acyclic join tree where `children[idx]` equi-joins
against `row.childKeys.getD idx none`.

Reference full flat join-tuple expansion (`flatMap` cartesian product) for a child
branch matching `parentKey = some pk` (empty when `expectedParent = none`).

Modeled claims

- `Goal` — Factorized join-tree bottom-up count propagation for acyclic multi-way
  `COUNT(*)` joins (LDBC L1 / JOB J4, J5 / STATS-CEB C1, C2, C5 / LDBC L2, L5 /
  DictionaryJoinsAndLateLabels B3): propagating per-key subtree multiplicities
  bottom-up without materializing intermediate M:N join tuples matches flattening the
  full join tree (`refRootTuples`) for every group `g` on any tree shape, including
  NULL join keys and unmatched dead branches (zero weight absorption).
-/

namespace TinylambOpt.ResearchGoals.FactorizedJoinTreeCountPropagation

structure NodeRow where
  payload : Int
  parentKey : Option Int
  childKeys : List (Option Int)
  deriving DecidableEq

/-- Arbitrary-depth, arbitrary-fanout acyclic join tree where `children[idx]`
    equi-joins against `row.childKeys.getD idx none`. -/
inductive JoinTree where
  | mk : List NodeRow → List JoinTree → JoinTree

mutual

/-- Reference full flat join-tuple expansion (`flatMap` cartesian product) for a
    child branch matching `parentKey = some pk` (empty when `expectedParent = none`). -/
def refSubtreeTuples (expectedParent : Option Int) : JoinTree → List Unit
  | .mk rows children =>
      match expectedParent with
      | none => []
      | some pk =>
          rows.flatMap fun row =>
            if row.parentKey == some pk then
              refChildrenCartesian row.childKeys 0 children
            else []

def refChildrenCartesian (rowKeys : List (Option Int)) (idx : Nat) :
    List JoinTree → List Unit
  | [] => [()]
  | child :: rest =>
      (refSubtreeTuples (rowKeys.getD idx none) child).flatMap fun _ =>
        refChildrenCartesian rowKeys (idx + 1) rest

/-- Full materialized multiset of root payloads produced by flattening every
    equi-join in `t`. -/
def refRootTuples : JoinTree → List Int
  | .mk rows children =>
      rows.flatMap fun row =>
        (refChildrenCartesian row.childKeys 0 children).map fun _ => row.payload

end

/-- Reference full-expansion group count for `payload == g`. -/
def refCount (t : JoinTree) (g : Int) : Nat :=
  ((refRootTuples t).filter (· == g)).length

mutual

/-- Bottom-up factorized subtree multiplicity for `parentKey = some pk` (returns
    0 if `pk = none`, without materializing child tuples). -/
def optKeyWeight (expectedParent : Option Int) : JoinTree → Nat
  | .mk rows children =>
      match expectedParent with
      | none => 0
      | some pk =>
          rows.foldl
            (fun acc row =>
              if row.parentKey == some pk then
                acc + optChildrenProd row.childKeys 0 children
              else acc)
            0

def optChildrenProd (rowKeys : List (Option Int)) (idx : Nat) : List JoinTree → Nat
  | [] => 1
  | child :: rest =>
      optKeyWeight (rowKeys.getD idx none) child *
        optChildrenProd rowKeys (idx + 1) rest

/-- Bottom-up tree count propagation: computes scalar multiplicities from leaves
    up to root without materializing intermediate cartesian products, then
    aggregates root `payload == g`. -/
def optRootCount (t : JoinTree) (g : Int) : Nat :=
  match t with
  | .mk rows children =>
      rows.foldl
        (fun acc row =>
          if row.payload == g then
            acc + optChildrenProd row.childKeys 0 children
          else acc)
        0

end

/-- Factorized join-tree bottom-up count propagation for acyclic multi-way
    `COUNT(*)` joins (LDBC L1 / JOB J4, J5 / STATS-CEB C1, C2, C5 / LDBC L2, L5
    / DictionaryJoinsAndLateLabels B3): propagating per-key subtree multiplicities bottom-up without
    materializing intermediate M:N join tuples matches flattening the full join
    tree (`refRootTuples`) for every group `g` on any tree shape, including
    NULL join keys and unmatched dead branches (zero weight absorption). -/
def Goal : Prop :=
  ∀ (t : JoinTree) (g : Int),
    optRootCount t g = refCount t g

private def leafMsg : JoinTree :=
  .mk
    [ { payload := 0, parentKey := some 100, childKeys := [] }
    , { payload := 0, parentKey := some 100, childKeys := [] }
    , { payload := 0, parentKey := some 200, childKeys := [] }
    , { payload := 0, parentKey := none, childKeys := [] }
    ]
    []

private def midLikes : JoinTree :=
  .mk
    [ { payload := 0, parentKey := some 10, childKeys := [some 100] }
    , { payload := 0, parentKey := some 10, childKeys := [some 100] }
    , { payload := 0, parentKey := some 10, childKeys := [some 999] } -- dead leaf
    , { payload := 0, parentKey := some 10, childKeys := [none] }     -- NULL fk
    ]
    [leafMsg]

private def branchPlace : JoinTree :=
  .mk
    [ { payload := 0, parentKey := some 1, childKeys := [] }
    , { payload := 0, parentKey := some 2, childKeys := [] }
    ]
    []

private def rootChainStar : JoinTree :=
  .mk
    [ { payload := 42, parentKey := none, childKeys := [some 10, some 1] }
    , { payload := 42, parentKey := none, childKeys := [some 10, some 2] }
    , { payload := 42, parentKey := none, childKeys := [some 10, some 3] } -- missing place
    , { payload := 7, parentKey := some 99, childKeys := [some 10, some 1] }
    ]
    [midLikes, branchPlace]

example :
    optRootCount rootChainStar 42 = refCount rootChainStar 42 ∧
    optRootCount rootChainStar 42 = 8 ∧
    optRootCount rootChainStar 7 = 4 ∧
    optRootCount rootChainStar 999 = 0 := by
  decide

example :
    let emptyRoot : JoinTree := .mk [] [midLikes]
    optRootCount emptyRoot 1 = refCount emptyRoot 1 ∧
    optRootCount emptyRoot 1 = 0 := by
  decide

end TinylambOpt.ResearchGoals.FactorizedJoinTreeCountPropagation

import Std

/-!
These claims compare an equi-join with joins whose inputs are restricted by the opposite
side's keys. A SQL plan can reduce the input while retaining join multiplicities when
the key conditions hold.

Modeled claims

- `Goal` — Bidirectional multi-hop semijoin filter propagation (TPC-H Q07/Q08, TPC-DS
  Q17/Q25/Q29/Q58/Q83) preserves exact join tuples & multiplicity across NULLs and
  duplicate keys; factoring symmetric OR pairs `(a=x∧b=y)∨(a=y∧b=x)` into unilateral
  domain filters + `a≠b` is exact iff `x ≠ y`.
-/

namespace TinylambOpt.ResearchGoals.BidirectionalEquiJoinSemijoin

structure RowA where
  ka : Option Int
  pa : Int
  deriving DecidableEq

structure RowB where
  kb1 : Option Int
  kb2 : Option Int
  pb : Int
  deriving DecidableEq

structure RowC where
  kc : Option Int
  pc : Int
  deriving DecidableEq

abbrev Triple := RowA × RowB × RowC

def refJoinChain (as : List RowA) (bs : List RowB) (cs : List RowC) : List Triple :=
  as.flatMap fun a =>
    bs.flatMap fun b =>
      cs.filterMap fun c =>
        match a.ka, b.kb1, b.kb2, c.kc with
        | some x, some y, some z, some w =>
            if x == y && z == w then some (a, b, c) else none
        | _, _, _, _ => none

def memKey (x : Option Int) (keys : List Int) : Bool :=
  match x with
  | none => false
  | some k => keys.contains k

def optBidirectionalSemijoinChain (as : List RowA) (bs : List RowB) (cs : List RowC) :
    List Triple :=
  let keysA := as.filterMap RowA.ka
  let keysC := cs.filterMap RowC.kc
  let bs' := bs.filter (fun b => memKey b.kb1 keysA && memKey b.kb2 keysC)
  let keysB1 := bs'.filterMap RowB.kb1
  let keysB2 := bs'.filterMap RowB.kb2
  let as' := as.filter (fun a => memKey a.ka keysB1)
  let cs' := cs.filter (fun c => memKey c.kc keysB2)
  refJoinChain as' bs' cs'

def symPairRef (x y a b : Int) : Bool :=
  (a == x && b == y) || (a == y && b == x)

def symPairFactored (x y a b : Int) : Bool :=
  (a == x || a == y) && (b == x || b == y) && (a != b)

/-- Bidirectional multi-hop semijoin filter propagation (TPC-H Q07/Q08, TPC-DS
    Q17/Q25/Q29/Q58/Q83) preserves exact join tuples & multiplicity across
    NULLs and duplicate keys; factoring symmetric OR pairs `(a=x∧b=y)∨(a=y∧b=x)`
    into unilateral domain filters + `a≠b` is exact iff `x ≠ y`. -/
def Goal : Prop :=
  (∀ (as : List RowA) (bs : List RowB) (cs : List RowC),
    optBidirectionalSemijoinChain as bs cs = refJoinChain as bs cs) ∧
  (∀ (x y a b : Int), x ≠ y → symPairFactored x y a b = symPairRef x y a b)

example :
    let as : List RowA := [⟨none, 1⟩, ⟨some 10, 2⟩, ⟨some 20, 3⟩]
    let bs : List RowB := [⟨some 10, some 2, 7⟩, ⟨some 20, none, 8⟩, ⟨some 10, some 3, 9⟩]
    let cs : List RowC := [⟨some 2, 100⟩, ⟨none, 200⟩]
    optBidirectionalSemijoinChain as bs cs = refJoinChain as bs cs ∧
    (optBidirectionalSemijoinChain as bs cs).length = 1 := by
  decide

example :
    symPairFactored 5 9 9 5 = symPairRef 5 9 9 5 ∧
    symPairFactored 5 5 5 5 = false ∧
    symPairRef 5 5 5 5 = true := by
  decide

end TinylambOpt.ResearchGoals.BidirectionalEquiJoinSemijoin

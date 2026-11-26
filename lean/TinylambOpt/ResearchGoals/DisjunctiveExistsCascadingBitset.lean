import Std

/-!
Cascading bitset-narrowing evaluation for `EXISTS(S1) AND (EXISTS(S2) OR EXISTS(S3))`
(TPC-DS Q10/Q35): filter candidates by `S1`, partition into hits in `S2` vs remaining
candidates `B_rem = B_1 \ B_2`, and probe `S3` ONLY for `B_rem`.

Modeled claims

- `Goal` — Disjunctive `EXISTS` cascading short-circuit bitset narrowing (TPC-DS
  Q10/Q35): rows satisfying `EXISTS(S1) ∧ EXISTS(S2)` never probe `S3`, preserving
  exact filtered output across NULLs while executing zero `S3` probes whenever `B_1 ⊆
  B_2`.
-/

namespace TinylambOpt.ResearchGoals.DisjunctiveExistsCascadingBitset

def optExistsIn (ok : Option Int) (s : List (Option Int)) : Bool :=
  match ok with
  | none => false
  | some k => s.contains (some k)

def refFilter (c s1 s2 s3 : List (Option Int)) : List (Option Int) :=
  c.filter (fun k => optExistsIn k s1 && (optExistsIn k s2 || optExistsIn k s3))

def insertIdx (x : Option Int × Nat) : List (Option Int × Nat) → List (Option Int × Nat)
  | [] => [x]
  | y :: ys => if x.2 ≤ y.2 then x :: y :: ys else y :: insertIdx x ys

def sortByIdx : List (Option Int × Nat) → List (Option Int × Nat)
  | [] => []
  | x :: xs => insertIdx x (sortByIdx xs)

structure CascadeResult where
  rows : List (Option Int)
  s3Probes : Nat
  deriving DecidableEq

/-- Cascading bitset-narrowing evaluation for `EXISTS(S1) AND (EXISTS(S2) OR
    EXISTS(S3))` (TPC-DS Q10/Q35): filter candidates by `S1`, partition into
    hits in `S2` vs remaining candidates `B_rem = B_1 \ B_2`, and probe `S3`
    ONLY for `B_rem`. -/
def optCascadingFilter (c s1 s2 s3 : List (Option Int)) : CascadeResult :=
  let step1 := c.zipIdx.filter (fun p => optExistsIn p.1 s1)
  let hit2 := step1.filter (fun p => optExistsIn p.1 s2)
  let rem := step1.filter (fun p => !optExistsIn p.1 s2)
  let hit3 := rem.filter (fun p => optExistsIn p.1 s3)
  let merged := (sortByIdx (hit2 ++ hit3)).map Prod.fst
  { rows := merged, s3Probes := rem.length }

/-- Disjunctive `EXISTS` cascading short-circuit bitset narrowing (TPC-DS
    Q10/Q35): rows satisfying `EXISTS(S1) ∧ EXISTS(S2)` never probe `S3`,
    preserving exact filtered output across NULLs while executing zero `S3`
    probes whenever `B_1 ⊆ B_2`. -/
def Goal : Prop :=
  ∀ (c s1 s2 s3 : List (Option Int)),
    let res := optCascadingFilter c s1 s2 s3
    res.rows = refFilter c s1 s2 s3 ∧
    res.s3Probes ≤ (c.filter (fun k => optExistsIn k s1)).length ∧
    ((∀ k ∈ c, optExistsIn k s1 = true → optExistsIn k s2 = true) →
      res.s3Probes = 0)

example :
    let c : List (Option Int) := [none, some 1, some 2, some 3, some 2, some 4]
    let s1 : List (Option Int) := [none, some 1, some 2, some 3]
    let s2 : List (Option Int) := [some 2]
    let s3 : List (Option Int) := [some 3]
    let res := optCascadingFilter c s1 s2 s3
    res.rows = refFilter c s1 s2 s3 ∧ res.s3Probes = 2 := by
  decide

example :
    let c : List (Option Int) := [none, some 1, some 2]
    let s1 : List (Option Int) := [some 1, some 2]
    let s2 : List (Option Int) := [some 1, some 2]
    let res := optCascadingFilter c s1 s2 [some 1]
    res.rows = [some 1, some 2] ∧ res.s3Probes = 0 := by
  decide

end TinylambOpt.ResearchGoals.DisjunctiveExistsCascadingBitset

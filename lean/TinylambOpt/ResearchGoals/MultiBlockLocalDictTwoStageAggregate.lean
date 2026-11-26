import Std

/-!
Reference single-stage global aggregate: decodes every row's local dictionary code to
`Option String` upfront and aggregates globally.

Stage 1: accumulates purely inside code-indexed local slots `[0..dict.length]`, where
`dict.length` represents `NULL`, never touching dictionary strings.

Modeled claims

- `Goal` — Multi-block per-block local-dictionary two-stage aggregation
  (DictionaryJoinsAndLateLabels B1 / JOB J4 / LDBC L1, L5): accumulating into local
  code arrays without reading dictionary strings per row and decoding only occupied
  dictionary slots at global merge matches eager full-string decoding across blocks
  with distinct codebooks, duplicate dictionary entries, and NULL keys/values.
-/

namespace TinylambOpt.ResearchGoals.MultiBlockLocalDictTwoStageAggregate

structure DictBlock where
  dict : List String
  rows : List (Option Nat × Option Int)
  deriving DecidableEq

def BlocksValid (blocks : List DictBlock) : Prop :=
  ∀ b : DictBlock, b ∈ blocks →
    ∀ p : Option Nat × Option Int, p ∈ b.rows →
      match p.1 with
      | none => True
      | some code => code < b.dict.length

def sumOptList (vals : List (Option Int)) : Option Int :=
  vals.foldl
    (fun acc v =>
      match acc, v with
      | none, y => y
      | x, none => x
      | some a, some b => some (a + b))
    none

/-- Reference single-stage global aggregate: decodes every row's local
    dictionary code to `Option String` upfront and aggregates globally. -/
def refGlobalAgg (blocks : List DictBlock) (target : Option String) :
    Nat × Option Int :=
  let decoded : List (Option String × Option Int) :=
    blocks.flatMap fun b =>
      b.rows.map fun (oc, ov) =>
        let strKey := oc.bind (fun c => b.dict[c]?)
        (strKey, ov)
  let hits := decoded.filter (fun p => p.1 == target)
  (hits.length, sumOptList (hits.map Prod.snd))

structure LocalAcc where
  cnt : Nat := 0
  nn : Nat := 0
  sum : Int := 0
  deriving DecidableEq

def incLocalSlot (slots : List LocalAcc) (idx : Nat) (ov : Option Int) :
    List LocalAcc :=
  slots.zipIdx.map fun (acc, i) =>
    if i == idx then
      match ov with
      | none => { acc with cnt := acc.cnt + 1 }
      | some x => { cnt := acc.cnt + 1, nn := acc.nn + 1, sum := acc.sum + x }
    else acc

/-- Stage 1: accumulates purely inside code-indexed local slots `[0..dict.length]`,
    where `dict.length` represents `NULL`, never touching dictionary strings. -/
def stage1BlockLocal (b : DictBlock) : List LocalAcc :=
  let slots := List.replicate (b.dict.length + 1) {}
  b.rows.foldl
    (fun acc (oc, ov) =>
      let idx := match oc with | some c => c | none => b.dict.length
      incLocalSlot acc idx ov)
    slots

/-- Stage 2: materializes strings ONLY for non-empty local dictionary slots and
    merges partial sums across blocks into the global target key. -/
def optTwoStageAgg (blocks : List DictBlock) (target : Option String) :
    Nat × Option Int :=
  let combined :=
    blocks.foldl
      (fun (st : Nat × Nat × Int) (b : DictBlock) =>
        let localSlots := stage1BlockLocal b
        let nullSlot := localSlots.getD b.dict.length {}
        let st' :=
          if target.isNone then
            (st.1 + nullSlot.cnt, st.2.1 + nullSlot.nn, st.2.2 + nullSlot.sum)
          else st
        b.dict.zipIdx.foldl
          (fun (acc : Nat × Nat × Int) (s, code) =>
            let ls := localSlots.getD code {}
            if 0 < ls.cnt && target == some s then
              (acc.1 + ls.cnt, acc.2.1 + ls.nn, acc.2.2 + ls.sum)
            else acc)
          st')
      (0, 0, 0)
  (combined.1, if combined.2.1 == 0 then none else some combined.2.2)

/-- Multi-block per-block local-dictionary two-stage aggregation (DictionaryJoinsAndLateLabels B1 /
    JOB J4 / LDBC L1, L5): accumulating into local code arrays without reading
    dictionary strings per row and decoding only occupied dictionary slots at
    global merge matches eager full-string decoding across blocks with distinct
    codebooks, duplicate dictionary entries, and NULL keys/values. -/
def Goal : Prop :=
  ∀ (blocks : List DictBlock),
    BlocksValid blocks →
    ∀ (target : Option String),
      optTwoStageAgg blocks target = refGlobalAgg blocks target

example :
    let b1 : DictBlock :=
      { dict := ["page-1", "page-2", "page-1"] -- deliberate duplicate string in dict
        rows := [(some 0, some 10), (some 2, some (-10)), (some 1, none), (none, some 50)] }
    let b2 : DictBlock :=
      { dict := ["page-9", "page-1"] -- same string gets code 1 instead of 0
        rows := [(some 1, some 7), (none, none), (some 0, some 3)] }
    let blocks := [b1, b2]
    let targets : List (Option String) := [none, some "page-1", some "page-2", some "page-9", some "missing"]
    targets.all (fun t => optTwoStageAgg blocks t == refGlobalAgg blocks t) = true ∧
    optTwoStageAgg blocks (some "page-1") = (3, some 7) ∧
    optTwoStageAgg blocks none = (2, some 50) := by
  decide

example :
    optTwoStageAgg [] (some "x") = (0, none) ∧
    refGlobalAgg [] (some "x") = (0, none) := by
  decide

end TinylambOpt.ResearchGoals.MultiBlockLocalDictTwoStageAggregate

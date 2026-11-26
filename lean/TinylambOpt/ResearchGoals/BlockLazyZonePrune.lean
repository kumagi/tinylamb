import Std

/-!
These claims relate block summaries to the qualifying rows of a reference scan. SQL can
omit a block only when the summary excludes every match.

Modeled claims

- `Goal` — Two-stage lazy block pruning (`table/tlcs_file.cpp`,
  `executor/relational_regression_test_part4.cpp`): ZoneMap headers gate key
  decoding, and payload columns are decoded only on blocks with ≥ 1 match.
-/

namespace TinylambOpt.ResearchGoals.BlockLazyZonePrune

structure Block where
  minKey : Int
  maxKey : Int
  hasNull : Bool
  rows : List (Option Int × Int)
  deriving DecidableEq

def ValidHeader (b : Block) : Prop :=
  ∀ r ∈ b.rows,
    match r.1 with
    | none => b.hasNull = true
    | some k => b.minKey ≤ k ∧ k ≤ b.maxKey

def inRange (lo hi : Int) : Option Int → Bool
  | none => false
  | some k => decide (lo ≤ k ∧ k ≤ hi)

def mayMatch (lo hi : Int) (b : Block) : Bool :=
  decide (lo ≤ hi ∧ b.minKey ≤ hi ∧ lo ≤ b.maxKey)

def refScan (lo hi : Int) (bs : List Block) : List Int :=
  (bs.flatMap Block.rows).filterMap fun r =>
    if inRange lo hi r.1 then some r.2 else none

structure ScanTrace where
  payloads : List Int := []
  keyOpened : List Nat := []
  payOpened : List Nat := []
  deriving DecidableEq

def twoStageScan (lo hi : Int) (bs : List Block) : ScanTrace :=
  bs.zipIdx.foldl
    (fun st (b, idx) =>
      if !mayMatch lo hi b then
        st
      else
        let matched := b.rows.filter (fun r => inRange lo hi r.1)
        if matched.isEmpty then
          { st with keyOpened := st.keyOpened ++ [idx] }
        else
          { payloads := st.payloads ++ matched.map Prod.snd
            keyOpened := st.keyOpened ++ [idx]
            payOpened := st.payOpened ++ [idx] })
    {}

/-- Two-stage lazy block pruning (`table/tlcs_file.cpp`,
    `executor/relational_regression_test_part4.cpp`): ZoneMap headers gate key
    decoding, and payload columns are decoded only on blocks with ≥ 1 match. -/
def Goal : Prop :=
  ∀ (lo hi : Int) (bs : List Block),
    (∀ b ∈ bs, ValidHeader b) →
      let tr := twoStageScan lo hi bs
      tr.payloads = refScan lo hi bs ∧
      (∀ idx : Nat, idx ∈ tr.keyOpened ↔
        ∃ b : Block, bs[idx]? = some b ∧ mayMatch lo hi b = true) ∧
      (∀ idx : Nat, idx ∈ tr.payOpened ↔
        ∃ b : Block, bs[idx]? = some b ∧
          b.rows.any (fun r => inRange lo hi r.1) = true) ∧
      (∀ idx : Nat, idx ∈ tr.payOpened → idx ∈ tr.keyOpened)

private def b0 : Block := ⟨1, 10, false, [(some 1, 100), (some 10, 200)]⟩
private def b1 : Block := ⟨6, 15, true, [(none, 300), (some 6, 400)]⟩
private def b2 : Block := ⟨20, 30, false, [(some 22, 500), (some 28, 600)]⟩

example :
    twoStageScan 5 6 [b0, b1, b2] =
      { payloads := [400], keyOpened := [0, 1], payOpened := [1] } := by
  decide

example :
    twoStageScan 10 5 [b0, b1, b2] =
      { payloads := [], keyOpened := [], payOpened := [] } := by
  decide

end TinylambOpt.ResearchGoals.BlockLazyZonePrune

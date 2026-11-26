import Std

/-!
Naive modulo remapping `outer % 2` collides distinct keys and causes false positives on
unseen pairs.

Modeled claims

- `Goal` — Exact 2D dense outer-key remap + bit-matrix
  (`executor/detail/build_input.cpp` `pair_bits` / `pair_outer_dense`): remapping
  sparse outer keys injectively via `eraseDups`/`findIdx?` and optionally swapping
  axes yields an exact filter with zero false positives and zero false negatives.
-/

namespace TinylambOpt.ResearchGoals.PairBitmapDenseRemap

def normPair (swap : Bool) (p : Int × Int) : Int × Int :=
  if swap then (p.2, p.1) else p

def outerKeys (pairs : List (Int × Int)) (swap : Bool) : List Int :=
  (pairs.map (fun p => (normPair swap p).1)).eraseDups

def innerMin? (pairs : List (Int × Int)) (swap : Bool) : Option Int :=
  pairs.foldl
    (fun acc p =>
      let v := (normPair swap p).2
      match acc with
      | none => some v
      | some m => some (if v < m then v else m))
    none

def innerMax? (pairs : List (Int × Int)) (swap : Bool) : Option Int :=
  pairs.foldl
    (fun acc p =>
      let v := (normPair swap p).2
      match acc with
      | none => some v
      | some m => some (if m < v then v else m))
    none

def bitIndex (outerIdx : Nat) (innerMin : Int) (span : Nat) (innerVal : Int) : Nat :=
  outerIdx * span + (innerVal - innerMin).toNat

def buildBitmap (pairs : List (Int × Int)) (swap : Bool) : Nat :=
  let okeys := outerKeys pairs swap
  match innerMin? pairs swap, innerMax? pairs swap with
  | some mn, some mx =>
      let span := (mx - mn).toNat + 1
      pairs.foldl
        (fun mask p =>
          let (o, iv) := normPair swap p
          match okeys.findIdx? (· == o) with
          | some oi => mask ||| (1 <<< bitIndex oi mn span iv)
          | none => mask)
        0
  | _, _ => 0

def probePairBitmap (pairs : List (Int × Int)) (swap : Bool) (q : Int × Int) : Bool :=
  let okeys := outerKeys pairs swap
  match innerMin? pairs swap, innerMax? pairs swap with
  | some mn, some mx =>
      let span := (mx - mn).toNat + 1
      let mask := buildBitmap pairs swap
      let (o, iv) := normPair swap q
      match okeys.findIdx? (· == o) with
      | none => false
      | some oi =>
          if decide (mn ≤ iv ∧ iv ≤ mx) then
            ((mask >>> bitIndex oi mn span iv) &&& 1) == 1
          else
            false
  | _, _ => false

/-- Exact 2D dense outer-key remap + bit-matrix (`executor/detail/build_input.cpp`
    `pair_bits` / `pair_outer_dense`): remapping sparse outer keys injectively
    via `eraseDups`/`findIdx?` and optionally swapping axes yields an exact filter
    with zero false positives and zero false negatives. -/
def Goal : Prop :=
  ∀ (pairs : List (Int × Int)) (swap : Bool) (q : Int × Int),
    probePairBitmap pairs swap q = pairs.contains q

/-- Naive modulo remapping `outer % 2` collides distinct keys and causes false
    positives on unseen pairs. -/
def badModuloProbe (pairs : List (Int × Int)) (q : Int × Int) : Bool :=
  let denseMod (x : Int) : Nat := (x % 2).toNat
  pairs.any (fun p => denseMod p.1 == denseMod q.1 && p.2 == q.2)

private def samplePairs : List (Int × Int) := [(1, 10), (3, 12), (10, 10)]

example :
    probePairBitmap samplePairs false (3, 12) = true ∧
    probePairBitmap samplePairs false (3, 10) = false ∧
    probePairBitmap samplePairs true (3, 10) = false ∧
    probePairBitmap samplePairs true (10, 10) = true := by
  decide

example :
    badModuloProbe samplePairs (3, 10) = true ∧
    samplePairs.contains (3, 10) = false := by
  decide

end TinylambOpt.ResearchGoals.PairBitmapDenseRemap

import Std

/-!
These claims constrain changes in aggregation order near a floating-point HAVING
threshold. SQL needs a sufficient numerical bound before treating regrouped sums as
equivalent for the filter.

Modeled claims

- `Goal` — Fubini sum identity + interval error-band pruning for scaled-threshold
  HAVING queries (TPC-H Q11 `HAVING SUM(v) > alpha * (SELECT SUM(v))`): sum of all
  group sums equals the global total sum, and any group outside the `[-(den*epsG +
  num*epsT), +(den*epsG + num*epsT)]` drift band has its `HAVING` comparison provably
  settled without exact summation.
-/

namespace TinylambOpt.ResearchGoals.FloatFubiniHavingBand

abbrev Key := Option Int
abbrev Row := Key × Int

def keysOf (rows : List Row) : List Key :=
  (rows.map Prod.fst).foldl
    (fun acc k => if acc.contains k then acc else acc ++ [k]) []

def groupSum (rows : List Row) (k : Key) : Int :=
  (rows.filterMap (fun r => if r.1 == k then some r.2 else none)).sum

def totalSum (rows : List Row) : Int :=
  (rows.map Prod.snd).sum

inductive BandVerdict where
  | certainPass
  | certainFail
  | uncertain
  deriving DecidableEq

def classifyBand (num den : Nat) (epsG epsT : Nat) (gHat tHat : Int) : BandVerdict :=
  let margin : Int := Int.ofNat den * Int.ofNat epsG + Int.ofNat num * Int.ofNat epsT
  let diff : Int := Int.ofNat den * gHat - Int.ofNat num * tHat
  if margin < diff then .certainPass
  else if margin ≤ -diff then .certainFail
  else .uncertain

def exactPasses (num den : Nat) (gExact tExact : Int) : Prop :=
  Int.ofNat num * tExact < Int.ofNat den * gExact

/-- Fubini sum identity + interval error-band pruning for scaled-threshold
    HAVING queries (TPC-H Q11 `HAVING SUM(v) > alpha * (SELECT SUM(v))`):
    sum of all group sums equals the global total sum, and any group outside
    the `[-(den*epsG + num*epsT), +(den*epsG + num*epsT)]` drift band has its
    `HAVING` comparison provably settled without exact summation. -/
def Goal : Prop :=
  (∀ (rows : List Row),
    ((keysOf rows).map (groupSum rows)).sum = totalSum rows) ∧
  (∀ (num den epsG epsT : Nat) (gHat gExact tHat tExact : Int),
    0 < den →
    (gHat - gExact).natAbs ≤ epsG →
    (tHat - tExact).natAbs ≤ epsT →
    match classifyBand num den epsG epsT gHat tHat with
    | .certainPass => exactPasses num den gExact tExact
    | .certainFail => ¬ exactPasses num den gExact tExact
    | .uncertain => True)

example :
    let rows : List Row := [(none, 10), (some 1, 200), (some 2, -5), (some 1, 50), (none, 5)]
    ((keysOf rows).map (groupSum rows)).sum = totalSum rows ∧
    classifyBand 1 100 1 1 250 1000 = .certainPass ∧
    classifyBand 1 100 1 1 5 1000 = .certainFail ∧
    classifyBand 1 100 1 1 10 1000 = .uncertain := by
  decide

example :
    ((keysOf ([] : List Row)).map (groupSum [])).sum = totalSum [] := by
  decide

end TinylambOpt.ResearchGoals.FloatFubiniHavingBand

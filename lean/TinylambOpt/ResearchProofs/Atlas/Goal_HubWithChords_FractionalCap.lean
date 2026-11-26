import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.HubWithChordsFractionalCap

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `HubWithChords.FractionalCap` — `∀ n db b weights den, cover pattern weights den →
  (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
  ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_HubWithChords_FractionalCap

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.HubWithChords

/-! Exhaustive small-input sanity check for the seven-atom hub-with-chords
pattern.  We sample 200 binary databases on a two-element domain (out of the
2^28 possible ones), and for each we test every weight vector in {0,1,2}^7 and
every den in {1,2,3}.  A nonzero result would be a concrete counterexample. -/

private def dbSmall (n code : Nat) : DB := fun r x y =>
  if code.testBit (r * n * n + x * n + y) then (1 : Int) else 0

private def nextCode (code : Nat) : Nat := (code * 1103515245 + 12345) % (2^28)

private def codeSamples : Nat → Nat → List Nat
  | 0, _ => []
  | k+1, c => c :: codeSamples k (nextCode c)

private def weightListsAux : Nat → List Nat → List (List Nat)
  | 0, acc => [acc]
  | k+1, acc => (List.range 3).flatMap (fun w => weightListsAux k (acc ++ [w]))

private def coverCheck (ws : List Nat) (den : Nat) : Bool :=
  0 < den && ws.length == pattern.atoms.length &&
    pattern.freeVars.all (fun v =>
      den ≤ ((pattern.atoms.zip ws).map
        (fun (a, k) => if v == a.src || v == a.dst then k else 0)).sum)

private def checkWeights (T : Nat) (supp ws : List Nat) (den : Nat) : Bool :=
  !coverCheck ws den ||
    decide (T ^ den ≤ (List.zipWith (fun s w => s ^ w) supp ws).prod)

private def checkCode (code : Nat) : Bool :=
  let n := 2
  let db := dbSmall n code
  let b : Bind := fun _ => 0
  let ind : DB := fun r x y => if db r x y == 0 then 0 else 1
  let T := (reference pattern n ind b).toNat
  let supp := pattern.atoms.map (fun a => support n db a pattern b)
  (weightListsAux 7 []).all (fun ws =>
    (List.range 3).all (fun d0 => checkWeights T supp ws (d0 + 1)))

#eval (codeSamples 200 12345).countP (fun c => !checkCode c)

/-! The general theorem is proved in `Atlas.JoinPlans.HubWithChordsFractionalCap`
by a Shearer-style fractional-cover argument specialised to this pattern;
re-export it here under the goal's name. -/
theorem sound_HubWithChords_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.HubWithChords.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.HubWithChordsFractionalCap.sound_FractionalCap

end TinylambOpt.ResearchProofs.Atlas.Goal_HubWithChords_FractionalCap

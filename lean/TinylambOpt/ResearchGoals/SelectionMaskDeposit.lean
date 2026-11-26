import Std

/-!
These claims map compact selection bits back to original lane positions. A SQL vector
scan must retain that mapping when combining masks from filtered batches.

Modeled claims

- `Goal` — `∀ parent child, lanes (deposit 64 child parent) = ((lanes
  parent).zipIdx.filter (fun p => child.getLsbD p.2)).map Prod.fst`
-/

namespace TinylambOpt.ResearchGoals.SelectionMaskDeposit

abbrev W := BitVec 64

-- Emulate PDEP: consume a low source bit and clear the lowest set mask bit.
def deposit : Nat → W → W → W
  | 0, _, _ => 0
  | n+1, src, mask =>
    if mask = 0 then 0 else
      let low := mask &&& (-mask)
      let chosen := low &&& (-(src &&& 1))
      chosen ||| deposit n (src >>> 1) (mask &&& (mask-1))

def lanes (w : W) := (List.range 64).filter w.getLsbD

def Goal : Prop := ∀ parent child,
  lanes (deposit 64 child parent) =
    ((lanes parent).zipIdx.filter (fun p => child.getLsbD p.2)).map Prod.fst

example : deposit 64 5 0x8000000000000009 = 0x8000000000000001 := by decide
example : deposit 64 0xffffffffffffffff 0 = 0 := by decide

end TinylambOpt.ResearchGoals.SelectionMaskDeposit

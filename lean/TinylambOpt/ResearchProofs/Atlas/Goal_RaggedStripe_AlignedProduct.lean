import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.RaggedStripeProgressions

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `RaggedStripe.AlignedProduct` — `∀ c (b : Lane), 0 < b.stride → ((rows layout c).map
  (fun r => ((List.range b.n).map (fun i => if r.1 == b.start+i*b.stride then
  r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum = ((layout.lanes c).map (aligned ·
  b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RaggedStripe_AlignedProduct

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RaggedStripe

/-! Brute-force sanity check: no counterexample for small configurations. -/
private def alignedProductHolds (c : Config) (b : Lane) : Bool :=
  let lhs := ((rows RaggedStripe.layout c).map (fun r =>
    ((List.range b.n).map (fun i =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)).sum
  let rhs := ((RaggedStripe.layout.lanes c).map (aligned · b)).sum
  lhs == rhs

#eval
  let configs := (List.range 7).flatMap (fun (n : Nat) =>
    (List.range 4).flatMap (fun (p : Nat) =>
      (List.range 3).flatMap (fun (bi : Nat) =>
        (List.range 3).map (fun (si : Nat) =>
          ({ n := n, split := 0, parameter := p, base := (Int.ofNat bi) - 1,
             step := (Int.ofNat si) - 1, bias := 0, dict := [] } : Config)))))
  let lanes := (List.range 4).flatMap (fun (s : Nat) =>
    (List.range 2).flatMap (fun (d : Nat) =>
      (List.range 4).flatMap (fun (m : Nat) =>
        (List.range 3).flatMap (fun (bi : Nat) =>
          (List.range 3).map (fun (si : Nat) =>
            ({ start := s, stride := d + 1, n := m, base := (Int.ofNat bi) - 1,
               step := (Int.ofNat si) - 1 } : Lane))))))
  configs.any (fun c => lanes.any (fun b => !(alignedProductHolds c b)))

theorem sound_RaggedStripe_AlignedProduct : RaggedStripe.AlignedProduct.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.RaggedStripeProgressions.sound_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.Goal_RaggedStripe_AlignedProduct

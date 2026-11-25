import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_P7

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_DeletedSpan_AlignedProduct

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.DeletedSpan

/-! Brute-force sanity check: no counterexample for small configurations
    (`n ≤ 6`, `parameter ≤ 3`, `split ≤ 4`, values in `[-1,1]`,
    lanes with `0 < stride`, `n ≤ 3`). -/
private def alignedProductHolds (c : Config) (b : Lane) : Bool :=
  let lhs := ((rows DeletedSpan.layout c).map (fun r =>
    ((List.range b.n).map (fun i =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)).sum
  let rhs := ((DeletedSpan.layout.lanes c).map (aligned · b)).sum
  lhs == rhs

#eval
  let configs := (List.range 7).flatMap (fun (n : Nat) =>
    (List.range 4).flatMap (fun (p : Nat) =>
      (List.range 5).flatMap (fun (sp : Nat) =>
        (List.range 3).flatMap (fun (bi : Nat) =>
          (List.range 3).map (fun (si : Nat) =>
            ({ n := n, split := sp, parameter := p, base := (Int.ofNat bi) - 1,
               step := (Int.ofNat si) - 1, bias := 0, dict := [] } : Config))))))
  let lanes := (List.range 4).flatMap (fun (s : Nat) =>
    (List.range 2).flatMap (fun (d : Nat) =>
      (List.range 4).flatMap (fun (m : Nat) =>
        (List.range 3).flatMap (fun (bi : Nat) =>
          (List.range 3).map (fun (si : Nat) =>
            ({ start := s, stride := d + 1, n := m, base := (Int.ofNat bi) - 1,
               step := (Int.ofNat si) - 1 } : Lane))))))
  configs.any (fun c => lanes.any (fun b => !(alignedProductHolds c b)))

/-- The goal holds. The proof reuses the established generic row/lane
    aligned-product lemma instantiated for `DeletedSpan.layout` in
    `Auto_EncodedPages_P7`. -/
theorem sound_DeletedSpan_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.DeletedSpan.AlignedProduct.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_P7.sound_DeletedSpan_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.Goal_DeletedSpan_AlignedProduct

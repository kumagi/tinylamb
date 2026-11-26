import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.TailRunAndWrappingResidueJoins

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `MirroredTail.ResidueJoin` — `∀ c (b : Lane), ((rows layout c).map (fun r =>
  (List.range b.n).countP (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum =
  ((layout.lanes c).map (joinCount · b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_MirroredTail_ResidueJoin

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.MirroredTail

/-- Brute-force sanity check over small configurations: expanding the physical
    rows and counting their join multiplicities agrees with summing `joinCount`
    over the two compressed lanes. -/
def residueJoinCheck (c : Config) (b : Lane) : Bool :=
  ((rows MirroredTail.layout c).map (fun r =>
     (List.range b.n).countP (fun (i : Nat) => r.2 == b.base + b.step*(i:Int)))).sum ==
   ((MirroredTail.layout.lanes c).map (joinCount · b)).sum

#eval ((List.range 6).flatMap (fun n => (List.range (n+1)).flatMap (fun s =>
         (List.range 5).map (fun st =>
           residueJoinCheck ⟨n, s, 0, 0, ((st-2:Nat):Int), 0, []⟩ ⟨0, 1, 3, 1, 1⟩)))).all id

/-- The mirrored-tail lanes cover exactly the physical rows (as a permutation),
    so the generic residue-join reduction applies. -/
theorem sound_MirroredTail_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.MirroredTail.ResidueJoin.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.TailRunAndWrappingResidueJoins.MirroredTail_ResidueJoin

end TinylambOpt.ResearchProofs.Atlas.Goal_MirroredTail_ResidueJoin

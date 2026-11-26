import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
import TinylambOpt.ResearchProofs.StorageExecution.SE_DerivedZones_BitwiseXor

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_CrossPageJoin

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BitwiseXor

/-! 真偽確認: 小さいグリッドを全探索し、仮説を満たす反例が無いことを確かめる。 -/

private def smallBounds : List Bounds :=
  (List.range 3).flatMap fun lo =>
    ((List.range 3).filter (fun hi => decide (lo ≤ hi))).map fun hi => ⟨lo, hi⟩

private def smallPages : List (List (Option Row)) :=
  [[]] ++ [[some (0, 0)], [some (1, 2)], [some (2, 1)], [some (2, 2)]]

private def coversB (p : List (Option Row)) (a b : Bounds) : Bool :=
  p.all fun r =>
    match r with
    | none => true
    | some (x, y) =>
        decide (a.lo ≤ x) && decide (x ≤ a.hi) &&
          decide (b.lo ≤ y) && decide (y ≤ b.hi)

private def hasBadCase : Bool :=
  (List.range 2).any fun t =>
  (List.range 2).any fun u =>
  smallBounds.any fun a =>
  smallBounds.any fun b =>
  smallBounds.any fun c =>
  smallBounds.any fun d =>
  smallPages.any fun p =>
  smallPages.any fun q =>
    coversB p a b && coversB q c d &&
      decide ((kernel.bounds t a b).hi < (kernel.bounds u c d).lo) &&
      (scores kernel t p).any (fun x => (scores kernel u q).contains x)

#eval hasBadCase

example : hasBadCase = false := by native_decide

/-- 証明: XOR kernel は soundInterval を満たすので、区間が離れていれば
    スコア集合は交わらない。 -/
theorem sound_CrossPageJoin : BitwiseXor.CrossPageJoin.Goal := by
  exact TinylambOpt.ResearchProofs.StorageExecution.DerivedZones.BitwiseXor.sound_CrossPageJoin

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_CrossPageJoin

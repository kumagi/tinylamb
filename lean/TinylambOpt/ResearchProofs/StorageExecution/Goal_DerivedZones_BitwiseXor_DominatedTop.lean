import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
import TinylambOpt.ResearchProofs.StorageExecution.SE_DerivedZones_BitwiseXor

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_DominatedTop

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BitwiseXor

/-! 真偽確認: 小さい矩形・小さいページ・小さい `seen`/`k` を全探索し、
    前提を満たすのに top-k が変わる反例が無いことを確かめる。 -/

private def smallBounds : List Bounds :=
  (List.range 4).flatMap fun lo =>
    ((List.range 4).filter (fun hi => decide (lo ≤ hi))).map fun hi => ⟨lo, hi⟩

private def smallPage (a b : Bounds) : List (Option Row) :=
  (List.range 4).flatMap fun x =>
    (List.range 4).map fun y =>
      if decide (a.lo ≤ x ∧ x ≤ a.hi) && decide (b.lo ≤ y ∧ y ≤ b.hi)
      then some (x, y) else none

private def coversB (p : List (Option Row)) (a b : Bounds) : Bool :=
  p.all fun r =>
    match r with
    | none => true
    | some (x, y) =>
        decide (a.lo ≤ x) && decide (x ≤ a.hi) &&
          decide (b.lo ≤ y) && decide (y ≤ b.hi)

private def smallSeen : List (List Nat) :=
  [[], [0], [7], [8, 7], [9, 8, 7], [6, 6, 6]]

private def hasBadCase : Bool :=
  smallBounds.any fun a =>
  smallBounds.any fun b =>
  smallSeen.any fun seen =>
  (List.range 4).any fun k =>
    let p := smallPage a b
    coversB p a b &&
      decide (k ≤ seen.countP (fun x => (kernel.bounds 0 a b).hi < x)) &&
      (((seen ++ scores kernel 0 p).mergeSort (· ≥ ·)).take k !=
        (seen.mergeSort (· ≥ ·)).take k)

example : hasBadCase = false := by native_decide

/-- XOR kernel は `soundInterval` を満たすので、ページのスコアはすべて
    証明済み区間の上端以下。区間上端より大きい `seen` の要素だけで
    descending top-k は決まり、ページを追加しても変わらない。 -/
theorem sound_DominatedTop : BitwiseXor.DominatedTop.Goal := by
  exact TinylambOpt.ResearchProofs.StorageExecution.DerivedZones.BitwiseXor.sound_DominatedTop

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_DominatedTop

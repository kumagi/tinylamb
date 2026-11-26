import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
import TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_MonotoneBreaks_Stitch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_MonotoneBreaks_ReorderNeighbors

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.MonotoneBreaks

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs])

private def probe (n : Nat) : Bool :=
  let ls := gen n
  ls.all (fun pre => ls.all (fun a => ls.all (fun b => ls.all (fun suf =>
    (agg.join (agg.reference a) (agg.reference b) ==
        agg.join (agg.reference b) (agg.reference a)) →
      (agg.reference (pre ++ a ++ b ++ suf) ==
        agg.reference (pre ++ b ++ a ++ suf))))))

#eval probe 2
#eval probe 3

/-- Any stitching fold can reassociate a four-way concatenation, so a
    commuting pair of neighbouring summaries may be swapped in place. -/
private theorem reorder_of_stitch (h : stitch agg) : reorderNeighbors agg := by
  intro pre a b suf hcomm
  have step : ∀ x y : List Nat,
      agg.reference (pre ++ x ++ y ++ suf) =
        agg.join (agg.join (agg.reference pre) (agg.join (agg.reference x) (agg.reference y)))
          (agg.reference suf) := by
    intro x y
    rw [show pre ++ x ++ y ++ suf = (pre ++ (x ++ y)) ++ suf by simp [List.append_assoc]]
    rw [← h, ← h, ← h]
  rw [step a b, step b a, hcomm]

theorem sound_ReorderNeighbors : MonotoneBreaks.ReorderNeighbors.Goal := by
  exact reorder_of_stitch (by
    simpa [MonotoneBreaks.Stitch.Goal] using
      TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_MonotoneBreaks_Stitch.sound_Stitch)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_MonotoneBreaks_ReorderNeighbors

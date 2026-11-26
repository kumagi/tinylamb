import TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.MergeOfLocalRevenueTopK
import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_OrderedRunQualifyingPrefixMerge

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
Each qualifying run is already sorted, so its local top ten is its first ten
qualifying rows. The reusable top-k merge identity then shows that merging
these local prefixes gives the same global top ten as merging all qualifying
rows before truncation.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.MergeOfLocalRevenueTopK

namespace EncodedScanAggregateOrderedRuns

private theorem leNat_of_le : ∀ a b : Nat, a ≤ b → leNat a b = true := by
  intro a b h
  simp [leNat, h]

private theorem best_of_sorted : ∀ (xs : List Nat), xs.Pairwise (fun a b => a ≤ b) →
    best 10 xs = xs.take 10 := by
  intro xs hs
  have hs' : xs.Pairwise (fun a b => leNat a b = true) := by
    simpa [leNat] using hs
  unfold best
  rw [List.mergeSort_of_pairwise hs']

end EncodedScanAggregateOrderedRuns

open EncodedScanAggregateOrderedRuns

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.OrderedRunQualifyingPrefixMerge.Goal := by
  intro runs pass hsorted
  induction runs with
  | nil => simp [best]
  | cons r rs ih =>
      have hr : r.Pairwise (fun a b => a ≤ b) := hsorted r (by simp)
      have hrs : ∀ q ∈ rs, q.Pairwise (fun a b => a ≤ b) := by
        intro q hq
        exact hsorted q (by simp [hq])
      have hih := ih hrs
      have hq : (r.filter pass).Pairwise (fun a b => a ≤ b) := hr.filter _
      have hlocal := best_of_sorted (r.filter pass) hq
      simp only [List.flatMap_cons, List.flatten_cons, List.filter_append]
      rw [← hlocal]
      rw [best_append_best 10 (best 10 (r.filter pass))
        (List.flatMap (fun r => (r.filter pass).take 10) rs), best_best]
      rw [best_append_best 10 (r.filter pass) ((rs.flatten).filter pass)]
      rw [← hih]
end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_OrderedRunQualifyingPrefixMerge

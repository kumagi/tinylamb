import TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments

/-!
# CandidateRestrictedNullableSupplierQuantity

Restricting an annual quantity aggregation to candidate keys preserves the nullable sum
for each candidate.

A semi-join or candidate-aware aggregate can avoid building state for irrelevant
partsupp keys while retaining NULL for inputs with no non-NULL quantity.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.CandidateRestrictedNullableSupplierQuantity
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments
open TinylambOpt.ResearchGoals.QueryRewrites


theorem filter_filter_key (pairs : List (Nat × Nat))
    (lines : List ((Nat × Nat) × Option Int)) (k : Nat × Nat) (hk : k ∈ pairs) :
    (lines.filter (fun r => pairs.eraseDups.contains r.1)).filter (fun r => r.1 == k) =
      lines.filter (fun r => r.1 == k) := by
  have hk' : pairs.eraseDups.contains k = true := by
    simpa [List.contains_iff_mem, List.mem_eraseDups] using hk
  rw [List.filter_filter]
  apply List.filter_congr
  intro r hr
  by_cases h : r.1 == k
  · have hrk : r.1 = k := by simpa using h
    rw [hrk] at h ⊢
    rw [hk']
    simp [h]
  · simp [h]


theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments.CandidateRestrictedNullableSupplierQuantity.Goal := by
  intro pairs lines k hk
  rw [forKey, forKey, TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.CandidateRestrictedNullableSupplierQuantity.filter_filter_key pairs lines k hk]
end TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.CandidateRestrictedNullableSupplierQuantity

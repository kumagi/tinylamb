import TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments
import TinylambOpt.ResearchProofs.QueryRewrites.NullableSum

/-!
# DateBoundBlockSubtotalReuse

Using a block subtotal when its maximum date is at or before the cutoff gives the same
nullable sum as filtering every row.

A scan can merge stored subtotals for qualifying blocks and decode quantities only where
the date bound does not certify the whole block.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.Goal_DateBoundBlockSubtotalReuse
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments
open TinylambOpt.ResearchProofs.QueryRewrites.NullableSum (nsum_append nsum_cons)

def scan (cutoff : Int) (p : List (Int × Option Int)) : List (Option Int) :=
  (p.filter (fun r => r.1 ≤ cutoff)).map Prod.snd

theorem scan_append (cutoff : Int) (p q : List (Int × Option Int)) :
    nsum (scan cutoff (p ++ q)) = NullableSum.merge (nsum (scan cutoff p)) (nsum (scan cutoff q)) := by
  simp [scan, List.filter_append, List.map_append, nsum_append]

theorem all_le_of_max (p : List (Int × Option Int)) (cutoff : Int)
    (h : (p.map Prod.fst).max?.any (· ≤ cutoff) = true) :
    ∀ r ∈ p, r.1 ≤ cutoff := by
  rw [Option.any_eq_true] at h
  rcases h with ⟨m, hm, hmle⟩
  have hmax := List.max?_eq_some_iff.mp hm
  intro r hr
  have hrmem : r.1 ∈ p.map Prod.fst := by
    exact List.mem_map.mpr ⟨(r.1, r.2), by simpa using hr, rfl⟩
  exact Int.le_trans (hmax.2 r.1 hrmem) (by simpa using hmle)

theorem state_eq_scan (p : List (Int × Option Int)) (cutoff : Int) :
    (if (p.map Prod.fst).max?.any (· ≤ cutoff) then nsum (p.map Prod.snd)
      else nsum (scan cutoff p)) = nsum (scan cutoff p) := by
  by_cases h : (p.map Prod.fst).max?.any (· ≤ cutoff) = true
  · have hall := all_le_of_max p cutoff h
    have hf : p.filter (fun r => r.1 ≤ cutoff) = p := by
      apply List.filter_eq_self.mpr
      intro r hr
      simpa using hall r hr
    simp [h, scan, hf]
  · simp [h]

theorem scan_pages (pages : List (List (Int × Option Int))) (cutoff : Int) :
    nsum (pages.map (fun p => nsum (scan cutoff p))) = nsum (scan cutoff pages.flatten) := by
  induction pages with
  | nil => simp [scan, nsum]
  | cons p ps ih =>
      rw [List.map_cons, nsum_cons, ih]
      simp only [List.flatten_cons]
      rw [scan_append]


theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments.DateBoundBlockSubtotalReuse.Goal := by
  intro pages cutoff
  dsimp
  have hstate : pages.map (fun p =>
      if (p.map Prod.fst).max?.any (· ≤ cutoff) then nsum (p.map Prod.snd)
      else nsum ((p.filter (fun r => r.1 ≤ cutoff)).map Prod.snd)) =
      pages.map (fun p => nsum (TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.Goal_DateBoundBlockSubtotalReuse.scan cutoff p)) := by
    apply List.map_congr_left
    intro p hp
    simpa [TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.Goal_DateBoundBlockSubtotalReuse.scan] using TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.Goal_DateBoundBlockSubtotalReuse.state_eq_scan p cutoff
  rw [hstate]
  exact TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.Goal_DateBoundBlockSubtotalReuse.scan_pages pages cutoff
end TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.Goal_DateBoundBlockSubtotalReuse

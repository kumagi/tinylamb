import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns
import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping

/-!
# StateAverageFromCustomerTotals

The modeled state average can be computed from the total and count of customer-level
values for that state.

The executor can build state aggregate state once and look it up for each candidate
customer.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.StateAverageFromCustomerTotals
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-- In a deduplicated list of keys, the entry for a present key occurs exactly
once. -/
theorem forKey_eraseDups_map {α β : Type} [BEq α] [LawfulBEq α] (f : α → β)
    (xs : List α) (k : α) (hk : k ∈ xs) :
      (((xs.eraseDups.map (fun x => (x, f x))).filter (fun r => r.1 == k)).map Prod.snd) =
        [f k] := by
  have hnodup : xs.eraseDups.Nodup := KeyGrouping.nodup_eraseDups xs
  have hmem : k ∈ xs.eraseDups := by simpa using hk
  have hcnt : xs.eraseDups.count k = 1 := hnodup.count_of_mem hmem
  let filtered := (xs.eraseDups.map (fun x => (x, f x))).filter (fun r => r.1 == k)
  have hlen : filtered.length = 1 := by
    rw [← List.countP_eq_length_filter]
    rw [List.countP_map]
    change List.countP (fun x : α => x == k) xs.eraseDups = 1
    simpa [List.count_eq_countP] using hcnt
  have hall : ∀ y ∈ filtered, y = (k, f k) := by
    intro y hy
    obtain ⟨p, hp, rfl⟩ := List.mem_map.mp (List.mem_filter.mp hy).1
    have hxeq : p = k := eq_of_beq (List.mem_filter.mp hy).2
    subst p
    rfl
  have hrep : filtered = List.replicate filtered.length (k, f k) := by
    rw [List.eq_replicate_iff]
    exact ⟨rfl, hall⟩
  change filtered.map Prod.snd = [f k]
  rw [hrep, List.map_replicate, hlen]
  rfl

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.StateAverageFromCustomerTotals.Goal := by
  intro totals georgia
  dsimp only
  intro r hr hg
  have hrstates : r.1 ∈ (totals.filter (fun x => georgia x.2.1)).map Prod.fst := by
    rw [List.mem_map]
    exact ⟨r, List.mem_filter.mpr ⟨hr, hg⟩, rfl⟩
  exact forKey_eraseDups_map (fun k => (((totals.filter (fun x => x.1 == k)).filterMap
      (fun x => x.2.2)).sum, ((totals.filter (fun x => x.1 == k)).filterMap
      (fun x => x.2.2)).length))
    ((totals.filter (fun x => georgia x.2.1)).map Prod.fst) r.1 hrstates
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.StateAverageFromCustomerTotals

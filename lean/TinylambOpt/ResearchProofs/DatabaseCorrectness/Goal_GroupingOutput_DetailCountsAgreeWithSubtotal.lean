import TinylambOpt.ResearchGoals.DatabaseCorrectness.GroupingOutput
import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping

/-! The detail grouping sets partition the input bag exactly, so their COUNT(*)
    values add up to the grand subtotal count, including NULL keys. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.GroupingOutput.Goal_DetailCountsAgreeWithSubtotal

open TinylambOpt.ResearchGoals.DatabaseCorrectness.GroupingOutput
open TinylambOpt.ResearchGoals.QueryRewrites (forKey)
open TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping (forKey_filter_perm)

private theorem filter_true {α : Type} : ∀ l : List α, l.filter (fun _ => true) = l
  | [] => rfl
  | a :: l => by simp [filter_true l]

theorem sound : DetailCountsAgreeWithSubtotal.Goal := by
  intro rs
  have hperm := forKey_filter_perm (p := fun _ : Key => true) rs
  rw [filter_true, filter_true] at hperm
  have hlen := hperm.length_eq
  simp only [List.length_map] at hlen
  have hsum : ((rs.map Prod.fst).eraseDups.map (fun k => (forKey rs k).length)).sum = rs.length := by
    simpa [List.length_flatMap] using hlen.symm
  have hfilt : (List.map (detail rs) (rs.map Prod.fst).eraseDups).filter
      (fun r => !r.rolled) = List.map (detail rs) (rs.map Prod.fst).eraseDups := by
    induction (rs.map Prod.fst).eraseDups with
    | nil => rfl
    | cons k ks ih => cases k <;> simp [detail, ih]
  have hcomp : List.map (Out.count ∘ detail rs) (rs.map Prod.fst).eraseDups =
      List.map (fun k => (forKey rs k).length) (rs.map Prod.fst).eraseDups := by
    induction (rs.map Prod.fst).eraseDups with
    | nil => rfl
    | cons k ks ih => simp [detail, ih]
  simp [rollup, hfilt, grand, hcomp, hsum]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.GroupingOutput.Goal_DetailCountsAgreeWithSubtotal

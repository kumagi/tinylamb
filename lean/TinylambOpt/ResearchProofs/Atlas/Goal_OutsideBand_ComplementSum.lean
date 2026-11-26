import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `OutsideBand.ComplementSum` — `∀ p rs, scan shape p rs = (rs.map Prod.snd).sum -
  ((rs.filter (fun r => (r.1.x-p.x).natAbs ≤ 2 && (r.1.y-p.y).natAbs ≤ 4)).map
  Prod.snd).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_OutsideBand_ComplementSum

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.OutsideBand

private theorem sum_filter_compl {α : Type} (f : α → Int) (P : α → Bool) (l : List α) :
    ((l.filter P).map f).sum = (l.map f).sum - ((l.filter (fun x => !P x)).map f).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
      cases h : P a <;> simp [h] at ih ⊢ <;> omega

theorem sound_OutsideBand_ComplementSum : ComplementSum.Goal := by
  intro p rs
  unfold scan
  have hcongr :
      (rs.filter (fun r => (r.1.x - p.x).natAbs ≤ 2 && (r.1.y - p.y).natAbs ≤ 4)) =
        (rs.filter (fun r => !OutsideBand.shape.qualifies p r.1)) := by
    apply List.filter_congr
    intro r hr
    by_cases hx : (r.1.x - p.x).natAbs ≤ 2 <;>
    by_cases hy : (r.1.y - p.y).natAbs ≤ 4 <;>
    simp [OutsideBand.shape, hx, hy] <;> omega
  rw [hcongr]
  exact sum_filter_compl Prod.snd (fun r => OutsideBand.shape.qualifies p r.1) rs

end TinylambOpt.ResearchProofs.Atlas.Goal_OutsideBand_ComplementSum

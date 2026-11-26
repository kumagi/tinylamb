import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `AlignedWeek.BucketEquality` — `∀ p (rs : Rows), let bucket := fun q : Point =>
  (q.x/7,q.y/10) let states := rs.map (fun r => (bucket r.1,r.2)) scan shape p rs =
  ((states.filter (fun e => e.1 == bucket p)).map Prod.snd).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_AlignedWeek_BucketEquality

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.AlignedWeek

theorem sound_AlignedWeek_BucketEquality : AlignedWeek.BucketEquality.Goal := by
  intro p rs
  induction rs with
  | nil => simp [scan, shape]
  | cons r rs ih =>
      simp [scan, shape] at ih ⊢
      by_cases h : (r.1.x / 7 == p.x / 7 && r.1.y / 10 == p.y / 10) = true
      · have hp : r.1.x / 7 = p.x / 7 ∧ r.1.y / 10 = p.y / 10 := by simpa using h
        simp [hp, ih]
      · have hp : ¬ (r.1.x / 7 = p.x / 7 ∧ r.1.y / 10 = p.y / 10) := by
          intro hp
          apply h
          simp [hp]
        simp [h, hp, ih]

end TinylambOpt.ResearchProofs.Atlas.Goal_AlignedWeek_BucketEquality

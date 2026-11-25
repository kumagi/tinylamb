import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

namespace TinylambOpt.ResearchProofs.Atlas.OrderedMeasuresPeakCount

private def m := TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount.measure
private def rep := TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.repeated

/-- Duplicating every row flattens strict peaks: `[0,1,0]` has one peak, while
    `[0,0,1,1,0,0]` has none. -/
theorem DuplicateLoss_disproof :
    ¬ TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount.DuplicateLoss.Goal := by
  intro h
  have hbad : ¬ m ([0,1,0].flatMap (rep 2)) = m [0,1,0] := by decide
  exact hbad (h [0,1,0])

end TinylambOpt.ResearchProofs.Atlas.OrderedMeasuresPeakCount

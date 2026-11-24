import TinylambOpt.ResearchGoals.CloudBI.Quotes
open TinylambOpt.ResearchGoals.CloudBI.Quotes
open TinylambOpt.ResearchGoals.AnalyticsWorkloads

theorem t1 : StaleRollup.Goal := by
  intro rows sym lo hi watermark snapshot h
  simp_all [eligible, stats, summarize, plus, weighted]

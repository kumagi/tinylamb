import TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins
import TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs
import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking
import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates
import TinylambOpt.ResearchGoals.QueryRewrites.FileRangesAndExtrema
import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates
import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates
import TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments
import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns
import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

/-! Collects operator contracts for encoded scans, grouped joins, nullable aggregates,
set operations, windows, ranking and file layout. Importing this module registers the
models used to evaluate candidate SQL rewrites. -/

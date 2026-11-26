-- Proof aggregation for ClickstreamLayout (Execution / Storage)
import TinylambOpt.ResearchProofs.ClickstreamLayout.Goal_Exec_AnchorDemand
import TinylambOpt.ResearchProofs.ClickstreamLayout.Goal_Exec_ClockRange
import TinylambOpt.ResearchProofs.ClickstreamLayout.Goal_Exec_ConcatSearch
import TinylambOpt.ResearchProofs.ClickstreamLayout.Goal_Exec_DefaultGroups
import TinylambOpt.ResearchProofs.ClickstreamLayout.Goal_Exec_LengthRollup
import TinylambOpt.ResearchProofs.ClickstreamLayout.Goal_Exec_SparseMark
import TinylambOpt.ResearchProofs.ClickstreamLayout.Goal_Storage_ClockForest
import TinylambOpt.ResearchProofs.ClickstreamLayout.Goal_Storage_ClockWindow
import TinylambOpt.ResearchProofs.ClickstreamLayout.Goal_Storage_DictionaryAdmission
import TinylambOpt.ResearchProofs.ClickstreamLayout.Goal_Storage_HashExceptions
import TinylambOpt.ResearchProofs.ClickstreamLayout.Goal_Storage_RunSidecar
import TinylambOpt.ResearchProofs.ClickstreamLayout.Goal_Storage_TieOrientation

/-!
This module collects proofs for Goal Exec Anchor Demand, Goal Exec Clock Range, Goal
Exec Concat Search, Goal Exec Default Groups and related cases.

These models describe encoded clickstream columns and the predicates and aggregates that
can consume them.
-/

import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
import TinylambOpt.ResearchGoals.Compression.Directories
import TinylambOpt.ResearchGoals.Frontiers.WorkloadBases

/-!
「強仮説」として登録された提出型のうち、偽であるものの反証。
各定理は `¬ Goal` を具体的な反例で示す（2026-11-25、型駆動ランダム探索で発見した反例を最小化）。
-/
namespace TinylambOpt.ResearchProofs.Claude1125

open TinylambOpt.ResearchGoals

/-- 全行を二重化するとピークは消える。`[0,1,0]` は 1 つ、二重化後 `[0,0,1,1,0,0]` は 0 個。 -/
theorem not_PeakCount_DuplicateLoss :
    ¬ Atlas.OrderedMeasures.PeakCount.DuplicateLoss.Goal := by
  intro h
  have := h [0, 1, 0]
  revert this
  decide

/-- 周辺分布が一致しても行ごとの差は 0 とは限らない。`[(0,1),(1,0)]` の cost は 2。 -/
theorem not_Couplings_AlignedMarginals :
    ¬ Atlas.SynopsisBounds.Couplings.AlignedMarginals.Goal := by
  intro h
  have := h [(0, 1), (1, 0)] (by decide)
  revert this
  decide

/-- 平均長から選んだ幅は最短とは限らない。`sizes=[0,2]` は幅 0 のほうが小さい。 -/
theorem not_Directories_AverageWidthOptimal :
    ¬ Compression.Directories.AverageWidthOptimal.Goal := by
  intro h
  have := h 0 [0, 2]
  revert this
  decide

/-- ファンアウトが同じ（`rc` が等しい）だけでは右の値 `rs` を代表で置き換えられない。 -/
theorem not_WorkloadBases_FanoutOnly :
    ¬ Frontiers.WorkloadBases.FanoutOnly.Goal := by
  intro h
  have := h 2 (fun _ => 0) (fun j => if j = 0 then 0 else 1) (fun _ => 1) (fun _ => 0)
    (by intro j _; simp)
  revert this
  decide

end TinylambOpt.ResearchProofs.Claude1125

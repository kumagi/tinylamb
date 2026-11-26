import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_PercentEscape_LoneIntroducer

open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.PercentEscape

-- 小さい入力での真偽確認（要素確認）。
#guard partialScan reader (Rope.cat (Rope.lit []) (Rope.lit [37, 37])) (extent (Rope.lit []) + 2) (0 % 4) = 1
#guard partialScan reader (Rope.cat (Rope.lit [65, 37]) (Rope.lit [37, 37])) (extent (Rope.lit [65, 37]) + 2) (3 % 4) = 1

theorem sound_LoneIntroducer : PercentEscape.LoneIntroducer.Goal := by
  intro x k
  have h : ¬ extent x + 2 ≤ extent x := by omega
  simp [partialScan, reader, h]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_PercentEscape_LoneIntroducer

import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_FixedPrefix

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BitwiseXor

-- 小さい入力での真偽確認（全ての組で成立）。
#eval (List.range 64).all (fun x => (List.range 64).all (fun y =>
  decide (kernel.bounds 0 ⟨x,x⟩ ⟨y,y⟩ = ⟨x ^^^ y, x ^^^ y⟩)))

-- 幅 0 の区間は一点になり、base は (x^^^y)/1*1 = x^^^y に還元される。
theorem sound_FixedPrefix : BitwiseXor.FixedPrefix.Goal := by
  intro x y
  simp [kernel, width]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_FixedPrefix

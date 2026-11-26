import TinylambOpt.ResearchGoals.BenchmarkRematch.Storage
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Storage_TpchDuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch.Storage
open TinylambOpt.ResearchGoals.Compression.Wire

theorem sound : TpchDuckDB.Goal := by
  intro ship commit receipt tail
  dsimp only
  have h1 : ∀ n tail, readU (uleb n ++ tail) = some (n,tail) := TinylambOpt.ResearchProofs.Compression.Wire.sound_DelimitedInteger
  have h2 : ∀ x, unzig (zig x) = x := TinylambOpt.ResearchProofs.Compression.Wire.sound_SignedInverse
  rw [List.append_assoc, h1 _ _]
  simp only [bind, Option.bind_some, pure]
  rw [h1 _ _]
  simp only [Option.bind_some, h2]
  have e1 : ship + (commit - ship) = commit := by omega
  have e2 : ship + (receipt - ship) = receipt := by omega
  rw [e1, e2]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Storage_TpchDuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Storage_TpchDuckDB.sound

import TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs
import TinylambOpt.ResearchProofs.Compression.Wire

namespace TinylambOpt.ResearchProofs.QueryRewrites.ColumnCodecs.Goal_SignedDateDeltaRoundtrip

open TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs


/-!
# SignedDateDeltaRoundtrip

Decoding signed commit and receipt date deltas relative to the ship date recovers both
dates and leaves the trailing bytes intact. A lineitem scan can store and decode those
dates relative to one base without changing any date observed by the query.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs
open TinylambOpt.ResearchGoals.Compression.Wire

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs.SignedDateDeltaRoundtrip.Goal := by
  intro ship commit receipt tail
  dsimp only
  have h1 : ∀ n tail, readU (uleb n ++ tail) = some (n,tail) :=
    TinylambOpt.ResearchProofs.Compression.Wire.sound_DelimitedInteger
  have h2 : ∀ x, unzig (zig x) = x :=
    TinylambOpt.ResearchProofs.Compression.Wire.sound_SignedInverse
  rw [List.append_assoc, h1 _ _]
  simp only [bind, Option.bind_some, pure]
  rw [h1 _ _]
  simp only [Option.bind_some, h2]
  have e1 : ship + (commit - ship) = commit := by omega
  have e2 : ship + (receipt - ship) = receipt := by omega
  rw [e1, e2]
end TinylambOpt.ResearchProofs.QueryRewrites.ColumnCodecs.Goal_SignedDateDeltaRoundtrip

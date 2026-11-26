import TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs
import TinylambOpt.ResearchProofs.Compression.Wire

/-!
# SignedDateDeltaRoundtrip

Decoding signed commit and receipt date deltas relative to the ship date recovers both
dates and leaves the trailing bytes intact. A lineitem scan can store and decode those
dates relative to one base.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.ColumnCodecs.SignedDateDeltaRoundtrip
open TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs
open TinylambOpt.ResearchGoals.Compression.Wire

theorem sound : SignedDateDeltaRoundtrip.Goal := by
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

end TinylambOpt.ResearchProofs.QueryRewrites.ColumnCodecs.SignedDateDeltaRoundtrip

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.ColumnCodecs.SignedDateDeltaRoundtrip.sound

-- C++ references to this model or its proof.
-- table/rematch_storage_codecs.hpp

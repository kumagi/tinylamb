import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `RunLimit.SpaceTest` — A shared language and row count can be amortized across
  equally sized bitmaps.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_RunLimit_SpaceTest

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.RunLimit
open TinylambOpt.ResearchGoals.Compression.Wire

-- The assertion is a pure integer identity about byte rounding, independent of
-- the machine: with P = pages * codeBits model n q and T = pages * byteCount n,
-- byteCount P = (P + 7) / 8, and
--   h + ceil(P/8) < T  <=>  h < T ∧ P ≤ 8 * (T - h - 1),
-- because floor((P+7)/8) ≤ K  <=>  P ≤ 8*K.  omega proves this directly.
theorem sound_SpaceTest : RunLimit.SpaceTest.Goal := by
  intro n q pages h hpages hcount
  unfold byteCount
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_RunLimit_SpaceTest

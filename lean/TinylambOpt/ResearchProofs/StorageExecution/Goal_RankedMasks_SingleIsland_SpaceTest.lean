import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `SingleIsland.SpaceTest` — A shared language and row count can be amortized across
  equally sized bitmaps.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_SingleIsland_SpaceTest

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.SingleIsland
open TinylambOpt.ResearchGoals.Compression.Wire

-- Truth check on small inputs: exhaustive `List.range` search over n,q ≤ 6,
-- pages ≤ 4, h ≤ 30 returned `true` (including cases with count > 0).
--
-- The assertion is a pure arithmetic identity about byte rounding, independent
-- of the machine: with P = pages * codeBits model n q and T = pages * byteCount n,
-- byteCount P = (P + 7) / 8, and
--   h + ceil(P/8) < T  <=>  h < T ∧ P ≤ 8 * (T - h - 1),
-- because floor((P+7)/8) ≤ K  <=>  P ≤ 8*K.  omega proves this directly.

theorem sound_SpaceTest : SingleIsland.SpaceTest.Goal := by
  intro n q pages h hpages hcount
  unfold byteCount
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_SingleIsland_SpaceTest

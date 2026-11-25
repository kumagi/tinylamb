import TinylambOpt.ResearchGoals.DistributedPageFence
import TinylambOpt.ResearchGoals.FilterLoopFootprint
import TinylambOpt.ResearchGoals.FooterOnlyOpenEquiv
import TinylambOpt.ResearchGoals.HammingOneSearch
import TinylambOpt.ResearchGoals.LazyBlockCacheReuse

import TinylambOpt.ResearchProofs.DistributedPageFence
import TinylambOpt.ResearchProofs.FilterLoopFootprint
import TinylambOpt.ResearchProofs.FooterOnlyOpenEquiv
import TinylambOpt.ResearchProofs.HammingOneSearch
import TinylambOpt.ResearchProofs.LazyBlockCacheReuse

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.O_B

/-!
All five assigned goals are already proved by prior art in
`TinylambOpt/ResearchProofs/`:

* `DistributedPageFence` — `ResearchProofs/DistributedPageFence.lean`
  (`theorem sound : Goal`, via the `fence`/`claim_a`/`claim_b` development).
* `FilterLoopFootprint` — `ResearchProofs/FilterLoopFootprint.lean`
  (`theorem sound : Goal`, via `schedule_cover` + `range_getElem_map_from`).
* `FooterOnlyOpenEquiv` — `ResearchProofs/FooterOnlyOpenEquiv.lean`
  (`theorem sound : Goal`, via `readAll_spec`/`slice_correct`).
* `HammingOneSearch` — `ResearchProofs/HammingOneSearch.lean`
  (`theorem sound : Goal`, via the bit-vector loop invariant
  `bits_lemma`/`hitsModel_reference`).
* `LazyBlockCacheReuse` — `ResearchProofs/LazyBlockCacheReuse.lean`
  (`theorem sound : Goal`, via the `optRun2`/`Inv`/`refFlags` development).

The theorems below re-export those proofs under the required
`<ModuleName>_sound` names with the exact goal types.
-/

theorem DistributedPageFence_sound :
    TinylambOpt.ResearchGoals.DistributedPageFence.Goal :=
  TinylambOpt.ResearchProofs.DistributedPageFence.sound

theorem FilterLoopFootprint_sound :
    TinylambOpt.ResearchGoals.FilterLoopFootprint.Goal :=
  TinylambOpt.ResearchProofs.FilterLoopFootprint.sound

theorem FooterOnlyOpenEquiv_sound :
    TinylambOpt.ResearchGoals.FooterOnlyOpenEquiv.Goal :=
  TinylambOpt.ResearchProofs.FooterOnlyOpenEquiv.sound

theorem HammingOneSearch_sound :
    TinylambOpt.ResearchGoals.HammingOneSearch.Goal :=
  TinylambOpt.ResearchProofs.HammingOneSearch.sound

theorem LazyBlockCacheReuse_sound :
    TinylambOpt.ResearchGoals.LazyBlockCacheReuse.Goal :=
  TinylambOpt.ResearchProofs.LazyBlockCacheReuse.sound

end TinylambOpt.ResearchProofs.DevinBatch.O_B

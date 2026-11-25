import TinylambOpt.ResearchGoals.BlockLazyZonePrune
import TinylambOpt.ResearchGoals.CanonicalEquivIdSetAlgebra
import TinylambOpt.ResearchGoals.CorrelatedAggPartitionKeyFilterPushdown
import TinylambOpt.ResearchGoals.CteSinglePassSplitFilter
import TinylambOpt.ResearchGoals.DictPrefixMonotonicRange
import TinylambOpt.ResearchProofs.BlockLazyZonePrune
import TinylambOpt.ResearchProofs.CanonicalEquivIdSetAlgebra
import TinylambOpt.ResearchProofs.CorrelatedAggPartitionKeyFilterPushdown
import TinylambOpt.ResearchProofs.CteSinglePassSplitFilter
import TinylambOpt.ResearchProofs.DictPrefixMonotonicRange

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.O_A

/-!
# Prior-art re-exports

All five assigned goals were already proved by covering theorems in the
existing `TinylambOpt.ResearchProofs.<Module>` files:

* `BlockLazyZonePrune.Goal` — `ResearchProofs.BlockLazyZonePrune.sound`
  (fold invariant over `twoStageScan` vs `refScan`; zone-map prune soundness).
* `CanonicalEquivIdSetAlgebra.Goal` — `ResearchProofs.CanonicalEquivIdSetAlgebra.sound`
  (injective `canon` preserves INTERSECT/EXCEPT cardinalities; surrogate-key
  counterexample for the second conjunct).
* `CorrelatedAggPartitionKeyFilterPushdown.Goal` —
  `ResearchProofs.CorrelatedAggPartitionKeyFilterPushdown.sound`
  (partition-key-only filter preserves aggregates; semi-join active-part prune;
  naive entity-predicate pushdown counterexample).
* `CteSinglePassSplitFilter.Goal` —
  `ResearchProofs.CteSinglePassSplitFilter.sound`
  (single-pass split-filter equals `k` separate scans under Kleene 3VL).
* `DictPrefixMonotonicRange.Goal` —
  `ResearchProofs.DictPrefixMonotonicRange.sound`
  (`lexLt` total order + binary-search prefix interval).

The theorems below re-export those proofs under the required
`<ModuleName>_sound` names; each statement is exactly the assigned `Goal`.
-/

theorem BlockLazyZonePrune_sound :
    TinylambOpt.ResearchGoals.BlockLazyZonePrune.Goal :=
  TinylambOpt.ResearchProofs.BlockLazyZonePrune.sound

theorem CanonicalEquivIdSetAlgebra_sound :
    TinylambOpt.ResearchGoals.CanonicalEquivIdSetAlgebra.Goal :=
  TinylambOpt.ResearchProofs.CanonicalEquivIdSetAlgebra.sound

theorem CorrelatedAggPartitionKeyFilterPushdown_sound :
    TinylambOpt.ResearchGoals.CorrelatedAggPartitionKeyFilterPushdown.Goal :=
  TinylambOpt.ResearchProofs.CorrelatedAggPartitionKeyFilterPushdown.sound

theorem CteSinglePassSplitFilter_sound :
    TinylambOpt.ResearchGoals.CteSinglePassSplitFilter.Goal :=
  TinylambOpt.ResearchProofs.CteSinglePassSplitFilter.sound

theorem DictPrefixMonotonicRange_sound :
    TinylambOpt.ResearchGoals.DictPrefixMonotonicRange.Goal :=
  TinylambOpt.ResearchProofs.DictPrefixMonotonicRange.sound

end TinylambOpt.ResearchProofs.DevinBatch.O_A

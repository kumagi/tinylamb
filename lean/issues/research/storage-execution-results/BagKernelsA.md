# BagKernelsA results

Scope: the 50 fixed Goals in `BagKernels` for `MultiplicityRuns`,
`IntervalCover`, `AdditiveProduct`, `DictionaryAliases`, and
`ExceptionOverlay`.

Proof aggregator:
`lean/TinylambOpt/ResearchProofs/StorageExecution/BagKernelsA.lean`.
Each Goal is proved in its corresponding `Goal_BagKernels_*.lean` module and
imported by the aggregator. Finite witness checks use kernel `decide`; the
proof closure contains no `sorry`, added axioms, or `native_decide`.

Verification (from `lean/`):

```bash
~/.elan/bin/lake build TinylambOpt.ResearchProofs.StorageExecution.BagKernelsA
```

Result: **50 PROVEN, 0 REFUTED**.

## MultiplicityRuns

| Full Goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.MultiplicityRuns.WeightedOrderStatistic.Goal` | PROVEN | Split on zero run lengths; cumulative multiplicity selects the first of the two sorted keys whose rank exceeds `i`. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.MultiplicityRuns.DistinctOutput.Goal` | PROVEN | Run multiplicities equal expansion counts, and every expanded key occurs in the run-key list. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.MultiplicityRuns.FilteredMoment.Goal` | PROVEN | Rewrite the faithful histogram as the filtered sum of squared expanded values. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.MultiplicityRuns.OrderStatistic.Goal` | PROVEN | The first sorted support key whose cumulative count exceeds `i` is the `i`-th expanded value. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.MultiplicityRuns.JoinFanout.Goal` | PROVEN | Expand both faithful histograms; the nested weighted sum is exactly the band-pair list length. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.MultiplicityRuns.AntiRevenue.Goal` | PROVEN | Zero right multiplicity is equivalent to absence from the right expansion. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.MultiplicityRuns.SelfPairs.Goal` | PROVEN | Per-key binomial multiplicity equals the number of later equal entries in the expansion. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.MultiplicityRuns.IntersectAll.Goal` | PROVEN | Replicating each live key by the minimum mass gives the minimum expanded count. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.MultiplicityRuns.OuterWeight.Goal` | PROVEN | Match multiplicity plus one placeholder for each absent left key gives `max 1` right mass. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.MultiplicityRuns.ZeroRuns.Goal` | PROVEN | Filtering zero-length runs preserves every key's mass and therefore the live support up to permutation. |

## IntervalCover

| Full Goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.IntervalCover.ClipRanges.Goal` | PROVEN | Each half-open interval contributes `min n (lim-s)` covered positions below the clipping limit. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.IntervalCover.DistinctOutput.Goal` | PROVEN | Coverage counts equal expansion counts, and all expanded positions lie below the peak key bound. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.IntervalCover.FilteredMoment.Goal` | PROVEN | Rewrite the faithful coverage histogram as the filtered squared expansion sum. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.IntervalCover.OrderStatistic.Goal` | PROVEN | Sorted cumulative coverage first crossing equals the expanded order statistic. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.IntervalCover.JoinFanout.Goal` | PROVEN | Expand both interval histograms and count band pairs directly. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.IntervalCover.AntiRevenue.Goal` | PROVEN | A position has zero right coverage exactly when it is absent from the right expansion. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.IntervalCover.SelfPairs.Goal` | PROVEN | Binomial coverage counts equal later equal-position pairs in the expansion. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.IntervalCover.IntersectAll.Goal` | PROVEN | Minimum per-position coverage is realized by minimum replication counts. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.IntervalCover.OuterWeight.Goal` | PROVEN | Coverage matches plus uncovered placeholders realize the left-join weight. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.IntervalCover.CoverageIntegral.Goal` | PROVEN | Swap the finite position/interval sums; every interval contributes exactly its length. |

## AdditiveProduct

| Full Goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.AdditiveProduct.SecondMoment.Goal` | PROVEN | Expand the Cartesian sum and use `(x+y)^2 = x^2 + 2xy + y^2` under list sums. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.AdditiveProduct.DistinctOutput.Goal` | PROVEN | The additive convolution mass equals the expansion count and its support is contained in the candidate keys. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.AdditiveProduct.FilteredMoment.Goal` | PROVEN | Rewrite the convolution histogram as the filtered squared product expansion. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.AdditiveProduct.OrderStatistic.Goal` | PROVEN | The convolution cumulative count is the expanded prefix count, so sorted first crossing gives the order statistic. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.AdditiveProduct.JoinFanout.Goal` | PROVEN | Expand both convolution histograms and count band pairs. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.AdditiveProduct.AntiRevenue.Goal` | PROVEN | Zero convolution mass is equivalent to absence from the product expansion. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.AdditiveProduct.SelfPairs.Goal` | PROVEN | Binomial convolution multiplicities equal later equal-key pairs in the expansion. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.AdditiveProduct.IntersectAll.Goal` | PROVEN | Minimum convolution multiplicities equal minimum expanded counts key by key. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.AdditiveProduct.OuterWeight.Goal` | PROVEN | Product matches plus unmatched left placeholders realize `max 1` right multiplicity. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.AdditiveProduct.ProductRevenue.Goal` | PROVEN | Sum the expanded Cartesian product as `a.sum*b.length + b.sum*a.length`. |

## DictionaryAliases

| Full Goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases.ManyToOne.Goal` | PROVEN | Appending dictionary slot `d.length` contributes exactly the number of ids referencing that slot. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases.DistinctOutput.Goal` | PROVEN | Alias-slot frequency equals valid-id expansion count, and positive mass implies dictionary-key membership. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases.FilteredMoment.Goal` | PROVEN | Rewrite the alias histogram as the filtered squared valid-id expansion. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases.OrderStatistic.Goal` | PROVEN | Alias multiplicities give expanded cumulative counts, so sorted first crossing gives the order statistic. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases.JoinFanout.Goal` | PROVEN | Expand both alias histograms and count band pairs over valid ids. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases.AntiRevenue.Goal` | PROVEN | Zero alias mass is equivalent to absence from the valid-id expansion. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases.SelfPairs.Goal` | PROVEN | Binomial alias multiplicities equal later equal-value pairs in the expansion. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases.IntersectAll.Goal` | PROVEN | Minimum alias multiplicities equal minimum valid-id expansion counts. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases.OuterWeight.Goal` | PROVEN | Alias matches plus unmatched valid left rows realize the outer-join weight. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases.UnusedSuffix.Goal` | PROVEN | In-bounds lookups ignore the appended suffix, and suffix-only keys have zero mass. |

## ExceptionOverlay

| Full Goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay.OverrideHead.Goal` | PROVEN | Prepending a patch replaces the old value at exactly one in-range ordinal, preserving first-patch-wins elsewhere. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay.DistinctOutput.Goal` | PROVEN | The default/override histogram equals expansion counts, and positive mass implies key-list membership. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay.FilteredMoment.Goal` | PROVEN | Rewrite the overlay histogram as the filtered squared expansion. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay.OrderStatistic.Goal` | PROVEN | Overlay multiplicities are expanded prefix counts, so sorted first crossing gives the order statistic. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay.JoinFanout.Goal` | PROVEN | The overlay mass recurrence expands both sides into the same band-pair count. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay.AntiRevenue.Goal` | PROVEN | Zero overlay mass is equivalent to absence from the expanded rows. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay.SelfPairs.Goal` | PROVEN | Binomial overlay multiplicities equal later equal-value pairs in the expansion. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay.IntersectAll.Goal` | PROVEN | Minimum overlay multiplicities equal minimum expanded counts at each key. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay.OuterWeight.Goal` | PROVEN | Overlay matches plus unmatched placeholders realize the outer-join weight. |
| `TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay.IgnoreTail.Goal` | PROVEN | An appended out-of-range patch changes neither valid positions, masses, nor nonzero weighted support. |

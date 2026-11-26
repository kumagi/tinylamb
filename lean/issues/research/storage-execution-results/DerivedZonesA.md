# DerivedZonesA results

Scope: the 50 goals in
`TinylambOpt.ResearchGoals.StorageExecution.DerivedZones` for `Product`,
`SaturatingLoss`, `Distance`, `FloorRatio`, and `BucketSum`.

Proof file:
`lean/TinylambOpt/ResearchProofs/StorageExecution/DerivedZonesA.lean`.

Verification (from `lean/`):

```bash
~/.elan/bin/lake env lean -DrelaxedAutoImplicit=false \
  TinylambOpt/ResearchProofs/StorageExecution/DerivedZonesA.lean
```

Result: **50 PROVEN, 0 REFUTED**. The proof file contains no `sorry`, added
axioms, or `native_decide`. No false statement was found in these five
namespaces. Concrete evaluation checks are included as `#guard` checks.

## Product

| Full goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product.SoundInterval.Goal` | PROVEN | Multiplication is monotone in both nonnegative factors. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product.SkipRange.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product.TakeAll.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product.ThresholdCount.Goal` | PROVEN | Generic certified/possible page count bounds from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product.DominatedTop.Goal` | PROVEN | Generic sorted top-k prefix argument from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product.CrossPageJoin.Goal` | PROVEN | Generic disjoint-interval consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product.ProductDivisor.Goal` | PROVEN | For positive `x`, `k ≤ x*y` is equivalent to the ceiling quotient `(k+x-1)/x ≤ y`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product.SemijoinFilter.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product.SumCeiling.Goal` | PROVEN | Generic sum bound from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product.UpperWitness.Goal` | PROVEN | The upper footer is definitionally the score at the two upper endpoints. |

## SaturatingLoss

| Full goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss.SoundInterval.Goal` | PROVEN | Truncated subtraction is monotone in the minuend and antitone in the subtrahend. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss.SkipRange.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss.TakeAll.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss.ThresholdCount.Goal` | PROVEN | Generic certified/possible page count bounds from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss.DominatedTop.Goal` | PROVEN | Generic sorted top-k prefix argument from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss.CrossPageJoin.Goal` | PROVEN | Generic disjoint-interval consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss.ThresholdInverse.Goal` | PROVEN | For positive `k`, `k ≤ x-(y+t)` exactly when `y+t+k ≤ x`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss.SemijoinFilter.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss.SumCeiling.Goal` | PROVEN | Generic sum bound from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss.ZeroCertificate.Goal` | PROVEN | The upper truncated subtraction is zero exactly when its minuend is at most its subtrahend. |

## Distance

| Full goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance.SoundInterval.Goal` | PROVEN | Every point distance lies between the independent endpoint gap bounds. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance.SkipRange.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance.TakeAll.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance.ThresholdCount.Goal` | PROVEN | Generic certified/possible page count bounds from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance.DominatedTop.Goal` | PROVEN | Generic sorted top-k prefix argument from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance.CrossPageJoin.Goal` | PROVEN | Generic disjoint-interval consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance.BandInverse.Goal` | PROVEN | A distance band is exactly the conjunction of the two directed skew bounds. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance.SemijoinFilter.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance.SumCeiling.Goal` | PROVEN | Generic sum bound from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance.OverlapFloor.Goal` | PROVEN | The minimum endpoint gap is zero exactly when the two intervals overlap. |

## FloorRatio

| Full goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio.SoundInterval.Goal` | PROVEN | Numerator monotonicity and positive-denominator division monotonicity give both footer bounds. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio.SkipRange.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio.TakeAll.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio.ThresholdCount.Goal` | PROVEN | Generic certified/possible page count bounds from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio.DominatedTop.Goal` | PROVEN | Generic sorted top-k prefix argument from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio.CrossPageJoin.Goal` | PROVEN | Generic disjoint-interval consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio.DenominatorPage.Goal` | PROVEN | Cross-multiplication by the regularized positive denominator characterizes the lower quotient. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio.SemijoinFilter.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio.SumCeiling.Goal` | PROVEN | Generic sum bound from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio.QuotientThreshold.Goal` | PROVEN | Strict quotient comparison is equivalent to the cross-multiplied strict inequality. |

## BucketSum

| Full goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum.SoundInterval.Goal` | PROVEN | Addition and division by the fixed positive bucket width are monotone. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum.SkipRange.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum.TakeAll.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum.ThresholdCount.Goal` | PROVEN | Generic certified/possible page count bounds from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum.DominatedTop.Goal` | PROVEN | Generic sorted top-k prefix argument from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum.CrossPageJoin.Goal` | PROVEN | Generic disjoint-interval consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum.BucketInverse.Goal` | PROVEN | The quotient equals `k` exactly on the half-open bucket interval `[k*(t+1),(k+1)*(t+1))`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum.SemijoinFilter.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum.SumCeiling.Goal` | PROVEN | Generic sum bound from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum.SingleGroup.Goal` | PROVEN | Equal endpoint quotients make the two definitional footer endpoints equal. |

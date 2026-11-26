# DerivedZonesB results

Scope: the 50 goals in
`TinylambOpt.ResearchGoals.StorageExecution.DerivedZones` for `WrappedSum`,
`Manhattan`, `BitwiseXor`, `CappedPair`, and `QuadraticGap`.

Proof file:
`lean/TinylambOpt/ResearchProofs/StorageExecution/DerivedZonesB.lean`.

Verification (from `lean/`):

```bash
~/.elan/bin/lake env lean -DrelaxedAutoImplicit=false \
  TinylambOpt/ResearchProofs/StorageExecution/DerivedZonesB.lean
```

Result: **49 PROVEN, 1 REFUTED**. The proof file contains no `sorry`, added
axioms, or `native_decide`. Concrete witness evaluations are included as
`#guard` checks.

## WrappedSum

| Full goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.WrappedSum.SoundInterval.Goal` | PROVEN | Split on endpoint quotient equality; equal quotients make endpoint remainders monotone, otherwise the footer is `[0,t]`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.WrappedSum.SkipRange.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.WrappedSum.TakeAll.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.WrappedSum.ThresholdCount.Goal` | PROVEN | Generic certified/possible page count bounds from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.WrappedSum.DominatedTop.Goal` | PROVEN | Generic sorted top-k prefix argument from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.WrappedSum.CrossPageJoin.Goal` | PROVEN | Generic disjoint-interval consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.WrappedSum.TranslateCycle.Goal` | PROVEN | Adding a multiple of `t+1` preserves the remainder. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.WrappedSum.SemijoinFilter.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.WrappedSum.SumCeiling.Goal` | PROVEN | Generic sum bound from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.WrappedSum.Rollover.Goal` | PROVEN | Strict endpoint quotient increase makes the wrap branch fire. |

## Manhattan

| Full goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Manhattan.SoundInterval.Goal` | PROVEN | Point-distance lower/upper bounds add independently across columns. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Manhattan.SkipRange.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Manhattan.TakeAll.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Manhattan.ThresholdCount.Goal` | PROVEN | Generic certified/possible page count bounds from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Manhattan.DominatedTop.Goal` | PROVEN | Generic sorted top-k prefix argument from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Manhattan.CrossPageJoin.Goal` | PROVEN | Generic disjoint-interval consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Manhattan.WitnessCorners.Goal` | PROVEN | Each coordinate maximum is attained at an endpoint; their sum is a corner score. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Manhattan.SemijoinFilter.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Manhattan.SumCeiling.Goal` | PROVEN | Generic sum bound from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Manhattan.InteriorMinimum.Goal` | PROVEN | If `t` lies in both intervals, both truncated distances are zero. |

## BitwiseXor

| Full goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BitwiseXor.SoundInterval.Goal` | PROVEN | Endpoint XOR width certifies a shared quotient block; quotient and remainder decomposition place every row XOR in that aligned block. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BitwiseXor.SkipRange.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BitwiseXor.TakeAll.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BitwiseXor.ThresholdCount.Goal` | PROVEN | Generic certified/possible page count bounds from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BitwiseXor.DominatedTop.Goal` | PROVEN | Generic sorted top-k prefix argument from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BitwiseXor.CrossPageJoin.Goal` | PROVEN | Generic disjoint-interval consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BitwiseXor.AlignedSiblings.Goal` | PROVEN | An aligned block's endpoint XOR is `2^k-1`, so the footer base is `(a ^^^ b)*2^k`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BitwiseXor.SemijoinFilter.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BitwiseXor.SumCeiling.Goal` | PROVEN | Generic sum bound from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BitwiseXor.FixedPrefix.Goal` | PROVEN | Singleton endpoint XORs have width zero, giving the exact singleton footer. |

## CappedPair

| Full goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.CappedPair.SoundInterval.Goal` | PROVEN | Monotonicity of `min` in both arguments. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.CappedPair.SkipRange.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.CappedPair.TakeAll.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.CappedPair.ThresholdCount.Goal` | PROVEN | Generic certified/possible page count bounds from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.CappedPair.DominatedTop.Goal` | PROVEN | Generic sorted top-k prefix argument from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.CappedPair.CrossPageJoin.Goal` | PROVEN | Generic disjoint-interval consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.CappedPair.PushBoth.Goal` | PROVEN | For `k ≤ t`, the cap is inactive and the predicate requires both inputs. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.CappedPair.SemijoinFilter.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.CappedPair.SumCeiling.Goal` | PROVEN | Generic sum bound from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.CappedPair.SaturatedPage.Goal` | PROVEN | If both column lower bounds meet the cap, the footer lower bound is the cap. |

## QuadraticGap

| Full goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.QuadraticGap.SoundInterval.Goal` | PROVEN | Gap lies between `gapMin` and `gapMax`; squaring is monotone on naturals. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.QuadraticGap.SkipRange.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.QuadraticGap.TakeAll.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.QuadraticGap.ThresholdCount.Goal` | PROVEN | Generic certified/possible page count bounds from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.QuadraticGap.DominatedTop.Goal` | PROVEN | Generic sorted top-k prefix argument from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.QuadraticGap.CrossPageJoin.Goal` | PROVEN | Generic disjoint-interval consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.QuadraticGap.SquaredRadius.Goal` | PROVEN | Cancel the additive offset, use `a*a ≤ r*r ↔ a ≤ r`, then characterize `gap ≤ r`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.QuadraticGap.SemijoinFilter.Goal` | PROVEN | Generic consequence of `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.QuadraticGap.SumCeiling.Goal` | PROVEN | Generic sum bound from `SoundInterval`. |
| `TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.QuadraticGap.GapWitness.Goal` | REFUTED | The statement permits malformed bounds. Take `t=0`, `a=⟨10,11⟩`, `b=⟨12,5⟩`: `a.hi < b.lo`, but the footer lower bound is `25` while `kernel.score 0 (11,12)=1`. Both values are checked by `#guard`. |

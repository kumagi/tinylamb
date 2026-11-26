# StorageExecution 1,000 Goals: proof and refutation reports

Each shard covers 50 fixed Goals. A result is valid only when its Lean module
typechecks and registers either `Goal` or `¬ Goal` without `sorry` or an
unexpected axiom.

## Shards

| Shard | Models | Report |
|---|---|---|
| BagKernelsA | MultiplicityRuns, IntervalCover, AdditiveProduct, DictionaryAliases, ExceptionOverlay | [BagKernelsA.md](BagKernelsA.md) |
| BagKernelsB | RotationCycle, EdgeDegrees, RepeatedSlices, DeletedOrdinals, CoGroup | [BagKernelsB.md](BagKernelsB.md) |
| BitLayoutsA | RowMajor, Vertical, SerpentineRow, SerpentinePlane, TileTranspose | [BitLayoutsA.md](BitLayoutsA.md) |
| BitLayoutsB | ByteAlignedRow, ByteAlignedPlane, NetworkBitOrder, RotatedPlane, SplitNibble | [BitLayoutsB.md](BitLayoutsB.md) |
| ClusterOrderA | Lexicographic, AttributeFirst, Interleaved, GrayPair, Diagonal | [ClusterOrderA.md](ClusterOrderA.md) |
| ClusterOrderB | UnorderedPair, RatioClass, PowerBucket, DyadicTile, XorClass | [ClusterOrderB.md](ClusterOrderB.md) |
| DerivedZonesA | Product, SaturatingLoss, Distance, FloorRatio, BucketSum | [DerivedZonesA.md](DerivedZonesA.md) |
| DerivedZonesB | WrappedSum, Manhattan, BitwiseXor, CappedPair, QuadraticGap | [DerivedZonesB.md](DerivedZonesB.md) |
| FrameSelectionA | SeparatedLengths, ConstantExceptions, DictionaryRuns, PatchedOctet, XorPredecessor | [FrameSelectionA.md](FrameSelectionA.md) |
| FrameSelectionB | OrdinalResidual, SplitHigh, MinimumDelta, SparsePlanes, SharedDivisor | [FrameSelectionB.md](FrameSelectionB.md) |
| GatherPlansA | Contiguous, Sector, MirrorZones, CodecRestarts, TwoTier | [GatherPlansA.md](GatherPlansA.md) |
| GatherPlansB | ObjectRanges, StripedWorkers, QuadraticDecode, DictionaryFault, TieredPrefetch | [GatherPlansB.md](GatherPlansB.md) |
| GrammarScansA | ContainsSlash, EndsInDigit, QuotedField, PercentEscape, LineBoundary | [GrammarScansA.md](GrammarScansA.md) |
| GrammarScansB | AsciiOnly, SlashDepth, DecimalMod, EscapedTail, CStringEnd | [GrammarScansB.md](GrammarScansB.md) |
| OrderedFootersA | LongestHole, Drawdown, MonotoneBreaks, DeltaBytes, PairSubsequence | [OrderedFootersA.md](OrderedFootersA.md) |
| OrderedFootersB | RollingFingerprint, BalanceFloor, BitOccupancy, FirstLastChange, IndexWeighted | [OrderedFootersB.md](OrderedFootersB.md) |
| PostingJoinsA | Following, Annulus, SameBucket, AdjacentBucket, ModuloClass | [PostingJoinsA.md](PostingJoinsA.md) |
| PostingJoinsB | SharedPrefix, Submask, ReciprocalPrice, ScaledFloor, Apart | [PostingJoinsB.md](PostingJoinsB.md) |
| RankedMasksA | FixedPopcount, AtMostHits, IndependentHits, PairedHoles, PrefixOnes | [RankedMasksA.md](RankedMasksA.md) |
| RankedMasksB | EvenParity, BalancedNesting, RunLimit, SingleIsland, Cooldown | [RankedMasksB.md](RankedMasksB.md) |

## Aggregate checks

From the repository root:

```sh
python3 lean/scripts/check_storage_execution_results.py
cd lean
~/.elan/bin/lake env lean -DrelaxedAutoImplicit=false CheckStorageExecutionProofs.lean
~/.elan/bin/lake build
```

The Python check compares the report inventory with the fixed `Goal`
declarations. The Lean check compares theorem types with the elaborated Goals,
rejects missing/conflicting outcomes, and inspects axiom dependencies.

# New unproven-goal batch (50 goals) — results

Batch selected 2026-11-24 from newly added goal modules:
`ResearchGoals/Frontiers/{Arithmetic,SparseRecovery}` and
`ResearchGoals/Compression/{BitPatterns,Wire,Container}` (5 goals × 10 agents).

Status is filled after all agents finish; `PROVED` means `theorem <Name>_sound`,
`REFUTED` means `theorem <Name>_disproof` with an explicit concrete witness.
No goal statement or definition is modified.

| # | Module | Goal | Agent | Status |
| --- | --- | --- | --- | --- |
| 1 | Frontiers.Arithmetic | SignedCRT | A01 | pending |
| 2 | Frontiers.Arithmetic | ZeroJoin | A01 | pending |
| 3 | Frontiers.Arithmetic | RoundingCertificate | A01 | pending |
| 4 | Frontiers.Arithmetic | LocalRounding | A01 | pending |
| 5 | Frontiers.Arithmetic | BinnedAccumulator | A01 | pending |
| 6 | Frontiers.Arithmetic | FirstOverflow | A02 | pending |
| 7 | Frontiers.Arithmetic | QuadraticJoin | A02 | pending |
| 8 | Frontiers.Arithmetic | QuantizedAverage | A02 | pending |
| 9 | Frontiers.Arithmetic | CenteredProduct | A02 | pending |
| 10 | Frontiers.Arithmetic | ShiftedCovariance | A02 | pending |
| 11 | Frontiers.SparseRecovery | BoundedMoments | A03 | pending |
| 12 | Frontiers.SparseRecovery | TwoRootMask | A03 | pending |
| 13 | Frontiers.SparseRecovery | SwapDecoder | A03 | pending |
| 14 | Frontiers.SparseRecovery | MonicFingerprint | A03 | pending |
| 15 | Frontiers.SparseRecovery | RationalDelta | A03 | pending |
| 16 | Frontiers.SparseRecovery | UniformBucket | A04 | pending |
| 17 | Frontiers.SparseRecovery | ThreeFromTwo | A04 | pending |
| 18 | Frontiers.SparseRecovery | IsolatedHash | A04 | pending |
| 19 | Frontiers.SparseRecovery | PeelingVariance | A04 | pending |
| 20 | Frontiers.SparseRecovery | DisjointResultant | A04 | pending |
| 21 | Compression.BitPatterns | XorChain | A05 | PROVED |
| 22 | Compression.BitPatterns | SignificantWindow | A05 | PROVED |
| 23 | Compression.BitPatterns | CommonHighBits | A05 | PROVED |
| 24 | Compression.BitPatterns | PreviousWindow | A05 | PROVED |
| 25 | Compression.BitPatterns | AmortizedWindow | A05 | PROVED |
| 26 | Compression.BitPatterns | RawDictionary | A06 | pending |
| 27 | Compression.BitPatterns | ZeroAlias | A06 | pending |
| 28 | Compression.BitPatterns | ConstantPlanes | A06 | pending |
| 29 | Compression.BitPatterns | SparsePlanes | A06 | pending |
| 30 | Compression.BitPatterns | ByteShuffle | A06 | pending |
| 31 | Compression.Wire | DelimitedInteger | A07 | PROVED |
| 32 | Compression.Wire | VarintCharge | A07 | PROVED |
| 33 | Compression.Wire | WidthCliff | A07 | PROVED |
| 34 | Compression.Wire | SignedInverse | A07 | PROVED |
| 35 | Compression.Wire | SignedCliff | A07 | PROVED |
| 36 | Compression.Wire | MaxWordBytes | A08 | pending |
| 37 | Compression.Wire | PrefixFree | A08 | pending |
| 38 | Compression.Wire | PackedInverse | A08 | pending |
| 39 | Compression.Wire | LocalRead | A08 | pending |
| 40 | Compression.Wire | PackedCharge | A08 | pending |
| 41 | Compression.Container | VerifiedContainer | A09 | pending |
| 42 | Compression.Container | RandomRead | A09 | pending |
| 43 | Compression.Container | RawEnvelope | A09 | pending |
| 44 | Compression.Container | BeatTarget | A09 | pending |
| 45 | Compression.Container | PerChunkChoice | A09 | pending |
| 46 | Compression.Container | PointFanout | A10 | PROVED |
| 47 | Compression.Container | ReadAmplification | A10 | PROVED |
| 48 | Compression.Container | BatchedReads | A10 | PROVED |
| 49 | Compression.Container | ReuseClean | A10 | PROVED |
| 50 | Compression.Container | BodyLocator | A10 | PROVED |

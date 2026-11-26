import TinylambOpt.SimdGroupHash

/-! Sampled string comparisons for a compiled grouping or join kernel. Compare the three byte windows themselves before reasoning about a mixed fingerprint; mixing may introduce additional collisions. -/
namespace TinylambOpt.ResearchGoals.JitKernels.SampledEquality

open TinylambOpt.SimdGroupHash (byteSamples)
abbrev Bytes := List UInt8
def sig (s : Bytes) := (s.length,byteSamples s)
def win (start : Nat) (s : Bytes) := (s.drop start).take 4
def offsets (n : Nat) := [0,(n-4)/2,n-4]
def covered (n i : Nat) := (offsets n).any (fun o => o ≤ i && i < o+4)
def residual (s : Bytes) := (s.zipIdx.filter (fun p => !covered s.length p.2)).map Prod.fst
def keys (pool : List Bytes) := pool.map sig

/-- Omit exact byte verification through length twelve when the length and all three raw sample windows match. -/
def ThreeWindowCoverageCutoff.Goal : Prop := ∀ a b, a.length ≤ 12 → sig a = sig b → a = b

/-- Once all sample windows match, compare only bytes omitted by their union. -/
def ResidualEqualityAfterSampleHit.Goal : Prop := ∀ a b, sig a = sig b → (a = b ↔ residual a = residual b)

/-- Omit the length from a padded first-word comparison for short strings whose last byte is nonzero. -/
def ShortStringSentinelLength.Goal : Prop := ∀ (a b : Bytes), a.length ≤ 4 → b.length ≤ 4 → a.getLast? ≠ some 0 → b.getLast? ≠ some 0 →
  (a++List.replicate 4 0).take 4 = (b++List.replicate 4 0).take 4 → a = b

/-- Elide verification for arbitrary probes when all sampled signatures in the build dictionary are unique. -/
def UniqueBuildSamplesCertifyProbe.Goal : Prop := ∀ (pool : List Bytes) probe (i : Nat), (keys pool).Nodup → (keys pool)[i]? = some (sig probe) → pool[i]? = some probe

/-- For two known dictionary members, a single injectivity check over the finite dictionary licenses sample equality. -/
def ClosedDictionarySampleEquality.Goal : Prop := ∀ (pool : List Bytes) a b, (keys pool).Nodup → a ∈ pool → b ∈ pool → sig a = sig b → a = b

/-- An equal-length suffix appended to two sample-equivalent keys permits reuse of the old equality class. -/
def AppendedSuffixCacheReuse.Goal : Prop := ∀ a b tail, sig a = sig b → sig (a++tail) = sig (b++tail)

/-- Materialize only exact string hits inside the matching signature bucket without dropping duplicate build rows. -/
def VerifiedBucketLateGather.Goal : Prop := ∀ (pool : List Bytes) probe, (pool.filter (fun s => sig s == sig probe)).filter (· == probe) = pool.filter (· == probe)

/-- Order unequal long strings by their sample tuples when first words agree and trailing windows differ. -/
def PrefixFirstWindowOrder.Goal : Prop := ∀ a b : Bytes, a.length = b.length → win 0 a = win 0 b → sig a ≠ sig b →
  (a < b ↔ (win ((a.length-4)/2) a ++ win (a.length-4) a) < (win ((b.length-4)/2) b ++ win (b.length-4) b))

/-- Patching an unsampled byte leaves the generated filter signature unchanged but may require rechecking cached hits. -/
def UnobservedMiddlePatch.Goal : Prop := ∀ s i c, i < s.length → covered s.length i = false → sig (s.set i c) = sig s

end TinylambOpt.ResearchGoals.JitKernels.SampledEquality

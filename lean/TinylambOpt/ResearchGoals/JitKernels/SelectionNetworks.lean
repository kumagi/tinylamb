import Std

/-! Generate SIMD gather, compress and expand schedules from selection vectors. Invalid ordinals are suppressed in this model; repeated ordinals are separate SQL rows. -/
namespace TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

def gather {α : Type} (xs : List α) (sel : List Nat) := sel.filterMap (fun i => xs[i]?)
def positions (m : List Bool) := (m.zipIdx.filter Prod.fst).map Prod.snd
def compress {α : Type} (xs : List α) (m : List Bool) := gather xs (positions m)
def tile (sel : List Nat) (n : Nat) := sel.mergeSort (fun a b => decide (a/n ≤ b/n))

/-- Fuse two gathers by composing their selection vectors without allocating an intermediate result. -/
def TwoStageGatherIndexComposition.Goal : Prop := ∀ (xs : List Nat) a b, (∀ i ∈ a, i < xs.length) → gather (gather xs a) b = gather xs (gather a b)

/-- Compose gathers even when a first-stage out-of-range ordinal was suppressed. -/
def InvalidGatherLaneFiltering.Goal : Prop := ∀ (xs a b : List Nat), gather (gather xs a) b = gather xs (gather a b)

/-- Turn arbitrary gathered ordinals into a membership bitmap for a SUM kernel without tracking repeated lanes. -/
def BitmapSelectionMultiplicity.Goal : Prop := ∀ (xs sel : List Nat), (gather xs sel).sum =
  (compress xs ((List.range xs.length).map sel.contains)).sum

/-- Reorder requests by memory tile then recover the original outputs using each request's rank in the permuted ordinal list. -/
def GatherByTileRestoreOrdinal.Goal : Prop := ∀ (xs sel : List Nat) n, 0 < n → (∀ i ∈ sel, i < xs.length) →
  gather (gather xs (tile sel n)) (sel.map (fun i => (tile sel n).idxOf i)) = gather xs sel

/-- Use tile order without restoring source order for a first-value aggregate. -/
def TileOrderKeepsOrderSensitiveAggregate.Goal : Prop := ∀ xs sel n, 0 < n → (gather xs (tile sel n)).head? = (gather (xs : List Nat) sel).head?

/-- Generate gathers over runs of repeated row references by loading the referred value once and broadcasting it. -/
def GatherOfRunExpansion.Goal : Prop := ∀ (xs : List Nat) (runs : List (Nat × Nat)), gather xs (runs.flatMap (fun (i,n) => List.replicate n i)) =
  runs.flatMap (fun (i,n) => (xs[i]?).toList.flatMap (List.replicate n))

/-- Index a child compact mask in parent-rank space before emitting final source ordinals. -/
def CascadeMaskCoordinates.Goal : Prop := ∀ (xs : List Nat) a b, xs.length = a.length →
  compress (compress xs a) b = gather xs (gather (positions a) (positions b))

/-- Reuse a compiled shuffle control when the mask changes but its population count is unchanged. -/
def PopulationOnlyShuffleCache.Goal : Prop := ∀ (xs : List Nat) a b, xs.length = a.length → a.length = b.length → a.count true = b.count true →
  compress xs a = compress xs b

/-- Skip the first k selected rows by clearing all source bits through their last source ordinal. -/
def PackThenDropPrefix.Goal : Prop := ∀ (xs : List Nat) m k i, xs.length = m.length → (positions m)[k]? = some i →
  (compress xs m).drop k = compress (xs.drop i) (m.drop i)

/-- Under a fetch-once-per-line kernel, line-sorted requests require one line transition per distinct line. -/
def CoalescedGatherFootprint.Goal : Prop := ∀ (sel : List Nat) n, 0 < n →
  let lines := (tile sel n).map (·/n)
  (lines.zip (none::lines.map some)).countP (fun p => some p.1 != p.2) = lines.eraseDups.length

end TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

import Std

/-! A masked vector probe or variable-length parser runs until its slowest lane finishes. Each work item has a known or predicted number of steps. The model counts vector iterations, lane refills and stable output ordinals. -/
namespace TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

def peak (xs : List Nat) := xs.max?.getD 0
def waves (width : Nat) (steps : List Nat) :=
  (List.range ((steps.length+width-1)/width)).map (fun j => peak ((steps.drop (j*width)).take width))
def ticks (w : Nat) (xs : List Nat) := (waves w xs).sum
def maskAt (xs : List Nat) (iteration : Nat) := xs.map (fun n => decide (iteration < n))
def assign (loads : List Nat) (steps : Nat) :=
  let i := loads.idxOf (loads.min?.getD 0)
  loads.modify i (·+steps)
def refill (w : Nat) (steps : List Nat) := peak (steps.foldl assign (List.replicate w 0))

/-- Stable-sort by actual work length to minimize vector iterations over all permutations at a fixed width. -/
def SortedLengthWavesMinimizeTicks.Goal : Prop := ∀ steps order w, 0 < w → List.Perm steps order → ticks w steps.mergeSort ≤ ticks w order

/-- Doubling SIMD width realizes the ideal halving of wave iterations after rounding. -/
def DoublingWidthHalvesIterations.Goal : Prop := ∀ steps w, 0 < w → ticks (2*w) steps = (ticks w steps+1)/2

/-- The area under the active-mask population curve equals exactly the scalar lane work. -/
def UsefulLaneIterationConservation.Goal : Prop := ∀ xs, ((List.range (peak xs)).map (fun t => (maskAt xs t).count true)).sum = xs.sum

/-- Immediately refill the first completed lane from the input queue; this finishes no later than fixed packet barriers. -/
def RefillNoBarrierDominance.Goal : Prop := ∀ steps w, 0 < w → refill w steps ≤ ticks w steps

/-- If each predicted length differs from the actual one by at most e, a fixed schedule loses at most e per wave in predicted ticks. -/
def PredictionErrorWaveEnvelope.Goal : Prop := ∀ (a b : List Nat) w e, 0 < w → a.length = b.length →
  (∀ p ∈ a.zip b, p.1 ≤ p.2+e) → ticks w a ≤ ticks w b + (waves w b).length*e

/-- Persistent lanes achieve the larger of the longest individual task and mean work per lane, for every input order. -/
def RefillAlwaysHitsWorkLowerBound.Goal : Prop := ∀ steps w, 0 < w → refill w steps = max (peak steps) ((steps.sum+w-1)/w)

/-- Compacting zero-work lanes pays for a shuffle instruction per original packet even when zero lanes were already adjacent. -/
def NullLanePackingRepaysShuffle.Goal : Prop := ∀ steps w, 0 < w → 0 ∈ steps → ticks w (steps.filter (· != 0)) + (waves w steps).length ≤ ticks w steps

/-- For one bucket with lengths in a band, the unused lane steps are bounded by the band width plus the partial tail. -/
def HomogeneousBucketPaddingBound.Goal : Prop := ∀ steps w l h, 0 < w → (∀ x ∈ steps, l ≤ x ∧ x ≤ h) →
  w*ticks w steps - steps.sum ≤ steps.length*(h-l) + ((waves w steps).length*w-steps.length)*h

end TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

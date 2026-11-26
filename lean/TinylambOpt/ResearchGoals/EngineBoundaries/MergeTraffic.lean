import Std

/-!
A merge or compaction can reorganize its input streams subject to the modeled ordering,
traffic, and retention constraints.
-/

namespace TinylambOpt.ResearchGoals.EngineBoundaries.MergeTraffic

/-! Binary external merge plans. Leaves already exist on disk. Each materialized
merge reads both inputs, writes a new run, then releases both input files.
Lengths count payload bytes. The page-rounded variant charges each run separately. -/
inductive Tree where
  | leaf (bytes : Nat)
  | merge (left right : Tree)
  deriving Repr

def mass : Tree → Nat
  | .leaf n => n
  | .merge a b => mass a + mass b

def io : Tree → Nat
  | .leaf _ => 0
  | .merge a b => io a + io b + 2 * (mass a + mass b)

def depthBytes : Nat → Tree → Nat
  | d, .leaf n => d * n
  | d, .merge a b => depthBytes (d+1) a + depthBytes (d+1) b

def stream : Tree → Nat
  | .leaf n => n
  | .merge a b => io a + io b + mass a + mass b

def pages (p n : Nat) := (n+p-1)/p
def pageIO (p : Nat) : Tree → Nat
  | .leaf _ => 0
  | .merge a b => pageIO p a + pageIO p b + pages p (mass a) +
      pages p (mass b) + pages p (mass a + mass b)

def peak : Tree → Nat
  | .leaf n => n
  | .merge a b => max (peak a + mass b) (max (mass a + peak b) (2*(mass a+mass b)))

/-- A weighted external path length prices all intermediate read/write traffic. -/
def WeightedDepth.Goal : Prop := ∀ t, io t = 2 * depthBytes 0 t

/-- The cheaper rotation puts the larger of the two outer inputs at shallower depth. -/
def RotationThreshold.Goal : Prop := ∀ a b c,
  io (.merge (.merge a b) c) ≤ io (.merge a (.merge b c)) ↔ mass a ≤ mass c

/-- Pipelining the root saves exactly one full write on a non-leaf plan. -/
def LastWrite.Goal : Prop := ∀ a b,
  stream (.merge a b) + mass a + mass b = io (.merge a b)

/-- Choosing a rotation by page-rounded traffic still admits the byte-optimal orientation. -/
def RoundedRotation.Goal : Prop := ∀ p a b c, 0 < p → mass a ≤ mass c →
  pageIO p (.merge (.merge a b) c) ≤ pageIO p (.merge a (.merge b c))

/-- Summing byte costs then rounding once can undercount per-run transfers. -/
def RoundOnce.Goal : Prop := ∀ p t, 0 < p → pages p (io t) = pageIO p t

/-- If every run occupies whole pages, multiplying page traffic recovers byte traffic. -/
def AlignedTraffic.Goal : Prop := ∀ p t, 0 < p →
  (let rec aligned : Tree → Prop
     | .leaf n => p ∣ n
     | .merge a b => aligned a ∧ aligned b
   aligned t) → pageIO p t * p = io t

/-- Peak scratch space with both inputs retained depends only on the combined root size. -/
def RootPeak.Goal : Prop := ∀ a b, peak (.merge a b) = 2 * (mass a + mass b)

/-- For four leaves ask when the balanced pairing beats a left-deep merge. -/
def FourRunCrossover.Goal : Prop := ∀ a b c d,
  io (.merge (.merge a b) (.merge c d)) ≤ io (.merge (.merge (.merge a b) c) d) ↔
    mass d ≤ mass a + mass b

/-- Pre-merging a zero-byte run is free even if the other input is large. -/
def EmptyRunElision.Goal : Prop := ∀ t, io (.merge (.leaf 0) t) = io t

/-- Streaming the root changes no cost ordering among plans for the same non-leaf input. -/
def StreamChoice.Goal : Prop := ∀ a b c d, mass a + mass b = mass c + mass d →
  (stream (.merge a b) ≤ stream (.merge c d) ↔ io (.merge a b) ≤ io (.merge c d))

#guard pages 8 (io (.merge (.leaf 1) (.leaf 1))) == 1
#guard pageIO 8 (.merge (.leaf 1) (.leaf 1)) == 3

end TinylambOpt.ResearchGoals.EngineBoundaries.MergeTraffic

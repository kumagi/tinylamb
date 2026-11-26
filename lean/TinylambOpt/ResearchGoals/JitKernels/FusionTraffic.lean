import Std

/-! Compare late gathering, eager SIMD loads and materialization using the actual cache-line footprint of a packet. Setup, per-line fetch and replay charges are explicit rather than elapsed-time guarantees. -/
namespace TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

def lineOf (base stride width i : Nat) := (base+stride*i)/width
def footprint (base stride width : Nat) (sel : List Nat) :=
  (sel.map (lineOf base stride width)).eraseDups
def lazyCost (base stride width start line : Nat) (sel : List Nat) :=
  start*sel.length+line*(footprint base stride width sel).length

/-- Select eager versus sparse payload loads solely by population count when the two selections are sorted. -/
def SelectionCountOnlyFusionGuard.Goal : Prop := ∀ (a b : List Nat) base stride width, 0 < width → a.Pairwise (· < ·) → b.Pairwise (· < ·) →
  a.length = b.length → (footprint base stride width a).length = (footprint base stride width b).length

/-- Translate a generated kernel to an aligned subview without rebuilding its relative cache-line groups. -/
def AlignedMorselLineRemap.Goal : Prop := ∀ base stride width sel k, 0 < width →
  footprint (base+k*width) stride width sel = (footprint base stride width sel).map (k+·)

/-- Fuse two projections over the same payload and charge shared cache lines exactly once. -/
def CrossConsumerLineReuse.Goal : Prop := ∀ base stride w a b, let f := footprint base stride w
  (f (a++b)).length + ((f a).filter (fun l => (f b).contains l)).length = (f a).length+(f b).length

/-- A contiguous set of accepted byte rows touches no more than one additional line over its rounded-up payload length. -/
def BroadwordLoadFootprintBound.Goal : Prop := ∀ base n w, 0 < w → (footprint base 1 w (List.range n)).length ≤ (n+w-1)/w+1

/-- Speculatively load dense lanes when saved gather-dispatch cost covers the additional lines. -/
def CacheLineCoalesceCrossover.Goal : Prop := ∀ base stride w (sel : List Nat) n issue gatherFee line, (∀ i ∈ sel, i < n) →
  let dense := (footprint base stride w (List.range n)).length
  let sparse := (footprint base stride w sel).length
  (issue+line*dense ≤ lazyCost base stride w gatherFee line sel ↔
   issue+line*(dense-sparse) ≤ gatherFee*sel.length)

/-- Once the first address in a cache line is proven readable, all speculative loads sharing that line stay in the logical column slice. -/
def PageGuardAmortizedByLineHit.Goal : Prop := ∀ base stride w i j limit, 0 < w → lineOf base stride w i = lineOf base stride w j →
  base+stride*i < limit → base+stride*j < limit

/-- Reorder gathers for an independent consumer while preserving the set of compulsory line fetches. -/
def ProbeReorderCacheLineParity.Goal : Prop := ∀ base stride w (a b : List Nat), a.Perm b → (footprint base stride w a).Perm (footprint base stride w b)

end TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

import Std

/-! Generated execution over delta-coded columns. The kth row equals the base plus the inclusive prefix of signed deltas. Selections address logical rows; adjacent packed frames can restart at different absolute bases. -/
namespace TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

def values (base : Int) (ds : List Int) := (ds.scanl (·+·) base).drop 1
def get (base : Int) (ds : List Int) (i : Nat) := base+(ds.take (i+1)).sum
def weighted (ds : List Int) := (ds.zipIdx.map (fun (v,i) => v*((ds.length-i : Nat):Int))).sum
def strideScan (m : Nat) (base : Int) (ds : List Int) (i : Nat) :=
  base + ((ds.zipIdx.filter (fun p => p.2 ≤ i && p.2%m == i%m)).map Prod.fst).sum

/-- Fuse decode with SUM using a reverse ordinal weight for each delta. -/
def TriangularDeltaSum.Goal : Prop := ∀ b ds, (values b ds).sum = (ds.length:Int)*b + weighted ds

/-- Remove unselected deltas before the lane prefix sum, then emit the compacted values. -/
def FilterBeforePrefixDecode.Goal : Prop := ∀ b ds (keep : Nat → Bool), values b ((ds.zipIdx.filter (fun p => keep p.2)).map Prod.fst) =
  (((values b ds).zipIdx.filter (fun p => keep p.2)).map Prod.fst)

/-- Continue a vector prefix scan from a single carry when crossing a morsel boundary. -/
def HorizontalScanChunkSeed.Goal : Prop := ∀ b a c, values b (a++c) = values b a ++ values (b+a.sum) c

/-- SIMD lanes can carry independent accumulators across iterations when all encoded deltas are nonnegative. -/
def CrossLanePrefixIsolation.Goal : Prop := ∀ b ds i m, 0 < m → i < ds.length → (∀ d ∈ ds, 0 ≤ d) → strideScan m b ds i = get b ds i

/-- With nonnegative deltas the final endpoint suffices to prove every decoded row lies below an upper bound. -/
def EndpointDeltaRangeFence.Goal : Prop := ∀ b ds threshold, (∀ d ∈ ds, 0 ≤ d) → b+ds.sum < threshold →
  (values b ds).all (fun v => v < threshold)

/-- Compute a selected row directly from base, run length and stride for a constant-delta segment. -/
def RepeatedDeltaRunDirectSelect.Goal : Prop := ∀ b (d : Int) n i, i < n → get b (List.replicate n d) i = b + (i+1:Nat)*d

/-- A narrow unsigned carry can be widened back at block end if both the initial and final decoded values fit. -/
def ScanWithModularCarry.Goal : Prop := ∀ (base : Nat) (ds : List Int) w, base < 2^w → 0 ≤ (base:Int)+ds.sum →
  (base:Int)+ds.sum < 2^w → ∀ x ∈ values base ds, 0 ≤ x ∧ x < 2^w

/-- Compute a difference of two materialized rows from just the intervening encoded deltas. -/
def SkipIntervalDifference.Goal : Prop := ∀ b ds i j, i ≤ j → j < ds.length → get b ds j - get b ds i = ((ds.drop (i+1)).take (j-i)).sum

/-- When net drift is positive, endpoints suffice for a zone-map test even if individual deltas have mixed signs. -/
def DeltaZoneMapFromEndpoints.Goal : Prop := ∀ b ds cap, 0 ≤ ds.sum → b+ds.sum ≤ cap → (values b ds).all (fun x => x ≤ cap)

/-- Sum an arbitrary selection with duplicates by weighting each delta by the number of selected suffix positions. -/
def SparseGatherWeightedPrefix.Goal : Prop := ∀ b ds (sel : List Nat), (∀ i ∈ sel, i < ds.length) →
  (sel.map (get b ds)).sum = (sel.length:Int)*b +
    (ds.zipIdx.map (fun p => p.1*((sel.countP (p.2 ≤ ·):Nat):Int))).sum

end TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

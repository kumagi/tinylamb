import Std

/-! A SIMD group-by combines lanes targeting the same accumulator. Keys are physical slots, values are signed deltas, and the initial accumulator may be nonzero. -/
namespace TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

abbrev Updates := List (Nat × Int)
def delta (us : Updates) (k : Nat) := ((us.filter (fun u => u.1 == k)).map Prod.snd).sum
def sequential (init : Nat → Int) (us : Updates) (k : Nat) := init k + delta us k
def leaders (us : Updates) := (us.map Prod.fst).eraseDups
def combine (us : Updates) := (leaders us).map (fun k => (k,delta us k))
def lastLane (us : Updates) (k : Nat) := (us.reverse.lookup k).getD 0
def check (lo hi : Int) (s : Int) (us : Updates) (k : Nat) :=
  (((us.filter (fun u => u.1 == k)).map Prod.snd).scanl (·+·) s).all (fun x => lo ≤ x && x ≤ hi)

/-- Issue one scatter per distinct slot after reducing all matching lanes. -/
def LeaderReducedScatter.Goal : Prop := ∀ us init k, sequential init (combine us) k = sequential init us k

/-- Use native last-writer-wins scatter of independently updated lanes for colliding nonzero counters. -/
def LastLaneRmwSubstitution.Goal : Prop := ∀ us init k, 0 < (us.map Prod.fst).count k → init k + lastLane us k = sequential init us k

/-- Coalesce contributions across the boundary of two vector batches before the final read-modify-write. -/
def StoreOncePerVectorPair.Goal : Prop := ∀ a b k, delta (combine (combine a ++ combine b)) k = delta (a++b) k

/-- Guard each reduced group using only initial and final counter bounds. -/
def CheckedScatterFromFinalExtrema.Goal : Prop := ∀ us k lo hi s, lo ≤ s → s ≤ hi → lo ≤ s+delta us k → s+delta us k ≤ hi → check lo hi s us k

/-- With sign-uniform deltas for a slot, validate the entire checked update from two endpoints. -/
def MonotoneScatterRangeCertificate.Goal : Prop := ∀ us k lo hi s, (∀ u ∈ us, u.1 = k → 0 ≤ u.2) → lo ≤ s → s+delta us k ≤ hi → check lo hi s us k

/-- Low address bits can replace equality when electing a single scatter leader for each bucket. -/
def TruncatedConflictBallot.Goal : Prop := ∀ us m k, 0 < m → k ∈ leaders us → (us.map Prod.fst).Pairwise (· ≤ ·) →
  ((leaders us).zip (leaders us).tail).all (fun (a,b) => a%m != b%m) →
  delta (us.map (fun u => (u.1%m,u.2))) (k%m) = delta us k

/-- Disjoint slot sets allow two compiled waves to publish independently without losing a contribution. -/
def DisjointWaveCommit.Goal : Prop := ∀ a b k, (∀ x ∈ leaders a, ¬ x ∈ leaders b) →
  delta (a++b) k = (if k ∈ leaders a then delta a k else delta b k)

/-- Skip the arithmetic store for a zero reduced delta while retaining correct numeric sums. -/
def ZeroDeltaLeaderElision.Goal : Prop := ∀ us k, delta ((combine us).filter (fun u => u.2 != 0)) k = delta us k

/-- Use nonzero reduced deltas as the occupied-slot list for emitting SQL groups. -/
def ZeroDeltaOccupancyElision.Goal : Prop := ∀ us, leaders ((combine us).filter (fun u => u.2 != 0)) = leaders us

/-- Partition lanes by ordinal parity and reconcile their signed counters once at the end of the loop. -/
def PrivatizedStripeMerge.Goal : Prop := ∀ (us : Updates) k, let lanes := us.zipIdx
  delta ((lanes.filter (fun u => u.2%2 == 0)).map Prod.fst) k +
    delta ((lanes.filter (fun u => u.2%2 == 1)).map Prod.fst) k = delta us k

end TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

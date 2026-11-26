import Std

/-! First-fit and best-fit placement into reusable contiguous holes. Requests do
not move previously granted buffers. Arena trace success is a stronger requirement
than just fitting the sum of its requests. Positive lengths are measured in bytes. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

def alloc (n : Nat) : List Nat → Option (List Nat)
  | [] => none
  | h::hs => if n ≤ h then some ((h-n)::hs) else (alloc n hs).map (h::·)
def place (holes : List Nat) (requests : List Nat) := requests.foldlM (fun hs n => alloc n hs) holes
def bestFit (holes : List Nat) (requests : List Nat) :=
  requests.foldlM (fun (hs : List Nat) n => alloc n hs.mergeSort) holes

/-- A completed trace charges exactly the requested bytes, independent of internal fragmentation. -/
def SuccessfulTraceByteConservation.Goal : Prop := ∀ hs rs final,
  place hs rs = some final → final.sum + rs.sum = hs.sum

/-- The largest free extent suffices to decide admission of one nonzero contiguous grant. -/
def LargestHoleSingleAdmission.Goal : Prop := ∀ hs n, 0 < n →
  (alloc n hs).isSome = decide (n ≤ hs.max?.getD 0)

/-- Descending-size placement is a sufficient exact admission test for a batch that fits in some order. -/
def DescendingSizeBatchAdmission.Goal : Prop := ∀ hs (rs order : List Nat), rs.Perm order →
  (place hs order).isSome = true → (place hs (rs.mergeSort (fun a b => decide (b ≤ a)))).isSome = true

/-- Best-fit placement never loses an allocation trace that first-fit can finish. -/
def BestFitTraceDominance.Goal : Prop := ∀ hs rs, (place hs rs).isSome = true → (bestFit hs rs).isSome = true

/-- Coalescing neighboring extents never loses a previously feasible first-fit trace in the same list order. -/
def NeighborCoalesceTraceDominance.Goal : Prop := ∀ front a b back rs,
  (place (front++[a,b]++back) rs).isSome = true → (place (front++[a+b]++back) rs).isSome = true

/-- Allowing scatter grants over arbitrary holes admits a trace whenever the sum fits after one ideal compaction. -/
def CompactedArenaTraceAdmission.Goal : Prop := ∀ (hs rs : List Nat),
  ((place [hs.sum] rs).isSome = true ↔ rs.sum ≤ hs.sum)

def rounded (classes : List Nat) (n : Nat) := (classes.filter (fun c => n ≤ c)).min?
def charge (classes requests : List Nat) := (requests.mapM (rounded classes)).map List.sum

/-- Adding an intermediate size class cannot increase reserved bytes for an already supported trace. -/
def SizeClassRefinement.Goal : Prop := ∀ cs rs extra old, charge cs rs = some old →
  ∃ fresh, charge (extra::cs) rs = some fresh ∧ fresh ≤ old

/-- Evaluate class-table changes only on requests that cross the proposed class boundary. -/
def ClassInsertionMarginalSavings.Goal : Prop := ∀ cs rs x a b, charge cs rs = some a →
  charge (x::cs) rs = some b →
  a-b = (rs.map (fun n => if n ≤ x then (rounded cs n).getD x - x else 0)).sum

/-- Reserving by the rounded mean request size bounds total allocation under arbitrary skew. -/
def MeanSizeClassReservation.Goal : Prop := ∀ cs rs total c, rs ≠ [] → charge cs rs = some total →
  rounded cs ((rs.sum+rs.length-1)/rs.length) = some c → total ≤ rs.length*c

/-- Growing individual holes preserves first-fit admission even if a request now lands in an earlier hole. -/
def LargerArenaTraceMonotonicity.Goal : Prop := ∀ (a b : List Nat) rs, a.length = b.length → (∀ p ∈ a.zip b, p.1 ≤ p.2) →
  (place a rs).isSome = true → (place b rs).isSome = true

#guard place [3,3] [4] == none
#guard charge [8,16,32] [1,9,15] == some 40

-- Extent order controls later first-fit decisions even when total space is fixed.
#guard (place [2,2,3] [3,2,2]).isSome && (place [4,3] [3,2,2]).isNone
#guard (place [3,2] [1,2,2]).isSome && (bestFit [3,2] [1,2,2]).isNone

end TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

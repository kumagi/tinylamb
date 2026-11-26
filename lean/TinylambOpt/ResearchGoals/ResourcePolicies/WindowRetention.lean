import TinylambOpt.ResearchGoals.QueryRewrites.Common

/-! Bounded ROWS frames across morsels and spill boundaries. The frame includes
the current row; positions before the partition are absent. Peer keys are modeled
separately for RANGE so that a long run of ties is not mistaken for bounded ROWS. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention
open TinylambOpt.ResearchGoals.QueryRewrites

def frame {α : Type} (rows : List α) (preceding i : Nat) :=
  (rows.take (i+1)).drop (i-preceding)
def tailRows {α : Type} (rows : List α) (n : Nat) := rows.drop (rows.length-n)

/-- At a morsel cut, retaining only the last w predecessor rows suffices for all later bounded ROWS frames. -/
def RowsFrameSpillHalo.Goal : Prop := ∀ (a b : List (Option Int)) w j, j < b.length →
  frame (a++b) w (a.length+j) = frame (tailRows a w ++ b) w ((tailRows a w).length+j)

/-- Bounded predecessor halos compose after a second spill without reviving discarded partition history. -/
def ConsecutiveHaloReplacement.Goal : Prop := ∀ (a b : List Nat) w,
  tailRows (tailRows a w ++ b) w = tailRows (a++b) w

/-- Retaining the w most recent non-NULL payloads suffices for SUM over a w-preceding physical-row frame. -/
def SkipNullsBeforeFrameBoundary.Goal : Prop := ∀ (old fresh : List (Option Int)) w j, j < fresh.length →
  nsum (frame (old++fresh) w (old.length+j)) =
    let halo := tailRows (old.filter Option.isSome) w
    nsum (frame (halo++fresh) w (halo.length+j))

/-- Sliding SUM uses an exact sum and non-NULL count so a zero sum and an empty input stay distinct. -/
def SlidingNullableFrameUpdate.Goal : Prop := ∀ (expired entered : Option Int) (middle : List (Option Int)),
  let old := expired::middle
  let sum := (old.filterMap id).sum - expired.getD 0 + entered.getD 0
  let count := (old.filterMap id).length - expired.toList.length + entered.toList.length
  (if count = 0 then none else some sum) = nsum (middle++[entered])

/-- Test unconditional erase on expiry as a cheaper replacement for reference counts in a sliding DISTINCT set. -/
def EvictedValueSetDifference.Goal : Prop := ∀ (x : Nat) middle entered,
  (((x::middle).eraseDups.erase x ++ [entered]).eraseDups).Perm (middle++[entered]).eraseDups

/-- A frequency decrement followed by an increment reconstructs the distinct membership of the next window. -/
def CountedWindowMembership.Goal : Prop := ∀ (x y k : Nat) middle,
  (k ∈ (middle++[y]).eraseDups ↔ 0 < (x::middle).count k - (if x == k then 1 else 0) + (if y == k then 1 else 0))

/-- Bound peer buffering by distinct sort keys when RANGE CURRENT ROW has no tie-breaker. -/
def PeerKeysBoundBufferedRows.Goal : Prop := ∀ (keys : List Nat) k,
  (keys.filter (· == k)).length ≤ keys.eraseDups.length

/-- A strictly larger following key certifies the complete peer group in a sorted, non-null partition. -/
def StrictKeyClosesPeerGroup.Goal : Prop := ∀ (a b : List Nat) k next, k < next →
  (a++next::b).Pairwise (· ≤ ·) → (a++next::b).count k = a.count k

/-- A fixed-width lag state suffices for IGNORE NULLS when its cap counts actual non-NULL values. -/
def IgnoreNullsLagState.Goal : Prop := ∀ (a b : List (Option Int)) k,
  tailRows (tailRows (a.filterMap id) k ++ b.filterMap id) k = tailRows ((a++b).filterMap id) k

#guard frame [1,2,3,4] 1 2 == [2,3]

end TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

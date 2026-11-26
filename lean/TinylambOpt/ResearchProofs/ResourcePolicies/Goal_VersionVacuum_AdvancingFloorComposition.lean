import TinylambOpt.ResearchProofs.Batch5.Proof_ResourcePolicies_VersionVacuum_AdvancingFloorComposition

/-!
Registers the existing checked result for `VersionVacuum.AdvancingFloorComposition.Goal`.

/-- One predecessor suffices across all future snapshot times above the oldest pin. -/
def DenseFloorFutureReads.Goal : Prop := ∀ vs f s, ordered vs → f ≤ s → readAt (dense vs f) s = readAt vs s

/-- Retaining actual winners can preserve pinned snapshots even when there are long gaps between them. -/
def SparseSnapshotWinnerRetention.Goal : Prop := ∀ vs pins s, s ∈ pins →
  winner (sparse vs pins) s = winner vs s

/-- Shrinking the pin set can compact an already pruned chain without resurrecting discarded versions. -/
def PinReleaseIncrementalPruning.Goal : Prop := ∀ vs (p q : List Nat), (∀ s ∈ q, s ∈ p) →
  sparse (sparse vs p) q = sparse vs q

/-- At most one version per distinct pin is needed for pinned reads when commit stamps are unique. -/
def DistinctPinStorageCap.Goal : Prop := ∀ vs pins, ordered vs →
  (sparse vs pins).length ≤ pins.eraseDups.length

/-- Keeping only pins at the endpoints of a continuous snapshot interval preserves each interior read. -/
def EndpointSnapshotInterpolation.Goal : Prop := ∀ vs lo hi s, ordered vs → lo ≤ s → s ≤ hi →
  readAt (sparse vs [lo,hi]) s = readAt vs s

/-- A sparse live-set collector may leave out the head only if future reads cannot observe a new value there. -/
def IncludeNewestForFutureReads.Goal : Prop := ∀ vs pins tip s, ordered vs →
  (∀ v ∈ vs, v.stamp ≤ tip) → tip ≤ s → readAt (sparse vs (tip::pins)) s = readAt vs s

/-- Vacuum of different physical fragments at the same floor may be performed independently before joining histories. -/
def FragmentLocalSnapshotFloor.Goal : Prop := ∀ a b f s, ordered (a++b) → f ≤ s →
  readAt (dense a f ++ dense b f) s = readAt (a++b) s

/-- Try discarding tombstones in the upper fragment without checking whether a lower fragment still carries a shadowed value. -/
def BottomCoverageBeforeTombstoneDrop.Goal : Prop := ∀ upper lower s,
  ordered (upper++lower) → (winner upper s).isSome →
  readAt ((upper.filter (fun v => v.value.isSome))++lower) s = readAt (upper++lower) s

/-- A second collection at a higher floor needs no copy of the original uncollected chain. -/

The theorem is contributed by `TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.VersionVacuum.Goal_AdvancingFloorComposition` and re-exported here so that
`VersionVacuum_All.lean` registers the goal. SQL execution may rely on the
modeled result only under the assumptions stated above.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_VersionVacuum_AdvancingFloorComposition

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum.AdvancingFloorComposition.Goal :=
  TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.VersionVacuum.Goal_AdvancingFloorComposition.sound

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_VersionVacuum_AdvancingFloorComposition

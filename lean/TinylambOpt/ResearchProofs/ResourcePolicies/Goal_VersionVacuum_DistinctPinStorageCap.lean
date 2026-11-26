import TinylambOpt.ResearchProofs.Batch5.Proof_ResourcePolicies_VersionVacuum_DistinctPinStorageCap

/-!
Registers the existing checked result for `VersionVacuum.DistinctPinStorageCap.Goal`.

/-- One predecessor suffices across all future snapshot times above the oldest pin. -/
def DenseFloorFutureReads.Goal : Prop := ∀ vs f s, ordered vs → f ≤ s → readAt (dense vs f) s = readAt vs s

/-- Retaining actual winners can preserve pinned snapshots even when there are long gaps between them. -/
def SparseSnapshotWinnerRetention.Goal : Prop := ∀ vs pins s, s ∈ pins →
  winner (sparse vs pins) s = winner vs s

/-- Shrinking the pin set can compact an already pruned chain without resurrecting discarded versions. -/
def PinReleaseIncrementalPruning.Goal : Prop := ∀ vs (p q : List Nat), (∀ s ∈ q, s ∈ p) →
  sparse (sparse vs p) q = sparse vs q

/-- At most one version per distinct pin is needed for pinned reads when commit stamps are unique. -/

The theorem is contributed by `TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.VersionVacuum.Goal_DistinctPinStorageCap` and re-exported here so that
`VersionVacuum_All.lean` registers the goal. SQL execution may rely on the
modeled result only under the assumptions stated above.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_VersionVacuum_DistinctPinStorageCap

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum.DistinctPinStorageCap.Goal :=
  TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.VersionVacuum.Goal_DistinctPinStorageCap.sound

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_VersionVacuum_DistinctPinStorageCap

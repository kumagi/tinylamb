import TinylambOpt.ResearchGoals.DatabaseCorrectness.PinnedScan

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_PinnedScan_LaterTombstoneDuringScan

open TinylambOpt.ResearchGoals.DatabaseCorrectness.PinnedScan
open TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum

theorem winner_cons_future (s : List Version) (snap t : Nat) (v : Option Nat) (hlt : snap < t) :
    winner (⟨t, v⟩ :: s) snap = winner s snap := by
  have htn : ¬ t ≤ snap := by omega
  simp [winner, htn]

theorem sound : LaterTombstoneDuringScan.Goal := by
  intro s snap t hlt
  simp [resolve, install, readAt, winner_cons_future s.versions snap t none hlt]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_PinnedScan_LaterTombstoneDuringScan

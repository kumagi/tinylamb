import TinylambOpt.ResearchGoals.DatabaseCorrectness.PinnedScan

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_PinnedScan_SnapshotScanAcrossMorselBoundary

open TinylambOpt.ResearchGoals.DatabaseCorrectness.PinnedScan
open TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum

theorem winner_cons_future (s : List Version) (snap t : Nat) (v : Option Nat) (hlt : snap < t) :
    winner (⟨t, v⟩ :: s) snap = winner s snap := by
  have htn : ¬ t ≤ snap := by omega
  simp [winner, htn]

theorem resolve_install_eq (s : Slot) (snap t : Nat) (v : Option Nat) (hlt : snap < t)
    (hreg : resolve s snap = readAt s.versions snap) :
    resolve (install s t v) snap = resolve s snap := by
  have hw := winner_cons_future s.versions snap t v hlt
  by_cases hsr : s.registered = true
  · simp [install, resolve, readAt, hw, hsr]
  · simp [resolve, hsr] at hreg ⊢
    simp [install, readAt, hw, hreg]

theorem sound : SnapshotScanAcrossMorselBoundary.Goal := by
  intro a b snap t v hlt hb
  rw [List.map_append]
  congr 1
  apply List.map_congr_left
  intro s hs
  exact resolve_install_eq s snap t v hlt (hb s hs)

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_PinnedScan_SnapshotScanAcrossMorselBoundary

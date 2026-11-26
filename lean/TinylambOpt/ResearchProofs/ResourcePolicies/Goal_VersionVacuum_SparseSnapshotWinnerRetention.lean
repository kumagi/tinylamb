import TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum

/-!
`SparseSnapshotWinnerRetention`: a pin set that contains snapshot `s` keeps the
version that `s` actually reads. SQL execution may therefore prune a version chain
to just the winners of active snapshot pins without changing pinned reads.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_VersionVacuum_SparseSnapshotWinnerRetention

open TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum

/-- The first element satisfying `q` remains first after filtering by `p` when it satisfies `p`. -/
private theorem find_filter_first {α : Type} [DecidableEq α] (p q : α → Bool) :
    ∀ (xs : List α) (v : α), xs.find? q = some v → p v = true → (xs.filter p).find? q = some v := by
  intro xs
  induction xs with
  | nil => intro v h; cases h
  | cons x xs ih =>
      intro v hv hp
      by_cases hqx : q x = true
      · have hxv : x = v := by simpa [hqx] using hv
        subst v
        simp [hqx, hp]
      · simp [hqx] at hv ⊢
        simpa [List.find?_filter] using ih v hv hp

theorem sound : SparseSnapshotWinnerRetention.Goal := by
  intro vs pins s hs
  unfold winner sparse
  cases hw : vs.find? (fun v => v.stamp ≤ s) with
  | none =>
      rw [List.find?_eq_none]
      intro v hv
      rcases List.mem_filter.mp hv with ⟨hvin, hpv⟩
      rw [List.any_eq_true] at hpv
      rcases hpv with ⟨s', hs', hws'⟩
      have hno := (List.find?_eq_none.mp hw) v hvin
      simp [hws'] at hno
      have hnot : ¬ v.stamp ≤ s := by omega
      simp [hnot]
  | some v =>
      have hp : pins.any (fun s' => vs.find? (fun w => w.stamp ≤ s') == some v) = true := by
        rw [List.any_eq_true]
        exact ⟨s, hs, by simp [hw]⟩
      exact find_filter_first (fun w => pins.any (fun s' => vs.find? (fun u => u.stamp ≤ s') == some w))
        (fun w => w.stamp ≤ s) vs v hw hp

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_VersionVacuum_SparseSnapshotWinnerRetention

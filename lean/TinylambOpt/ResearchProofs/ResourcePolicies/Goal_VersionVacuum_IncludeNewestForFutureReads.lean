import TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum
import TinylambOpt.ResearchProofs.ResourcePolicies.Goal_VersionVacuum_SparseSnapshotWinnerRetention

/-!
`IncludeNewestForFutureReads`: when the chain has no version newer than `tip`,
pinning `tip` keeps the version that every later snapshot reads. SQL execution
may include the tip pin and omit newer-version bookkeeping.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_VersionVacuum_IncludeNewestForFutureReads

open TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum

private theorem find_eq_of {α : Type} [DecidableEq α] (p q : α → Bool) :
    ∀ xs : List α, (∀ x ∈ xs, p x = q x) → xs.find? p = xs.find? q := by
  intro xs
  induction xs with
  | nil => intro h; rfl
  | cons x xs ih =>
      intro h
      have hx := h x (by simp)
      by_cases hpx : p x = true
      · have hqx : q x = true := by rw [← hx]; exact hpx
        simp [hpx, hqx]
      · have hqx : q x ≠ true := by rw [← hx]; exact hpx
        simp [hpx, hqx]
        exact ih (fun y hy => h y (by simp [hy]))

theorem sound : IncludeNewestForFutureReads.Goal := by
  intro vs pins tip s hord hall hts
  let ps := tip :: pins
  have hpred : ∀ v ∈ vs, decide (v.stamp ≤ s) = decide (v.stamp ≤ tip) := by
    intro v hv
    have hvle := hall v hv
    by_cases hle : v.stamp ≤ tip
    · have hleS : v.stamp ≤ s := by omega
      simp [hle, hleS]
    · have hleS : ¬ v.stamp ≤ s := by omega
      simp [hle, hleS]
  have hwin : winner vs s = winner vs tip := by
    unfold winner
    exact find_eq_of (fun v : Version => v.stamp ≤ s) (fun v => v.stamp ≤ tip) vs hpred
  have hspred : ∀ v ∈ sparse vs ps, decide (v.stamp ≤ s) = decide (v.stamp ≤ tip) := by
    intro v hv
    exact hpred v (List.mem_filter.mp hv).1
  have hswin : winner (sparse vs ps) s = winner (sparse vs ps) tip := by
    unfold winner
    exact find_eq_of (fun v : Version => v.stamp ≤ s) (fun v => v.stamp ≤ tip) (sparse vs ps) hspred
  have htipwin : winner (sparse vs ps) tip = winner vs tip :=
    TinylambOpt.ResearchProofs.ResourcePolicies.Goal_VersionVacuum_SparseSnapshotWinnerRetention.sound
      vs ps tip (by simp [ps])
  unfold readAt
  rw [hswin, htipwin, hwin]

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_VersionVacuum_IncludeNewestForFutureReads

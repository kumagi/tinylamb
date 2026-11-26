import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.EvictionWriteback.Goal_BoundedTicketTagAlias

open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback


/-!
Compact (incarnation, revision) ticket tags are injective inside a bounded
window of both counters. Equal residues plus a strictly-sub-modulus distance
force the counters themselves to agree, so a recycled slot cannot alias a
still-live flush ticket inside the window. This justifies tag-only dispatch
without consulting the full ticket image.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

private theorem mod_eq_of_diff_lt {m x y : Nat} (hm : 0 < m) (hle : x ≤ y)
    (hdiff : y - x < m) (hmod : y % m = x % m) : y = x := by
  let d := y - x
  have hy : y = x + d := by dsimp [d]; omega
  have hdlt : d < m := by simpa [d] using hdiff
  have hdmod : d % m = d := Nat.mod_eq_of_lt hdlt
  rw [hy, Nat.add_mod, hdmod] at hmod
  let r := x % m
  have hr : r < m := Nat.mod_lt x hm
  have hEq : (r + d) % m = r := by simpa [r] using hmod
  by_cases hlt : r + d < m
  · have hm1 : (r + d) % m = r + d := Nat.mod_eq_of_lt hlt
    rw [hm1] at hEq
    dsimp [d] at hy
    omega
  · have hge : r + d ≥ m := by omega
    have hsublt : r + d - m < m := by omega
    have hm2 : (r + d) % m = (r + d - m) % m := Nat.mod_eq_sub_mod hge
    rw [hm2, Nat.mod_eq_of_lt hsublt] at hEq
    dsimp [d] at hy
    omega

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback.BoundedTicketTagAlias.Goal := by
  intro m a b hm hdi hdr ht
  have hinci : a.incarnation % m = b.incarnation % m := by
    have h := congrArg Prod.fst ht
    simpa [tag] using h
  have hinc : a.incarnation = b.incarnation := by
    by_cases hle : a.incarnation ≤ b.incarnation
    · have hd : b.incarnation - a.incarnation < m := by
        simpa [Nat.max_eq_right hle, Nat.min_eq_left hle] using hdi
      exact (mod_eq_of_diff_lt hm hle hd hinci.symm).symm
    · have hle' : b.incarnation ≤ a.incarnation := by omega
      have hd : a.incarnation - b.incarnation < m := by
        simpa [Nat.max_eq_left hle', Nat.min_eq_right hle'] using hdi
      exact mod_eq_of_diff_lt hm hle' hd hinci
  have hrevmod : a.revision % m = b.revision % m := by
    have h := congrArg Prod.snd ht
    simpa [tag] using h
  have hrev : a.revision = b.revision := by
    by_cases hle : a.revision ≤ b.revision
    · have hd : b.revision - a.revision < m := by
        simpa [Nat.max_eq_right hle, Nat.min_eq_left hle] using hdr
      exact (mod_eq_of_diff_lt hm hle hd hrevmod.symm).symm
    · have hle' : b.revision ≤ a.revision := by omega
      have hd : a.revision - b.revision < m := by
        simpa [Nat.max_eq_left hle', Nat.min_eq_right hle'] using hdr
      exact mod_eq_of_diff_lt hm hle' hd hrevmod
  exact ⟨hinc, hrev⟩
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.EvictionWriteback.Goal_BoundedTicketTagAlias

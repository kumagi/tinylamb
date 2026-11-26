import TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

namespace TinylambOpt.ResearchProofs.ResourcePolicies.EvictionWriteback

open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

private theorem eq_of_mod_eq_of_span_lt {a b m : Nat} (hm : 0 < m)
    (hmod : a % m = b % m) (hspan : max a b - min a b < m) : a = b := by
  by_cases h : a = b
  · exact h
  · rcases Nat.lt_or_gt_of_ne h with hlt | hgt
    · have hle : a ≤ b := Nat.le_of_lt hlt
      have hsub : (b - a) % m = 0 := Nat.sub_mod_eq_zero_of_mod_eq hmod.symm
      have hpos : 0 < b - a := by omega
      have hdvd : m ∣ (b - a) := Nat.dvd_of_mod_eq_zero hsub
      have hge : m ≤ b - a := Nat.le_of_dvd hpos hdvd
      have hspan' : max a b - min a b = b - a := by simp [Nat.max_eq_right hle, Nat.min_eq_left hle]
      omega
    · have hle : b ≤ a := Nat.le_of_lt hgt
      have hsub : (a - b) % m = 0 := Nat.sub_mod_eq_zero_of_mod_eq hmod
      have hpos : 0 < a - b := by omega
      have hdvd : m ∣ (a - b) := Nat.dvd_of_mod_eq_zero hsub
      have hge : m ≤ a - b := Nat.le_of_dvd hpos hdvd
      have hspan' : max a b - min a b = a - b := by simp [Nat.max_eq_left hle, Nat.min_eq_right hle]
      omega

theorem boundedTicketTagAlias : BoundedTicketTagAlias.Goal := by
  intro modulus a b hmod hinc hrev htag
  unfold tag at htag
  simp only [Prod.mk.injEq] at htag
  rcases htag with ⟨h1, h2⟩
  exact ⟨eq_of_mod_eq_of_span_lt hmod h1 hinc, eq_of_mod_eq_of_span_lt hmod h2 hrev⟩

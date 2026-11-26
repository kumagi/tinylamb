import TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_EvictionWriteback_BoundedTicketTagAlias
open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

private theorem add_mod_eq_self_iff {m z k : Nat} (hm : 0 < m) (hz : z < m) (hk : k < m) :
    (z + k) % m = z % m ↔ k = 0 := by
  constructor
  · intro h
    rw [Nat.mod_eq_of_lt hz] at h
    by_cases hzk : z + k < m
    · rw [Nat.mod_eq_of_lt hzk] at h
      omega
    · have hle : m ≤ z + k := Nat.not_lt.mp hzk
      have hzk2 : z + k - m < m := by omega
      have h2 : (z + k) % m = z + k - m := by
        rw [Nat.mod_eq_sub_mod hle, Nat.mod_eq_of_lt hzk2]
      rw [h2] at h
      omega
  · rintro rfl
    simp

private theorem nat_eq_of_mod_eq_of_lt {m a b : Nat} (hm : 0 < m)
    (hab : max a b - min a b < m) (hmod : a % m = b % m) : a = b := by
  rcases Nat.le_total a b with hle | hle
  · have hlt : b - a < m := by simpa [Nat.max_eq_right hle, Nat.min_eq_left hle] using hab
    have hbeq : b = a + (b - a) := (Nat.add_sub_of_le hle).symm
    have h1 : (a % m + (b - a)) % m = a % m := by
      calc
        (a % m + (b - a)) % m = (a % m + (b - a) % m) % m := by rw [Nat.mod_eq_of_lt hlt]
        _ = (a + (b - a)) % m := by rw [← Nat.add_mod a (b - a) m]
        _ = b % m := by rw [← hbeq]
        _ = a % m := hmod.symm
    have h1 := h1.trans (Nat.mod_eq_of_lt (Nat.mod_lt a hm)).symm
    have hk : b - a = 0 := (add_mod_eq_self_iff hm (Nat.mod_lt a hm) hlt).mp h1
    omega
  · have hlt : a - b < m := by simpa [Nat.max_eq_left hle, Nat.min_eq_right hle] using hab
    have haeq : a = b + (a - b) := (Nat.add_sub_of_le hle).symm
    have h1 : (b % m + (a - b)) % m = b % m := by
      calc
        (b % m + (a - b)) % m = (b % m + (a - b) % m) % m := by rw [Nat.mod_eq_of_lt hlt]
        _ = (b + (a - b)) % m := by rw [← Nat.add_mod b (a - b) m]
        _ = a % m := by rw [← haeq]
        _ = b % m := hmod
    have h1 := h1.trans (Nat.mod_eq_of_lt (Nat.mod_lt b hm)).symm
    have hk : a - b = 0 := (add_mod_eq_self_iff hm (Nat.mod_lt b hm) hlt).mp h1
    omega

theorem sound : BoundedTicketTagAlias.Goal := by
  intro modulus a b hm hinc hrev htag
  simp only [tag, Prod.mk.injEq] at htag
  exact ⟨nat_eq_of_mod_eq_of_lt hm hinc htag.1, nat_eq_of_mod_eq_of_lt hm hrev htag.2⟩
end Nov26.Proof_EvictionWriteback_BoundedTicketTagAlias

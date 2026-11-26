import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-!
Proof contribution for ResourcePolicies_SlabReuse_ClassInsertionMarginalSavings. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.ResourcePolicies.SlabReuse.Goal_ClassInsertionMarginalSavings

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse


namespace SlabReuseProof

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

set_option maxHeartbeats 0

/-- A successful charge exposes the underlying rounded-class list. -/
theorem charge_eq_mapM_sum (cs : List Nat) (rs : List Nat) (t : Nat) :
    charge cs rs = some t → ∃ l, rs.mapM (rounded cs) = some l ∧ t = l.sum := by
  intro h
  unfold charge at h
  cases hm : rs.mapM (rounded cs) with
  | none => simp [hm] at h
  | some l =>
      have ht : t = l.sum := by simpa [hm] using h.symm
      exact ⟨l, rfl, ht⟩

/-- Decompose a successful charge of a nonempty trace into its head and tail. -/
theorem charge_cons_some (cs : List Nat) (n : Nat) (rs : List Nat) (old : Nat) :
    charge cs (n :: rs) = some old →
    ∃ c t, rounded cs n = some c ∧ charge cs rs = some t ∧ old = c + t := by
  intro h
  unfold charge at h
  cases hcn : rounded cs n with
  | none => simp [hcn] at h
  | some c =>
      cases htail : rs.mapM (rounded cs) with
      | none => simp [hcn, htail] at h
      | some tl =>
          have hold : old = c + tl.sum := by simpa [hcn, htail] using h.symm
          refine ⟨c, tl.sum, rfl, ?_, hold⟩
          unfold charge
          simp [htail]

/-- Subtraction distributes over a sum of two pairs of ordered naturals. -/
theorem sub_add_sub_add (c d t u : Nat) (hdc : d ≤ c) (hut : u ≤ t) :
    (c + t) - (d + u) = (c - d) + (t - u) := by
  omega

/-- Inserting a class makes each rounded size equal to the min of the new class and the old rounded size. -/
theorem rounded_cons_eq (cs : List Nat) (n x c : Nat) (h : rounded cs n = some c) :
    rounded (x :: cs) n = if n ≤ x then some (min x c) else some c := by
  by_cases hn : n ≤ x
  · unfold rounded at h ⊢
    simp [hn, h]
  · unfold rounded at h ⊢
    simp [hn, h]

/-- The marginal saving of one request is its old rounded size minus the min with the inserted class. -/
theorem rounded_cons_sub (cs : List Nat) (n x c d : Nat)
    (hc : rounded cs n = some c) (hd : rounded (x :: cs) n = some d) :
    c - d = if n ≤ x then c - x else 0 := by
  have heq := rounded_cons_eq cs n x c hc
  rw [heq] at hd
  by_cases hn : n ≤ x
  · simp [hn] at hd
    subst d
    by_cases hcx : c ≤ x
    · rw [Nat.min_eq_right hcx]
      simp [hn, hcx]
    · rw [Nat.min_eq_left (Nat.le_of_not_ge hcx)]
      simp [hn]
  · simp [hn] at hd
    subst d
    simp [hn]

/-- Inserting a class can never increase the charge of a supported trace. -/
theorem charge_cons_le (cs : List Nat) (x : Nat) : ∀ rs t u,
    charge cs rs = some t → charge (x :: cs) rs = some u → u ≤ t := by
  intro rs
  induction rs with
  | nil =>
      intro t u ht hu
      simp [charge] at ht hu
      subst t
      subst u
      exact Nat.le_refl 0
  | cons n rs ih =>
      intro t u ht hu
      obtain ⟨c, tr, hc, htr, rfl⟩ := charge_cons_some cs n rs t ht
      obtain ⟨d, ur, hd, hur, rfl⟩ := charge_cons_some (x :: cs) n rs u hu
      have heq := rounded_cons_eq cs n x c hc
      rw [heq] at hd
      have hdc : d ≤ c := by
        by_cases hn : n ≤ x
        · simp [hn] at hd
          subst d
          exact Nat.min_le_right x c
        · simp [hn] at hd
          subst d
          exact Nat.le_refl c
      exact Nat.add_le_add hdc (ih tr ur htr hur)

end SlabReuseProof

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-- Inserting a size class only saves bytes on requests the new class covers. -/
theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse.ClassInsertionMarginalSavings.Goal := by
  intro cs rs x a b ha hb
  induction rs generalizing a b with
  | nil =>
      simp [charge] at ha hb
      subst a
      subst b
      simp
  | cons n rs ih =>
      obtain ⟨c, t, hc, ht, hra⟩ := SlabReuseProof.charge_cons_some cs n rs a ha
      obtain ⟨d, u, hd, hu, hrb⟩ := SlabReuseProof.charge_cons_some (x :: cs) n rs b hb
      subst a
      subst b
      have hcd := SlabReuseProof.rounded_cons_sub cs n x c d hc hd
      have hut : u ≤ t := SlabReuseProof.charge_cons_le cs x rs t u ht hu
      have htu := ih t u ht hu
      have heq := SlabReuseProof.rounded_cons_eq cs n x c hc
      rw [heq] at hd
      have hdc : d ≤ c := by
        by_cases hn : n ≤ x
        · simp [hn] at hd
          subst d
          exact Nat.min_le_right x c
        · simp [hn] at hd
          subst d
          exact Nat.le_refl c
      simp [List.map_cons, List.sum_cons, hc] at htu ⊢
      rw [SlabReuseProof.sub_add_sub_add c d t u hdc hut, hcd, htu]
end TinylambOpt.ResearchProofs.ResourcePolicies.SlabReuse.Goal_ClassInsertionMarginalSavings

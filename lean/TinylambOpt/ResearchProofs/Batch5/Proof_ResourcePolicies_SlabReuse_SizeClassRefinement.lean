import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-!
Proof contribution for ResourcePolicies_SlabReuse_SizeClassRefinement. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SlabReuse.Goal_SizeClassRefinement

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse


namespace SlabReuseProof

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-- `min?` of a list is none only for the empty list. -/
theorem min?_eq_none_iff (l : List Nat) : l.min? = none ↔ l = [] := by
  cases l with
  | nil => simp
  | cons x xs => simp [List.min?_cons]

/-- The minimum of a list is at most every member. -/
theorem min?_le_of_mem : ∀ (l : List Nat) (a b : Nat), a ∈ l → l.min? = some b → b ≤ a := by
  intro l
  induction l with
  | nil =>
      intro a b hmem hmin
      simp at hmem
  | cons x xs ih =>
      intro a b hmem hmin
      rw [List.min?_cons] at hmin
      simp at hmin
      subst b
      simp at hmem
      rcases hmem with rfl | hmemxs
      · cases hx : xs.min? with
        | none => simp
        | some m => simpa using Nat.min_le_left a m
      · cases hx : xs.min? with
        | none =>
            have : xs = [] := (min?_eq_none_iff xs).mp hx
            subst xs
            simp at hmemxs
        | some m =>
            simpa [hx] using Nat.le_trans (Nat.min_le_right x m) (ih a m hmemxs hx)

/-- If a list has a member then its minimum exists. -/
theorem exists_min?_of_mem : ∀ (l : List Nat) (a : Nat), a ∈ l → ∃ b, l.min? = some b := by
  intro l
  cases l with
  | nil =>
      intro a hmem
      simp at hmem
  | cons x xs =>
      intro a hmem
      exact ⟨xs.min?.elim x (min x), by simp [List.min?_cons]⟩

/-- The minimum of a list is itself a member. -/
theorem mem_of_min?_eq_some : ∀ (l : List Nat) (b : Nat), l.min? = some b → b ∈ l := by
  intro l
  induction l with
  | nil =>
      intro b hmin
      simp at hmin
  | cons x xs ih =>
      intro b hmin
      rw [List.min?_cons] at hmin
      simp at hmin
      subst b
      cases hx : xs.min? with
      | none =>
          simp
      | some m =>
          change min x m ∈ x :: xs
          have hmx : m ∈ xs := ih m hx
          by_cases hxm : x ≤ m
          · rw [Nat.min_eq_left hxm]
            simp
          · rw [Nat.min_eq_right (Nat.le_of_not_ge hxm)]
            exact List.mem_cons_of_mem x hmx

/-- A rounded size class that is defined is a member of the class list and covers the request. -/
theorem rounded_mem (cs : List Nat) (n c : Nat) (h : rounded cs n = some c) :
    c ∈ cs ∧ n ≤ c := by
  unfold rounded at h
  have hm := mem_of_min?_eq_some (cs.filter (fun x => n ≤ x)) c h
  simpa using hm

/-- Adding a class preserves admission and can only lower the rounded size. -/
theorem rounded_cons_le (cs : List Nat) (extra n c : Nat) (h : rounded cs n = some c) :
    ∃ d, rounded (extra :: cs) n = some d ∧ d ≤ c := by
  have hmem := rounded_mem cs n c h
  unfold rounded
  have hfilter : c ∈ (extra :: cs).filter (fun x => n ≤ x) := by
    simp [hmem.1, hmem.2]
  obtain ⟨d, hd⟩ := exists_min?_of_mem ((extra :: cs).filter (fun x => n ≤ x)) c hfilter
  exact ⟨d, hd, min?_le_of_mem ((extra :: cs).filter (fun x => n ≤ x)) c d hfilter hd⟩

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

/-- Inserting a class cannot increase the total reserved bytes of a supported trace. -/
theorem charge_cons_le (cs : List Nat) (extra : Nat) : ∀ rs old,
    charge cs rs = some old → ∃ fresh, charge (extra :: cs) rs = some fresh ∧ fresh ≤ old := by
  intro rs
  induction rs with
  | nil =>
      intro old h
      simp [charge] at h
      subst old
      exact ⟨0, by simp [charge], Nat.le_refl 0⟩
  | cons n rs ih =>
      intro old h
      obtain ⟨c, t, hc, ht, rold⟩ := charge_cons_some cs n rs old h
      subst old
      obtain ⟨d, hd, hdc⟩ := rounded_cons_le cs extra n c hc
      obtain ⟨u, hu, hut⟩ := ih t ht
      refine ⟨d + u, ?_, Nat.add_le_add hdc hut⟩
      obtain ⟨l, hl, hul⟩ := charge_eq_mapM_sum (extra :: cs) rs u hu
      subst u
      unfold charge
      simp [hd, hl]

end SlabReuseProof

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-- Adding an intermediate size class cannot increase reserved bytes for a supported trace. -/
theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse.SizeClassRefinement.Goal := by
  intro cs rs extra old hold
  exact SlabReuseProof.charge_cons_le cs extra rs old hold
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SlabReuse.Goal_SizeClassRefinement

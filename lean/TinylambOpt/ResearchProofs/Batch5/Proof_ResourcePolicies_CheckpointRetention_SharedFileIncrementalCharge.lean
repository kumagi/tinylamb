import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CheckpointRetention.Goal_SharedFileIncrementalCharge

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention


set_option maxHeartbeats 0

/-! Deduplicating the union of a new generation's files and the live roots is
a permutation of (live root files) followed by (new files absent from live).
Since list sums are permutation invariant, the incremental bytes charge adds
up without double-counting a shared file id. -/

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

private theorem mem_removeAll (f : Nat) (bs as : List Nat) :
    f ∈ bs.removeAll as ↔ f ∈ bs ∧ f ∉ as := by
  induction as generalizing bs with
  | nil => simp
  | cons y ys ih =>
      simp [List.removeAll_cons, ih, and_left_comm, and_assoc]

private theorem not_contains_eraseDups (f : Nat) (bs : List Nat) :
    (!(bs.eraseDups).contains f) = true ↔ f ∉ bs := by
  change (!List.elem f bs.eraseDups) = true ↔ f ∉ bs
  rw [List.elem_eq_mem]
  by_cases h : f ∈ bs.eraseDups
  · have hb : f ∈ bs := List.mem_eraseDups.mp h
    simp [h, hb]
  · have hb : f ∉ bs := by
      intro hb
      exact h (List.mem_eraseDups.mpr hb)
    simp [h, hb]

private theorem count_eraseDups (f : Nat) (l : List Nat) :
    List.count f l.eraseDups = if f ∈ l then 1 else 0 := by
  let P : Nat → Prop := fun n => ∀ l : List Nat, l.length = n →
      List.count f l.eraseDups = if f ∈ l then 1 else 0
  have hP : ∀ n, P n := fun n =>
    Nat.strongRecOn n (fun n ih => by
      intro l hlen
      cases l with
      | nil => simp
      | cons x xs =>
          rw [List.eraseDups_cons]
          have hlt (y : Nat) :
              (List.filter (fun b : Nat => !b == y) xs).length < n := by
            rw [← hlen]
            simp only [List.length_cons]
            have hle := List.length_filter_le (fun b : Nat => !b == y) xs
            omega
          by_cases hfx : f = x
          · subst x
            have ih' := ih _ (hlt f) (List.filter (fun b : Nat => !b == f) xs) rfl
            rw [List.count, List.countP_cons]
            simp [beq_iff_eq]
          · have ih' := ih _ (hlt x) (List.filter (fun b : Nat => !b == x) xs) rfl
            rw [List.count, List.countP_cons]
            have hxfne : x ≠ f := fun h => hfx h.symm
            have hbeq : (x == f) = false := by simp [hxfne]
            rw [hbeq]
            simp only [Bool.false_eq_true, ite_false, Nat.add_zero]
            change List.count f (List.filter (fun b : Nat => !b == x) xs).eraseDups =
              if f ∈ x :: xs then 1 else 0
            rw [ih']
            simp [hfx, List.mem_filter])
  exact hP l.length l rfl

private theorem count_filter_mem (f : Nat) (l : List Nat) (p : Nat → Bool) :
    List.count f (l.filter p) = if p f = true then List.count f l else 0 := by
  by_cases hp : p f = true
  · simp [hp, List.count_filter hp]
  · have hfnot : f ∉ l.filter p := by
      intro hfmem
      exact hp (List.mem_filter.mp hfmem).2
    simp [hp, List.count_eq_zero.mpr hfnot]

private theorem eraseDups_union_perm (as bs : List Nat) :
    (as ++ bs).eraseDups.Perm
      (bs.eraseDups ++ as.eraseDups.filter (fun f => !(bs.eraseDups).contains f)) := by
  apply List.perm_iff_count.mpr
  intro f
  rw [List.eraseDups_append]
  rw [List.count_append, List.count_append]
  rw [count_eraseDups, count_eraseDups, count_eraseDups]
  rw [count_filter_mem]
  rw [count_eraseDups]
  by_cases has : f ∈ as <;> by_cases hbs : f ∈ bs
  all_goals simp [has, hbs, mem_removeAll]

private theorem perm_sum {l₁ l₂ : List Nat} (h : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction h with
  | nil => rfl
  | cons x h ih => simp [List.sum_cons, ih]
  | swap x y l => simp [List.sum_cons, Nat.add_left_comm]
  | trans h₁ h₂ ih₁ ih₂ => exact Eq.trans ih₁ ih₂

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention.SharedFileIncrementalCharge.Goal := by
  intro roots g bytes
  have hperm := eraseDups_union_perm g.files (roots.flatMap Generation.files)
  have hsum := perm_sum (hperm.map bytes)
  simpa using hsum
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CheckpointRetention.Goal_SharedFileIncrementalCharge

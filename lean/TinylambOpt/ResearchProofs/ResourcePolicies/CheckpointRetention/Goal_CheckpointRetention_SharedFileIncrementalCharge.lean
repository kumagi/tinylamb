import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-! Proves `SharedFileIncrementalCharge.Goal`: deduplicating `g.files ++ live`
charges each file id once, and the resulting list is a permutation of
`live ++ added` (same elements; both duplicate-free), so its byte total splits
exactly into the live charge plus the incremental charge of files `g` newly
introduces. -/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CheckpointRetention_SharedFileIncrementalCharge
open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

private theorem nodup_reverse {α : Type} {l : List α} : l.Nodup → l.reverse.Nodup := by
  induction l with
  | nil => exact fun h => h
  | cons x xs ih =>
    intro h
    rw [List.reverse_cons]
    obtain ⟨hx0, hxs⟩ := List.nodup_cons.mp h
    refine (List.pairwise_append (R := (· ≠ ·))).mpr ⟨ih hxs, by simp [List.Nodup], ?_⟩
    intro a ha b hb
    obtain rfl : b = x := List.mem_singleton.mp hb
    intro he
    subst he
    rw [List.mem_reverse] at ha
    exact hx0 ha
private theorem nodup_eraseDups : ∀ (l : List Nat), l.eraseDups.Nodup := by
  have key : ∀ (as bs : List Nat), bs.Nodup → (List.eraseDupsBy.loop (· == ·) as bs).Nodup := by
    intro as
    induction as with
    | nil => intro bs hbs; show bs.reverse.Nodup; exact nodup_reverse hbs
    | cons a as ih =>
      intro bs hbs
      show (List.eraseDupsBy.loop (· == ·) (a :: as) bs).Nodup
      unfold List.eraseDupsBy.loop
      split <;> rename_i h
      · exact ih bs hbs
      · have hcons : (a :: bs).Nodup := by
          rw [List.nodup_cons]
          refine ⟨?_, hbs⟩
          intro ham
          have hany : (bs.any fun b => a == b) = true := by
            rw [List.any_eq_true]
            exact ⟨a, ham, beq_self_eq_true a⟩
          rw [hany] at h
          cases h
        exact ih (a :: bs) hcons
  intro l
  show (List.eraseDupsBy.loop (· == ·) l []).Nodup
  exact key l [] (by simp [List.Nodup])
private theorem nodup_filter {α : Type} (p : α → Bool) {l : List α} :
    l.Nodup → (l.filter p).Nodup :=
  List.Pairwise.filter p
private theorem count_nodup : ∀ (l : List Nat), l.Nodup →
    ∀ a, List.count a l = if a ∈ l then 1 else 0 := by
  intro l
  induction l with
  | nil => intro _ a; simp
  | cons x xs ih =>
    intro hn a
    obtain ⟨hx0, hxs⟩ := List.nodup_cons.mp hn
    by_cases hxa : x = a
    · subst hxa
      rw [List.count_cons_self, ih hxs]
      simp [hx0]
    · rw [List.count_cons_of_ne hxa, ih hxs]
      by_cases ham : a ∈ xs
      · have hc : a ∈ x :: xs := by
          rw [List.mem_cons]
          exact Or.inr ham
        rw [if_pos ham, if_pos hc]
      · have hc : ¬ a ∈ x :: xs := by
          rw [List.mem_cons]
          exact fun he => he.elim (fun hax => hxa hax.symm) ham
        rw [if_neg ham, if_neg hc]
private theorem perm_of_nodup_mem {l₁ l₂ : List Nat} (h₁ : l₁.Nodup) (h₂ : l₂.Nodup)
    (h : ∀ a, a ∈ l₁ ↔ a ∈ l₂) : List.Perm l₁ l₂ := by
  rw [List.perm_iff_count]
  intro a
  simp only [count_nodup l₁ h₁, count_nodup l₂ h₂, h]
private theorem sum_perm (bytes : Nat → Nat) :
    ∀ (l₁ l₂ : List Nat), List.Perm l₁ l₂ → (l₁.map bytes).sum = (l₂.map bytes).sum :=
  fun l₁ l₂ h => (List.Perm.map bytes h).sum_nat

theorem sound : SharedFileIncrementalCharge.Goal := by
  intro roots g bytes
  show (((g :: roots).flatMap Generation.files).eraseDups.map bytes).sum
      = (((roots.flatMap Generation.files).eraseDups).map bytes).sum
        + (((g.files.eraseDups.filter fun f =>
              !((roots.flatMap Generation.files).eraseDups).contains f).map bytes)).sum
  rw [List.flatMap_cons]
  have hmem : ∀ x, x ∈ (g.files ++ roots.flatMap Generation.files).eraseDups ↔
      x ∈ (roots.flatMap Generation.files).eraseDups
        ++ (g.files.eraseDups.filter fun f =>
          !((roots.flatMap Generation.files).eraseDups).contains f) := by
    intro x
    constructor
    · intro h
      rw [List.mem_eraseDups, List.mem_append] at h
      cases h with
      | inl hg =>
        by_cases hx : x ∈ (roots.flatMap Generation.files).eraseDups
        · exact (List.mem_append).mpr (Or.inl hx)
        · refine (List.mem_append).mpr (Or.inr ?_)
          refine (List.mem_filter).mpr ⟨List.mem_eraseDups.mpr hg, ?_⟩
          rw [Bool.not_eq_true', List.contains_eq_mem, decide_eq_false_iff_not]
          exact hx
      | inr hr =>
        exact (List.mem_append).mpr (Or.inl (List.mem_eraseDups.mpr hr))
    · intro h
      rcases (List.mem_append).mp h with hl | ha
      · exact List.mem_eraseDups.mpr (List.mem_append.mpr (Or.inr (List.mem_eraseDups.mp hl)))
      · rcases (List.mem_filter).mp ha with ⟨hd, _⟩
        exact List.mem_eraseDups.mpr (List.mem_append.mpr (Or.inl (List.mem_eraseDups.mp hd)))
  have hE : (g.files ++ roots.flatMap Generation.files).eraseDups.Nodup :=
    nodup_eraseDups _
  have hA : ((roots.flatMap Generation.files).eraseDups
      ++ g.files.eraseDups.filter (fun f =>
        !((roots.flatMap Generation.files).eraseDups).contains f)).Nodup := by
    refine (List.pairwise_append (R := (· ≠ ·))).mpr
      ⟨nodup_eraseDups _, nodup_filter _ (nodup_eraseDups _), fun a ha b hb => ?_⟩
    rcases (List.mem_filter).mp hb with ⟨_, hb2⟩
    have hcf : ((roots.flatMap Generation.files).eraseDups).contains b = false := by
      rw [← Bool.not_eq_true']
      exact hb2
    rw [List.contains_eq_mem, decide_eq_false_iff_not, List.mem_eraseDups] at hcf
    intro he
    exact hcf (he ▸ List.mem_eraseDups.mp ha)
  rw [← List.sum_append, ← List.map_append]
  exact sum_perm bytes _ _ (perm_of_nodup_mem hE hA hmem)

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CheckpointRetention_SharedFileIncrementalCharge

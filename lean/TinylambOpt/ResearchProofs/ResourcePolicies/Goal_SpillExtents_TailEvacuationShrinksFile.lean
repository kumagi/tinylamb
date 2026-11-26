import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

/-!
`TailEvacuationShrinksFile`: moving the highest live block into a lower unused
hole strictly lowers the physical high-water mark. SQL spill compaction can
therefore shrink its backing file after relocating the tail block.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_TailEvacuationShrinksFile

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

private theorem max_lt_of_all {xs : List Nat} {k : Nat} (hne : xs ≠ []) :
    (∀ x ∈ xs, x < k) → xs.max?.getD 0 < k := by
  induction xs with
  | nil => intro h; contradiction
  | cons x xs ih =>
      intro hall
      cases hm : xs.max? with
      | none =>
          simp [List.max?_cons, hm]
          exact hall x (by simp)
      | some m =>
          have hm_lt : m < k := by
            have h := ih (by intro hnil; simp [hnil] at hm)
              (fun y hy => hall y (by simp [hy]))
            simpa [hm] using h
          have hx_lt := hall x (by simp)
          simp [List.max?_cons, hm]
          exact Nat.max_lt.mpr ⟨hx_lt, hm_lt⟩

theorem sound : TailEvacuationShrinksFile.Goal := by
  intro ids last hole hmax hhole hlt
  have hall_le : ∀ i ∈ ids, i ≤ last := by
    intro i hi
    exact ((List.max?_le_iff hmax (x := last)).mp (by omega)) i hi
  let mapped := ids.map (fun i => if i == last then hole else i)
  have hmapped_lt : ∀ j ∈ mapped, j < last := by
    intro j hj
    rcases List.mem_map.mp hj with ⟨i, hi, rfl⟩
    by_cases hil : i == last
    · rw [if_pos hil]
      exact hlt
    · rw [if_neg hil]
      have hile := hall_le i hi
      have hne : i ≠ last := by simpa using hil
      omega
  have hmapped_ne : mapped ≠ [] := by
    cases ids with
    | nil => simp at hmax
    | cons i is => simp [mapped]
  have hmmax := max_lt_of_all (xs := mapped) (k := last) hmapped_ne hmapped_lt
  cases hmm : mapped.max? with
  | none =>
      exfalso
      apply hmapped_ne
      cases mapped with
      | nil => rfl
      | cons x xs => simp [List.max?_cons] at hmm; exact (hmapped_ne hmm).elim
  | some m =>
      simp [mapped] at hmm
      have hm_lt : m < last := by simpa [mapped, hmm] using hmmax
      unfold roof
      rw [hmax]
      simp [mapped, hmm]
      omega

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_TailEvacuationShrinksFile

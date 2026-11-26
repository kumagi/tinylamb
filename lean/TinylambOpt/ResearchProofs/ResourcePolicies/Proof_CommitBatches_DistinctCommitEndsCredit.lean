import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

/-!
Proof contribution for ResourcePolicies_CommitBatches_DistinctCommitEndsCredit. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.ResourcePolicies.CommitBatches.Goal_DistinctCommitEndsCredit

open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches


open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

/-- A nodup list whose elements all occur in another list is no longer than
    that list. Used to charge distinct commit ends to WAL byte offsets. -/
theorem nodup_mem_length_le {α : Type} [BEq α] [LawfulBEq α] :
    ∀ (l₁ l₂ : List α), l₁.Nodup → (∀ x ∈ l₁, x ∈ l₂) → l₁.length ≤ l₂.length := by
  intro l₁
  induction l₁ with
  | nil =>
      intro l₂ hnd hmem
      simp
  | cons a l ih =>
      intro l₂ hnd hmem
      have hparts : a ∉ l ∧ l.Nodup := by simpa [List.nodup_cons] using hnd
      have ha2 : a ∈ l₂ := hmem a (by simp)
      have hnot : a ∉ l := hparts.1
      have hndl : l.Nodup := hparts.2
      have hmeml : ∀ x ∈ l, x ∈ l₂.erase a := by
        intro x hx
        have hx2 : x ∈ l₂ := hmem x (by simp [hx])
        have hxa : x ≠ a := by intro heq; subst x; exact hnot hx
        simpa [List.mem_erase_of_ne hxa] using hx2
      have hle := ih (l₂.erase a) hndl hmeml
      have hlen : (l₂.erase a).length + 1 = l₂.length := by
        rw [List.length_erase]
        rw [show (if a ∈ l₂ then l₂.length - 1 else l₂.length) = l₂.length - 1 by simp [ha2]]
        have hpos := List.length_pos_of_mem ha2
        omega
      change l.length + 1 ≤ l₂.length
      omega

/-- Removing duplicates yields a nodup list. -/
theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] : ∀ l : List α, l.eraseDups.Nodup := by
  intro l
  exact Nat.strongRecOn l.length
    (motive := fun n => ∀ l : List α, l.length = n → l.eraseDups.Nodup)
    (fun n ih l hl => by
      cases l with
      | nil => simp
      | cons a t =>
          rw [List.eraseDups_cons]
          have htlen : t.length < n := by
            rw [← hl]
            simp
          have hflen : (t.filter (fun b => !b == a)).length < n := by
            have hf := List.length_filter_le (fun b => !b == a) t
            omega
          have hrec : (t.filter (fun b => !b == a)).eraseDups.Nodup :=
            ih (t.filter (fun b => !b == a)).length hflen (t.filter (fun b => !b == a)) rfl
          have hnot : a ∉ (t.filter (fun b => !b == a)).eraseDups := by
            intro hm
            simp at hm
          simpa [hnot] using hrec) l rfl

/-- Each distinct exclusive end lies in the half-open byte interval
    `(durable, tip]`, which has exactly `tip - durable` integer points, so
    deduplicated ends fit in one acknowledgement slot per byte. -/
theorem sound : DistinctCommitEndsCredit.Goal := by
  intro ws durable tip h
  let dups : List Nat := (ws.map Waiter.stop).eraseDups
  let slots : List Nat := (List.range (tip - durable)).map (fun i => durable + 1 + i)
  have hmem : ∀ x ∈ dups, x ∈ slots := by
    intro x hx
    have hxmap : x ∈ ws.map Waiter.stop := by simpa [dups] using hx
    rcases List.mem_map.1 hxmap with ⟨w, hw, hstop⟩
    have hwb := h w hw
    subst x
    apply List.mem_map.2
    refine ⟨w.stop - (durable + 1), ?_, ?_⟩
    · simp [List.mem_range]
      omega
    · omega
  have hnd : dups.Nodup := by
    simpa [dups] using nodup_eraseDups (ws.map Waiter.stop)
  have hle := nodup_mem_length_le dups slots hnd hmem
  simpa [dups, slots] using hle
end TinylambOpt.ResearchProofs.ResourcePolicies.CommitBatches.Goal_DistinctCommitEndsCredit

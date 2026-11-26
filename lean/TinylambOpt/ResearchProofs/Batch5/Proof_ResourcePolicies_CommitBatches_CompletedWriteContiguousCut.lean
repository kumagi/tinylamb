import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

/-!
Proof contribution for ResourcePolicies_CommitBatches_CompletedWriteContiguousCut. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CommitBatches.Goal_CompletedWriteContiguousCut

open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches


open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

/-- `takeWhile` never produces more elements than its input list. -/
theorem takeWhile_length_le {α : Type} (p : α → Bool) : ∀ l : List α,
    (l.takeWhile p).length ≤ l.length := by
  intro l
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases h : p a = true
      · simp [h]
        omega
      · simp [h]

/-- For a Boolean predicate on the indices `0 .. n-1`, the length of the
    `takeWhile` prefix is the first index at which the predicate fails. -/
theorem range_takeWhile_length_spec (p : Nat → Bool) : ∀ n t, t ≤ n →
    (t ≤ ((List.range n).takeWhile p).length ↔ ∀ i, i < t → p i = true) := by
  intro n
  induction n with
  | zero =>
      intro t ht
      have ht0 : t = 0 := by omega
      subst t
      simp
  | succ n ih =>
      intro t ht
      by_cases hfull : ((List.range n).takeWhile p).length = n
      · have halln : ∀ i, i < n → p i = true := (ih n (by omega)).1 (by omega)
        have hlen : ((List.range (n + 1)).takeWhile p).length =
              if p n = true then n + 1 else n := by
          rw [List.range_succ, List.takeWhile_append]
          have hfl : (List.range n).length = n := by simp
          rw [show ((List.range n).takeWhile p).length = (List.range n).length by omega]
          by_cases hpn : p n = true <;> simp [hpn]
        by_cases htn : t ≤ n
        · constructor
          · intro _ i hi
            exact halln i (by omega)
          · intro _
            rw [hlen]
            by_cases hpn : p n = true
            · simp [hpn]
              omega
            · simp [hpn]
              exact htn
        · have ht_eq : t = n + 1 := by omega
          subst t
          by_cases hpn : p n = true
          · have hlhs : n + 1 ≤ ((List.range (n + 1)).takeWhile p).length := by
              rw [hlen]
              simp [hpn]
            have hrhs : ∀ i, i < n + 1 → p i = true := by
              intro i hi
              by_cases hin : i < n
              · exact halln i hin
              · have : i = n := by omega
                subst i
                exact hpn
            exact ⟨fun _ => hrhs, fun _ => hlhs⟩
          · constructor
            · intro hle
              rw [hlen] at hle
              simp [hpn] at hle
              omega
            · intro hall
              exact False.elim (hpn (hall n (by omega)))
      · have hlen : ((List.range (n + 1)).takeWhile p).length =
            ((List.range n).takeWhile p).length := by
          rw [List.range_succ, List.takeWhile_append]
          by_cases h : ((List.range n).takeWhile p).length = (List.range n).length
          · have heq : ((List.range n).takeWhile p).length = n := by simpa using h
            omega
          · have h' : ((List.range n).takeWhile p).length ≠ n := by
              simpa using h
            simp [h']
        have hnotall : ¬ (∀ i, i < n → p i = true) := by
          intro hall
          have hnle : n ≤ ((List.range n).takeWhile p).length := (ih n (by omega)).2 hall
          have hle : ((List.range n).takeWhile p).length ≤ n := by
            have := takeWhile_length_le p (List.range n)
            simpa using this
          omega
        by_cases htn : t ≤ n
        · rw [hlen]
          exact ih t htn
        · have ht_eq : t = n + 1 := by omega
          subst t
          constructor
          · intro hle
            rw [hlen] at hle
            have hle' : ((List.range n).takeWhile p).length ≤ n := by
              have := takeWhile_length_le p (List.range n)
              simpa using this
            omega
          · intro hall
            exact False.elim (hnotall (fun i hi => hall i (by omega)))

/-- A prospective durable target is reachable exactly when every byte before
    it is covered: `prefixEnd` is the first uncovered byte (or the limit). -/
theorem sound : CompletedWriteContiguousCut.Goal := by
  intro ws base limit t hbt htl
  let p : Nat → Bool := fun i => cover ws (base + i)
  have hspec := range_takeWhile_length_spec p (limit - base) (t - base) (by omega)
  constructor
  · intro hle
    have hlen : t - base ≤ ((List.range (limit - base)).takeWhile p).length := by
      simpa [prefixEnd, p] using (show t - base ≤ (prefixEnd ws base limit) - base by omega)
    have hp := hspec.1 hlen
    intro i hbi hit
    have hi : i - base < t - base := by omega
    have hcover := hp (i - base) hi
    simpa [p, Nat.add_sub_of_le hbi] using hcover
  · intro hall
    have hp : ∀ i, i < t - base → p i = true := by
      intro i hi
      exact hall (base + i) (by omega) (by omega)
    have hlen := hspec.2 hp
    have hres : t ≤ base + ((List.range (limit - base)).takeWhile p).length := by omega
    simpa [prefixEnd, p] using hres
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CommitBatches.Goal_CompletedWriteContiguousCut

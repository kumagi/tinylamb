import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

/-!
Proof contribution for ResourcePolicies_CommitBatches_IncrementalCompletionScan. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CommitBatches.Goal_IncrementalCompletionScan

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

/-- Number of contiguously covered bytes in `[base, limit)`. -/
def scanLen (p : Nat → Bool) (base limit : Nat) : Nat :=
  ((List.range (limit - base)).takeWhile (fun i => p (base + i))).length

/-- A covered first byte consumes one step of the scan. -/
theorem scanLen_succ (p : Nat → Bool) (base limit : Nat)
    (hbl : base < limit) (hp : p base = true) :
    scanLen p base limit = 1 + scanLen p (base + 1) limit := by
  unfold scanLen
  have hsucc : limit - base = (limit - (base + 1)) + 1 := by omega
  rw [hsucc, List.range_succ_eq_map]
  rw [List.takeWhile_cons]
  have hp0 : (fun i => p (base + i)) 0 = true := by simpa using hp
  simp [hp, List.takeWhile_map, Function.comp_def, Nat.add_comm, Nat.add_left_comm]

/-- Skipping `k` already covered bytes does not change where the remaining
    contiguous scan ends. -/
theorem scanLen_skip_aux (p : Nat → Bool) (base limit k : Nat)
    (hk : base + k ≤ limit) (hp : ∀ j, j < k → p (base + j) = true) :
    scanLen p base limit = k + scanLen p (base + k) limit := by
  induction k generalizing base with
  | zero => simp [scanLen]
  | succ k ih =>
      have hb_lt : base < limit := by omega
      have hpbase : p base = true := hp 0 (by omega)
      rw [scanLen_succ p base limit hb_lt hpbase]
      have hih := ih (base + 1) (by omega) (by
        intro j hj
        have h := hp (j + 1) (by omega)
        simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using h)
      rw [hih]
      have heq : base + (k + 1) = base + 1 + k := by omega
      rw [heq]
      omega

/-- Skipping bytes already known to be covered does not change where the
    remaining contiguous scan ends. -/
theorem scanLen_skip (p : Nat → Bool) (base cut limit : Nat)
    (hbc : base ≤ cut) (hcl : cut ≤ limit)
    (hp : ∀ i, base ≤ i → i < cut → p i = true) :
    scanLen p base limit = (cut - base) + scanLen p cut limit := by
  have hk : base + (cut - base) = cut := by omega
  simpa [hk] using scanLen_skip_aux p base limit (cut - base) (by omega)
    (by intro j hj; exact hp (base + j) (by omega) (by omega))

/-- Coverage by the original completions implies coverage after appending retries. -/
theorem cover_append_mono (a b : List Write) : ∀ i, cover a i = true → cover (a ++ b) i = true := by
  intro i hi
  have hia : (a.any (fun w => w.start ≤ i && i < w.start + w.count)) = true := by
    simpa [cover] using hi
  simp only [cover, List.any_append, hia, Bool.true_or]

/-- Every byte before a confirmed prefix end is covered. -/
theorem prefixEnd_covered_prefix (ws : List Write) (base limit : Nat) :
    ∀ i, base ≤ i → i < prefixEnd ws base limit → cover ws i = true := by
  intro i hbi hi
  let len := ((List.range (limit - base)).takeWhile (fun j => cover ws (base + j))).length
  have hlenle : len ≤ limit - base := by
    have := takeWhile_length_le (fun j => cover ws (base + j)) (List.range (limit - base))
    simpa [len] using this
  have hpe : prefixEnd ws base limit = base + len := by
    simp [prefixEnd, len]
  have hspec := range_takeWhile_length_spec (fun j => cover ws (base + j))
      (limit - base) len (by omega)
  have hcov := hspec.1 (by omega) (i - base) (by omega)
  simpa [len, prefixEnd, Nat.add_sub_of_le hbi] using hcov

/-- Resuming a partial scan from the cut confirmed by the original writes
    gives the same final cut as scanning from the start with all completions. -/
theorem sound : IncrementalCompletionScan.Goal := by
  intro a b base limit hbl
  let p : Nat → Bool := fun i => cover (a ++ b) i
  have hlenle : ((List.range (limit - base)).takeWhile (fun j => cover a (base + j))).length
      ≤ limit - base := by
    have := takeWhile_length_le (fun j => cover a (base + j)) (List.range (limit - base))
    simpa using this
  have hcut_le : prefixEnd a base limit ≤ limit := by
    simp [prefixEnd]
    omega
  have hp : ∀ i, base ≤ i → i < prefixEnd a base limit → p i = true := by
    intro i hbi hi
    have hc := prefixEnd_covered_prefix a base limit i hbi hi
    exact cover_append_mono a b i hc
  have hbc : base ≤ prefixEnd a base limit := by simp [prefixEnd]
  have hskip := scanLen_skip p base (prefixEnd a base limit) limit hbc hcut_le hp
  change prefixEnd (a ++ b) (prefixEnd a base limit) limit = prefixEnd (a ++ b) base limit
  simp only [prefixEnd]
  change prefixEnd a base limit + scanLen p (prefixEnd a base limit) limit =
    base + scanLen p base limit
  rw [hskip]
  omega
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CommitBatches.Goal_IncrementalCompletionScan

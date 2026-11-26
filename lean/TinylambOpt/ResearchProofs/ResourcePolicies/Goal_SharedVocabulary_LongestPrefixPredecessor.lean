import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
/-!
Registers the checked result for `SharedVocabulary.LongestPrefixPredecessor.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/

/-!
`LongestPrefixPredecessor`: the front-coding cost of a symbol depends only on the
shared prefix length `k` and the payload length `L`, as `ubytes k + ubytes (L-k) +
(L-k)`, and this is monotone decreasing in `k` on `[0, L]`.  Varint size cliffs
(`ubytes` grows by at most one byte when its argument doubles) never overturn the
ordering: a dictionary placement with a longer common prefix is always at least as
cheap to encode, so a compressor may greedily pick the longest-prefix predecessor.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SharedVocabulary_LongestPrefixPredecessor
open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
open TinylambOpt.ResearchGoals.Compression.Wire

/-! ## monotonicity of the bit/byte width functions -/

/-- The bit width is monotone in the largest value. -/
private theorem width_mono {u v : Nat} (h : u ≤ v) : width u ≤ width v := by
  show (if u == 0 then 0 else u.log2 + 1) ≤ (if v == 0 then 0 else v.log2 + 1)
  by_cases hu : u = 0
  · subst hu
    rw [if_pos (by simp)]
    exact Nat.zero_le _
  · have hv : ¬ (v = 0) := by
      intro heq
      subst heq
      exact hu (Nat.le_zero.mp h)
    rw [if_neg (by simpa using hu), if_neg (by simpa using hv)]
    have hl : u.log2 ≤ v.log2 :=
      (Nat.le_log2 (by simpa using hv)).mpr (Nat.le_trans (Nat.log2_self_le (by simpa using hu)) h)
    omega

/-- Growing the value by one grows its bit width by at most one. -/
private theorem width_succ_le (n : Nat) : width (n + 1) ≤ width n + 1 := by
  show (if n + 1 == 0 then 0 else (n + 1).log2 + 1) ≤ (if n == 0 then 0 else n.log2 + 1) + 1
  rw [if_neg (by simpa using Nat.succ_ne_zero n)]
  by_cases hn : n = 0
  · subst hn
    rw [if_pos (by simp)]
    decide
  · rw [if_neg (by simpa using hn)]
    have h1 : n < 2 ^ n.log2 * 2 := by
      have h := Nat.lt_log2_self (n := n)
      rw [Nat.pow_succ] at h
      exact h
    have hlg : (n + 1).log2 < n.log2 + 2 :=
      (Nat.log2_lt (by omega)).mpr (by
        have h2 : 2 ^ (n.log2 + 2) = 2 ^ n.log2 * 4 := by
          rw [show n.log2 + 2 = n.log2 + 1 + 1 from by omega, Nat.pow_succ, Nat.pow_succ]
          omega
        omega)
    omega

/-- The varint byte count is monotone. -/
private theorem ubytes_mono {u v : Nat} (h : u ≤ v) : ubytes u ≤ ubytes v := by
  unfold ubytes
  have hw := width_mono h
  have hd : (width u + 6) / 7 ≤ (width v + 6) / 7 :=
    Nat.div_le_div_right (Nat.add_le_add_right hw 6)
  omega

/-- Growing the value by one grows its varint length by at most one byte. -/
private theorem ubytes_succ (n : Nat) : ubytes (n + 1) ≤ ubytes n + 1 := by
  unfold ubytes
  have hw := width_succ_le n
  omega

/-! ## the prefix cost as a function of the shared prefix length -/

/-- The prefix cost model as a function of the shared prefix length and total length. -/
private def F (k L : Nat) : Nat := ubytes k + ubytes (L - k) + (L - k)

/-- One more shared prefix byte never increases the entry cost. -/
private theorem F_step {k L : Nat} (h : k < L) : F (k + 1) L ≤ F k L := by
  show ubytes (k + 1) + ubytes (L - (k + 1)) + (L - (k + 1))
       ≤ ubytes k + ubytes (L - k) + (L - k)
  have h1 := ubytes_succ k
  have h2 := ubytes_mono (show L - (k + 1) ≤ L - k from by omega)
  omega

/-- On the feasible range `k ≤ L` the cost is decreasing in the prefix length. -/
private theorem F_anti (L : Nat) :
    ∀ (B : Nat), ∀ (A : Nat), A ≤ B → B ≤ L → F B L ≤ F A L := by
  intro B
  induction B with
  | zero =>
      intro A hAB _
      have : A = 0 := by omega
      subst this
      exact Nat.le_refl _
  | succ B' ih =>
      intro A hAB hBL
      rcases Nat.lt_or_ge A (B' + 1) with h | h
      · have hstep : F (B' + 1) L ≤ F B' L := F_step (by omega)
        have htail := ih A (by omega) (by omega)
        omega
      · have : A = B' + 1 := by omega
        subst this
        exact Nat.le_refl _

/-- The encoded cost is exactly `F` at the shared prefix length. -/
private theorem cost_eq (prev s : Symbol) :
    cost prev s = F (common prev s) s.length := by
  show ubytes (common prev s) + ubytes (s.drop (common prev s)).length
         + (s.drop (common prev s)).length
       = _
  rw [List.length_drop]
  rfl

/-- The shared prefix never exceeds the payload length. -/
private theorem common_le (prev s : Symbol) : common prev s ≤ s.length := by
  induction prev generalizing s with
  | nil =>
      show 0 ≤ s.length
      exact Nat.zero_le _
  | cons x ps ih =>
      cases s with
      | nil =>
          show 0 ≤ 0
          exact Nat.le_refl _
      | cons y ss =>
          show (if x == y then 1 + common ps ss else 0) ≤ ss.length + 1
          by_cases hxy : x == y
          · rw [if_pos hxy]
            have := ih ss
            omega
          · rw [if_neg hxy]
            omega

theorem sound : LongestPrefixPredecessor.Goal := by
  intro a b s h
  rw [cost_eq, cost_eq]
  exact F_anti s.length (common b s) (common a s) h (common_le b s)

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SharedVocabulary_LongestPrefixPredecessor

import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Equal exception counts do not predict the better patched-frame width.

Refutation (`ExceptionCountWidthChoice.Goal`): if exception payloads are
varints, two same-length frames with the same number of exceptions can still
rank the patched layout against the fully packed layout differently. Fix
`w = 2` and frames of 100 rows with exactly one exception: `a` ends in 1023 and
`b` ends in 4. Both patched frames pay `byteCount (2*100) + byteCount 100 = 38`
inline bytes plus the exception varints, costing 40 (1023 takes 2 bytes) versus
39 (4 takes 1). But the packed alternative depends on the peak value: 1023
forces width 10 (125 bytes) while 4 forces width 3 (38 bytes). So
`patched ≤ packed` holds for `a` and fails for `b`, even though every quantity
the goal inspects agrees — width selection must inspect exception *magnitudes*,
not only counts.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory_ExceptionCountWidthChoice
open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
open TinylambOpt.ResearchGoals.Compression.Wire

/-- Bounds characterization: `2^k ≤ n < 2^(k+1)` pins `log2 n = k`. -/
private theorem log2_eq_of_bounds (n k : Nat) (hn : n ≠ 0)
    (hlo : ¬ (n < 2 ^ k)) (hhi : n < 2 ^ (k + 1)) : Nat.log2 n = k := by
  rw [Nat.log2_eq_iff hn]
  exact ⟨by omega, hhi⟩

private theorem log2_4 : Nat.log2 4 = 2 :=
  log2_eq_of_bounds 4 2 (by decide) (by decide) (by decide)

private theorem log2_1023 : Nat.log2 1023 = 9 :=
  log2_eq_of_bounds 1023 9 (by decide) (by decide) (by decide)

private theorem ub4 : ubytes 4 = 1 := by unfold ubytes width; rw [log2_4]; decide

private theorem ub1023 : ubytes 1023 = 2 := by unfold ubytes width; rw [log2_1023]; decide

private theorem width1023 : width 1023 = 10 := by unfold width; rw [log2_1023]; decide

private theorem width4 : width 4 = 3 := by unfold width; rw [log2_4]; decide

private theorem bnot {b : Bool} (h : b = false) : ¬ b = true := by
  intro h2
  rw [h] at h2
  exact absurd h2 (by decide)

/-- A predicate false everywhere filters to nothing (the predicate is a lambda,
so `rw` cannot use the `filter_cons` lemmas directly). -/
private theorem filter_eq_nil (p : Nat → Bool) (l : List Nat) (h : ∀ x ∈ l, p x = false) :
    List.filter (fun x => p x) l = [] := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      rw [List.filter_cons_of_neg (bnot (h x (by simp)))]
      exact ih (fun y hy => h y (by simp [hy]))

private theorem rz : ∀ x ∈ List.replicate 99 (0 : Nat), flagged 2 x = false := by
  intro x hx
  rw [List.mem_replicate] at hx
  rw [hx.2]
  decide

private theorem fl_a : List.filter (flagged 2) (List.replicate 99 0 ++ [1023]) = [1023] := by
  show List.filter (fun x => flagged 2 x) (List.replicate 99 0 ++ [1023]) = [1023]
  rw [List.filter_append, filter_eq_nil (flagged 2) (List.replicate 99 0) rz,
    List.nil_append, List.filter_cons_of_pos (by decide : flagged 2 1023 = true),
    List.filter_nil]

private theorem fl_b : List.filter (flagged 2) (List.replicate 99 0 ++ [4]) = [4] := by
  show List.filter (fun x => flagged 2 x) (List.replicate 99 0 ++ [4]) = [4]
  rw [List.filter_append, filter_eq_nil (flagged 2) (List.replicate 99 0) rz,
    List.nil_append, List.filter_cons_of_pos (by decide : flagged 2 4 = true),
    List.filter_nil]

private theorem pk_a : peak (List.replicate 99 0 ++ [1023]) = 1023 := by decide
private theorem pk_b : peak (List.replicate 99 0 ++ [4]) = 4 := by decide

private theorem sb_a : streamBytes [1023] = 2 := by
  unfold streamBytes
  rw [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, ub1023]
  rfl

private theorem sb_b : streamBytes [4] = 1 := by
  unfold streamBytes
  rw [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, ub4]
  rfl

private theorem len_a : (List.replicate 99 0 ++ [1023]).length = 100 := by
  rw [List.length_append, List.length_replicate, List.length_cons, List.length_nil]

private theorem len_b : (List.replicate 99 0 ++ [4]).length = 100 := by
  rw [List.length_append, List.length_replicate, List.length_cons, List.length_nil]

private theorem pb_a : patchedBytes 2 (List.replicate 99 0 ++ [1023]) = 40 := by
  unfold patchedBytes
  rw [fl_a, sb_a, len_a]
  decide

private theorem pb_b : patchedBytes 2 (List.replicate 99 0 ++ [4]) = 39 := by
  unfold patchedBytes
  rw [fl_b, sb_b, len_b]
  decide

private theorem ps_a : packedSize (List.replicate 99 0 ++ [1023]) = 125 := by
  unfold packedSize
  rw [pk_a, width1023, len_a]
  decide

private theorem ps_b : packedSize (List.replicate 99 0 ++ [4]) = 38 := by
  unfold packedSize
  rw [pk_b, width4, len_b]
  decide

theorem refute : ¬ ExceptionCountWidthChoice.Goal := by
  intro h
  have hlen : (List.replicate 99 0 ++ [1023]).length = (List.replicate 99 0 ++ [4]).length := by
    rw [len_a, len_b]
  have hcnt : (List.filter (flagged 2) (List.replicate 99 0 ++ [1023])).length
      = (List.filter (flagged 2) (List.replicate 99 0 ++ [4])).length := by
    rw [fl_a, fl_b]
    decide
  have hpq := h 2 (List.replicate 99 0 ++ [1023]) (List.replicate 99 0 ++ [4]) hlen hcnt
  have hP : patchedBytes 2 (List.replicate 99 0 ++ [1023])
      ≤ packedSize (List.replicate 99 0 ++ [1023]) := by
    rw [pb_a, ps_a]
    decide
  have hQ : ¬ (patchedBytes 2 (List.replicate 99 0 ++ [4])
      ≤ packedSize (List.replicate 99 0 ++ [4])) := by
    rw [pb_b, ps_b]
    decide
  exact absurd (hpq.1 hP) hQ

end TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory_ExceptionCountWidthChoice


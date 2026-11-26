import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ConnectionFlow.Goal_FieldDecoderChunkCarry

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow


/-! Chunk-carried field decoding: `splitOn` emits every completed field of a
received chunk and carries only its final unterminated piece into the next read.
The reversal of that carried piece is exactly the accumulator used by
`splitOnPPrepend`, so TCP fragmentation at any byte boundary preserves the
concatenated field sequence. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.deprecated false
set_option linter.unusedSimpArgs false

namespace FdcProof

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

/-- Every split of a list into delimiter-separated pieces has at least one
piece: the final (possibly empty) unterminated suffix. -/
theorem splitPrepend_ne_nil (p : Nat → Bool) : ∀ (l acc : List Nat),
    List.splitOnPPrepend p l acc ≠ []
  | [], _ => by rw [List.splitOnPPrepend.eq_1]; simp
  | x :: xs, acc => by
      rw [List.splitOnPPrepend.eq_2 p acc x xs]
      by_cases hx : p x = true
      · simp [hx]
      · simp [hx]

/-- Nonemptiness specialized to an empty carried prefix. -/
theorem splitOnP_ne_nil (p : Nat → Bool) : ∀ l : List Nat, List.splitOnP p l ≠ [] := by
  intro l
  change List.splitOnPPrepend p l [] ≠ []
  exact splitPrepend_ne_nil p l []

/-- Appending a chunk to a split with a carried prefix: drop the last piece of
the first split and continue splitting the next chunk with that piece reversed
as the accumulator. -/
theorem splitPrepend_append_aux (p : Nat → Bool) : ∀ (l b acc : List Nat),
    List.splitOnPPrepend p (l ++ b) acc =
      let parts := List.splitOnPPrepend p l acc
      parts.dropLast ++ List.splitOnPPrepend p b (parts.getLast?.getD []).reverse
  | [], b, acc => by
      rw [List.splitOnPPrepend.eq_1]
      simp
  | x :: xs, b, acc => by
      rw [List.splitOnPPrepend.eq_2 p acc x xs]
      change List.splitOnPPrepend p (x :: (xs ++ b)) acc = _
      rw [List.splitOnPPrepend.eq_2 p acc x (xs ++ b)]
      by_cases hx : p x = true
      · simp only [if_pos hx]
        rw [splitPrepend_append_aux p xs b []]
        have hne : List.splitOnPPrepend p xs [] ≠ [] := splitPrepend_ne_nil p xs []
        cases hs : List.splitOnPPrepend p xs [] with
        | nil => exact False.elim (hne hs)
        | cons _ _ =>
            simp [List.dropLast_cons_cons, hs]
      · simp only [if_neg hx]
        rw [splitPrepend_append_aux p xs b (x :: acc)]

/-- A carried prefix acts on the head piece exactly like an accumulator:
`splitOnPPrepend p b acc` is `splitOnP p b` with `acc.reverse` prepended to its
first piece. -/
theorem splitPrepend_eq_modifyHead (p : Nat → Bool) : ∀ (b acc : List Nat),
    List.splitOnPPrepend p b acc =
      List.modifyHead (fun v => acc.reverse ++ v) (List.splitOnP p b)
  | [], acc => by
      rw [List.splitOnPPrepend.eq_1, List.splitOnP_nil]
      simp [List.modifyHead]
  | x :: xs, acc => by
      rw [List.splitOnPPrepend.eq_2 p acc x xs, List.splitOnP_cons]
      by_cases hx : p x = true
      · simp only [if_pos hx]
        rw [splitPrepend_eq_modifyHead p xs []]
        cases hs : List.splitOnP p xs with
        | nil => exact False.elim (splitOnP_ne_nil p xs hs)
        | cons _ _ => simp [List.modifyHead, hs]
      · simp only [if_neg hx]
        rw [splitPrepend_eq_modifyHead p xs (x :: acc)]
        cases hs : List.splitOnP p xs with
        | nil => exact False.elim (splitOnP_ne_nil p xs hs)
        | cons _ _ => simp [List.modifyHead, hs, List.reverse_cons]

/-- The final piece of a delimiter split is unterminated: it contains no
delimiter, so a subsequent read may safely prepend new bytes to it. -/
theorem splitPrepend_last_no_delim (p : Nat → Bool) : ∀ l : List Nat,
    ((List.splitOnP p l).getLast?.getD []).any p = false
  | [] => by simp [List.splitOnP_nil]
  | x :: xs => by
      rw [List.splitOnP_cons]
      by_cases hx : p x = true
      · cases hs : List.splitOnP p xs with
        | nil => exact False.elim (splitOnP_ne_nil p xs hs)
        | cons y ys =>
            have hne : y :: ys ≠ [] := by simp
            simpa only [if_pos hx, hs, List.getLast?_cons_of_ne_nil hne]
              using splitPrepend_last_no_delim p xs
      · have hxtrue : ¬ p x = true := by simp [hx]
        cases hs : List.splitOnP p xs with
        | nil => exact False.elim (splitOnP_ne_nil p xs hs)
        | cons y ys =>
            have hlast := splitPrepend_last_no_delim p xs
            cases ys with
            | nil =>
                have hy : y.any p = false := by
                  simpa only [hs, List.getLast?_singleton, Option.getD_some] using hlast
                have hy' : ∀ z ∈ y, ¬ p z = true := List.any_eq_false.mp hy
                have hxy' : ∀ z ∈ x :: y, ¬ p z = true := by
                  intro z hz
                  simp only [List.mem_cons] at hz
                  rcases hz with rfl | hz
                  · exact hxtrue
                  · exact hy' z hz
                have hxy : (x :: y).any p = false := List.any_eq_false.mpr hxy'
                simpa only [if_neg hxtrue, hs, List.modifyHead,
                  List.getLast?_cons, List.getLast?_nil, Option.getD_some, Option.getD_none] using hxy
            | cons y₂ ys₂ =>
                have htailne : y₂ :: ys₂ ≠ [] := by simp
                simpa only [if_neg hxtrue, hs, List.modifyHead,
                  List.getLast?_cons_of_ne_nil htailne] using hlast

/-- Splitting a delimiter-free carried piece followed by a new chunk is the
same as splitting the new chunk with the reversed piece as accumulator. -/
theorem splitOnP_prepend_of_no_delim (p : Nat → Bool) : ∀ (v b : List Nat),
    v.any p = false → List.splitOnP p (v ++ b) = List.splitOnPPrepend p b v.reverse
  | [], _, _ => by simp [List.splitOnP_nil]
  | x :: xs, b, h => by
      have hx : p x = false := by
        cases hpx : p x
        · rfl
        · simp [hpx] at h
      have hxs : xs.any p = false := by
        cases hxs' : xs.any p
        · rfl
        · simp [hxs'] at h
      have hxtrue : ¬ p x = true := by simp [hx]
      change List.splitOnP p (x :: (xs ++ b)) = List.splitOnPPrepend p b (x :: xs).reverse
      rw [List.splitOnP_cons]
      simp only [if_neg hxtrue]
      rw [splitOnP_prepend_of_no_delim p xs b hxs]
      rw [splitPrepend_eq_modifyHead p b xs.reverse]
      rw [show (x :: xs).reverse = xs.reverse ++ [x] by simp]
      rw [splitPrepend_eq_modifyHead p b (xs.reverse ++ [x])]
      cases hs : List.splitOnP p b with
      | nil => exact False.elim (splitOnP_ne_nil p b hs)
      | cons _ _ => simp [List.modifyHead, hs, List.reverse_cons]

end FdcProof

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow.FieldDecoderChunkCarry.Goal := by
  intro a b
  let p : Nat → Bool := fun x => x == 0
  let last := (List.splitOnP p a).getLast?.getD []
  have hlast : last.any p = false := FdcProof.splitPrepend_last_no_delim p a
  have hpiece := FdcProof.splitOnP_prepend_of_no_delim p last b hlast
  have haux := FdcProof.splitPrepend_append_aux p a b []
  change (List.splitOnP p a).dropLast ++ List.splitOnP p (last ++ b) =
    List.splitOnP p (a ++ b)
  rw [hpiece]
  change (List.splitOnPPrepend p a []).dropLast ++
      List.splitOnPPrepend p b ((List.splitOnPPrepend p a []).getLast?.getD []).reverse =
    List.splitOnPPrepend p (a ++ b) []
  exact haux.symm
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ConnectionFlow.Goal_FieldDecoderChunkCarry

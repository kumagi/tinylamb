import TinylambOpt.ResearchGoals.EngineDecisions.Executor

/-!
These models compare concrete implementation choices in planning, execution,
concurrency, or recovery against their stated observable contracts.

A predicate that is downward closed for `≤`: on a sorted list its satisfying elements
form a prefix, so `seek` counts exactly `countP`.

On a sorted list with a downward-closed test, the satisfying elements are exactly the
leading `countP`-prefix.

Named results include `sound`.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.EngineDecisions.Exec_RangeFingers

open TinylambOpt.ResearchGoals.EngineDecisions.Executor.RangeFingers

/-- A predicate that is downward closed for `≤`: on a sorted list its
satisfying elements form a prefix, so `seek` counts exactly `countP`. -/
private def Downward (p : Int → Bool) : Prop :=
  ∀ a b : Int, a ≤ b → p b = true → p a = true

private theorem downward_lt (x : Int) : Downward (fun y => y < x) := by
  intro a b hab h
  have hb : b < x := of_decide_eq_true h
  apply decide_eq_true
  omega

private theorem downward_le (x : Int) : Downward (fun y => y ≤ x) := by
  intro a b hab h
  have hb : b ≤ x := of_decide_eq_true h
  apply decide_eq_true
  omega

private theorem head_pairwise {x : Int} {xs : List Int} (h : (x :: xs).Pairwise (· ≤ ·)) :
    (∀ y ∈ xs, x ≤ y) ∧ xs.Pairwise (· ≤ ·) := List.pairwise_cons.mp h

private theorem suffix_pairwise (la t : List Int) (h : (la ++ t).Pairwise (· ≤ ·)) :
    t.Pairwise (· ≤ ·) := by
  obtain ⟨_, ht, _⟩ := List.pairwise_append.mp h
  exact ht

/-- On a sorted list with a downward-closed test, the satisfying elements are
exactly the leading `countP`-prefix. -/
private theorem filter_take {p : Int → Bool} (hp : Downward p) :
    (xs : List Int) → xs.Pairwise (· ≤ ·) → xs.filter p = xs.take (xs.countP p)
  | [], _ => rfl
  | x :: xs, h => by
      obtain ⟨hxle, hxs⟩ := head_pairwise h
      by_cases hpx : p x = true
      · rw [List.filter_cons, ite_eq_left hpx, filter_take hp xs hxs, List.countP_cons_of_pos hpx,
          List.take_succ_cons]
      · rw [List.filter_cons, ite_eq_right hpx]
        have hnil : xs.filter p = [] := by
          rw [List.filter_eq_nil_iff]
          intro y hy hh
          exact hpx (hp x y (hxle y hy) hh)
        have hcz : (x :: xs).countP p = 0 := by
          rw [List.countP_cons_of_neg hpx, List.countP_eq_zero]
          intro y hy hh
          exact hpx (hp x y (hxle y hy) hh)
        rw [hnil, hcz]
        simp

/-- A seek into a sorted run returns exactly the count, the suffix after the
matching prefix, and its work is one per consumed key plus one probe on a
miss. -/
private theorem seek_spec {p : Int → Bool} (hp : Downward p) :
    (xs : List Int) → xs.Pairwise (· ≤ ·) →
    seek p xs = (xs.countP p, xs.drop (xs.countP p),
      xs.countP p + if xs.drop (xs.countP p) = [] then 0 else 1)
  | [], _ => rfl
  | x :: xs, h => by
      obtain ⟨hxle, hxs⟩ := head_pairwise h
      by_cases hpx : p x = true
      · have hrec := seek_spec hp xs hxs
        have hcp : (x :: xs).countP p = xs.countP p + 1 := List.countP_cons_of_pos hpx
        show (if p x = true then
            (let (n, rest, work) := seek p xs; (n + 1, rest, work + 1))
          else (0, x :: xs, 1)) =
          ((x :: xs).countP p, (x :: xs).drop ((x :: xs).countP p),
            (x :: xs).countP p +
              if (x :: xs).drop ((x :: xs).countP p) = [] then 0 else 1)
        rw [ite_eq_left hpx, hrec, hcp]
        by_cases hd : xs.drop (xs.countP p) = [] <;>
          simp only [hd, ite_true, ite_false, List.drop] <;> omega
      · have hcz : (x :: xs).countP p = 0 := by
          rw [List.countP_cons_of_neg hpx, List.countP_eq_zero]
          intro y hy hh
          exact hpx (hp x y (hxle y hy) hh)
        show (if p x = true then
            (let (n, rest, work) := seek p xs; (n + 1, rest, work + 1))
          else (0, x :: xs, 1)) =
          ((x :: xs).countP p, (x :: xs).drop ((x :: xs).countP p),
            (x :: xs).countP p +
              if (x :: xs).drop ((x :: xs).countP p) = [] then 0 else 1)
        rw [ite_eq_right hpx, hcz]
        simp

/-- The unfolded sweep step, with the internal `let` bindings made explicit
through projections. -/
private theorem sweep_cons_eq (lo hi : Nat) (x : Int) (xs lowTail highTail : List Int) :
    (sweep lo hi (x :: xs) lowTail highTail).1 =
      ((lo + (seek (fun y => y < x) lowTail).1, hi + (seek (fun y => y ≤ x) highTail).1) ::
        (sweep (lo + (seek (fun y => y < x) lowTail).1)
          (hi + (seek (fun y => y ≤ x) highTail).1) xs
          (seek (fun y => y < x) lowTail).2.1 (seek (fun y => y ≤ x) highTail).2.1).1) ∧
    (sweep lo hi (x :: xs) lowTail highTail).2 =
      (seek (fun y => y < x) lowTail).2.2 + (seek (fun y => y ≤ x) highTail).2.2 +
        (sweep (lo + (seek (fun y => y < x) lowTail).1)
          (hi + (seek (fun y => y ≤ x) highTail).1) xs
          (seek (fun y => y < x) lowTail).2.1 (seek (fun y => y ≤ x) highTail).2.1).2 := by
  constructor <;> rfl

/-- The generalized sweep: two fingers walking suffixes of one sorted `right`
list emit exact equal ranges, and their work is bounded by the two remaining
suffix lengths plus two charges per probe element. -/
private theorem sweep_correct (right : List Int) (hr : right.Pairwise (· ≤ ·)) :
    (left : List Int) → left.Pairwise (· ≤ ·) →
    ∀ (lo hi : Nat) (lowTail highTail : List Int),
      (∃ la : List Int, right = la ++ lowTail ∧ la.length = lo ∧
        ∀ x ∈ left, ∀ y ∈ la, y < x) →
      (∃ ha : List Int, right = ha ++ highTail ∧ ha.length = hi ∧
        ∀ x ∈ left, ∀ y ∈ ha, y ≤ x) →
      (sweep lo hi left lowTail highTail).1 =
        left.map (fun x => (right.countP (fun y => y < x), right.countP (fun y => y ≤ x))) ∧
      (sweep lo hi left lowTail highTail).2 ≤
        lowTail.length + highTail.length + 2 * left.length
  | [], _, lo, hi, lowTail, highTail, _, _ => by
      simp [sweep]
  | x :: xs, h, lo, hi, lowTail, highTail, ⟨la, hrla, hlenla, hlt⟩, ⟨ha, hsha, hlenha, hle⟩ => by
      obtain ⟨hxle, hxs⟩ := head_pairwise h
      have hlowS : lowTail.Pairwise (· ≤ ·) := suffix_pairwise la lowTail (hrla ▸ hr)
      have hhighS : highTail.Pairwise (· ≤ ·) := suffix_pairwise ha highTail (hsha ▸ hr)
      have es := seek_spec (downward_lt x) lowTail hlowS
      have ee := seek_spec (downward_le x) highTail hhighS
      obtain ⟨hc1, hc2⟩ := sweep_cons_eq lo hi x xs lowTail highTail
      have eL1 : (seek (fun y => y < x) lowTail).1 = lowTail.countP (fun y => y < x) := by
        rw [es]
      have eL2 : (seek (fun y => y < x) lowTail).2.1 = lowTail.drop (lowTail.countP (fun y => y < x)) := by
        rw [es]
      have eL3 : (seek (fun y => y < x) lowTail).2.2 =
          lowTail.countP (fun y => y < x) +
            if lowTail.drop (lowTail.countP (fun y => y < x)) = [] then 0 else 1 := by
        rw [es]
      have eH1 : (seek (fun y => y ≤ x) highTail).1 = highTail.countP (fun y => y ≤ x) := by
        rw [ee]
      have eH2 : (seek (fun y => y ≤ x) highTail).2.1 = highTail.drop (highTail.countP (fun y => y ≤ x)) := by
        rw [ee]
      have eH3 : (seek (fun y => y ≤ x) highTail).2.2 =
          highTail.countP (fun y => y ≤ x) +
            if highTail.drop (highTail.countP (fun y => y ≤ x)) = [] then 0 else 1 := by
        rw [ee]
      rw [hc1, hc2, eL1, eL2, eL3, eH1, eH2, eH3]
      -- induction step invariants for the tails after both seeks
      have hftL : lowTail.filter (fun y => y < x) =
          lowTail.take (lowTail.countP (fun y => y < x)) :=
        filter_take (downward_lt x) lowTail hlowS
      have hftH : highTail.filter (fun y => y ≤ x) =
          highTail.take (highTail.countP (fun y => y ≤ x)) :=
        filter_take (downward_le x) highTail hhighS
      obtain ⟨ih1, ih2⟩ := sweep_correct right hr xs hxs
        (lo + lowTail.countP (fun y => y < x))
        (hi + highTail.countP (fun y => y ≤ x))
        (lowTail.drop (lowTail.countP (fun y => y < x)))
        (highTail.drop (highTail.countP (fun y => y ≤ x)))
        ⟨la ++ lowTail.take (lowTail.countP (fun y => y < x)),
          by rw [hrla, List.append_assoc, List.take_append_drop],
          by rw [List.length_append, List.length_take, Nat.min_eq_left List.countP_le_length, hlenla],
          by
            intro x' hx' y hy
            rcases List.mem_append.mp hy with hyl | hyt
            · have h1 : y < x := hlt x (by simp) y hyl
              have h2 : x ≤ x' := hxle x' hx'
              omega
            · rw [← hftL] at hyt
              have hpy : y < x := by simpa using (List.mem_filter.mp hyt).2
              have h2 : x ≤ x' := hxle x' hx'
              omega⟩
        ⟨ha ++ highTail.take (highTail.countP (fun y => y ≤ x)),
          by rw [hsha, List.append_assoc, List.take_append_drop],
          by rw [List.length_append, List.length_take, Nat.min_eq_left List.countP_le_length, hlenha],
          by
            intro x' hx' y hy
            rcases List.mem_append.mp hy with hyl | hyt
            · have h1 : y ≤ x := hle x (by simp) y hyl
              have h2 : x ≤ x' := hxle x' hx'
              omega
            · rw [← hftH] at hyt
              have hpy : y ≤ x := by simpa using (List.mem_filter.mp hyt).2
              have h2 : x ≤ x' := hxle x' hx'
              omega⟩
      rw [ih1]
      constructor
      · -- bounds pair for the head element x
        have hpair :
            (lo + lowTail.countP (fun y => y < x), hi + highTail.countP (fun y => y ≤ x)) =
              (right.countP (fun y => y < x), right.countP (fun y => y ≤ x)) := by
          apply Prod.ext
          · show lo + lowTail.countP (fun y => y < x) = right.countP (fun y => y < x)
            rw [← hlenla, hrla, List.countP_append, List.countP_eq_length.mpr
              (fun y hy => decide_eq_true (hlt x (by simp) y hy))]
          · show hi + highTail.countP (fun y => y ≤ x) = right.countP (fun y => y ≤ x)
            rw [hsha, List.countP_append,
              List.countP_eq_length.mpr (fun y hy => decide_eq_true (hle x (by simp) y hy)), hlenha]
        rw [List.map_cons]
        exact congrArg (fun p => p :: xs.map
          (fun x => (right.countP (fun y => y < x), right.countP (fun y => y ≤ x)))) hpair
      · -- work accounting
        have hndL : (if lowTail.drop (lowTail.countP (fun y => y < x)) = [] then 0 else 1) ≤ 1 := by
          split <;> omega
        have hndH : (if highTail.drop (highTail.countP (fun y => y ≤ x)) = [] then 0 else 1) ≤ 1 := by
          split <;> omega
        have hlenDropL :
            (lowTail.drop (lowTail.countP (fun y => y < x))).length
              = lowTail.length - lowTail.countP (fun y => y < x) :=
          List.length_drop
        have hlenDropH :
            (highTail.drop (highTail.countP (fun y => y ≤ x))).length
              = highTail.length - highTail.countP (fun y => y ≤ x) :=
          List.length_drop
        have hmid :
            (sweep (lo + lowTail.countP (fun y => y < x))
              (hi + highTail.countP (fun y => y ≤ x)) xs
              (lowTail.drop (lowTail.countP (fun y => y < x)))
              (highTail.drop (highTail.countP (fun y => y ≤ x)))).2 ≤
              (lowTail.drop (lowTail.countP (fun y => y < x))).length +
                (highTail.drop (highTail.countP (fun y => y ≤ x))).length +
                2 * xs.length := ih2
        have hcPL : lowTail.countP (fun y => y < x) ≤ lowTail.length := List.countP_le_length
        have hcPH : highTail.countP (fun y => y ≤ x) ≤ highTail.length := List.countP_le_length
        have hgoal :
            (lowTail.countP (fun y => y < x) +
                if lowTail.drop (lowTail.countP (fun y => y < x)) = [] then 0 else 1) +
              (highTail.countP (fun y => y ≤ x) +
                if highTail.drop (highTail.countP (fun y => y ≤ x)) = [] then 0 else 1) +
              (sweep (lo + lowTail.countP (fun y => y < x))
                (hi + highTail.countP (fun y => y ≤ x)) xs
                (lowTail.drop (lowTail.countP (fun y => y < x)))
                (highTail.drop (highTail.countP (fun y => y ≤ x)))).2 ≤
            lowTail.length + highTail.length + 2 * (xs.length + 1) := by
          omega
        exact hgoal

theorem sound : Goal := by
  intro left right hl hr
  have H := sweep_correct right hr left hl 0 0 right right
    ⟨[], by simp, rfl, by intro x hx y hy; cases hy⟩
    ⟨[], by simp, rfl, by intro x hx y hy; cases hy⟩
  refine ⟨H.1, ?_⟩
  show (sweep 0 0 left right right).2 ≤ 2 * (left.length + right.length)
  omega

end TinylambOpt.ResearchProofs.EngineDecisions.Exec_RangeFingers

-- C++ references to this model or its proof.
-- executor/relational_scalar_counts.cpp

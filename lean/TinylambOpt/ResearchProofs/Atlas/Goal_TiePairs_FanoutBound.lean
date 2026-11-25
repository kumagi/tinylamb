import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_TiePairs_FanoutBound

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
  `TiePairs.FanoutBound`: if no key occurs more than `c` times, twice the number
  of equal-valued ordered row pairs is at most `length * (c - 1)`.

  Small-input exhaustive check over lists of length < 5 on a three-value
  alphabet and all capacities `c < 5` (`#eval` prints `true`).

  The proof uses the identity `2 * tieCount x = Σ_{a ∈ x} (count_x a - 1)`:
  adding a head `a` adds `2 * xs.count a` to both sides. Each summand is then
  bounded by `c - 1`.
-/

#eval
  let vals : List Int := [0, 1, 2]
  let rec lists : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => (lists n).flatMap (fun xs => vals.map (fun a => a :: xs))
  (List.range 5).all (fun c =>
    (List.range 5).all (fun n =>
      (lists n).all (fun x =>
        (∀ a ∈ x, x.count a ≤ c) →
          decide (2 * TiePairs.measure x ≤ size x * (c - 1 : Nat)))))

private def tieCount (x : Series) : Nat :=
  (pairs x).countP (fun p => p.1 == p.2)

private def countSum (x : Series) : Nat :=
  (x.map (fun a => x.count a - 1)).sum

private theorem countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => by_cases h : P (g x) = true <;> simp [h, ih]

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  rw [show xs.zipIdx 1 = xs.zipIdx.map (fun p : Int × Nat => (p.1, p.2 + 1)) by
    simpa using (List.zipIdx_succ (l := xs) (i := 0))]
  simp [List.flatMap_cons, List.flatMap_map]

private theorem countP_eq_count_comm (x : Series) (a : Int) :
    x.countP (fun b => a == b) = x.count a := by
  rw [List.count_eq_countP]
  congr 1
  funext b
  apply Bool.eq_iff_iff.mpr
  simpa only [beq_iff_eq] using eq_comm

private theorem tieCount_cons (a : Int) (xs : Series) :
    tieCount (a :: xs) = xs.count a + tieCount xs := by
  unfold tieCount
  rw [pairs_cons, List.countP_append, countP_map_comp]
  change xs.countP (fun b => a == b) + (pairs xs).countP (fun p : Int × Int => p.1 == p.2) = _
  rw [countP_eq_count_comm]

private theorem count_cons_eq (a b : Int) (xs : Series) :
    (a :: xs).count b = xs.count b + (if b == a then 1 else 0) := by
  by_cases h : b == a
  · have hba : b = a := of_decide_eq_true h
    subst b
    simp
  · have hne : a ≠ b := by
      intro heq
      apply h
      subst heq
      simp
    simp [h, hne]

private theorem sum_map_add_nat {α : Type} (xs : List α) (f g : α → Nat) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem count_pos_of_mem (xs : Series) (a : Int) (ha : a ∈ xs) : 0 < xs.count a := by
  induction xs with
  | nil => simp at ha
  | cons b bs ih =>
      by_cases h : b == a
      · have hba : b = a := of_decide_eq_true h
        subst b
        simp
      · have hne : a ≠ b := by
          intro heq
          apply h
          subst heq
          simp
        have hmem : a ∈ bs := by
          simpa [hne] using ha
        have hi := ih hmem
        simp [hne]
        omega

private theorem sum_ite_count (xs : Series) (a : Int) :
    (xs.map (fun b => if b == a then 1 else 0)).sum = xs.count a := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      by_cases h : b == a
      · have hba : b = a := of_decide_eq_true h
        subst b
        rw [List.map_cons, List.sum_cons, ih]
        simp
        omega
      · have hne : b ≠ a := by
          intro heq
          apply h
          subst heq
          simp
        rw [List.map_cons, List.sum_cons, ih]
        simp [h, hne]

private theorem countSum_cons (a : Int) (xs : Series) :
    countSum (a :: xs) = 2 * xs.count a + countSum xs := by
  unfold countSum
  rw [List.map_cons, List.sum_cons]
  have hhead : (a :: xs).count a = xs.count a + 1 := by simp
  rw [hhead]
  simp only [Nat.add_sub_cancel]
  have hpoint : ∀ b ∈ xs, ((a :: xs).count b - 1) =
      (xs.count b - 1) + (if b == a then 1 else 0) := by
    intro b hb
    rw [count_cons_eq]
    by_cases h : b == a
    · have hpos : 0 < xs.count b := count_pos_of_mem xs b hb
      simp [h]
      omega
    · simp [h]
  have hmap : (xs.map (fun b => (a :: xs).count b - 1)).sum =
      (xs.map (fun b => (xs.count b - 1) + (if b == a then 1 else 0))).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro b hb
    exact hpoint b hb
  rw [hmap, sum_map_add_nat]
  rw [sum_ite_count]
  omega

private theorem two_tie_eq_countSum (x : Series) : 2 * tieCount x = countSum x := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      rw [tieCount_cons, countSum_cons]
      omega

private theorem sum_le_sum_of_pointwise {α : Type} (xs : List α) (f g : α → Nat)
    (h : ∀ a ∈ xs, f a ≤ g a) : (xs.map f).sum ≤ (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      have htail : ∀ b ∈ as, f b ≤ g b := by
        intro b hb
        exact h b (by simp [hb])
      have hi := ih htail
      have hf := h a (by simp)
      omega

private theorem sum_const_nat {α : Type} (xs : List α) (k : Nat) :
    (xs.map (fun _ => k)).sum = xs.length * k := by
  induction xs with
  | nil => simp
  | cons a as ih =>
      rw [List.map_cons, List.sum_cons, ih, List.length_cons, Nat.succ_mul]
      omega

theorem sound_TiePairs_FanoutBound : TiePairs.FanoutBound.Goal := by
  intro x c hc
  change 2 * (tieCount x : Int) ≤ (x.length : Int) * ((c - 1 : Nat) : Int)
  have hnat : 2 * tieCount x ≤ x.length * (c - 1) := by
    rw [two_tie_eq_countSum]
    unfold countSum
    have hpoint : ∀ a ∈ x, x.count a - 1 ≤ c - 1 := by
      intro a ha
      exact Nat.sub_le_sub_right (hc a ha) 1
    have hsum : (x.map (fun a => x.count a - 1)).sum ≤ (x.map (fun _ => c - 1)).sum :=
      sum_le_sum_of_pointwise x (fun a => x.count a - 1) (fun _ => c - 1) hpoint
    rw [← sum_const_nat x (c - 1)]
    exact hsum
  exact_mod_cast hnat

end TinylambOpt.ResearchProofs.Atlas.Goal_TiePairs_FanoutBound

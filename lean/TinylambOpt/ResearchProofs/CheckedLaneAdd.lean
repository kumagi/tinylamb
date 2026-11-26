import TinylambOpt.ResearchGoals.CheckedLaneAdd

/-!
Proved

- `Goal` — `∀ m a b, (∀ i < 64, observe m a b i = reference m a b i) ∧ (List.range
  64).find? (errors m a b).getLsbD = (List.range 64).find? (fun i => decide
  (reference m a b i = .overflow))`

The reference model is defined in `TinylambOpt.ResearchGoals.CheckedLaneAdd`.
-/

namespace TinylambOpt.ResearchProofs.CheckedLaneAdd

open TinylambOpt.ResearchGoals.CheckedLaneAdd

/-! Per-lane analysis of the masked checked INT64 addition:
the `signedOverflow` bit formula is shown equivalent to `BitVec.saddOverflow`
(i.e. to the out-of-range test on mathematical integers used by `reference`),
`pack` is characterised bitwise, and the per-lane cells as well as the
first-error mask agree with the sequential reference. -/

/-- The mask formula picks the sign bit, i.e. it is the classical
"same input signs, different output sign" test in terms of `msb`. -/
private theorem signedOverflow_msb (x y : BitVec 64) :
    signedOverflow x y = (x.msb == y.msb && !((x + y).msb == x.msb)) := by
  unfold signedOverflow
  have hx : x.msb = x.getLsbD 63 := by simp [BitVec.msb, BitVec.getMsbD]
  have hy : y.msb = y.getLsbD 63 := by simp [BitVec.msb, BitVec.getMsbD]
  have hs : (x + y).msb = (x + y).getLsbD 63 := by simp [BitVec.msb, BitVec.getMsbD]
  rw [hx, hy, hs]
  simp only [BitVec.getLsbD_and, BitVec.getLsbD_not, BitVec.getLsbD_xor]
  generalize hxa : x.getLsbD 63 = a, hya : y.getLsbD 63 = b,
    hsa : (x + y).getLsbD 63 = c
  cases a <;> cases b <;> cases c <;> rfl

/-- `signedOverflow` is exactly core's signed-addition overflow flag. -/
private theorem signedOverflow_sadd (x y : BitVec 64) :
    signedOverflow x y = BitVec.saddOverflow x y := by
  rw [signedOverflow_msb, ← BitVec.saddOverflow_eq]

/-- The out-of-range test used in `reference` (in the exact shape it is
elaborated to) is `saddOverflow`. -/
private theorem sadd_decide (x y : BitVec 64) :
    (decide (x.toInt + y.toInt < -(2 ^ 63)) || decide ((2 ^ 63 : Int) ≤ x.toInt + y.toInt)) =
      BitVec.saddOverflow x y := by
  show (decide (x.toInt + y.toInt < -(2 ^ 63)) ||
      decide ((2 ^ 63 : Int) ≤ x.toInt + y.toInt)) =
    (decide ((2 ^ 63 : Int) ≤ x.toInt + y.toInt) || decide (x.toInt + y.toInt < -(2 ^ 63)))
  exact Bool.or_comm _ _

/-- Each packed contribution only touches the bits at and above its shift. -/
private theorem shifted_getLsbD (f : Nat → Bool) (i j : Nat) (hj : j < 64) :
    ((BitVec.ofNat 64 (f i).toNat) <<< i).getLsbD j = (decide (i = j) && f i) := by
  rw [BitVec.getLsbD_shiftLeft, BitVec.getLsbD_ofNat, Nat.testBit_bool_toNat]
  have h1 : decide (j < 64) = true := decide_eq_true hj
  have h3 : decide (j - i < 64) = true := decide_eq_true (by omega : j - i < 64)
  rw [h1, h3]
  rcases Nat.lt_trichotomy i j with h | h | h
  · rw [decide_eq_false (by omega : ¬ j < i), decide_eq_false (by omega : ¬ (j - i = 0)),
      decide_eq_false (by omega : ¬ i = j)]
    simp
  · rw [h]
    rw [decide_eq_false (Nat.lt_irrefl j), decide_eq_true (Nat.sub_self j),
      decide_eq_true (Eq.refl j)]
    simp
  · rw [decide_eq_true h, decide_eq_false (by omega : ¬ i = j)]
    simp

/-- Fold form of `pack`: a bit below 64 is set iff the accumulator had it or
some packed lane wrote it. -/
private theorem pack_fold (f : Nat → Bool) :
    ∀ (L : List Nat), (∀ i ∈ L, i < 64) →
      ∀ (w : BitVec 64) (j : Nat), j < 64 →
        (L.foldl (fun w i => w ||| ((BitVec.ofNat 64 (f i).toNat) <<< i)) w).getLsbD j =
          (w.getLsbD j || L.any (fun i => decide (i = j) && f i)) := by
  intro L hL
  induction L with
  | nil => intro w j _; simp
  | cons a t ih =>
    intro w j hj
    have ht : ∀ i ∈ t, i < 64 := fun i hi => hL i (List.mem_cons_of_mem a hi)
    rw [List.foldl_cons, ih ht _ _ hj]
    rw [BitVec.getLsbD_or, shifted_getLsbD f a j hj, List.any_cons]
    exact Bool.or_assoc _ _ _

/-- On a range, the packed disjunction collapses to the single test at `j`. -/
private theorem any_range (f : Nat → Bool) :
    ∀ (n j : Nat), (List.range n).any (fun i => decide (i = j) && f i) =
      (decide (j < n) && f j) := by
  intro n
  induction n with
  | zero =>
    intro j
    rw [List.range_zero, List.any_nil, decide_eq_false (Nat.not_lt_zero j), Bool.false_and]
  | succ n ih =>
    intro j
    rw [List.range_succ, List.any_append, ih]
    simp only [List.any_cons, List.any_nil, Bool.or_false]
    rcases Nat.lt_trichotomy j n with h | h | h
    · rw [decide_eq_true h, decide_eq_false (by omega : ¬ n = j),
        decide_eq_true (by omega : j < n + 1)]
      simp
    · rw [h]
      rw [decide_eq_false (Nat.lt_irrefl n), decide_eq_true (Eq.refl n),
        decide_eq_true (Nat.lt_succ_self n)]
      simp
    · rw [decide_eq_false (by omega : ¬ j < n), decide_eq_false (by omega : ¬ n = j),
        decide_eq_false (by omega : ¬ j < n + 1)]
      simp

private theorem pack_getLsbD (f : Nat → Bool) (j : Nat) (hj : j < 64) :
    (pack f).getLsbD j = f j := by
  unfold pack
  rw [pack_fold f (List.range 64) (fun i hi => List.mem_range.mp hi) 0 j hj,
    any_range,
    show ((0 : TinylambOpt.ResearchGoals.CheckedLaneAdd.W).getLsbD j) = false from
      BitVec.getLsbD_zero (w := 64) (i := j),
    Bool.false_or, decide_eq_true hj, Bool.true_and]

/-- Bitwise characterisation of the validity conjunction. -/
private theorem live_getLsbD (m : Masks) (i : Nat) :
    (live m).getLsbD i =
      (m.active.getLsbD i && m.leftValid.getLsbD i && m.rightValid.getLsbD i) := by
  unfold live
  rw [BitVec.getLsbD_and, BitVec.getLsbD_and]

/-- Bitwise characterisation of the error mask. -/
private theorem errors_getLsbD (m : Masks) (a b : Nat → BitVec 64) (i : Nat) (hi : i < 64) :
    (errors m a b).getLsbD i =
      ((live m).getLsbD i && signedOverflow (a i) (b i)) := by
  unfold errors
  rw [BitVec.getLsbD_and, pack_getLsbD _ _ hi]

/-- Non-overflowing lanes compute the wrapping sum as the integer sum. -/
private theorem value_ok (x y : BitVec 64) (h : ¬ BitVec.saddOverflow x y) :
    (x + y).toInt = x.toInt + y.toInt :=
  BitVec.toInt_add_of_not_saddOverflow h

/-- Core per-lane correspondence between `observe` and `reference`. -/
private theorem lane_eq (m : Masks) (a b : Nat → BitVec 64) (i : Nat) (hi : i < 64) :
    observe m a b i = reference m a b i := by
  unfold observe reference
  dsimp only
  rw [errors_getLsbD m a b i hi, live_getLsbD m i, signedOverflow_sadd, sadd_decide]
  generalize h1 : m.active.getLsbD i = A, h2 : m.leftValid.getLsbD i = B,
    h3 : m.rightValid.getLsbD i = C, h4 : BitVec.saddOverflow (a i) (b i) = S
  cases A <;> cases B <;> cases C <;> cases S
  all_goals
    first
    | rfl
    | exact congrArg Cell.value (value_ok (a i) (b i) (by simpa using h4))

/-- The error-mask bit equals the reference predicate `= .overflow`. -/
private theorem lane_overflow_bit (m : Masks) (a b : Nat → BitVec 64) (i : Nat) (hi : i < 64) :
    (errors m a b).getLsbD i = decide (reference m a b i = Cell.overflow) := by
  rw [errors_getLsbD m a b i hi, live_getLsbD m i, signedOverflow_sadd]
  unfold reference
  dsimp only
  simp only [sadd_decide]
  generalize h1 : m.active.getLsbD i = A, h2 : m.leftValid.getLsbD i = B,
    h3 : m.rightValid.getLsbD i = C, h4 : BitVec.saddOverflow (a i) (b i) = S
  cases A <;> cases B <;> cases C <;> cases S
  all_goals rfl

/-- Pointwise-congruent predicates find the same element. -/
private theorem find?_congr (p q : Nat → Bool) :
    ∀ (L : List Nat), (∀ x ∈ L, p x = q x) → L.find? p = L.find? q := by
  intro L
  induction L with
  | nil => intro _; rfl
  | cons a t ih =>
    intro h
    simp only [List.find?_cons]
    rw [h a List.mem_cons_self]
    rw [ih (fun x hx => h x (List.mem_cons_of_mem a hx))]

/-- The first error bit is the first lane where the reference overflows. -/
private theorem find?_errors (m : Masks) (a b : Nat → BitVec 64) :
    (List.range 64).find? (errors m a b).getLsbD =
      (List.range 64).find? (fun i => decide (reference m a b i = Cell.overflow)) :=
  find?_congr _ _ (List.range 64)
    (fun i hi => lane_overflow_bit m a b i (List.mem_range.mp hi))

theorem sound : TinylambOpt.ResearchGoals.CheckedLaneAdd.Goal := by
  intro m a b
  exact ⟨fun i hi => lane_eq m a b i hi, find?_errors m a b⟩

end TinylambOpt.ResearchProofs.CheckedLaneAdd

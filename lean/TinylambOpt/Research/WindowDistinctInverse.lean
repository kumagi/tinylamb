import Std.Tactic

/-!
# Inverse multiplicity updates for sliding-window DISTINCT aggregates

C++ counterpart: `executor/detail/window_eval.cpp`.

The **reference** recomputes, for every output row `i`, the multiplicity of
each value in the ROWS frame `[i-a, i+b]` clipped to the partition and with
`EXCLUDE CURRENT ROW` applied.  COUNT DISTINCT is the number of distinct
classes with positive multiplicity (SQL NULL is one class of its own) and
nullable SUM DISTINCT is the sum of the distinct non-NULL classes, or NULL if
there is no such class.

The **optimized** computation keeps one integer multiplicity per value.  When
the frame moves by one row it subtracts one unit for every value that left and
adds one unit for every value that entered.  The leaving/entering lists are
multiset differences of consecutive frame value lists, so a value with two
copies survives the exit of a single copy.

Sufficient hypotheses: the value type has decidable equality, the universe of
keys contains every input value exactly once (we build it with `distinct`),
and the frame is defined by the same ROWS offsets as the reference.  Empty
edge frames, negative values, tied rows (equal values at different positions)
and NULL are all covered.

`window_distinct_inverse_sound` proves that every row produced by the
incremental plan has the same COUNT DISTINCT and nullable SUM DISTINCT as the
per-window recomputation.  `update_count_bound` evaluates the number of
multiplicity updates by the frame movement.  The counterexample shows that a
set-valued state which deletes a value outright when one of two copies leaves
loses a still-present value at the next window.
-/

namespace TinylambOpt.Research.WindowDistinctInverse

abbrev V := Option Int

def mult (v : V) (l : List V) : Nat := l.count v

/-! ## DISTINCT key universe -/

def distinctAcc (seen : List V) : List V → List V
  | [] => seen
  | x :: xs => if x ∈ seen then distinctAcc seen xs else distinctAcc (seen ++ [x]) xs

def distinct (xs : List V) : List V := distinctAcc [] xs

theorem mem_distinctAcc (v : V) (seen xs : List V) :
    v ∈ distinctAcc seen xs ↔ v ∈ seen ∨ v ∈ xs := by
  induction xs generalizing seen with
  | nil => simp [distinctAcc]
  | cons x xs ih =>
      by_cases hx : x ∈ seen
      · have h1 : distinctAcc seen (x :: xs) = distinctAcc seen xs := by simp [distinctAcc, hx]
        rw [h1, ih]
        simp only [List.mem_cons]
        constructor
        · intro h
          rcases h with hs | hxs
          · exact Or.inl hs
          · exact Or.inr (Or.inr hxs)
        · intro h
          rcases h with hs | hrest
          · exact Or.inl hs
          · rcases hrest with hvx | hxs
            · subst v
              exact Or.inl hx
            · exact Or.inr hxs
      · have h1 : distinctAcc seen (x :: xs) = distinctAcc (seen ++ [x]) xs := by simp [distinctAcc, hx]
        rw [h1, ih]
        simp [or_assoc, or_comm, or_left_comm]

theorem nodup_append_singleton (s : List V) (x : V) (hs : s.Nodup) (hx : x ∉ s) :
    (s ++ [x]).Nodup := by
  rw [List.nodup_append]
  refine ⟨hs, by simp, ?_⟩
  intro a ha b hb
  simp at hb
  subst b
  intro haeq
  subst a
  exact hx ha

theorem nodup_distinctAcc (seen xs : List V) (hseen : seen.Nodup) :
    (distinctAcc seen xs).Nodup := by
  induction xs generalizing seen with
  | nil => exact hseen
  | cons x xs ih =>
      change (if x ∈ seen then distinctAcc seen xs else distinctAcc (seen ++ [x]) xs).Nodup
      by_cases hx : x ∈ seen
      · simp [hx]
        exact ih seen hseen
      · simp [hx]
        exact ih (seen ++ [x]) (nodup_append_singleton seen x hseen hx)

theorem mem_distinct (v : V) (xs : List V) : v ∈ distinct xs ↔ v ∈ xs := by
  simpa [distinct] using mem_distinctAcc v [] xs

theorem nodup_distinct (xs : List V) : (distinct xs).Nodup := by
  simpa [distinct] using nodup_distinctAcc [] xs (by simp)

def universeOf (rows : List V) : List V := distinct rows

/-! ## ROWS frame with EXCLUDE CURRENT ROW -/

def inFrame (n a b i j : Nat) : Bool :=
  j < n && j != i && decide (i - a ≤ j ∧ j ≤ i + b)

def framePos (n a b i : Nat) : List Nat :=
  (List.range n).filter (fun j => inFrame n a b i j)

def frameVals (rows : List V) (a b i : Nat) : List V :=
  (framePos rows.length a b i).map (fun j => rows.getD j none)

def presentKeys (keys xs : List V) : List V :=
  keys.filter (fun v => decide (0 < mult v xs))

/-! ## Multiset difference and the incremental multiplicity map -/

def mdiff (xs ys : List V) : List V :=
  ys.foldl (fun acc y => acc.erase y) xs

theorem mult_mdiff (v : V) (xs ys : List V) :
    mult v (mdiff xs ys) = mult v xs - mult v ys := by
  induction ys generalizing xs with
  | nil => simp [mdiff, mult]
  | cons y ys ih =>
      rw [show mdiff xs (y :: ys) = mdiff (xs.erase y) ys by rfl]
      rw [ih]
      simp [mult, List.count_cons, List.count_erase]
      omega

theorem mdiff_length_le (xs ys : List V) : (mdiff xs ys).length ≤ xs.length := by
  induction ys generalizing xs with
  | nil => simp [mdiff]
  | cons y ys ih =>
      rw [show mdiff xs (y :: ys) = mdiff (xs.erase y) ys by rfl]
      have hle : (xs.erase y).length ≤ xs.length := List.Sublist.length_le List.erase_sublist
      have := ih (xs.erase y)
      omega

abbrev Map := V → Nat

def stepMap (m : Map) (outs ins : List V) : Map :=
  fun v => (m v - mult v outs) + mult v ins

def optState (rows : List V) (a b : Nat) : Nat → Map
  | 0 => fun v => mult v (frameVals rows a b 0)
  | i + 1 =>
      stepMap (optState rows a b i)
        (mdiff (frameVals rows a b i) (frameVals rows a b (i + 1)))
        (mdiff (frameVals rows a b (i + 1)) (frameVals rows a b i))

theorem optState_mult (rows : List V) (a b i : Nat) (v : V) :
    optState rows a b i v = mult v (frameVals rows a b i) := by
  induction i with
  | zero => rfl
  | succ i ih =>
      rw [optState]
      dsimp [stepMap]
      rw [ih, mult_mdiff, mult_mdiff]
      omega

/-! ## Reference recomputation vs incremental results -/

def mapKeys (keys : List V) (m : Map) : List V :=
  keys.filter (fun v => decide (0 < m v))

def countDistinctOf (ks : List V) : Nat := ks.length

def sumDistinctOf (ks : List V) : Option Int :=
  let nonNull := (ks.filter (fun v => v.isSome)).map (fun v => v.getD 0)
  if nonNull.isEmpty then none else some nonNull.sum

def refKeys (rows : List V) (a b i : Nat) : List V :=
  presentKeys (universeOf rows) (frameVals rows a b i)

def optKeys (rows : List V) (a b i : Nat) : List V :=
  mapKeys (universeOf rows) (optState rows a b i)

def refCount (rows : List V) (a b i : Nat) : Nat := countDistinctOf (refKeys rows a b i)
def refSum (rows : List V) (a b i : Nat) : Option Int := sumDistinctOf (refKeys rows a b i)
def optCount (rows : List V) (a b i : Nat) : Nat := countDistinctOf (optKeys rows a b i)
def optSum (rows : List V) (a b i : Nat) : Option Int := sumDistinctOf (optKeys rows a b i)

theorem keys_agree (rows : List V) (a b i : Nat) :
    optKeys rows a b i = refKeys rows a b i := by
  unfold optKeys refKeys mapKeys presentKeys
  apply List.filter_congr
  intro v hv
  simp [optState_mult]

theorem window_distinct_inverse_sound (rows : List V) (a b i : Nat) :
    optCount rows a b i = refCount rows a b i ∧
    optSum rows a b i = refSum rows a b i := by
  simp [optCount, refCount, optSum, refSum, keys_agree]

/-! ## Cost: one event per value leaving or entering the frame -/

def outVals (rows : List V) (a b i : Nat) : List V :=
  mdiff (frameVals rows a b i) (frameVals rows a b (i + 1))

def inVals (rows : List V) (a b i : Nat) : List V :=
  mdiff (frameVals rows a b (i + 1)) (frameVals rows a b i)

def allOuts (rows : List V) (a b : Nat) : List (List V) :=
  (List.range (rows.length - 1)).map (fun i => outVals rows a b i)

def allIns (rows : List V) (a b : Nat) : List (List V) :=
  (List.range (rows.length - 1)).map (fun i => inVals rows a b i)

def totalUpdates (rows : List V) (a b : Nat) : Nat :=
  ((List.range (rows.length - 1)).map (fun i => (outVals rows a b i).length)).sum +
  ((List.range (rows.length - 1)).map (fun i => (inVals rows a b i).length)).sum

theorem sum_le_length_mul {l : List Nat} {C : Nat} (h : ∀ x ∈ l, x ≤ C) :
    l.sum ≤ l.length * C := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hx : x ≤ C := h x (by simp)
      have ht : ∀ y ∈ xs, y ≤ C := by intro y hy; exact h y (by simp [hy])
      have hih := ih ht
      rw [List.sum_cons, List.length_cons]
      calc
        x + xs.sum ≤ C + xs.length * C := Nat.add_le_add hx hih
        _ = xs.length * C + C := Nat.add_comm _ _
        _ = (xs.length + 1) * C := (Nat.succ_mul xs.length C).symm

theorem update_count_bound (rows : List V) (a b M : Nat)
    (hM : ∀ i, i < rows.length → (frameVals rows a b i).length ≤ M) :
    totalUpdates rows a b ≤ 2 * M * rows.length := by
  unfold totalUpdates
  have hlen : (List.range (rows.length - 1)).length = rows.length - 1 := by simp
  have hout : ∀ x ∈ (List.range (rows.length - 1)).map (fun i => (outVals rows a b i).length),
      x ≤ M := by
    intro x hx
    rcases List.mem_map.mp hx with ⟨i, hi, rfl⟩
    have hi' : i < rows.length - 1 := List.mem_range.mp hi
    have hb : i < rows.length := by omega
    change (mdiff (frameVals rows a b i) (frameVals rows a b (i + 1))).length ≤ M
    have hle := mdiff_length_le (frameVals rows a b i) (frameVals rows a b (i + 1))
    have hM' := hM i hb
    omega
  have hin : ∀ x ∈ (List.range (rows.length - 1)).map (fun i => (inVals rows a b i).length),
      x ≤ M := by
    intro x hx
    rcases List.mem_map.mp hx with ⟨i, hi, rfl⟩
    have hi' : i < rows.length - 1 := List.mem_range.mp hi
    have hb : i + 1 < rows.length := by omega
    change (mdiff (frameVals rows a b (i + 1)) (frameVals rows a b i)).length ≤ M
    have hle := mdiff_length_le (frameVals rows a b (i + 1)) (frameVals rows a b i)
    have hM' := hM (i + 1) hb
    omega
  have ho := sum_le_length_mul hout
  have hi := sum_le_length_mul hin
  simp only [List.length_map, hlen] at ho hi
  calc
    totalUpdates rows a b ≤ (rows.length - 1) * M + (rows.length - 1) * M := by
      unfold totalUpdates
      exact Nat.add_le_add ho hi
    _ = 2 * ((rows.length - 1) * M) := by omega
    _ ≤ 2 * (rows.length * M) :=
      Nat.mul_le_mul_left 2 (Nat.mul_le_mul_right M (by omega : rows.length - 1 ≤ rows.length))
    _ = 2 * M * rows.length := by ac_rfl

/-! ## Counterexample: set-style deletion drops a still-present value -/

abbrev SetState := V → Bool

def setStep (s : SetState) (outs ins : List V) : SetState :=
  fun v => (s v && !outs.contains v) || ins.contains v

def setCount (keys : List V) (s : SetState) : Nat :=
  (keys.filter s).length

theorem set_deletion_loses_second_copy_counterexample :
    let v : V := some 7
    let keys : List V := [v]
    let s0 : SetState := fun w => w == v
    let s1 := setStep s0 [v] []
    setCount keys s1 = 0 ∧
    (presentKeys keys [v]).length = 1 ∧
    stepMap (fun w => mult w [v, v]) [v] [] v = 1 := by
  decide

theorem equal_values_are_one_distinct_class :
    let v : V := some 7
    (presentKeys [v] [v, v]).length = 1 ∧
    (presentKeys [none] [none, none]).length = 1 ∧
    (presentKeys [none, v] [none, v]).length = 2 := by
  decide

theorem empty_edge_frame :
    let rows : List V := [some 1, some 2]
    (frameVals rows 0 0 0).length = 0 ∧
    (frameVals rows 0 0 1).length = 0 := by
  decide

end TinylambOpt.Research.WindowDistinctInverse

-- C++ references to this model or its proof.
-- executor/detail/window_eval.cpp

import TinylambOpt.ResearchGoals.ExceptAllDebt

namespace TinylambOpt.ResearchProofs.ExceptAllDebt

open TinylambOpt.ResearchGoals.ExceptAllDebt

/-!
# Soundness of EXCEPT ALL debt cancellation across spill subtrees

For every row `k`, `reference p` contains `max 0 (l.count k - r.count k)` copies of
`k`, where `(l, r) = inputs p`: each right row erases at most one equal left row.
`balance p` stores the signed histogram `#l(k) - #r(k)` per key (which may be
negative at intermediate nodes) and `output p` materializes it through `Int.toNat`,
which truncates negatives to zero exactly like `max 0`.  Pointwise count equality
then yields `Perm` via `List.perm_iff_count`.
-/

/-! ## Small helpers -/

private theorem beq_eq_false_of_ne_true {b : Bool} (h : ¬(b = true)) : b = false := by
  cases b with
  | true => exact (h rfl).elim
  | false => rfl

/-- `Int.toNat` of the signed difference matches truncated `Nat` subtraction. -/
private theorem toNat_sub_cast (a b : Nat) :
    ((a : Int) - (b : Int)).toNat = max 0 (a - b) := by
  by_cases h : a ≤ b
  · have hn : ((a : Int) - (b : Int)) ≤ 0 := by omega
    rw [Int.toNat_of_nonpos hn, Nat.sub_eq_zero_of_le h]
    omega
  · have hab : b ≤ a := by omega
    have hnn : (0 : Int) ≤ (a : Int) - (b : Int) := by omega
    rw [← Int.ofNat_inj, Int.toNat_of_nonneg hnn,
      Nat.max_eq_right (Nat.zero_le (a - b)), Int.ofNat_sub hab]

/-! ## Reference side: `foldl List.erase` removes one occurrence per right row -/

private theorem count_foldl_erase (k : Row) :
    ∀ (l r : List Row), (List.foldl List.erase l r).count k =
      max 0 (l.count k - r.count k) := by
  intro l r
  induction r generalizing l with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons, ih, List.count_erase, List.count_cons]
      by_cases h : (x == k) = true <;> simp [h] <;> omega

private theorem reference_count (p : Part) (k : Row) :
    (reference p).count k = max 0 ((inputs p).1.count k - (inputs p).2.count k) := by
  show ((inputs p).2.foldl List.erase (inputs p).1).count k =
    max 0 ((inputs p).1.count k - (inputs p).2.count k)
  exact count_foldl_erase k (inputs p).1 (inputs p).2

/-! ## Keyed sums over association lists -/

/-- Sum of the values attached to key `k`. -/
private def keySum (k : Row) : List (Row × Int) → Int
  | [] => 0
  | (r, n) :: l => (if (r == k) = true then n else 0) + keySum k l

/-- Same, but each value is truncated to a `Nat` first (as `output` materializes it). -/
private def natKeySum (k : Row) : List (Row × Int) → Nat
  | [] => 0
  | (r, n) :: l => (if (r == k) = true then n.toNat else 0) + natKeySum k l

private theorem keySum_cons (k r : Row) (n : Int) (l : List (Row × Int)) :
    keySum k ((r, n) :: l) = (if (r == k) = true then n else 0) + keySum k l := rfl

private theorem natKeySum_cons (k r : Row) (n : Int) (l : List (Row × Int)) :
    natKeySum k ((r, n) :: l) = (if (r == k) = true then n.toNat else 0) + natKeySum k l := rfl

private theorem keySum_eq_zero (k : Row) :
    ∀ (l : List (Row × Int)), (∀ q ∈ l, (q.1 == k) = false) → keySum k l = 0 := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons x xs ih =>
      obtain ⟨r, n⟩ := x
      intro h
      rw [keySum_cons]
      have hr : ¬ ((r == k) = true) := by
        have := h (r, n) List.mem_cons_self
        simpa using this
      rw [if_neg hr, ih (fun q hq => h q (List.mem_cons_of_mem _ hq))]
      simp
private theorem natKeySum_eq_zero (k : Row) :
    ∀ (l : List (Row × Int)), (∀ q ∈ l, (q.1 == k) = false) → natKeySum k l = 0 := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons x xs ih =>
      obtain ⟨r, n⟩ := x
      intro h
      rw [natKeySum_cons]
      have hr : ¬ ((r == k) = true) := by
        have := h (r, n) List.mem_cons_self
        simpa using this
      rw [if_neg hr, ih (fun q hq => h q (List.mem_cons_of_mem _ hq))]

private theorem keySum_eq_of_mem (k : Row) (v : Int) :
    ∀ (l : List (Row × Int)),
      l.Pairwise (fun a b => (a.1 == b.1) = false) → (k, v) ∈ l → keySum k l = v := by
  intro l
  induction l with
  | nil => intro _ h; exact absurd h List.not_mem_nil
  | cons x xs ih =>
      obtain ⟨r, n⟩ := x
      intro hpw hmem
      rw [List.mem_cons] at hmem
      rw [keySum_cons]
      cases hmem with
      | inl heq =>
          have inj := Prod.mk.inj heq
          rw [← inj.1, ← inj.2]
          rw [if_pos (beq_self_eq_true k)]
          have hall : ∀ q ∈ xs, (q.1 == k) = false := by
            intro q hq
            have h1 := List.rel_of_pairwise_cons hpw hq
            rw [Bool.beq_comm, inj.1]
            simpa using h1
          rw [keySum_eq_zero k xs hall]
          omega
      | inr hmem' =>
          have hrk : ¬ ((r == k) = true) := by
            have := List.rel_of_pairwise_cons hpw hmem'
            simpa using this
          rw [if_neg hrk, Int.zero_add]
          exact ih (List.Pairwise.of_cons hpw) hmem'

private theorem natKeySum_eq_of_mem (k : Row) (v : Int) :
    ∀ (l : List (Row × Int)),
      l.Pairwise (fun a b => (a.1 == b.1) = false) → (k, v) ∈ l → natKeySum k l = v.toNat := by
  intro l
  induction l with
  | nil => intro _ h; exact absurd h List.not_mem_nil
  | cons x xs ih =>
      obtain ⟨r, n⟩ := x
      intro hpw hmem
      rw [List.mem_cons] at hmem
      rw [natKeySum_cons]
      cases hmem with
      | inl heq =>
          have inj := Prod.mk.inj heq
          rw [← inj.1, ← inj.2]
          rw [if_pos (beq_self_eq_true k)]
          have hall : ∀ q ∈ xs, (q.1 == k) = false := by
            intro q hq
            have h1 := List.rel_of_pairwise_cons hpw hq
            rw [Bool.beq_comm, inj.1]
            simpa using h1
          rw [natKeySum_eq_zero k xs hall]
          omega
      | inr hmem' =>
          have hrk : ¬ ((r == k) = true) := by
            have := List.rel_of_pairwise_cons hpw hmem'
            simpa using this
          rw [if_neg hrk, Nat.zero_add]
          exact ih (List.Pairwise.of_cons hpw) hmem'

/-- No key equal to `k` occurs among the entries of `m`. -/
private theorem not_beq_mem_toList (m : Counts) (k : Row)
    (hc : ¬(m.contains k = true)) : ∀ q ∈ m.toList, (q.1 == k) = false := by
  have hc' : m.contains k = false := beq_eq_false_of_ne_true hc
  intro q hq
  by_cases hqk : (q.1 == k) = true
  · have hqke : q.1 = k := beq_iff_eq.mp hqk
    have hsome : m[k]? = some q.2 := by
      have h := (Std.HashMap.mem_toList_iff_getElem?_eq_some (α := Row)).mp hq
      rw [hqke] at h
      exact h
    have hnone := Std.HashMap.getElem?_eq_none_of_contains_eq_false hc'
    rw [hnone] at hsome
    cases hsome
  · exact beq_eq_false_of_ne_true hqk

/-- The keyed sum over a map's entry list recovers `getD`. -/
private theorem keySum_toList (m : Counts) (k : Row) : keySum k m.toList = m.getD k 0 := by
  by_cases hc : m.contains k = true
  · have hsome : m[k]? = some (m.getD k 0) :=
      Std.HashMap.getElem?_eq_some_getD_of_contains (fallback := (0 : Int)) hc
    have hmem : (k, m.getD k 0) ∈ m.toList :=
      (Std.HashMap.mem_toList_iff_getElem?_eq_some (α := Row)).mpr hsome
    exact keySum_eq_of_mem k (m.getD k 0) m.toList Std.HashMap.distinct_keys_toList hmem
  · rw [keySum_eq_zero k m.toList (not_beq_mem_toList m k hc)]
    exact (Std.HashMap.getD_eq_fallback_of_contains_eq_false (fallback := (0 : Int))
      (beq_eq_false_of_ne_true hc)).symm

private theorem natKeySum_toList (m : Counts) (k : Row) :
    natKeySum k m.toList = (m.getD k 0).toNat := by
  by_cases hc : m.contains k = true
  · have hsome : m[k]? = some (m.getD k 0) :=
      Std.HashMap.getElem?_eq_some_getD_of_contains (fallback := (0 : Int)) hc
    have hmem : (k, m.getD k 0) ∈ m.toList :=
      (Std.HashMap.mem_toList_iff_getElem?_eq_some (α := Row)).mpr hsome
    exact natKeySum_eq_of_mem k (m.getD k 0) m.toList Std.HashMap.distinct_keys_toList hmem
  · rw [natKeySum_eq_zero k m.toList (not_beq_mem_toList m k hc)]
    rw [Std.HashMap.getD_eq_fallback_of_contains_eq_false (fallback := (0 : Int))
      (beq_eq_false_of_ne_true hc)]
    simp

/-! ## Output side -/

private theorem count_flatMap_replicate (k : Row) :
    ∀ (l : List (Row × Int)),
      (List.flatMap (fun (r, n) => List.replicate n.toNat r) l).count k = natKeySum k l := by
  intro l
  induction l with
  | nil => simp [natKeySum]
  | cons x xs ih =>
      obtain ⟨r, n⟩ := x
      rw [List.flatMap_cons, List.count_append, ih, natKeySum_cons, List.count_replicate]

private theorem count_entries (m : Counts) (k : Row) :
    (m.toList.flatMap fun (r, n) => List.replicate n.toNat r).count k = (m.getD k 0).toNat := by
  rw [count_flatMap_replicate, natKeySum_toList]

/-! ## Balance side -/

private theorem countRows_fold_getD (delta : Int) :
    ∀ (xs : List Row) (m : Counts) (k : Row),
      (List.foldl (fun m r => m.insert r (m.getD r 0 + delta)) m xs).getD k 0 =
        m.getD k 0 + delta * (xs.count k : Int) := by
  intro xs
  induction xs with
  | nil => intro m k; simp
  | cons x xs ih =>
      intro m k
      rw [List.foldl_cons, ih, Std.HashMap.getD_insert]
      by_cases hx : (x == k) = true
      · rw [List.count_cons, if_pos hx, if_pos hx]
        have hx' : x = k := by simpa using hx
        subst hx'
        rw [← Int.ofNat_add_ofNat, Int.ofNat_one, Int.mul_add, Int.mul_one]
        omega
      · rw [List.count_cons, if_neg hx, if_neg hx, Nat.add_zero]

private theorem getD_foldl_entries (k : Row) :
    ∀ (l : List (Row × Int)) (m : Counts),
      (List.foldl (fun acc p => acc.insert p.1 (acc.getD p.1 0 + p.2)) m l).getD k 0 =
        m.getD k 0 + keySum k l := by
  intro l
  induction l with
  | nil => intro m; simp [keySum]
  | cons x l ih =>
      obtain ⟨r, n⟩ := x
      intro m
      rw [List.foldl_cons]
      show (List.foldl (fun acc p => acc.insert p.1 (acc.getD p.1 0 + p.2))
          (m.insert r (m.getD r 0 + n)) l).getD k 0 =
        m.getD k 0 + ((if (r == k) = true then n else 0) + keySum k l)
      rw [ih, Std.HashMap.getD_insert]
      by_cases h : (r == k) = true
      · have hrk : r = k := by simpa using h
        rw [if_pos h, if_pos h, ← hrk]
        omega
      · rw [if_neg h, if_neg h]
        omega

private theorem getD_merge_fold (m₁ m₂ : Counts) (k : Row) :
    (m₂.fold (fun m x v => m.insert x (m.getD x 0 + v)) m₁).getD k 0 =
      m₁.getD k 0 + m₂.getD k 0 := by
  -- `fold` is a left fold over `m₂.toList`.
  rw [Std.HashMap.fold_eq_foldl_toList]
  show (List.foldl (fun acc p => acc.insert p.1 (acc.getD p.1 0 + p.2)) m₁ m₂.toList).getD k 0 =
    m₁.getD k 0 + m₂.getD k 0
  rw [getD_foldl_entries, keySum_toList]

private theorem inputs_leftRows (xs : List Row) : inputs (Part.leftRows xs) = (xs, []) := rfl

private theorem inputs_rightRows (xs : List Row) : inputs (Part.rightRows xs) = ([], xs) := rfl

private theorem inputs_merge (a b : Part) :
    inputs (Part.merge a b) =
      ((inputs a).1 ++ (inputs b).1, (inputs a).2 ++ (inputs b).2) := by
  show (match inputs a with
    | (l, r) => match inputs b with
      | (u, v) => (l ++ u, r ++ v)) =
    ((inputs a).1 ++ (inputs b).1, (inputs a).2 ++ (inputs b).2)
  cases h : inputs a with
  | mk l r =>
      cases h' : inputs b with
      | mk u v => rfl

private theorem balance_getD (p : Part) (k : Row) :
    (balance p).getD k 0 = ((inputs p).1.count k : Int) - ((inputs p).2.count k : Int) := by
  induction p with
  | leftRows xs =>
      show (countRows xs 1).getD k 0 =
        ((inputs (Part.leftRows xs)).1.count k : Int) -
          ((inputs (Part.leftRows xs)).2.count k : Int)
      simp only [countRows]
      rw [countRows_fold_getD (delta := (1 : Int)), Std.HashMap.getD_empty, inputs_leftRows]
      simp only [List.count_nil, Int.ofNat_zero]
      omega
  | rightRows xs =>
      show (countRows xs (-1)).getD k 0 =
        ((inputs (Part.rightRows xs)).1.count k : Int) -
          ((inputs (Part.rightRows xs)).2.count k : Int)
      simp only [countRows]
      rw [countRows_fold_getD (delta := (-1 : Int)), Std.HashMap.getD_empty, inputs_rightRows]
      simp only [List.count_nil, Int.ofNat_zero]
      omega
  | merge a b ih₁ ih₂ =>
      show ((balance b).fold (fun m x v => m.insert x (m.getD x 0 + v)) (balance a)).getD k 0 =
        ((inputs (Part.merge a b)).1.count k : Int) -
          ((inputs (Part.merge a b)).2.count k : Int)
      rw [getD_merge_fold, ih₁, ih₂]
      simp only [inputs_merge, List.count_append, ← Int.ofNat_add_ofNat]
      omega

private theorem output_count (p : Part) (k : Row) :
    (output p).count k =
      (((inputs p).1.count k : Int) - ((inputs p).2.count k : Int)).toNat := by
  show ((balance p).toList.flatMap fun (r, n) => List.replicate n.toNat r).count k =
    (((inputs p).1.count k : Int) - ((inputs p).2.count k : Int)).toNat
  rw [count_entries, balance_getD]

/-! ## Main theorem -/

theorem sound : TinylambOpt.ResearchGoals.ExceptAllDebt.Goal := by
  intro p
  rw [List.perm_iff_count]
  intro k
  rw [output_count, reference_count, toNat_sub_cast]

end TinylambOpt.ResearchProofs.ExceptAllDebt

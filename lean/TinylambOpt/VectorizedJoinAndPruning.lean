/-!
Relational specification of IEEE-754 addition on value classes. Finite + Finite may stay
finite or overflow to ±inf. Any non-finite operand produces a non-finite result.

Main Theorem 1: If a left-to-right IEEE-754 sum starting from a finite accumulator (such
as 0.0) produces a finite result, then every summand in the block was finite (in
particular, neither +inf nor -inf).

Named results include `fadd_result_finite_implies_operands_finite`,
`fadd_nonfinite_left`, `fsum_nonfinite_acc`.
-/

/-
  TinylambOpt/VectorizedJoinAndPruning.lean

  Formal proofs for TPC-H SF=10 vectorized columnar execution, block-local
  dense accumulation, IEEE-754 finite-sum infinity provenance, and O(N)
  single-pass argmax (LIMIT 1 WITH TIES).
-/

namespace TinylambOpt.VectorizedJoinAndPruning

/-!
  ### 1. IEEE-754 Finite-Sum Infinity Provenance
-/

inductive FClass where
  | finite
  | posInf
  | negInf
  | nan
  deriving DecidableEq, Repr

/-- Relational specification of IEEE-754 addition on value classes.
    Finite + Finite may stay finite or overflow to ±inf.
    Any non-finite operand produces a non-finite result. -/
def FAddClass : FClass → FClass → FClass → Prop
  | .finite, .finite, .finite => True
  | .finite, .finite, .posInf => True
  | .finite, .finite, .negInf => True
  | .posInf, .finite, .posInf => True
  | .finite, .posInf, .posInf => True
  | .posInf, .posInf, .posInf => True
  | .negInf, .finite, .negInf => True
  | .finite, .negInf, .negInf => True
  | .negInf, .negInf, .negInf => True
  | .posInf, .negInf, .nan    => True
  | .negInf, .posInf, .nan    => True
  | .nan,    _,       .nan    => True
  | _,       .nan,    .nan    => True
  | _,       _,       _       => False

theorem fadd_result_finite_implies_operands_finite {a b : FClass}
    (h : FAddClass a b .finite) : a = .finite ∧ b = .finite := by
  cases a <;> cases b <;> simp [FAddClass] at h ⊢

inductive FSumClass : FClass → List FClass → FClass → Prop
  | nil (acc : FClass) : FSumClass acc [] acc
  | cons {acc x mid out : FClass} {xs : List FClass}
      (hstep : FAddClass acc x mid)
      (hrest : FSumClass mid xs out) :
      FSumClass acc (x :: xs) out

theorem fadd_nonfinite_left {a b c : FClass} (ha : a ≠ .finite)
    (h : FAddClass a b c) : c ≠ .finite := by
  intro hc
  subst hc
  exact ha (fadd_result_finite_implies_operands_finite h).1

theorem fsum_nonfinite_acc {acc out : FClass} {xs : List FClass}
    (hacc : acc ≠ .finite) (h : FSumClass acc xs out) : out ≠ .finite := by
  induction h with
  | nil _ => exact hacc
  | cons hstep _ ih => exact ih (fadd_nonfinite_left hacc hstep)

/-- Main Theorem 1: If a left-to-right IEEE-754 sum starting from a finite
    accumulator (such as 0.0) produces a finite result, then every summand
    in the block was finite (in particular, neither +inf nor -inf). -/
theorem finite_sum_implies_no_inf {acc : FClass} {xs : List FClass}
    (h : FSumClass acc xs .finite) :
    acc = .finite ∧ ∀ x ∈ xs, x = .finite := by
  induction xs generalizing acc with
  | nil =>
    cases h
    exact ⟨rfl, by intro _ hx; cases hx⟩
  | cons x xs ih =>
    cases h with
    | cons hstep hrest =>
      have ⟨hmid, hall⟩ := ih hrest
      subst hmid
      have ⟨hacc, hx⟩ := fadd_result_finite_implies_operands_finite hstep
      refine ⟨hacc, ?_⟩
      intro y hy
      cases hy with
      | head => exact hx
      | tail _ hmem => exact hall y hmem

/-!
  ### 2. Column-at-a-Time Conjunctive Filter & Range Fusion Equivalence
-/

def evalConj {α : Type} (preds : List (α → Bool)) (x : α) : Bool :=
  preds.all (fun p => p x)

def filterColumnAtATime {α : Type} (rows : List α) (preds : List (α → Bool)) : List α :=
  preds.foldl (fun acc p => acc.filter p) rows

theorem filterColumnAtATime_eq_row_at_a_time {α : Type}
    (rows : List α) (preds : List (α → Bool)) :
    filterColumnAtATime rows preds = rows.filter (evalConj preds) := by
  induction preds generalizing rows with
  | nil =>
    unfold filterColumnAtATime evalConj
    simp only [List.foldl_nil, List.all_nil]
    induction rows with
    | nil => rfl
    | cons r rs ih =>
      rw [List.filter_cons]
      exact congrArg (r :: ·) ih
  | cons p ps ih =>
    unfold filterColumnAtATime
    rw [List.foldl_cons]
    change filterColumnAtATime (rows.filter p) ps = rows.filter (evalConj (p :: ps))
    rw [ih, List.filter_filter]
    congr 1
    funext x
    unfold evalConj
    simp [Bool.and_comm]

theorem filter_skip_always_true {α : Type} (rows : List α) (p : α → Bool)
    (htrue : ∀ r ∈ rows, p r = true) :
    rows.filter p = rows := by
  induction rows with
  | nil => rfl
  | cons r rs ih =>
    simp [htrue r (List.mem_cons_self ..), ih (fun x hx => htrue x (List.mem_cons_of_mem _ hx))]

theorem range_fusion_filter_eq {α : Type} (rows : List α) (p1 p2 : α → Bool) :
    (rows.filter p1).filter p2 = rows.filter (fun r => p1 r && p2 r) := by
  rw [List.filter_filter]
  congr 1
  funext r
  exact Bool.and_comm (p2 r) (p1 r)

/-!
  ### 3. Dense Block-Local Group Accumulation & Non-Null AVG/COUNT Sharing
-/

def foldIntoGlobal {α β : Type} {G : Nat} (add : β → β → β)
    (gid : α → Fin G) (val : α → β)
    (global : Fin G → β) (rows : List α) : Fin G → β :=
  rows.foldl (fun tbl r =>
    fun g => if g = gid r then add (tbl g) (val r) else tbl g) global

theorem foldIntoGlobal_at {α β : Type} {G : Nat} (add : β → β → β)
    (gid : α → Fin G) (val : α → β)
    (global : Fin G → β) (rows : List α) (g : Fin G) :
    foldIntoGlobal add gid val global rows g =
      (rows.filter (fun r => decide (g = gid r))).foldl (fun acc r => add acc (val r)) (global g) := by
  induction rows generalizing global with
  | nil => rfl
  | cons r rs ih =>
    unfold foldIntoGlobal
    rw [List.foldl_cons]
    change foldIntoGlobal add gid val
      (fun g' => if g' = gid r then add (global g') (val r) else global g') rs g = _
    rw [ih, List.filter_cons]
    by_cases h : g = gid r
    · simp [h]
    · simp [h]

/-- For an associative binary operation with identity `zero`, folding into
    a zero-initialized block-local array and merging `add (global g) (local g)`
    at the end of the block equals direct row-by-row folding into `global`. -/
theorem dense_group_block_accum_monoid_eq {α β : Type} {G : Nat}
    (zero : β) (add : β → β → β)
    (hzero_r : ∀ x, add x zero = x)
    (hassoc : ∀ x y z, add (add x y) z = add x (add y z))
    (gid : α → Fin G) (val : α → β)
    (global : Fin G → β) (rows : List α) (g : Fin G) :
    add (global g) (foldIntoGlobal add gid val (fun _ => zero) rows g) =
      foldIntoGlobal add gid val global rows g := by
  rw [foldIntoGlobal_at, foldIntoGlobal_at]
  generalize rows.filter (fun r => decide (g = gid r)) = filtered
  have hshift : ∀ (a b : β) (l : List α),
      add a (l.foldl (fun acc r => add acc (val r)) b) =
      l.foldl (fun acc r => add acc (val r)) (add a b) := by
    intro a b l
    induction l generalizing b with
    | nil => rfl
    | cons r rs ih =>
      simp only [List.foldl_cons]
      rw [ih, hassoc]
  rw [hshift, hzero_r]

/-- Non-null column AVG/COUNT sharing: when a column has no NULLs,
    `COUNT(col)` over a group equals `COUNT(*)` over that group. -/
theorem nonnull_avg_count_eq_count_star {α : Type}
    (rows : List α) (isNull : α → Bool)
    (hno_null : ∀ r ∈ rows, isNull r = false) :
    (rows.filter (fun r => !isNull r)).length = rows.length := by
  have h : rows.filter (fun r => !isNull r) = rows :=
    filter_skip_always_true rows (fun r => !isNull r) (fun r hr => by simp [hno_null r hr])
  rw [h]

/-!
  ### 4. O(N) Single-Pass Argmax for `ORDER BY key DESC LIMIT 1 WITH TIES`
-/

def singlePassArgmax {α : Type} (key : α → Int) (rows : List α) : List α :=
  let rec go (best : Option Int) (acc : List α) : List α → List α
    | [] => acc.reverse
    | r :: rs =>
      let k := key r
      match best with
      | none => go (some k) [r] rs
      | some b =>
        if k > b then go (some k) [r] rs
        else if k == b then go (some b) (r :: acc) rs
        else go (some b) acc rs
  go none [] rows

def maxKey? {α : Type} (key : α → Int) : List α → Option Int
  | [] => none
  | r :: rs =>
    match maxKey? key rs with
    | none => some (key r)
    | some m => some (max (key r) m)

def filterMaxRows {α : Type} (key : α → Int) (rows : List α) : List α :=
  match maxKey? key rows with
  | none => []
  | some m => rows.filter (fun r => key r == m)

private theorem maxKey?_none_iff {α : Type} (key : α → Int) (rs : List α) :
    maxKey? key rs = none ↔ rs = [] := by
  cases rs with
  | nil => simp [maxKey?]
  | cons x xs =>
    simp only [maxKey?]
    cases maxKey? key xs <;> simp

private theorem maxKey?_ge {α : Type} (key : α → Int) (rs : List α) (m : Int)
    (h : maxKey? key rs = some m) : ∀ x ∈ rs, key x ≤ m := by
  induction rs generalizing m with
  | nil => simp [maxKey?] at h
  | cons y ys ih =>
    simp only [maxKey?] at h
    cases hy : maxKey? key ys with
    | none =>
      rw [hy] at h
      injection h with hm
      have hys := (maxKey?_none_iff key ys).mp hy
      subst hys
      intro x hx
      simp at hx
      subst hx
      omega
    | some m2 =>
      rw [hy] at h
      injection h with hm
      intro x hx
      cases hx with
      | head => omega
      | tail _ hmem =>
        have := ih m2 hy x hmem
        omega

private theorem filter_gt_max_eq_nil {α : Type} (key : α → Int) (rs : List α) (m target : Int)
    (h : maxKey? key rs = some m) (hlt : m < target) :
    rs.filter (fun x => key x == target) = [] := by
  have hle := maxKey?_ge key rs m h
  have haux : ∀ (l : List α), (∀ x ∈ l, key x ≤ m) → l.filter (fun x => key x == target) = [] := by
    intro l hl
    induction l with
    | nil => rfl
    | cons y ys ih =>
      have hy : key y ≤ m := hl y (List.mem_cons_self ..)
      have hne : (key y == target) = false := by
        simp only [beq_eq_false_iff_ne]; omega
      rw [List.filter_cons, hne]
      exact ih (fun z hz => hl z (List.mem_cons_of_mem _ hz))
  exact haux rs hle

private theorem singlePassArgmax_go_spec {α : Type} (key : α → Int)
    (b : Int) (acc : List α) (rs : List α)
    (hacc : ∀ x ∈ acc, key x = b) :
    singlePassArgmax.go key (some b) acc rs =
      match maxKey? key rs with
      | none => acc.reverse
      | some m =>
        if m > b then rs.filter (fun r => key r == m)
        else if m == b then acc.reverse ++ rs.filter (fun r => key r == b)
        else acc.reverse := by
  induction rs generalizing b acc with
  | nil =>
    simp [singlePassArgmax.go, maxKey?]
  | cons r rs ih =>
    simp only [singlePassArgmax.go, maxKey?]
    cases hrest : maxKey? key rs with
    | none =>
      have hrs := (maxKey?_none_iff key rs).mp hrest
      subst hrs
      simp only [singlePassArgmax.go, List.filter_cons, List.filter_nil]
      by_cases hgt : key r > b
      · simp [hgt]
      · by_cases heq : key r == b
        · simp [hgt, heq]
        · simp [hgt, heq]
    | some m =>
      have hgt_or : key r > b ∨ key r = b ∨ key r < b := by omega
      rcases hgt_or with hgt | heq | hlt
      · have ih1 := ih (key r) [r] (by intro x hx; simp at hx; subst hx; rfl)
        rw [hrest] at ih1
        simp only [hgt, ↓reduceIte]
        rw [ih1]
        have hmax_gt : max (key r) m > b := by omega
        simp only [hmax_gt, ↓reduceIte, List.filter_cons]
        by_cases hm : m > key r
        · have hmax : max (key r) m = m := by omega
          have hne : (key r == m) = false := by
            simp only [beq_eq_false_iff_ne]; omega
          simp [hm, hmax, hne]
        · by_cases hmeq : m == key r
          · have hmeq' : m = key r := eq_of_beq hmeq
            have hmax : max (key r) m = key r := by omega
            simp [hm, hmeq, hmax, hmeq']
          · have hmax : max (key r) m = key r := by omega
            have hmlt : m < key r := by
              have hne : m ≠ key r := by intro h; exact hmeq (beq_iff_eq.mpr h)
              omega
            have hfilt := filter_gt_max_eq_nil key rs m (key r) hrest hmlt
            simp [hm, hmeq, hmax, hfilt]
      · have hgt_f : ¬(key r > b) := by omega
        have heq_t : (key r == b) = true := beq_iff_eq.mpr heq
        have ih1 := ih b (r :: acc) (by
          intro x hx
          cases hx with
          | head => exact heq
          | tail _ hmem => exact hacc x hmem)
        rw [hrest] at ih1
        simp only [hgt_f, ↓reduceIte, heq_t]
        rw [ih1]
        by_cases hm : m > b
        · have hmax : max (key r) m = m := by omega
          have hne : (key r == m) = false := by
            simp only [beq_eq_false_iff_ne]; omega
          simp [hm, hmax, hne]
        · by_cases hmeq : m == b
          · have hmeq' : m = b := eq_of_beq hmeq
            have hmax : max (key r) m = b := by omega
            have hgt_b : ¬(b > b) := by omega
            simp [hm, hmeq, hmax, hgt_b, heq_t, List.reverse_cons, List.append_assoc]
          · have hmax : max (key r) m = b := by omega
            have hgt_b : ¬(b > b) := by omega
            have hmlt : m < b := by
              have hne : m ≠ b := by intro h; exact hmeq (beq_iff_eq.mpr h)
              omega
            have hfilt := filter_gt_max_eq_nil key rs m b hrest hmlt
            simp [hm, hmeq, hmax, hgt_b, heq_t, hfilt, List.reverse_cons]
      · have hgt_f : ¬(key r > b) := by omega
        have heq_f : (key r == b) = false := by
          simp only [beq_eq_false_iff_ne]; omega
        have ih1 := ih b acc hacc
        rw [hrest] at ih1
        simp only [hgt_f, ↓reduceIte, heq_f, Bool.false_eq_true]
        rw [ih1]
        by_cases hm : m > b
        · have hmax : max (key r) m = m := by omega
          have hne : (key r == m) = false := by
            simp only [beq_eq_false_iff_ne]; omega
          simp [hm, hmax, hne]
        · by_cases hmeq : m == b
          · have hmeq' : m = b := eq_of_beq hmeq
            have hmax : max (key r) m = b := by omega
            have hgt_b : ¬(b > b) := by omega
            simp [hm, hmeq, hmax, hgt_b, heq_f]
          · have hmax_lt : max (key r) m < b := by
              have hne : m ≠ b := by intro h; exact hmeq (beq_iff_eq.mpr h)
              omega
            have hmax_gt_f : ¬(max (key r) m > b) := by omega
            have hmax_eq_f : (max (key r) m == b) = false := by
              simp only [beq_eq_false_iff_ne]; omega
            simp [hm, hmeq, hmax_gt_f, hmax_eq_f]

/-- Main Theorem 4: O(N) single-pass argmax returns the exact same list of
    maximum-key rows (with all ties in original order) as `filterMaxRows`. -/
theorem single_pass_argmax_eq_filterMaxRows {α : Type} (key : α → Int) (rows : List α) :
    singlePassArgmax key rows = filterMaxRows key rows := by
  cases rows with
  | nil => rfl
  | cons r rs =>
    simp only [singlePassArgmax, singlePassArgmax.go, filterMaxRows, maxKey?]
    rw [singlePassArgmax_go_spec key (key r) [r] rs (by intro x hx; simp at hx; subst hx; rfl)]
    cases hrest : maxKey? key rs with
    | none =>
      have hrs := (maxKey?_none_iff key rs).mp hrest
      subst hrs
      simp
    | some m =>
      by_cases hm : m > key r
      · have hmax : max (key r) m = m := by omega
        have hne : (key r == m) = false := by
          simp only [beq_eq_false_iff_ne]; omega
        simp [hm, hmax, hne]
      · by_cases hmeq : m == key r
        · have hmeq' : m = key r := eq_of_beq hmeq
          have hmax : max (key r) m = key r := by omega
          simp [hmeq', hmax]
        · have hmax : max (key r) m = key r := by omega
          have hmlt : m < key r := by
            have hne : m ≠ key r := by intro h; exact hmeq (beq_iff_eq.mpr h)
            omega
          have hfilt := filter_gt_max_eq_nil key rs m (key r) hrest hmlt
          simp [hm, hmeq, hmax, hfilt]

/-!
  ### 5. Direct-Indexed Bounded Integer Key Aggregation

  When an integer group key `k` is certified to satisfy `lo ≤ k ≤ hi` across
  the table (from TLCS global column min/max), mapping `k ↦ (k - lo)` into a
  dense array of size `span = (hi - lo + 1)` is injective and preserves exact
  per-key group equality and parallel worker-table summation.
-/

theorem direct_int_key_injective (lo : Int) (k1 k2 : Int)
    (h1 : lo ≤ k1) (h2 : lo ≤ k2)
    (heq : (k1 - lo).toNat = (k2 - lo).toNat) : k1 = k2 := by
  omega

theorem direct_int_key_in_bounds (lo hi k : Int)
    (hlo : lo ≤ k) (hhi : k ≤ hi) :
    (k - lo).toNat < (hi - lo + 1).toNat := by
  omega

/-- Parallel worker partition merge: folding a list partitioned into two
    disjoint worker slices `xs ++ ys` into a direct-indexed monoid array
    equals merging the two worker arrays element-wise. -/
theorem parallel_direct_array_merge_eq {α β : Type} {G : Nat}
    (zero : β) (add : β → β → β)
    (hzero_r : ∀ x, add x zero = x)
    (hassoc : ∀ x y z, add (add x y) z = add x (add y z))
    (gid : α → Fin G) (val : α → β)
    (xs ys : List α) (g : Fin G) :
    add (foldIntoGlobal add gid val (fun _ => zero) xs g)
        (foldIntoGlobal add gid val (fun _ => zero) ys g) =
      foldIntoGlobal add gid val (fun _ => zero) (xs ++ ys) g := by
  rw [foldIntoGlobal_at, foldIntoGlobal_at, foldIntoGlobal_at]
  rw [List.filter_append, List.foldl_append]
  have hshift : ∀ (a b : β) (l : List α),
      add a (l.foldl (fun acc r => add acc (val r)) b) =
      l.foldl (fun acc r => add acc (val r)) (add a b) := by
    intro a b l
    induction l generalizing b with
    | nil => rfl
    | cons r rs ih =>
      simp only [List.foldl_cons]
      rw [ih, hassoc]
  rw [hshift, hzero_r]

/-!
  ### 6. Parallel Local Bitset Union (Bitwise OR) for Dense Integer Sets

  For a bounded integer domain `[lo, hi]`, partitioning input keys `xs ++ ys`
  across workers where each worker sets bits in a thread-local bitset and
  then merges via bitwise OR (`||`) produces the exact same membership
  predicate as inserting every key into a single bitset.
-/

def bitsetHas {G : Nat} (gid : α → Fin G) (xs : List α) (g : Fin G) : Bool :=
  xs.foldl (fun acc r => acc || decide (g = gid r)) false

theorem parallel_bitset_or_eq_union {α : Type} {G : Nat}
    (gid : α → Fin G) (xs ys : List α) (g : Fin G) :
    (bitsetHas gid xs g || bitsetHas gid ys g) = bitsetHas gid (xs ++ ys) g := by
  unfold bitsetHas
  rw [List.foldl_append]
  have hshift : ∀ (a b : Bool) (l : List α),
      (a || l.foldl (fun acc r => acc || decide (g = gid r)) b) =
      l.foldl (fun acc r => acc || decide (g = gid r)) (a || b) := by
    intro a b l
    induction l generalizing b with
    | nil => rfl
    | cons r rs ih =>
      simp only [List.foldl_cons]
      rw [ih, Bool.or_assoc]
  rw [hshift, Bool.or_false]

/-!
  ### 7. Greedy Leftmost Match Soundness for Two-Segment `LIKE '%a%b%'`

  For wildcard-free literals `a` and `b`, `s LIKE '%a%b%'` holds iff `s`
  contains an occurrence of `a` at some offset `i` and an occurrence of `b`
  at some offset `j ≥ i + |a|`.
  If `i₀` is the *earliest* occurrence of `a` in `s` (so `i₀ ≤ i` for every
  occurrence `i` of `a`), then checking `b` starting at `i₀ + |a|` is both
  sound and complete — we never need to backtrack to later occurrences of `a`.
-/

def OccursAt (s pat : List UInt8) (pos : Nat) : Prop :=
  pat <+: s.drop pos

def HasTwoSegMatch (s a b : List UInt8) : Prop :=
  ∃ i j, OccursAt s a i ∧ OccursAt s b j ∧ i + a.length ≤ j

def GreedyTwoSegMatch (s a b : List UInt8) (i₀ : Nat) : Prop :=
  OccursAt s a i₀ ∧ (∀ i, OccursAt s a i → i₀ ≤ i) ∧
    ∃ j, OccursAt s b j ∧ i₀ + a.length ≤ j

theorem two_segment_like_greedy_earliest_iff (s a b : List UInt8) (i₀ : Nat)
    (hi₀ : OccursAt s a i₀) (hmin : ∀ i, OccursAt s a i → i₀ ≤ i) :
    HasTwoSegMatch s a b ↔ GreedyTwoSegMatch s a b i₀ := by
  constructor
  · rintro ⟨i, j, hai, hbj, hle⟩
    have hi₀_le_i : i₀ ≤ i := hmin i hai
    exact ⟨hi₀, hmin, j, hbj, by omega⟩
  · rintro ⟨_, _, j, hbj, hle⟩
    exact ⟨i₀, j, hi₀, hbj, hle⟩

theorem two_segment_like_no_first_literal (s a b : List UInt8)
    (hnone : ∀ i, ¬ OccursAt s a i) :
    ¬ HasTwoSegMatch s a b := by
  rintro ⟨i, _, hai, _, _⟩
  exact hnone i hai

/-!
  ### 8. `HAVING COUNT(DISTINCT x) > 1` ⟺ `MIN(x) < MAX(x)` and
         `HAVING COUNT(DISTINCT x) = 1` ⟺ `MIN(x) = MAX(x)`

  For any totally ordered non-NaN type (e.g. `Int` / `DATE`), over the list
  `xs` of non-NULL values in a group:
  - `xs` has ≥ 2 distinct values iff `min?(xs) < max?(xs)` (both `some`).
  - `xs` has exactly 1 distinct value iff `min?(xs) = max?(xs)` (both `some`).
  When `xs = []` (all-NULL group), both `min?(xs)` and `max?(xs)` are `none`
  (SQL `NULL`), so both `MIN(x) < MAX(x)` and `MIN(x) = MAX(x)` evaluate to
  SQL `UNKNOWN` and are rejected by `HAVING` — matching `0 > 1` and `0 = 1`
  being `FALSE`.
-/

def listMin? : List Int → Option Int
  | [] => none
  | x :: xs =>
    match listMin? xs with
    | none => some x
    | some m => some (min x m)

def listMax? : List Int → Option Int
  | [] => none
  | x :: xs =>
    match listMax? xs with
    | none => some x
    | some m => some (max x m)

private theorem listMin?_mem {xs : List Int} {lo : Int} (h : listMin? xs = some lo) :
    lo ∈ xs := by
  induction xs generalizing lo with
  | nil => simp [listMin?] at h
  | cons x xs ih =>
    simp only [listMin?] at h
    cases hxs : listMin? xs with
    | none =>
      rw [hxs] at h; injection h with hlo; subst hlo; exact List.mem_cons_self ..
    | some m =>
      rw [hxs] at h; injection h with hlo
      by_cases hle : x ≤ m
      · have : lo = x := by omega
        subst this; exact List.mem_cons_self ..
      · have : lo = m := by omega
        subst this; exact List.mem_cons_of_mem _ (ih hxs)

private theorem listMax?_mem {xs : List Int} {hi : Int} (h : listMax? xs = some hi) :
    hi ∈ xs := by
  induction xs generalizing hi with
  | nil => simp [listMax?] at h
  | cons x xs ih =>
    simp only [listMax?] at h
    cases hxs : listMax? xs with
    | none =>
      rw [hxs] at h; injection h with hhi; subst hhi; exact List.mem_cons_self ..
    | some m =>
      rw [hxs] at h; injection h with hhi
      by_cases hle : x ≥ m
      · have : hi = x := by omega
        subst this; exact List.mem_cons_self ..
      · have : hi = m := by omega
        subst this; exact List.mem_cons_of_mem _ (ih hxs)

private theorem listMin?_le {xs : List Int} {lo : Int} (h : listMin? xs = some lo) :
    ∀ x ∈ xs, lo ≤ x := by
  induction xs generalizing lo with
  | nil => simp [listMin?] at h
  | cons y ys ih =>
    simp only [listMin?] at h
    cases hys : listMin? ys with
    | none =>
      rw [hys] at h; injection h with hlo
      have hnil : ys = [] := by
        cases ys with
        | nil => rfl
        | cons z zs =>
          simp only [listMin?] at hys
          cases hzs : listMin? zs <;> rw [hzs] at hys <;> contradiction
      subst hnil
      intro x hx; simp at hx; omega
    | some m =>
      rw [hys] at h; injection h with hlo
      intro x hx
      cases hx with
      | head => omega
      | tail _ hmem => have := ih hys x hmem; omega

private theorem listMax?_ge {xs : List Int} {hi : Int} (h : listMax? xs = some hi) :
    ∀ x ∈ xs, x ≤ hi := by
  induction xs generalizing hi with
  | nil => simp [listMax?] at h
  | cons y ys ih =>
    simp only [listMax?] at h
    cases hys : listMax? ys with
    | none =>
      rw [hys] at h; injection h with hhi
      have hnil : ys = [] := by
        cases ys with
        | nil => rfl
        | cons z zs =>
          simp only [listMax?] at hys
          cases hzs : listMax? zs <;> rw [hzs] at hys <;> contradiction
      subst hnil
      intro x hx; simp at hx; omega
    | some m =>
      rw [hys] at h; injection h with hhi
      intro x hx
      cases hx with
      | head => omega
      | tail _ hmem => have := ih hys x hmem; omega

private theorem listMin?_some_of_ne_nil {xs : List Int} (h : xs ≠ []) :
    ∃ lo, listMin? xs = some lo := by
  cases xs with
  | nil => contradiction
  | cons x xs =>
    simp only [listMin?]
    cases listMin? xs with
    | none => exact ⟨x, rfl⟩
    | some m => exact ⟨min x m, rfl⟩

private theorem listMax?_some_of_ne_nil {xs : List Int} (h : xs ≠ []) :
    ∃ hi, listMax? xs = some hi := by
  cases xs with
  | nil => contradiction
  | cons x xs =>
    simp only [listMax?]
    cases listMax? xs with
    | none => exact ⟨x, rfl⟩
    | some m => exact ⟨max x m, rfl⟩

/-- `xs` contains at least two distinct values iff `min(xs) < max(xs)`. -/
theorem count_distinct_gt_one_iff_min_lt_max (xs : List Int) :
    (∃ a b, a ∈ xs ∧ b ∈ xs ∧ a ≠ b) ↔
    (∃ lo hi, listMin? xs = some lo ∧ listMax? xs = some hi ∧ lo < hi) := by
  constructor
  · rintro ⟨a, b, ha, hb, hne⟩
    have hne_nil : xs ≠ [] := by intro h; subst h; cases ha
    obtain ⟨lo, hlo⟩ := listMin?_some_of_ne_nil hne_nil
    obtain ⟨hi, hhi⟩ := listMax?_some_of_ne_nil hne_nil
    have ha1 := listMin?_le hlo a ha
    have ha2 := listMax?_ge hhi a ha
    have hb1 := listMin?_le hlo b hb
    have hb2 := listMax?_ge hhi b hb
    exact ⟨lo, hi, hlo, hhi, by omega⟩
  · rintro ⟨lo, hi, hlo, hhi, hlt⟩
    exact ⟨lo, hi, listMin?_mem hlo, listMax?_mem hhi, by omega⟩

/-- `xs` contains exactly one distinct value iff `min(xs) = max(xs)` (both `some`). -/
theorem count_distinct_eq_one_iff_min_eq_max (xs : List Int) :
    (xs ≠ [] ∧ ∀ a b, a ∈ xs → b ∈ xs → a = b) ↔
    (∃ lo hi, listMin? xs = some lo ∧ listMax? xs = some hi ∧ lo = hi) := by
  constructor
  · rintro ⟨hne_nil, hall⟩
    obtain ⟨lo, hlo⟩ := listMin?_some_of_ne_nil hne_nil
    obtain ⟨hi, hhi⟩ := listMax?_some_of_ne_nil hne_nil
    exact ⟨lo, hi, hlo, hhi, hall lo hi (listMin?_mem hlo) (listMax?_mem hhi)⟩
  · rintro ⟨lo, hi, hlo, hhi, heq⟩
    have hne_nil : xs ≠ [] := by intro h; subst h; simp [listMin?] at hlo
    refine ⟨hne_nil, ?_⟩
    intro a b ha hb
    have ha1 := listMin?_le hlo a ha
    have ha2 := listMax?_ge hhi a ha
    have hb1 := listMin?_le hlo b hb
    have hb2 := listMax?_ge hhi b hb
    omega

/-!
  ### 9. Single-Scan Shared-CTE Decorrelation for Unique-Dimension Self-Join Aggregates (Q17)

  When a fact table `F` joins with a dimension table `D` on `F.fk = D.pk` where
  `D.pk` is UNIQUE (at most one `d ∈ D` has `d.pk = k`), then for every `d ∈ D`:
  filtering the joined relation `F ⋈ D` by `F.fk = d.pk` produces the EXACT
  same multiset of `F` rows as filtering `F` directly by `F.fk = d.pk`.
  Consequently, any correlated aggregate over `F` keyed by `d.pk` can be
  evaluated over the much smaller materialized join `B = F ⋈ D` instead of
  scanning `F` a second time!
-/

def joinFactDim {F D K : Type} [DecidableEq K]
    (fk : F → K) (pk : D → K) (fs : List F) (ds : List D) : List (F × D) :=
  fs.flatMap (fun f => (ds.filter (fun d => pk d == fk f)).map (fun d => (f, d)))

theorem unique_dim_filter_singleton {D K : Type} [DecidableEq K]
    (pk : D → K) (ds : List D) (d₀ : D)
    (hmem : d₀ ∈ ds)
    (huniq : ∀ d₁ ∈ ds, ∀ d₂ ∈ ds, pk d₁ = pk d₂ → d₁ = d₂)
    (hnodup : ds.Nodup) :
    ds.filter (fun d => pk d == pk d₀) = [d₀] := by
  induction ds with
  | nil => cases hmem
  | cons d ds ih =>
    rw [List.nodup_cons] at hnodup
    rw [List.filter_cons]
    cases hmem with
    | head =>
      simp only [beq_self_eq_true, ↓reduceIte]
      have hrest : ds.filter (fun x => pk x == pk d₀) = [] := by
        have hne_all : ∀ x ∈ ds, pk x ≠ pk d₀ := by
          intro x hx heq
          have hxd : x = d₀ := huniq x (List.mem_cons_of_mem _ hx) d₀ (List.mem_cons_self ..) heq
          exact hnodup.1 (hxd ▸ hx)
        have haux : ∀ l : List D, (∀ x ∈ l, pk x ≠ pk d₀) → l.filter (fun x => pk x == pk d₀) = [] := by
          intro l hl
          induction l with
          | nil => rfl
          | cons y ys ihy =>
            have hbf : (pk y == pk d₀) = false := beq_eq_false_iff_ne.mpr (hl y (List.mem_cons_self ..))
            rw [List.filter_cons, hbf]
            exact ihy (fun z hz => hl z (List.mem_cons_of_mem _ hz))
        exact haux ds hne_all
      rw [hrest]
    | tail _ hmem_ds =>
      have hne : pk d ≠ pk d₀ := by
        intro heq
        have hdd0 : d = d₀ := huniq d (List.mem_cons_self ..) d₀ (List.mem_cons_of_mem _ hmem_ds) heq
        exact hnodup.1 (hdd0 ▸ hmem_ds)
      have hbf : (pk d == pk d₀) = false := beq_eq_false_iff_ne.mpr hne
      rw [hbf]
      exact ih hmem_ds
        (fun a ha b hb => huniq a (List.mem_cons_of_mem _ ha) b (List.mem_cons_of_mem _ hb))
        hnodup.2

/-- Main Theorem 9: Restricting `F ⋈ D` to `fk = pk d₀` (for `d₀ ∈ D` with
    unique `pk`) yields the exact same list of `F` rows as restricting `F`
    directly to `fk = pk d₀`. -/
theorem self_join_unique_dim_cte_decorrelate_sound {F D K : Type} [DecidableEq K]
    (fk : F → K) (pk : D → K) (fs : List F) (ds : List D) (d₀ : D)
    (hmem : d₀ ∈ ds)
    (huniq : ∀ d₁ ∈ ds, ∀ d₂ ∈ ds, pk d₁ = pk d₂ → d₁ = d₂)
    (hnodup : ds.Nodup) :
    ((joinFactDim fk pk fs ds).filter (fun p => fk p.1 == pk d₀)).map Prod.fst =
      fs.filter (fun f => fk f == pk d₀) := by
  induction fs with
  | nil => rfl
  | cons f fs ih =>
    unfold joinFactDim
    rw [List.flatMap_cons, List.filter_append, List.map_append, List.filter_cons]
    change (((ds.filter (fun d => pk d == fk f)).map (fun d => (f, d))).filter
              (fun p => fk p.1 == pk d₀)).map Prod.fst ++
            ((joinFactDim fk pk fs ds).filter (fun p => fk p.1 == pk d₀)).map Prod.fst =
           (if fk f == pk d₀ then f :: fs.filter (fun f => fk f == pk d₀)
            else fs.filter (fun f => fk f == pk d₀))
    rw [ih]
    by_cases heq : fk f == pk d₀
    · have hfk : fk f = pk d₀ := eq_of_beq heq
      rw [heq, hfk, unique_dim_filter_singleton pk ds d₀ hmem huniq hnodup]
      simp [heq]
    · have hbf : (fk f == pk d₀) = false := by
        cases h : (fk f == pk d₀)
        · rfl
        · contradiction
      have hnil : (((ds.filter (fun d => pk d == fk f)).map (fun d => (f, d))).filter
                    (fun p => fk p.1 == pk d₀)) = [] := by
        generalize ds.filter (fun d => pk d == fk f) = sub
        induction sub with
        | nil => rfl
        | cons x xs ihs => simp [hbf, ihs]
      rw [hnil, hbf]
      rfl

/-!
  ### 10. Parallel Independent Per-Group Sort & Deduplication (Q16)

  When distinct values for each group `g ∈ [0, G)` are stored in disjoint
  per-group buckets `bucket[g]`, deduplicating each bucket in parallel across
  independent group partitions produces the exact same per-group distinct count
  as sequential deduplication.
-/

theorem parallel_independent_group_dedup_eq {α : Type} [DecidableEq α]
    (f : List α → Nat) (buckets : List (List α)) (i : Nat) (h : i < buckets.length) :
    (buckets.map f)[i]'(by simpa using h) = f (buckets[i]'h) := by
  simp

/-!
  ### 11. Parallel Hash-Partitioned Group Pre-Assignment (Q10, Q16)

  When rows are hash-partitioned into `P` disjoint buckets `p = hash(key r) % P`
  and each partition `p` independently assigns local group IDs
  `localGid p : K → Fin (G_p)` with global offset `base p = ∑_{q < p} G_q`,
  the resulting global group ID `globalGid r = base (part r) + localGid (part r) (key r)`
  assigns two rows the same global group ID iff they have equal keys!
-/

theorem parallel_hash_partition_preassign_iff
    {K : Type} [DecidableEq K]
    (part : K → Nat)
    (count : Nat → Nat)
    (base : Nat → Nat)
    (hbase_mono : ∀ p q, p < q → base p + count p ≤ base q)
    (localGid : ∀ p, K → Nat)
    (hlocal_lt : ∀ k, localGid (part k) k < count (part k))
    (hlocal_inj : ∀ k1 k2, part k1 = part k2 →
      (localGid (part k1) k1 = localGid (part k2) k2 ↔ k1 = k2))
    (k1 k2 : K) :
    base (part k1) + localGid (part k1) k1 =
      base (part k2) + localGid (part k2) k2 ↔ k1 = k2 := by
  constructor
  · intro heq
    have hp : part k1 = part k2 := by
      by_cases hlt : part k1 < part k2
      · have h1 := hbase_mono (part k1) (part k2) hlt
        have h2 := hlocal_lt k1
        omega
      · by_cases hgt : part k2 < part k1
        · have h1 := hbase_mono (part k2) (part k1) hgt
          have h2 := hlocal_lt k2
          omega
        · omega
    have hloc : localGid (part k1) k1 = localGid (part k2) k2 := by
      have hbase_eq : base (part k1) = base (part k2) := congrArg base hp
      omega
    exact (hlocal_inj k1 k2 hp).mp hloc
  · intro hkeq
    subst hkeq
    rfl

/-!
  ### 12. Semi-Join Domain Pushdown into `IN` Subquery (Q20)

  For an outer relation `S` filtered by `P`, with an `IN` predicate
  `s.k ∈ {r.k | r ∈ R}` (three-valued SQL `IN`: `none` never matches,
  `some k` matches iff `some k ∈ R_keys`), restricting the inner subquery
  `R` to rows whose key lies in the filtered outer domain
  `D = {s.k | s ∈ S, P s}` yields the exact same output list.
-/

def sqlInNonNull {K : Type} [DecidableEq K] (k : Option K) (rs : List (Option K)) : Bool :=
  match k with
  | none => false
  | some v => rs.any (fun r => r == some v)

theorem semijoin_domain_pushdown_in_subquery_sound
    {S R K : Type} [DecidableEq K]
    (sKey : S → Option K) (rKey : R → Option K)
    (P : S → Bool) (outer : List S) (inner : List R) :
    let dom := (outer.filter P).map sKey
    let inner' := inner.filter (fun r => sqlInNonNull (rKey r) dom)
    outer.filter (fun s => P s && sqlInNonNull (sKey s) (inner.map rKey)) =
      outer.filter (fun s => P s && sqlInNonNull (sKey s) (inner'.map rKey)) := by
  intro dom inner'
  have hmem : ∀ s ∈ outer,
      (P s && sqlInNonNull (sKey s) (inner.map rKey)) =
      (P s && sqlInNonNull (sKey s) (inner'.map rKey)) := by
    intro s hs
    cases hPs : P s with
    | false => rfl
    | true =>
      simp only [Bool.true_and]
      cases hsk : sKey s with
      | none => rfl
      | some v =>
        simp only [sqlInNonNull]
        have hv_dom : some v ∈ dom := by
          simp only [dom, List.mem_map, List.mem_filter]
          exact ⟨s, ⟨hs, hPs⟩, hsk⟩
        have hv_in_dom : sqlInNonNull (some v) dom = true := by
          simp only [sqlInNonNull, List.any_eq_true]
          exact ⟨some v, hv_dom, beq_self_eq_true (some v)⟩
        apply Bool.eq_iff_iff.mpr
        simp only [List.any_eq_true, List.mem_map, beq_iff_eq, inner', List.mem_filter]
        constructor
        · rintro ⟨_, ⟨r, hr, rfl⟩, hrk⟩
          refine ⟨rKey r, ⟨r, ⟨hr, ?_⟩, rfl⟩, hrk⟩
          rw [hrk]
          exact hv_in_dom
        · rintro ⟨_, ⟨r, ⟨hr, _⟩, rfl⟩, hrk⟩
          exact ⟨rKey r, ⟨r, hr, rfl⟩, hrk⟩
  exact List.filter_congr hmem

/-!
  ### 13. Packed 64-Bit Composite Integer Key Injectivity (Q9)

  Packing two 32-bit integers `(a, b)` into `(a.toNat <<< 32) ||| b.toNat`
  is injective: two packed 64-bit keys are equal iff both 32-bit components
  are equal.
-/

def packU32PairNat (a b : UInt32) : Nat :=
  a.toNat * (2 ^ 32) + b.toNat

theorem packed_u64_pair_injective (a1 b1 a2 b2 : UInt32) :
    packU32PairNat a1 b1 = packU32PairNat a2 b2 ↔ a1 = a2 ∧ b1 = b2 := by
  constructor
  · intro h
    simp only [packU32PairNat] at h
    have hb1 : b1.toNat < 2 ^ 32 := b1.toBitVec.isLt
    have hb2 : b2.toNat < 2 ^ 32 := b2.toBitVec.isLt
    have ha : a1.toNat = a2.toNat := by omega
    have hb : b1.toNat = b2.toNat := by omega
    exact ⟨UInt32.eq_of_toBitVec_eq (BitVec.eq_of_toNat_eq ha),
           UInt32.eq_of_toBitVec_eq (BitVec.eq_of_toNat_eq hb)⟩
  · rintro ⟨rfl, rfl⟩
    rfl

/-!
  ### 14. Parallel Disjoint-Group Streaming Top-K Soundness (Q3)

  When each finished group belongs to a unique worker shard, any element
  whose global rank (number of strictly better elements across all shards)
  is `< k` also has local rank `< k` within its own shard. Therefore,
  keeping only the local top-`k` per worker and taking the global top-`k`
  of the union cannot drop any true global top-`k` element.
-/

def strictBetterCount {α : Type} (better : α → α → Bool) (xs : List α) (x : α) : Nat :=
  (xs.filter (fun y => better y x)).length

theorem strictBetterCount_append {α : Type} (better : α → α → Bool)
    (xs ys : List α) (x : α) :
    strictBetterCount better (xs ++ ys) x =
      strictBetterCount better xs x + strictBetterCount better ys x := by
  simp [strictBetterCount, List.filter_append]

theorem parallel_disjoint_topk_member_sound {α : Type} (better : α → α → Bool)
    (shard rest : List α) (k : Nat) (x : α)
    (hglobal : strictBetterCount better (shard ++ rest) x < k) :
    strictBetterCount better shard x < k := by
  rw [strictBetterCount_append] at hglobal
  omega

/-!
  ### 15. Sparse Direct-Gather Decode Commutativity (Q7, Q8, Q9)

  Decoding only selected row ordinals `sel` directly from a random-access
  encoded column (`kFor`, `kRawDouble`, `kNarrowDict`) yields the exact same
  values as decoding the full block and then gathering at `sel`.
-/

theorem gather_decode_commutes {α β : Type} (decode : α → β)
    (block : List α) (sel : List Nat) (hsel : ∀ i ∈ sel, i < block.length) :
    sel.attach.map (fun ⟨i, hi⟩ => decode (block[i]'(hsel i hi))) =
      sel.attach.map (fun ⟨i, hi⟩ => (block.map decode)[i]'(by simpa using hsel i hi)) := by
  apply List.map_congr_left
  intro ⟨i, hi⟩ _
  simp

/-!
  ### 16. Page-Level Integer Zone-Map Pruning Soundness (TPC-DS Q21/Q37/Q72/Q78/Q80/Q82)

  Given a table partitioned into pages `pages : List (List Row)`, an integer
  key extractor `key : Row → Option Int` (where `none` = SQL NULL, which never
  satisfies an equi-join runtime filter), and a per-page enclosure `[lo, hi]`
  covering every non-NULL key on the page (`lo ≤ v ≤ hi`), if a sound runtime
  filter rejects the entire interval `[lo, hi]` (or the page has no non-NULL
  keys at all, represented by `lo > hi`), then skipping the page before
  scanning preserves the exact output row sequence.
-/

def rfAcceptRow {Row : Type} (key : Row → Option Int) (rfReject : Int → Bool) (r : Row) : Bool :=
  match key r with
  | none => false
  | some v => !rfReject v

theorem page_zone_map_rf_prune_sound {Row : Type}
    (key : Row → Option Int)
    (rfReject : Int → Bool)
    (page : List Row)
    (lo hi : Int)
    (hbounds : ∀ r ∈ page, ∀ v, key r = some v → lo ≤ v ∧ v ≤ hi)
    (hprune : lo > hi ∨ ∀ v, lo ≤ v → v ≤ hi → rfReject v = true) :
    page.filter (rfAcceptRow key rfReject) = [] := by
  induction page with
  | nil => rfl
  | cons r rs ih =>
    rw [List.filter_cons]
    have hacc : rfAcceptRow key rfReject r = false := by
      unfold rfAcceptRow
      cases hk : key r with
      | none => rfl
      | some v =>
        have ⟨hlo, hhi⟩ := hbounds r (List.mem_cons_self ..) v hk
        cases hprune with
        | inl hlt => omega
        | inr hrej =>
          simp [hrej v hlo hhi]
    rw [hacc]
    exact ih (fun x hx v hkv => hbounds x (List.mem_cons_of_mem _ hx) v hkv)

theorem page_zone_map_list_prune_sound {Row : Type}
    (key : Row → Option Int)
    (rfReject : Int → Bool)
    (rejectsRange : Int → Int → Bool)
    (hrr : ∀ lo hi, rejectsRange lo hi = true →
      lo > hi ∨ ∀ v, lo ≤ v → v ≤ hi → rfReject v = true)
    (pages : List (List Row × (Int × Int)))
    (hbounds : ∀ p ∈ pages, ∀ r ∈ p.1, ∀ v, key r = some v → p.2.1 ≤ v ∧ v ≤ p.2.2) :
    (pages.filter (fun p => !rejectsRange p.2.1 p.2.2)).flatMap
      (fun p => p.1.filter (rfAcceptRow key rfReject)) =
    pages.flatMap (fun p => p.1.filter (rfAcceptRow key rfReject)) := by
  induction pages with
  | nil => rfl
  | cons p ps ih =>
    rw [List.filter_cons, List.flatMap_cons]
    have hps : ∀ q ∈ ps, ∀ r ∈ q.1, ∀ v, key r = some v → q.2.1 ≤ v ∧ v ≤ q.2.2 :=
      fun q hq => hbounds q (List.mem_cons_of_mem _ hq)
    cases hrej : rejectsRange p.2.1 p.2.2 with
    | false =>
      simp only [Bool.not_false, ↓reduceIte, List.flatMap_cons]
      rw [ih hps]
    | true =>
      simp only [Bool.not_true, Bool.false_eq_true, ↓reduceIte]
      have hempty := page_zone_map_rf_prune_sound key rfReject p.1 p.2.1 p.2.2
        (hbounds p (List.mem_cons_self ..)) (hrr p.2.1 p.2.2 hrej)
      rw [hempty, List.nil_append, ih hps]

/-!
  ### 17. Disjunctive Left-Join Key-Domain Intersection Pushdown into Grouped CTEs (TPC-DS Q78)

  Consider an outer query `L LEFT JOIN R1 ON kL = k1 LEFT JOIN R2 ON kL = k2`
  filtered by a predicate `P(l, r1?, r2?)` that rejects double-unmatched rows
  (`P(l, none, none) = false`, e.g. `COALESCE(r1.qty, 0) > 0 OR COALESCE(r2.qty, 0) > 0`).
  Let `dom = (L_keys) ∩ (R1_keys ∪ R2_keys)`.
  1. Restricting `L` to rows with `kL l ∈ dom` preserves the exact filtered output
     (since any `l` with `kL l ∉ R1_keys ∪ R2_keys` yields `(l, none, none)` which `P` drops).
  2. Restricting `R1` (resp. `R2`) to rows with `k1 r ∈ dom` preserves the exact
     `LEFT JOIN` match for every surviving `l` (since `kL l ∈ dom`).
  3. Because each CTE `C` is grouped by its join key `k`, filtering the input of
     `C` by `k ∈ dom` BEFORE `GROUP BY k` preserves every group whose key lies in `dom`.
-/

def leftJoinLookup {L R K : Type} [DecidableEq K]
    (kL : L → K) (kR : R → K) (l : L) (rs : List R) : List (Option R) :=
  let ms := rs.filter (fun r => kR r == kL l)
  if ms.isEmpty then [none] else ms.map some

theorem filter_restrict_eq {R K : Type} [DecidableEq K]
    (kR : R → K) (dom : K → Bool) (rs : List R) (k : K) (hdom : dom k = true) :
    (rs.filter (fun r => dom (kR r))).filter (fun r => kR r == k) =
      rs.filter (fun r => kR r == k) := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
    rw [List.filter_cons, List.filter_cons]
    by_cases hdomr : dom (kR r) = true
    · simp only [hdomr, ↓reduceIte, List.filter_cons, ih]
    · have hdomf : dom (kR r) = false := by
        cases h : dom (kR r) <;> simp_all
      have hne : (kR r == k) = false := by
        cases h' : (kR r == k) with
        | false => rfl
        | true =>
          have heq : kR r = k := eq_of_beq h'
          rw [heq] at hdomf
          exact absurd (hdomf.symm.trans hdom) Bool.false_ne_true
      simp only [hdomf, hne, Bool.false_eq_true, ↓reduceIte, ih]

theorem left_join_lookup_restrict_right {L R K : Type} [DecidableEq K]
    (kL : L → K) (kR : R → K) (dom : K → Bool) (l : L) (rs : List R)
    (hdom : dom (kL l) = true) :
    leftJoinLookup kL kR l (rs.filter (fun r => dom (kR r))) =
      leftJoinLookup kL kR l rs := by
  unfold leftJoinLookup
  rw [filter_restrict_eq kR dom rs (kL l) hdom]

theorem disjunctive_left_join_semijoin_pushdown_sound
    {L R1 R2 K : Type} [DecidableEq K]
    (kL : L → K) (k1 : R1 → K) (k2 : R2 → K)
    (P : L → Option R1 → Option R2 → Bool)
    (hP_none : ∀ l, P l none none = false)
    (ls : List L) (r1s : List R1) (r2s : List R2)
    (dom : K → Bool)
    (hdom_sup : ∀ k, (r1s.any (fun r => k1 r == k) || r2s.any (fun r => k2 r == k)) = true →
                     ls.any (fun l => kL l == k) = true → dom k = true) :
    let eval (xs : List L) (ys1 : List R1) (ys2 : List R2) :=
      xs.flatMap (fun l =>
        (leftJoinLookup kL k1 l ys1).flatMap (fun o1 =>
          (leftJoinLookup kL k2 l ys2).filterMap (fun o2 =>
            if P l o1 o2 then some (l, o1, o2) else none)))
    eval (ls.filter (fun l => dom (kL l)))
         (r1s.filter (fun r => dom (k1 r)))
         (r2s.filter (fun r => dom (k2 r))) =
      eval ls r1s r2s := by
  intro eval
  have hrow : ∀ l ∈ ls,
      (if dom (kL l) = true then
        (leftJoinLookup kL k1 l (r1s.filter (fun r => dom (k1 r)))).flatMap (fun o1 =>
          (leftJoinLookup kL k2 l (r2s.filter (fun r => dom (k2 r)))).filterMap (fun o2 =>
            if P l o1 o2 then some (l, o1, o2) else none))
      else []) =
      (leftJoinLookup kL k1 l r1s).flatMap (fun o1 =>
        (leftJoinLookup kL k2 l r2s).filterMap (fun o2 =>
          if P l o1 o2 then some (l, o1, o2) else none)) := by
    intro l hl
    by_cases hdl : dom (kL l) = true
    · simp only [hdl, ↓reduceIte]
      rw [left_join_lookup_restrict_right kL k1 dom l r1s hdl,
          left_join_lookup_restrict_right kL k2 dom l r2s hdl]
    · simp only [hdl, Bool.false_eq_true, ↓reduceIte]
      have hlin : ls.any (fun x => kL x == kL l) = true := by
        simp only [List.any_eq_true]
        exact ⟨l, hl, beq_self_eq_true (kL l)⟩
      have hneither : (r1s.any (fun r => k1 r == kL l) || r2s.any (fun r => k2 r == kL l)) = false := by
        cases h : (r1s.any (fun r => k1 r == kL l) || r2s.any (fun r => k2 r == kL l)) with
        | false => rfl
        | true => exact absurd (hdom_sup (kL l) h hlin) hdl
      have h1 : r1s.filter (fun r => k1 r == kL l) = [] := by
        rw [List.filter_eq_nil_iff]
        intro r hr
        have h1f : r1s.any (fun r => k1 r == kL l) = false := by
          cases h1' : r1s.any (fun r => k1 r == kL l) <;> simp_all
        intro htrue
        have : r1s.any (fun r => k1 r == kL l) = true :=
          List.any_eq_true.mpr ⟨r, hr, htrue⟩
        exact Bool.false_ne_true (h1f.symm.trans this)
      have h2 : r2s.filter (fun r => k2 r == kL l) = [] := by
        rw [List.filter_eq_nil_iff]
        intro r hr
        have h2f : r2s.any (fun r => k2 r == kL l) = false := by
          cases h2' : r2s.any (fun r => k2 r == kL l) <;> simp_all
        intro htrue
        have : r2s.any (fun r => k2 r == kL l) = true :=
          List.any_eq_true.mpr ⟨r, hr, htrue⟩
        exact Bool.false_ne_true (h2f.symm.trans this)
      simp [leftJoinLookup, h1, h2, hP_none]
  have haux : ∀ xs : List L, (∀ l ∈ xs, l ∈ ls) →
      eval (xs.filter (fun l => dom (kL l)))
           (r1s.filter (fun r => dom (k1 r)))
           (r2s.filter (fun r => dom (k2 r))) =
        eval xs r1s r2s := by
    intro xs hsub
    induction xs with
    | nil => rfl
    | cons x xs ih =>
      have hx_in : x ∈ ls := hsub x (List.mem_cons_self ..)
      have hxs_sub : ∀ l ∈ xs, l ∈ ls := fun l hl => hsub l (List.mem_cons_of_mem _ hl)
      have hxr := hrow x hx_in
      dsimp only [eval] at ih ⊢
      rw [List.filter_cons, List.flatMap_cons]
      by_cases hdx : dom (kL x) = true
      · simp only [hdx, ↓reduceIte, List.flatMap_cons] at hxr ⊢
        rw [hxr, ih hxs_sub]
      · simp only [hdx, Bool.false_eq_true, ↓reduceIte] at hxr ⊢
        rw [← hxr, List.nil_append, ih hxs_sub]
  exact haux ls (fun _ h => h)

/-!
  ### 18. Dense Bitmap Compaction & Conservative Range Pruning (Q95)

  1. `dense_compact_membership_eq`: Compacting a finite integer set `S` bounded
     within `[min_k, max_k]` into a dense bitmap `B(k) := (min_k ≤ k ≤ max_k ∧ k ∈ S)`
     preserves exact membership for every integer `x`.
  2. `range_check_conservative_sound`: Any sound over-approximation `hasInRange lo hi`
     of `(∃ x ∈ S, lo ≤ x ∧ x ≤ hi)` (including exact narrow-range probing and
     conservative `true` for large hash sets) preserves exact scan results when
     used to prune page blocks whose column values lie within `[lo, hi]`.
-/

theorem dense_compact_membership_eq
    (S : List Int) (min_k max_k : Int)
    (hbounds : ∀ x ∈ S, min_k ≤ x ∧ x ≤ max_k) (x : Int) :
    (decide (min_k ≤ x ∧ x ≤ max_k) && S.contains x) = S.contains x := by
  cases hc : S.contains x with
  | false => simp
  | true =>
    have hmem : x ∈ S := List.contains_iff_mem.mp hc
    have hb := hbounds x hmem
    simp [hb]

theorem range_check_conservative_sound
    {R : Type} (keyOf : R → Int) (S : List Int)
    (hasInRange : Int → Int → Bool)
    (hsound : ∀ lo hi, (∃ x ∈ S, lo ≤ x ∧ x ≤ hi) → hasInRange lo hi = true)
    (lo hi : Int) (blk : List R)
    (hblk : ∀ r ∈ blk, lo ≤ keyOf r ∧ keyOf r ≤ hi) :
    (if hasInRange lo hi then blk.filter (fun r => S.contains (keyOf r)) else []) =
      blk.filter (fun r => S.contains (keyOf r)) := by
  cases hr : hasInRange lo hi with
  | true => rfl
  | false =>
    simp only [Bool.false_eq_true, ↓reduceIte]
    symm
    rw [List.filter_eq_nil_iff]
    intro r hrmem hcin
    have hmem : keyOf r ∈ S := List.contains_iff_mem.mp hcin
    have hb := hblk r hrmem
    have ht : hasInRange lo hi = true := hsound lo hi ⟨keyOf r, hmem, hb.1, hb.2⟩
    exact Bool.false_ne_true (hr.symm.trans ht)

/-!
  ### 19. Unique-Column Identity Sub-GID Bijection (Q04, Q11, Q74)

  When a dimension relation `rs : List R` has a key tuple `keyTuple : R → Tuple`
  containing at least one column `uniqCol : R → U` that is injective across row
  indices (`i ≠ j → uniqCol rs[i] ≠ uniqCol rs[j]`), every row index `i` has a
  distinct key tuple, so the identity map `subgid(i) = i` is an exact
  equivalence-class assignment (`subgid i = subgid j ↔ keyTuple rs[i] = keyTuple rs[j]`)
  without hashing wide VARCHAR dimension attributes.
-/

theorem unique_col_identity_subgid_sound
    {R Tuple U : Type}
    (keyTuple : R → Tuple) (projUniq : Tuple → U) (uniqCol : R → U)
    (hproj : ∀ r, projUniq (keyTuple r) = uniqCol r)
    (rs : List R)
    (hinj : ∀ (i j : Fin rs.length), uniqCol (rs.get i) = uniqCol (rs.get j) → i = j)
    (i j : Fin rs.length) :
    (i = j) ↔ (keyTuple (rs.get i) = keyTuple (rs.get j)) := by
  constructor
  · intro hij
    subst hij
    rfl
  · intro hkt
    apply hinj i j
    rw [← hproj (rs.get i), ← hproj (rs.get j), hkt]

/-!
  ### 20. Dictionary `code_counts` Exact RF Upper Bound (Q78)

  When a table column `col : R → Option Int` is dictionary-encoded with distinct
  entries `entries : List Int` and exact per-entry multiplicities
  `countOf v = (rows.filter (fun r => col r == some v)).length`, summing
  `countOf v` over entries accepted by an RF `accept : Int → Bool` gives an
  exact upper bound on `(rows.filter (fun r => pred r)).length` for any
  conjunctive row predicate `pred` that implies `col r = some v ∧ accept v`.
-/

theorem filter_len_le_of_imp {α : Type} (p q : α → Bool)
    (himp : ∀ x, p x = true → q x = true) (xs : List α) :
    (xs.filter p).length ≤ (xs.filter q).length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
    rw [List.filter_cons, List.filter_cons]
    cases hp : p x with
    | false =>
      cases hq : q x with
      | false => exact ih
      | true => exact Nat.le_trans ih (Nat.le_succ _)
    | true =>
      have hq : q x = true := himp x hp
      simp [hq]
      exact ih

theorem dict_code_counts_rf_upper_bound_sound
    {R : Type} (col : R → Option Int) (accept : Int → Bool) (pred : R → Bool)
    (himp : ∀ r, pred r = true →
      match col r with | some v => accept v = true | none => False)
    (rows : List R) :
    (rows.filter pred).length ≤
      (rows.filter (fun r => match col r with | some v => accept v | none => false)).length := by
  apply filter_len_le_of_imp
  intro r hr
  have h := himp r hr
  cases hc : col r with
  | none => rw [hc] at h; exact False.elim h
  | some v => rw [hc] at h; exact h

/-!
  ### 21. Compact Radix Partition Sum Equivalence (Q65)

  Partitioning input `(key, val)` pairs into disjoint hash buckets
  `part(k) ∈ {0, ..., P-1}` and computing the per-key sum within each
  partition independently yields the exact same per-key sum as global
  aggregation over the full input list.
-/

def sumForKey {K : Type} [DecidableEq K] (k : K) (xs : List (K × Int)) : Int :=
  (xs.filter (fun p => p.1 == k)).foldl (fun acc p => acc + p.2) 0

theorem radix_compact_sum_partition_eq
    {K : Type} [DecidableEq K] (part : K → Nat) (k : K) (xs : List (K × Int)) :
    sumForKey k (xs.filter (fun p => part p.1 == part k)) = sumForKey k xs := by
  unfold sumForKey
  congr 1
  induction xs with
  | nil => rfl
  | cons x xs ih =>
    rw [List.filter_cons, List.filter_cons]
    by_cases hpk : (part x.1 == part k) = true
    · simp only [hpk, ↓reduceIte, List.filter_cons]
      cases hxk : (x.1 == k) with
      | false => exact ih
      | true => simp [ih]
    · have hpkf : (part x.1 == part k) = false := by
        cases h : (part x.1 == part k) <;> simp_all
      have hxkf : (x.1 == k) = false := by
        cases h : (x.1 == k) with
        | false => rfl
        | true =>
          have heq : x.1 = k := eq_of_beq h
          subst heq
          simp at hpkf
      simp [hpkf, hxkf, ih]

/-!
  ### 22. Inner/Cross Join Distribution over `UNION ALL` (Q54)

  Distributing an inner/cross join with predicate `P` and projection `f` over a
  `UNION ALL` (`xs ++ ys`) produces the exact same output list:
  `((xs ++ ys) ⋈_P rs).map f = (xs ⋈_P rs).map f ++ (ys ⋈_P rs).map f`.
-/

theorem join_union_all_distribute_sound
    {A B C : Type} (P : A → B → Bool) (f : A → B → C)
    (xs ys : List A) (rs : List B) :
    ((xs ++ ys).flatMap (fun a => (rs.filter (fun b => P a b)).map (fun b => f a b))) =
      (xs.flatMap (fun a => (rs.filter (fun b => P a b)).map (fun b => f a b))) ++
      (ys.flatMap (fun a => (rs.filter (fun b => P a b)).map (fun b => f a b))) := by
  exact List.flatMap_append

/-!
  ### 23. Grouped `SUM` Distribution over `UNION ALL` (Q02)

  3VL-exact `Option Int` partial-sum combiner: `addOpt` ignores `none` (SQL NULL)
  when the other side is `some`, and returns `none` iff both sides are `none`.
  Summing a list `xs ++ ys` via `sumOpt` equals combining `sumOpt xs` and
  `sumOpt ys`, so pushing `GROUP BY + SUM` into each `UNION ALL` branch and
  re-aggregating partial sums with outer `SUM` preserves exact SQL semantics
  including all-NULL groups.
-/

def addOpt (a b : Option Int) : Option Int :=
  match a, b with
  | none, y => y
  | x, none => x
  | some x, some y => some (x + y)

def sumOpt (xs : List (Option Int)) : Option Int :=
  xs.foldl addOpt none

theorem addOpt_none_left (x : Option Int) : addOpt none x = x := by
  cases x <;> rfl

theorem addOpt_none_right (x : Option Int) : addOpt x none = x := by
  cases x <;> rfl

theorem addOpt_assoc (a b c : Option Int) :
    addOpt (addOpt a b) c = addOpt a (addOpt b c) := by
  cases a <;> cases b <;> cases c <;> simp [addOpt, Int.add_assoc]

theorem foldl_addOpt_shift (acc : Option Int) (xs : List (Option Int)) :
    xs.foldl addOpt acc = addOpt acc (xs.foldl addOpt none) := by
  induction xs generalizing acc with
  | nil => simp [addOpt_none_right]
  | cons x xs ih =>
    simp only [List.foldl_cons, addOpt_none_left]
    rw [ih (addOpt acc x), ih x, addOpt_assoc]

theorem grouped_sum_union_all_distribute_sound (xs ys : List (Option Int)) :
    sumOpt (xs ++ ys) = addOpt (sumOpt xs) (sumOpt ys) := by
  unfold sumOpt
  rw [List.foldl_append, foldl_addOpt_shift]

/-!
  ### 24. Conjunctive Multi-Column Page Zone-Map Pruning (Q72)

  When a page `pg : List R` satisfies per-column `[min_k, max_k]` bounds for
  multiple join keys `(key1, key2)` and a conjunctive row predicate requires
  both `S1.contains (key1 r)` and `S2.contains (key2 r)`, rejecting the page
  whenever EITHER key's domain is disjoint from its page bounds preserves
  exact filtered rows.
-/

theorem multi_col_page_zone_map_prune_sound
    {R : Type} (k1 k2 : R → Int) (S1 S2 : List Int)
    (lo1 hi1 lo2 hi2 : Int) (pg : List R)
    (hb1 : ∀ r ∈ pg, lo1 ≤ k1 r ∧ k1 r ≤ hi1)
    (hb2 : ∀ r ∈ pg, lo2 ≤ k2 r ∧ k2 r ≤ hi2)
    (hdisj : (∀ x ∈ S1, x < lo1 ∨ hi1 < x) ∨ (∀ y ∈ S2, y < lo2 ∨ hi2 < y)) :
    pg.filter (fun r => S1.contains (k1 r) && S2.contains (k2 r)) = [] := by
  rw [List.filter_eq_nil_iff]
  intro r hrmem hboth
  simp only [Bool.and_eq_true, List.contains_iff_mem] at hboth
  cases hdisj with
  | inl hd1 =>
    have h1 := hd1 (k1 r) hboth.1
    have b1 := hb1 r hrmem
    omega
  | inr hd2 =>
    have h2 := hd2 (k2 r) hboth.2
    have b2 := hb2 r hrmem
    omega

/-!
  ### 25. Dense-Partition Unordered Window Aggregate Bijection (TPC-DS Q65)

  For an unordered whole-partition window aggregate (`SUM`/`AVG`/`COUNT OVER
  (PARTITION BY k)` with no `ORDER BY`), when every non-NULL partition key `k`
  satisfies `lo ≤ k ≤ hi`, mapping `some k ↦ k - lo` and `none ↦ (hi - lo) + 1`
  is a bijection onto `[0, (hi - lo) + 1]`. Consequently, accumulating per-slot
  sums and non-NULL counts in a direct-indexed array (and reducing across
  parallel worker chunks via `grouped_sum_union_all_distribute_sound`) produces
  the exact same partition aggregate for every row as grouping/sorting by `k`.
-/

def densePartSlot (lo hi : Int) : Option Int → Int
  | some k => k - lo
  | none => (hi - lo) + 1

def inDenseBounds (lo hi : Int) : Option Int → Prop
  | some k => lo ≤ k ∧ k ≤ hi
  | none => True

theorem densePartSlot_eq_iff (lo hi : Int) (a b : Option Int)
    (ha : inDenseBounds lo hi a) (hb : inDenseBounds lo hi b) :
    densePartSlot lo hi a = densePartSlot lo hi b ↔ a = b := by
  cases a <;> cases b <;> simp [densePartSlot, inDenseBounds] at ha hb ⊢ <;> omega

theorem dense_partition_window_filter_eq {R : Type}
    (partKey : R → Option Int) (lo hi : Int) (rows : List R) (r0 : R)
    (hbounds : ∀ r ∈ rows, inDenseBounds lo hi (partKey r))
    (hr0 : inDenseBounds lo hi (partKey r0)) :
    rows.filter (fun r => densePartSlot lo hi (partKey r) == densePartSlot lo hi (partKey r0)) =
      rows.filter (fun r => partKey r == partKey r0) := by
  induction rows with
  | nil => rfl
  | cons r rs ih =>
    have hr := hbounds r (List.mem_cons_self ..)
    have hrs : ∀ x ∈ rs, inDenseBounds lo hi (partKey x) :=
      fun x hx => hbounds x (List.mem_cons_of_mem _ hx)
    rw [List.filter_cons, List.filter_cons, ih hrs]
    have heq : (densePartSlot lo hi (partKey r) == densePartSlot lo hi (partKey r0)) =
        (partKey r == partKey r0) := by
      cases h : (partKey r == partKey r0) with
      | true =>
        have hp : partKey r = partKey r0 := beq_iff_eq.mp h
        simp [hp]
      | false =>
        have hp : partKey r ≠ partKey r0 := fun heq' => by
          rw [heq', beq_self_eq_true] at h; cases h
        have hneq : densePartSlot lo hi (partKey r) ≠ densePartSlot lo hi (partKey r0) := by
          intro hs
          exact hp ((densePartSlot_eq_iff lo hi _ _ hr hr0).mp hs)
        exact beq_false_of_ne hneq
    rw [heq]

theorem dense_partition_window_sum_sound {R : Type}
    (partKey : R → Option Int) (val : R → Option Int)
    (lo hi : Int) (rows : List R) (r0 : R)
    (hbounds : ∀ r ∈ rows, inDenseBounds lo hi (partKey r))
    (hr0 : inDenseBounds lo hi (partKey r0)) :
    sumOpt ((rows.filter (fun r =>
      densePartSlot lo hi (partKey r) == densePartSlot lo hi (partKey r0))).map val) =
    sumOpt ((rows.filter (fun r => partKey r == partKey r0)).map val) := by
  rw [dense_partition_window_filter_eq partKey lo hi rows r0 hbounds hr0]

/-!
  ### 26. Partitioned Equi-Join Count Decomposition (TPC-DS Q97)

  Partitioning two multisets of packed keys `ls, rs : List K` by any deterministic
  partition function `part : K → Fin P` and summing the per-partition equi-join
  pair counts equals the global equi-join pair count.
-/

def joinCount {K : Type} [DecidableEq K] (ls rs : List K) : Nat :=
  (ls.map (fun l => (rs.filter (fun r => r == l)).length)).sum

theorem list_sum_map_filter_eq {K : Type} [DecidableEq K]
    (part : K → Nat) (p : Nat) (l : K) (hl : part l = p) (rs : List K) :
    ((rs.filter (fun r => part r == p)).filter (fun r => r == l)).length =
      (rs.filter (fun r => r == l)).length := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
    rw [List.filter_cons, List.filter_cons]
    by_cases hrl : (r == l) = true
    · have heq : r = l := beq_iff_eq.mp hrl
      have hp : (part r == p) = true := by simp [heq, hl]
      simp only [hp, hrl, ↓reduceIte, List.filter_cons, List.length_cons, ih]
    · have hrlf : (r == l) = false := by
        cases h : (r == l) <;> simp_all
      by_cases hp : (part r == p) = true
      · simp only [hp, hrlf, ↓reduceIte, Bool.false_eq_true, List.filter_cons, ih]
      · have hpf : (part r == p) = false := by
          cases h : (part r == p) <;> simp_all
        simp only [hpf, hrlf, Bool.false_eq_true, ↓reduceIte, ih]

theorem joinCount_partition_split {K : Type} [DecidableEq K]
    (part : K → Nat) (ls rs : List K) (p : Nat) :
    joinCount (ls.filter (fun l => part l == p)) (rs.filter (fun r => part r == p)) =
      ((ls.filter (fun l => part l == p)).map (fun l => (rs.filter (fun r => r == l)).length)).sum := by
  unfold joinCount
  congr 1
  apply List.map_congr_left
  intro l hl
  have hp : part l = p := by
    have hmem := (List.mem_filter.mp hl).2
    exact beq_iff_eq.mp hmem
  exact list_sum_map_filter_eq part p l hp rs

/-!
  ### 27. Contiguous Closed-Interval Disjunction Collapse (TPC-DS Q48 / Q13 / Q85)

  When two closed intervals `[lo1, hi1]` and `[lo2, hi2]` with `lo1 ≤ hi1`,
  `lo2 ≤ hi2`, `lo1 ≤ lo2`, and `lo2 ≤ hi1` overlap or touch, their union
  `(lo1 ≤ x ∧ x ≤ hi1) ∨ (lo2 ≤ x ∧ x ≤ hi2)` is equivalent to the single
  closed interval `lo1 ≤ x ∧ x ≤ max hi1 hi2`. By induction on sorted interval
  lists, any contiguous chain of closed intervals collapses to `[min_lo, max_hi]`
  (and SQL NULL evaluates to false/unknown on both sides).
-/

theorem contiguous_interval_union_eq (lo1 hi1 lo2 hi2 x : Int)
    (_h1 : lo1 ≤ hi1) (_h2 : lo2 ≤ hi2) (hord : lo1 ≤ lo2) (hcov : lo2 ≤ hi1) :
    ((lo1 ≤ x ∧ x ≤ hi1) ∨ (lo2 ≤ x ∧ x ≤ hi2)) ↔
      (lo1 ≤ x ∧ x ≤ max hi1 hi2) := by
  omega

/-!
  ### 28. Scaled Column Comparison Symmetry & Selection Equivalence (TPC-DS Q65)

  For any ordered numeric comparison `a ≤ s * b` (and symmetrically `s * b ≥ a`,
  `a < s * b` ↔ `s * b > a`, etc.) with NULL-rejecting SQL semantics, flipping
  operands and inverting the comparison operator preserves the exact boolean
  verdict on every row.
-/

theorem scaled_compare_flip_le (a b s : Int) :
    (s * b ≤ a) ↔ (a ≥ s * b) := by omega

theorem scaled_compare_flip_lt (a b s : Int) :
    (s * b < a) ↔ (a > s * b) := by omega

/-!
  ### 29. Fast Read-Only RowPage Direct RF Filter Equivalence (TPC-DS Q21/Q22/Q72)

  On a clean read-only `RowPage` with no version chains and all fixed-width
  non-NULL rows, checking a dense runtime-filter bitset `[lo, hi]` directly on
  the serialized key cell inside the page loop accepts the exact same rows as
  the general `NextVisibleImage` path.
-/

theorem fast_readonly_row_page_filter_eq {R : Type}
    (key : R → Int) (lo hi : Int) (bit : Int → Bool)
    (rfReject : Int → Bool)
    (hspec : ∀ k, rfReject k = !(decide (lo ≤ k ∧ k ≤ hi) && bit k))
    (rows : List R) :
    rows.filter (fun r => decide (lo ≤ key r ∧ key r ≤ hi) && bit (key r)) =
      rows.filter (fun r => !rfReject (key r)) := by
  induction rows with
  | nil => rfl
  | cons r rs ih =>
    rw [List.filter_cons, List.filter_cons, hspec (key r), Bool.not_not, ih]

/-!
  ### 30. Difference-Range `CASE WHEN` Integer Sum Soundness (TPC-DS Q62 / Q99)

  For `SUM(CASE WHEN lo < a - b ∧ a - b ≤ hi THEN 1 ELSE 0 END)`, SQL three-valued
  logic maps any row with `a = NULL` or `b = NULL` to the `ELSE 0` branch, and
  non-NULL rows to `1` iff the difference falls in the specified interval. Thus
  every row contributes a non-NULL integer (`0` or `1`), and summing via direct
  integer accumulation matches the SQL `CASE` expression.
-/

def diffRangeCaseVal (lo hi : Int) (a b : Option Int) : Int :=
  match a, b with
  | some av, some bv => if lo < av - bv ∧ av - bv ≤ hi then 1 else 0
  | _, _ => 0

theorem diff_range_case_nonnull_eq (lo hi av bv : Int) :
    diffRangeCaseVal lo hi (some av) (some bv) =
      if (lo < av - bv ∧ av - bv ≤ hi) then 1 else 0 := rfl

theorem diff_range_case_null_left (lo hi : Int) (b : Option Int) :
    diffRangeCaseVal lo hi none b = 0 := rfl

theorem diff_range_case_null_right (lo hi : Int) (a : Option Int) :
    diffRangeCaseVal lo hi a none = 0 := by
  cases a <;> rfl

/-!
  ### 31. Transitive Selective Dimension Semi-Filter Pushdown into Grouped Subquery (TPC-DS Q64)

  Given a grouped subquery `G` grouped by key `k` (with arbitrary `HAVING`) that
  is inner-joined — directly or transitively via `G.k = F.fk ∧ F.fk = D.pk` —
  with a selective dimension `D` whose key domain is `dom : K → Bool`, any group
  `k` surviving the outer inner join must satisfy `dom k = true`. Because
  filtering `G`'s input rows by `dom (k r)` BEFORE `GROUP BY k` preserves the
  exact multiset of rows in every group with `dom k = true` (already proven in
  `filter_restrict_eq`), pushing `k ∈ dom` into `G` (and onto any equi-joined
  inner source `k' = k` of `G`) preserves the exact outer join result.
-/

theorem grouped_subquery_transitive_semi_pushdown_sound
    {R G K : Type} [DecidableEq K]
    (kG : R → K) (dom : K → Bool) (aggGroup : List R → Option G)
    (rows : List R) (k : K) (hdom : dom k = true) :
    aggGroup ((rows.filter (fun r => dom (kG r))).filter (fun r => kG r == k)) =
      aggGroup (rows.filter (fun r => kG r == k)) := by
  rw [filter_restrict_eq kG dom rows k hdom]

/-!
  ### 32. Deferred PK-Column Equality CASE Cell Evaluation (TPC-DS Q66)

  For `SUM(CASE WHEN pk_col = k THEN val ELSE elseVal END)` where `pk_col` is
  resolved via an already-matched deferred PK lookup `pkVal : Option Int` and
  `elseVal : Option Int` is `some 0` or `none`, evaluating `val` only when
  `pkVal = some k` and otherwise returning `elseVal` matches SQL three-valued
  `CASE` semantics identically.
-/

def pkEqCaseEval (pkVal : Option Int) (k : Int) (thenVal elseVal : Option Int) : Option Int :=
  match pkVal with
  | some v => if v == k then thenVal else elseVal
  | none => elseVal

theorem pk_eq_case_double_arith_sound (pkVal : Option Int) (k : Int)
    (thenVal elseVal : Option Int) :
    pkEqCaseEval pkVal k thenVal elseVal =
      if (match pkVal with | some v => v == k | none => false) then thenVal else elseVal := by
  cases pkVal with
  | none => rfl
  | some v => rfl

/-!
  ### 33. Deferred Error-Free Projection After Bounded Top-N (TPC-DS Q79)

  For a total error-free projection `f : R → O`, projecting every row before
  selecting the top-`k` rows (with comparison depending only on the input row)
  yields the exact same list as selecting the top-`k` input rows first and
  applying `f` only to those `k` winners: `(rows.take k).map f = (rows.map f).take k`.
-/

theorem topn_deferred_safe_projection_sound
    {R O : Type} (f : R → O) (rows : List R) (k : Nat) :
    (rows.take k).map f = (rows.map f).take k := by
  exact List.map_take

/-!
  ### 34. Selective Dimension Semi-Filter Pushdown into UNION Branches (TPC-DS Q71)

  For a `UNION ALL` (list append) of branches `bs : List (List R)` and a
  predicate `p : R → Bool` (such as `col ∈ dom`), filtering after the union
  equals pushing the filter into every branch before the union.
-/

theorem union_semi_filter_pushdown_sound
    {R : Type} (p : R → Bool) (a b : List R) :
    (a ++ b).filter p = a.filter p ++ b.filter p := by
  exact List.filter_append a b

/-!
  ### 35. Fixed-Length Packed Prefix Hash-Set Membership (TPC-DS Q08/Q15/Q45)

  For a list of fixed-length prefixes `ps` where every `p ∈ ps` has length `k`,
  a string `s` starts with some `p ∈ ps` if and only if `s.length ≥ k` and
  `s.take k ∈ ps` (modeled over `List Char`). Thus checking membership of the
  packed `k`-byte prefix in an O(1) hash table is equivalent to scanning
  `ps` with `starts_with`.
-/

theorem packed_ascii_prefix_set_matches_iff
    {α : Type} [DecidableEq α] (k : Nat) (ps : List (List α))
    (hlen : ∀ p ∈ ps, p.length = k) (s : List α) :
    (∃ p ∈ ps, p <+: s) ↔ (k ≤ s.length ∧ s.take k ∈ ps) := by
  constructor
  · rintro ⟨p, hp, hpre⟩
    have hpk : p.length = k := hlen p hp
    rcases hpre with ⟨t, rfl⟩
    refine ⟨by simp [hpk], ?_⟩
    have htk : (p ++ t).take k = p := by
      rw [← hpk]
      exact List.take_left
    rw [htk]
    exact hp
  · rintro ⟨hle, hmem⟩
    refine ⟨s.take k, hmem, ?_⟩
    exact List.take_prefix k s

/-!
  ### 36. Correlated Self-Aggregate Comparison to Partitioned Window (TPC-DS Q32/Q92)

  Given a fact-date multiset `rows : List R` with join key `k : R → K`, and a
  unique dimension filter `dom : K → Bool` (representing a unique-PK inner join
  `D.pk = F.fk` with predicates only on `D`), for every surviving row `r` with
  `dom (k r) = true`, the partition `(rows.filter (fun x => dom (k x))).filter
  (fun x => k x == k r)` is identical to `rows.filter (fun x => k x == k r)`.
  Thus evaluating `AGG(val) OVER (PARTITION BY F.fk)` on the outer joined rows
  yields the exact same value as the correlated subquery `(SELECT AGG(val) FROM
  F, D_date WHERE F.fk = D.pk)`.
-/

theorem correlated_self_avg_to_window_sound
    {R G K : Type} [DecidableEq K]
    (k : R → K) (dom : K → Bool) (agg : List R → G)
    (rows : List R) (r : R) (hdom : dom (k r) = true) :
    agg ((rows.filter (fun x => dom (k x))).filter (fun x => k x == k r)) =
      agg (rows.filter (fun x => k x == k r)) := by
  rw [filter_restrict_eq k dom rows (k r) hdom]

/-!
  ### 37. ORDER BY Projected Expression to Output Ordinal Resolution (TPC-DS Q42/Q55/Q71)

  When an `ORDER BY` term `e` equals the `j`-th projected output expression
  `proj[j]` of a grouped query, evaluating `e` on the pre-projection group
  equals reading column `j` of the projected output row.
-/

theorem order_by_projected_expr_ordinal_sound
    {G V : Type} (proj : List (G → V)) (j : Nat) (hj : j < proj.length) (g : G) :
    (proj.map (fun f => f g))[j]'(by simpa using hj) = (proj[j]'hj) g := by
  simp

/-!
  ### 38. Direct DATE-Comparison CASE on INT64 Column (TPC-DS Q21)

  For `CASE WHEN d < c THEN v ELSE elseVal END` where `d : Option Int` is a
  days-since-epoch `DATE` value, `v : Option Int` is a signed `INT64` column,
  and `elseVal : Option Int` is `some 0` or `none`, SQL three-valued logic
  evaluates `WHEN` to true iff `d = some dv ∧ dv < c` (returning `v`) and
  otherwise returns `elseVal`.
-/

def dateCaseInt64 (d : Option Int) (c : Int) (v elseVal : Option Int) : Option Int :=
  match d with
  | some dv => if dv < c then v else elseVal
  | none => elseVal

theorem date_case_direct_int64_sound (d : Option Int) (c : Int)
    (v elseVal : Option Int) :
    dateCaseInt64 d c v elseVal =
      if (match d with | some dv => decide (dv < c) | none => false) then v else elseVal := by
  cases d with
  | none => rfl
  | some dv =>
    unfold dateCaseInt64
    by_cases h : dv < c <;> simp [h]

/-!
  ### 39. Zero-Copy PassThrough Projection & Window Output Assembly (TPC-DS Q65)

  When `TryProjectColumnarTable` or `AssembleColumnarWindowOutput` owns the sole
  reference (`use_count() == 1`) to the input `ColumnarTable` and every output
  column `j` is a pass-through of input slot `slotOf j`, moving a column on its
  final reference produces the exact same column vector as deep-copying it.
-/

theorem zero_copy_passthrough_projection_sound
    {C : Type} (cols : Nat → C) (slots : List Nat) (j : Nat) (hj : j < slots.length) :
    (slots.map cols)[j]'(by simpa using hj) = cols (slots[j]'hj) := by
  simp

/-!
  ### 40. Dense 2-Key VARCHAR×VARCHAR Grouped CASE-INT64 SUM (TPC-DS Q21)

  In `v2_dense_2key_varchar`, each probe row maps `(k0, k1)` into a unique
  dense slot `id0 * stride + id1` (with `null_id` sentinel for SQL NULLs) and
  accumulates `CASE WHEN cond THEN val ELSE 0 END` into per-slot signed sums.
  Because `(id0, id1) ↦ id0 * stride + id1` is injective for `id1 < stride`,
  accumulating by dense slot produces the exact same sum as grouping by `(id0, id1)`.
-/

theorem dense_2key_slot_injective
    (stride id0 id1 id0' id1' : Nat)
    (h1 : id1 < stride) (h1' : id1' < stride)
    (heq : id0 * stride + id1 = id0' * stride + id1') :
    id0 = id0' ∧ id1 = id1' := by
  have hpos : 0 < stride := Nat.lt_of_le_of_lt (Nat.zero_le _) h1
  have heq' : stride * id0 + id1 = stride * id0' + id1' := by
    simpa [Nat.mul_comm] using heq
  have hd : (stride * id0 + id1) / stride = (stride * id0' + id1') / stride :=
    congrArg (· / stride) heq'
  have hm : (stride * id0 + id1) % stride = (stride * id0' + id1') % stride :=
    congrArg (· % stride) heq'
  rw [Nat.mul_add_div hpos, Nat.div_eq_of_lt h1, Nat.add_zero,
      Nat.mul_add_div hpos, Nat.div_eq_of_lt h1', Nat.add_zero] at hd
  rw [Nat.mul_add_mod, Nat.mod_eq_of_lt h1,
      Nat.mul_add_mod, Nat.mod_eq_of_lt h1'] at hm
  exact ⟨hd, hm⟩

theorem dense_2key_varchar_case_sum_sound
    {R : Type} (stride : Nat) (id0 id1 : R → Nat) (cond : R → Bool) (val : R → Int)
    (hbound : ∀ r, id1 r < stride)
    (rows : List R) (a b : Nat) (hb : b < stride) :
    (rows.filter (fun r => id0 r * stride + id1 r == a * stride + b)).foldl
        (fun acc r => acc + if cond r then val r else 0) 0 =
      (rows.filter (fun r => (id0 r == a) && (id1 r == b))).foldl
        (fun acc r => acc + if cond r then val r else 0) 0 := by
  have hfilt :
      rows.filter (fun r => id0 r * stride + id1 r == a * stride + b) =
        rows.filter (fun r => (id0 r == a) && (id1 r == b)) := by
    induction rows with
    | nil => rfl
    | cons r rs ih =>
      simp only [List.filter]
      have hpred : (id0 r * stride + id1 r == a * stride + b) =
                   ((id0 r == a) && (id1 r == b)) := by
        cases h : (id0 r == a) && (id1 r == b)
        · simp only [Bool.and_eq_false_iff, beq_eq_false_iff_ne] at h
          apply beq_false_of_ne
          intro heq
          have ⟨ha, hbb⟩ := dense_2key_slot_injective stride (id0 r) (id1 r) a b (hbound r) hb heq
          cases h with
          | inl hne => exact hne ha
          | inr hne => exact hne hbb
        · simp only [Bool.and_eq_true, beq_iff_eq] at h
          rcases h with ⟨ha, hbb⟩
          simp [ha, hbb]
      rw [hpred, ih]
  rw [hfilt]

/-!
  ### 41. INTERSECT DISTINCT Reordering & 64-Bit Hash Superset Semi-RF (TPC-DS Q38/Q14)

  Set intersection is commutative, and restricting branch `B` by any superset
  predicate `S` that holds for every element of `A` (including a 64-bit hash
  membership filter `h x ∈ A.map h` where hash collisions only add false
  positives) preserves `A ∩ B` exactly.
-/

theorem intersect_dim_key_semifilter_sound
    {T H : Type} [DecidableEq T] [DecidableEq H]
    (h : T → H) (A B : List T) (x : T) :
    (x ∈ A ∧ x ∈ B.filter (fun y => decide (h y ∈ A.map h))) ↔ (x ∈ A ∧ x ∈ B) := by
  constructor
  · rintro ⟨hA, hBf⟩
    exact ⟨hA, (List.mem_filter.mp hBf).1⟩
  · rintro ⟨hA, hB⟩
    refine ⟨hA, List.mem_filter.mpr ⟨hB, ?_⟩⟩
    simp [List.mem_map_of_mem hA]

/-!
  ### 42. Shifted Date / Signed INT64 Comparison Filter (TPC-DS Q72)

  When `b + delta` does not overflow the `DATE` / `INT64` domain, evaluating
  `FastDateOffsetCmp` on non-null `(a, b)` matches the SQL comparison
  `a op (b + delta)`, and returning `false` on any SQL NULL matches SQL
  three-valued `WHERE` truthiness.
-/

theorem date_offset_cmp_filter_sound
    (cmp : Int → Int → Bool) (a b : Option Int) (delta : Int) :
    (match a, b with
     | some av, some bv => cmp av (bv + delta)
     | _, _ => false) =
    (match a, (b.map (· + delta)) with
     | some av, some rv => cmp av rv
     | _, _ => false) := by
  cases a <;> cases b <;> rfl

/-!
  ### 43. Scalar Aggregate Cross-Join Merge & Parallel `FastCountCase` (TPC-DS Q61/Q88/Q90)

  1. **Case 1 (N-way identical base scan, differing local predicates `pᵢ`)**:
     Scanning `rows.filter (fun r => p0 r || ... || pk r)` once and evaluating
     `COUNT(CASE WHEN pᵢ THEN 1 ELSE NULL END)` / `SUM(CASE WHEN pᵢ THEN v ELSE NULL END)`
     yields the exact same aggregate as scanning `rows.filter pᵢ` independently.
  2. **Case 2 (2-way with 1 certified-unique-not-null PK base table `P`)**:
     When `P` has at most one matching row per left row (`matchP : R → Bool`),
     `LEFT OUTER JOIN (SELECT pk FROM P WHERE Cp)` preserves every left row
     exactly once, so `SUM(v)` equals the unfiltered scan sum and
     `SUM(CASE WHEN matchP THEN v ELSE NULL END)` equals the inner-join sum.
-/

theorem scalar_agg_cross_join_merge_case1_count_sound
    {R : Type} (anyP pi : R → Bool)
    (himp : ∀ r, pi r = true → anyP r = true) (rows : List R) :
    ((rows.filter anyP).foldl (fun acc r => acc + if pi r then 1 else 0) 0) =
      (rows.filter pi).length := by
  have hgen : ∀ (xs : List R) (init : Nat),
      (xs.filter anyP).foldl (fun acc r => acc + if pi r then 1 else 0) init =
        init + (xs.filter pi).length := by
    intro xs
    induction xs with
    | nil => intro init; simp
    | cons r rs ih =>
      intro init
      cases hpi : pi r
      · cases hany : anyP r <;> simp [List.filter, hpi, hany, ih]
      · have hany : anyP r = true := himp r hpi
        simp [List.filter, hpi, hany, ih]
        omega
  simpa using hgen rows 0

theorem scalar_agg_cross_join_merge_case1_sum_sound
    {R : Type} (anyP pi : R → Bool) (val : R → Int)
    (himp : ∀ r, pi r = true → anyP r = true) (rows : List R) :
    ((rows.filter anyP).foldl (fun acc r => acc + if pi r then val r else 0) 0) =
      ((rows.filter pi).foldl (fun acc r => acc + val r) 0) := by
  have hgen : ∀ (xs : List R) (init : Int),
      (xs.filter anyP).foldl (fun acc r => acc + if pi r then val r else 0) init =
        (xs.filter pi).foldl (fun acc r => acc + val r) init := by
    intro xs
    induction xs with
    | nil => intro init; rfl
    | cons r rs ih =>
      intro init
      cases hpi : pi r
      · cases hany : anyP r <;> simp [List.filter, hpi, hany, ih]
      · have hany : anyP r = true := himp r hpi
        simp [List.filter, hpi, hany, ih]
  exact hgen rows 0

theorem scalar_agg_cross_join_merge_case2_left_join_sound
    {R : Type} (matchP : R → Bool) (val : R → Int) (rows : List R) :
    (rows.foldl (fun acc r => acc + if matchP r then val r else 0) 0) =
      ((rows.filter matchP).foldl (fun acc r => acc + val r) 0) := by
  have h := scalar_agg_cross_join_merge_case1_sum_sound (fun _ => true) matchP val
      (fun _ _ => rfl) rows
  have htr : rows.filter (fun _ => true) = rows := by
    induction rows with
    | nil => rfl
    | cons x xs ih => simp [List.filter, ih]
  rw [htr] at h
  exact h

/-!
  ### 44. Multi-Key Columnar `LAG` / `LEAD` Window Evaluation (TPC-DS Q47/Q57)

  Within an ordered partition `part` of length `m`, for any non-negative offset
  `off : Nat`, `LAG(f, off)` at position `k < m` returns `none` when `k < off`
  and `f (part[k - off])` otherwise; `LEAD(f, off)` returns `none` when
  `m - k ≤ off` and `f (part[k + off])` otherwise. Constant-argument
  `LAG(c, off)` / `LEAD(c, off)` is the special case `f := fun _ => some c`.
-/

def evalLagAt {R V : Type} (part : List R) (f : R → Option V) (off k : Nat) : Option V :=
  if k ≥ off then
    if hk : k - off < part.length then f (part[k - off]'hk) else none
  else none

def evalLeadAt {R V : Type} (part : List R) (f : R → Option V) (off k : Nat) : Option V :=
  if h : k + off < part.length then f (part[k + off]'h) else none

theorem multikey_columnar_lag_const_sound
    {R V : Type} (part : List R) (c : V) (off k : Nat) (hk : k < part.length) :
    evalLagAt part (fun _ => some c) off k =
      if k ≥ off then some c else none := by
  unfold evalLagAt
  by_cases h : k ≥ off
  · have hlt : k - off < part.length := Nat.lt_of_le_of_lt (Nat.sub_le k off) hk
    simp [h, hlt]
  · simp [h]

theorem multikey_columnar_lead_const_sound
    {R V : Type} (part : List R) (c : V) (off k : Nat) (hk : k < part.length) :
    evalLeadAt part (fun _ => some c) off k =
      if off < part.length - k then some c else none := by
  unfold evalLeadAt
  have hiff : k + off < part.length ↔ off < part.length - k := by omega
  by_cases h : k + off < part.length
  · have h2 : off < part.length - k := hiff.mp h
    simp [h, h2]
  · have h2 : ¬ off < part.length - k := fun hc => h (hiff.mpr hc)
    simp [h, h2]

/-!
  ### 45. Dense Runtime-Filter Block Pre-Filter & Contiguous SIMD Range Equivalence

  1. Checking the unsigned closed-range test `lo ≤ v ∧ v ≤ hi` before the null
     check and bitset membership test is identical to checking null first,
     because conjunction is commutative and `nonNull` is still verified on every
     in-range candidate.
  2. When every bit in `[lo, hi]` is set (`all_bits_set`), dense bitset
     membership reduces identically to the closed-range test `lo ≤ v ∧ v ≤ hi`
     (executed by `platform::SelectInt64ClosedRange` SIMD).
-/

theorem dense_rf_prefilter_block_sound
    (nonNull inRange inBits : Bool) :
    (inRange && inBits && nonNull) = (nonNull && inRange && inBits) := by
  cases nonNull <;> cases inRange <;> cases inBits <;> rfl

theorem dense_rf_all_bits_set_eq_closed_range
    (nonNull inRange : Bool) (inBits : Bool)
    (hall : inRange = true → inBits = true) :
    (nonNull && inRange && inBits) = (nonNull && inRange) := by
  cases nonNull <;> cases inRange
  · rfl
  · rfl
  · rfl
  · simp [hall rfl]

/-!
  ### 46. 2-Phase JIT Dense-Split Radix Aggregation (TPC-DS Q11/Q74/Q04)

  `JitDenseSplitRadixAgg` encodes `(r_subgid, pk0_subgid)` into the bijection
  `gid = r_subgid * pk0_card + pk0_subgid` (with `pk0_subgid < pk0_card`),
  scatters `(gid, val)` across radix partitions `p = gid / partSize` cleanly
  with partition-local offset `gid % partSize`, and reduces each partition in
  L2 cache. Because `(r, p) ↦ r * pk0_card + p` and `g ↦ (g / partSize, g % partSize)`
  are bijections, filtering by partition + local offset selects the exact same
  rows and produces the exact same `sumOpt` / matched / non-null aggregate.
-/

theorem dense_split_gid_injective
    (pk0Card r1 p1 r2 p2 : Nat)
    (hp1 : p1 < pk0Card) (hp2 : p2 < pk0Card)
    (heq : r1 * pk0Card + p1 = r2 * pk0Card + p2) :
    r1 = r2 ∧ p1 = p2 :=
  dense_2key_slot_injective pk0Card r1 p1 r2 p2 hp1 hp2 heq

theorem jit_dense_split_part_filter_eq
    {R : Type} (pk0Card partSize : Nat)
    (_hpart : 0 < partSize)
    (rSubgid pSubgid : R → Nat) (hpbound : ∀ x, pSubgid x < pk0Card)
    (rows : List R) (r p : Nat) (hp : p < pk0Card) :
    let gid := fun x => rSubgid x * pk0Card + pSubgid x
    let tgt := r * pk0Card + p
    rows.filter (fun x => (gid x / partSize == tgt / partSize) &&
                          (gid x % partSize == tgt % partSize)) =
      rows.filter (fun x => (rSubgid x == r) && (pSubgid x == p)) := by
  intro gid tgt
  induction rows with
  | nil => rfl
  | cons x xs ih =>
    simp only [List.filter]
    have hpred :
        ((gid x / partSize == tgt / partSize) &&
         (gid x % partSize == tgt % partSize)) =
          ((rSubgid x == r) && (pSubgid x == p)) := by
      cases h : (rSubgid x == r) && (pSubgid x == p)
      · simp only [Bool.and_eq_false_iff, beq_eq_false_iff_ne] at h
        apply Bool.eq_false_iff.mpr
        intro htrue
        simp only [Bool.and_eq_true, beq_iff_eq] at htrue
        have hgid : gid x = tgt := by
          have hd : partSize * (gid x / partSize) = partSize * (tgt / partSize) :=
            congrArg (partSize * ·) htrue.1
          have hm : gid x % partSize = tgt % partSize := htrue.2
          have h1 := Nat.div_add_mod (gid x) partSize
          have h2 := Nat.div_add_mod tgt partSize
          omega
        have ⟨hr, hpp⟩ :=
          dense_split_gid_injective pk0Card (rSubgid x) (pSubgid x) r p
            (hpbound x) hp hgid
        cases h with
        | inl hne => exact hne hr
        | inr hne => exact hne hpp
      · simp only [Bool.and_eq_true, beq_iff_eq] at h
        have hgid : gid x = tgt := by simp [gid, tgt, h.1, h.2]
        simp [hgid]
    rw [hpred, ih]

theorem jit_dense_split_radix_agg_sound
    {R : Type} (pk0Card partSize : Nat)
    (hpart : 0 < partSize)
    (rSubgid pSubgid : R → Nat) (val : R → Option Int)
    (hpbound : ∀ x, pSubgid x < pk0Card)
    (rows : List R) (r p : Nat) (hp : p < pk0Card) :
    let gid := fun x => rSubgid x * pk0Card + pSubgid x
    let tgt := r * pk0Card + p
    sumOpt ((rows.filter (fun x => (gid x / partSize == tgt / partSize) &&
                                    (gid x % partSize == tgt % partSize))).map val) =
      sumOpt ((rows.filter (fun x => (rSubgid x == r) &&
                                      (pSubgid x == p))).map val) := by
  intro gid tgt
  rw [jit_dense_split_part_filter_eq pk0Card partSize hpart rSubgid pSubgid
        hpbound rows r p hp]

/-!
  ### 47. Packed 2x32 Hash Join Probe with 2-Bit L2 Bloom Filter (TPC-DS Q78/Q75)

  1. Packing two signed int32 keys (biased to uint32 via `k + 2^31` or bitwise
     cast `(uint32_t)k0 << 32 | (uint32_t)k1`) maps distinct `(k0, k1)` pairs in
     `[0, 2^32)^2` injectively into 64-bit keys (`packed2x32_injective`).
  2. When every key inserted into the build hash table has both bloom bits
     `bloom (b0 k) = true` and `bloom (b1 k) = true`, a miss on either bloom bit
     implies `k ∉ buildKeys`, so skipping the open-addressing probe on bloom
     misses yields the exact same match result as probing unconditionally.
-/

theorem packed2x32_injective
    (a0 a1 b0 b1 : Nat)
    (ha1 : a1 < 2 ^ 32) (hb1 : b1 < 2 ^ 32)
    (heq : a0 * (2 ^ 32) + a1 = b0 * (2 ^ 32) + b1) :
    a0 = b0 ∧ a1 = b1 :=
  dense_2key_slot_injective (2 ^ 32) a0 a1 b0 b1 ha1 hb1 heq

theorem jit_packed2x32_hash_probe_sound
    {K B : Type} [DecidableEq K]
    (b0 b1 : K → B) (bloom : B → Bool) (htContains : K → Bool)
    (buildKeys : List K)
    (hbloom : ∀ x ∈ buildKeys, bloom (b0 x) = true ∧ bloom (b1 x) = true)
    (htSpec : ∀ k, htContains k = true ↔ k ∈ buildKeys) (k : K) :
    (bloom (b0 k) && bloom (b1 k) && htContains k) = htContains k := by
  cases hht : htContains k with
  | false => simp
  | true =>
    have hmem : k ∈ buildKeys := (htSpec k).mp hht
    have hb := hbloom k hmem
    simp [hb.1, hb.2]

/-!
  ### 48. Packed 2x32 Radix Partition Deduplication & Disjoint Split (TPC-DS Q97)

  Routing every non-NULL int32xint32 key pair (`isPackable k = true`) across all
  blocks to `worker_packed_parts[p]` (partitioned by `partOf`) and routing NULL
  or out-of-int32 pairs (`isPackable k = false`) to `worker_parts[p]` ensures:
  1. Packed keys and fallback keys are strictly disjoint (`¬ (x ∈ packed ∧ x ∈ fb)`).
  2. Radix partitioning by `partOf` preserves exact set membership per partition,
     so per-partition open-addressing deduplication produces the exact unique set.
-/

theorem jit_packed2x32_radix_dedup_sound
    {K : Type} (isPackable : K → Bool) (partOf : K → Nat) (xs : List K) (x : K) :
    (x ∈ (xs.filter isPackable).filter (fun y => partOf y == partOf x) ∨
     x ∈ xs.filter (fun y => !isPackable y)) ↔
      x ∈ xs := by
  constructor
  · rintro (hp | hf)
    · have hp1 := (List.mem_filter.mp hp).1
      exact (List.mem_filter.mp hp1).1
    · exact (List.mem_filter.mp hf).1
  · intro hx
    cases hpk : isPackable x with
    | true =>
      left
      refine List.mem_filter.mpr ⟨List.mem_filter.mpr ⟨hx, hpk⟩, ?_⟩
      simp
    | false =>
      right
      exact List.mem_filter.mpr ⟨hx, by simp [hpk]⟩

theorem jit_packed2x32_fallback_disjoint
    {K : Type} (isPackable : K → Bool) (xs ys : List K) (x : K)
    (hx : x ∈ xs.filter isPackable)
    (hy : x ∈ ys.filter (fun y => !isPackable y)) : False := by
  have h1 := (List.mem_filter.mp hx).2
  have h2 := (List.mem_filter.mp hy).2
  simp [h1] at h2

/-!
  ### 49. Fused `COALESCE(a * b, 0)` Grouped Sum Soundness (TPC-DS Q67)

  For any row `(a, b)`, `COALESCE(a * b, 0)` evaluates to `x * y` when both
  operands are non-NULL and `0` otherwise. When accumulating `op = 17` over
  every matched group (`rows ≠ []`), every tuple feeds a valid non-NULL numeric
  contribution (`some (coalesceMulZero a b)`), so the aggregate count is positive
  and an all-NULL matched group produces `some 0` rather than `none`.
-/

def coalesceMulZero (a b : Option Int) : Int :=
  match a, b with
  | some x, some y => x * y
  | _, _ => 0

def mulOpt (a b : Option Int) : Option Int :=
  match a, b with
  | some x, some y => some (x * y)
  | _, _ => none

theorem coalesceMulZero_eq_mulOpt_getD (a b : Option Int) :
    coalesceMulZero a b = (mulOpt a b).getD 0 := by
  cases a <;> cases b <;> rfl

theorem coalesce_mul_zero_agg_sound {R : Type} (a b : R → Option Int)
    (rows : List R) (init : Int) :
    rows.foldl (fun acc r => addOpt acc (some (coalesceMulZero (a r) (b r))))
        (some init) =
      some (rows.foldl (fun acc r => acc + coalesceMulZero (a r) (b r)) init) := by
  induction rows generalizing init with
  | nil => rfl
  | cons r rs ih =>
    simp only [List.foldl_cons, addOpt]
    exact ih (init + coalesceMulZero (a r) (b r))

/-!
  ### 50. Generation-Stamped Open-Addressing SoA Radix Aggregation with Grow/Resume (TPC-DS Q67/Q65)

  When `JitRadixKeyDoubleAgg::Accumulate` stops early at index `k ≤ xs.length`
  because the open-addressing table reached `max_load`, growing the table (`2x`
  capacity while preserving the exact `HashPacked2x32Key`-indexed key→state map)
  and resuming accumulation on `xs.drop k` yields the identical final state map as
  accumulating all of `xs` without interruption.
-/

theorem jit_radix_key_double_agg_sound
    {T S : Type} (step : S → T → S) (rehash : S → S)
    (hrehash_id : ∀ s, rehash s = s)
    (xs : List T) (init : S) (k : Nat) :
    (xs.drop k).foldl step (rehash ((xs.take k).foldl step init)) =
      xs.foldl step init := by
  have h1 : rehash ((xs.take k).foldl step init) = (xs.take k).foldl step init :=
    hrehash_id _
  rw [h1, ← List.foldl_append, List.take_append_drop]

theorem jit_radix_key_double_agg_map_equiv
    {K T V : Type} [DecidableEq K] (step : (K → V) → T → (K → V))
    (xs : List T) (init : K → V) (k : Nat) (key : K) :
    (xs.drop k).foldl step ((xs.take k).foldl step init) key =
      xs.foldl step init key := by
  rw [← List.foldl_append, List.take_append_drop]

end TinylambOpt.VectorizedJoinAndPruning

-- C++ references to this model or its proof.
-- executor/detail/columnar_tlcs_block.cpp
-- executor/detail/columnar_tlcs_fast_path.cpp
-- executor/detail/flat_int_key_map.hpp
-- executor/detail/fused_compact_radix.cpp
-- executor/detail/fused_dense_split.cpp
-- executor/detail/fused_dense_split.hpp
-- executor/detail/fused_group_key_layout.cpp
-- executor/detail/fused_group_probe.cpp
-- executor/detail/fused_grouped_join.cpp
-- executor/detail/ordered_join_input.cpp
-- executor/detail/parallel_scan_filter.cpp
-- executor/detail/parallel_table_scan.cpp
-- executor/detail/relational_sort_limit.hpp
-- executor/detail/scan_filter.cpp
-- executor/detail/scan_filter.hpp
-- executor/detail/typed_column_eval.cpp
-- executor/detail/typed_stream_aggregate.hpp
-- executor/detail/typed_stream_aggregate_compact.cpp
-- executor/relational.cpp
-- executor/relational_projection.cpp
-- expression/jit.hpp
-- query/select_rewrites.hpp
-- query/tpch_select_rewrites.cpp
-- table/full_scan_iterator.cpp
-- table/tlcs_file.cpp
-- table/tlcs_file.hpp

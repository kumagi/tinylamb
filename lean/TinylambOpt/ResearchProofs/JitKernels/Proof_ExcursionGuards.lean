import Std
import TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

/-!
# Excursion guard audit

This module audits the exact goals in
`TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards`.  A prefix path records
the accumulator before the first addition and after every addition in a chunk.
The interval guard is therefore about every observed accumulator, not merely
the final result.

Six goals are proved: prefix extrema give an exact single-entry guard;
concatenation, accumulator-offset reuse, periodic unrolling, first-fault
resumption, and reverse scans preserve the modeled result under their stated
assumptions.  Four goals are concretely refuted: endpoint-only checks,
zero-based horizontal subtotals, predicate erasure, and endpoint bisection can
all change whether SQL execution reports an intermediate overflow.

All auxiliary lemmas are private to this audit namespace.  The proofs use only
the goal model and `Std`; no additional execution or overflow assumptions are
introduced.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.ExcursionGuardsAudit

open TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

/-- A minimum with a `getD` default is characterized by membership and a lower bound. -/
private theorem min_getD_eq_of_mem {xs : List Int} {m : Int}
    (hm : m ∈ xs) (hleast : ∀ x ∈ xs, m ≤ x) :
    xs.min?.getD 0 = m := by
  cases hmin : xs.min? with
  | none =>
      have hnil := List.min?_eq_none_iff.mp hmin
      simp [hnil] at hm
  | some n =>
      have hprops := List.min?_eq_some_iff.mp hmin
      have hmn := hprops.2 m hm
      have hnm := hleast n hprops.1
      simpa [hmin] using Int.le_antisymm hmn hnm

/-- A maximum with a `getD` default is characterized by membership and an upper bound. -/
private theorem max_getD_eq_of_mem {xs : List Int} {m : Int}
    (hm : m ∈ xs) (hgreatest : ∀ x ∈ xs, x ≤ m) :
    xs.max?.getD 0 = m := by
  cases hmax : xs.max? with
  | none =>
      have hnil := List.max?_eq_none_iff.mp hmax
      simp [hnil] at hm
  | some n =>
      have hprops := List.max?_eq_some_iff.mp hmax
      have hnm := hgreatest n hprops.1
      have hmn := hprops.2 m hm
      simpa [hmax] using Int.le_antisymm hnm hmn

/-- The defaulted minimum of a nonempty integer list belongs to that list. -/
private theorem min_getD_mem {xs : List Int} (hne : xs ≠ []) :
    xs.min?.getD 0 ∈ xs := by
  cases hmin : xs.min? with
  | none =>
      exact False.elim (hne (List.min?_eq_none_iff.mp hmin))
  | some m =>
      have hm := (List.min?_eq_some_iff.mp hmin).1
      simpa [hmin] using hm

/-- The defaulted maximum of a nonempty integer list belongs to that list. -/
private theorem max_getD_mem {xs : List Int} (hne : xs ≠ []) :
    xs.max?.getD 0 ∈ xs := by
  cases hmax : xs.max? with
  | none =>
      exact False.elim (hne (List.max?_eq_none_iff.mp hmax))
  | some m =>
      have hm := (List.max?_eq_some_iff.mp hmax).1
      simpa [hmax] using hm

/-- Every member of a nonempty list is at least its defaulted minimum. -/
private theorem min_getD_le {xs : List Int} {v : Int} (hv : v ∈ xs) :
    xs.min?.getD 0 ≤ v := by
  cases hmin : xs.min? with
  | none =>
      have hnil := List.min?_eq_none_iff.mp hmin
      simp [hnil] at hv
  | some m =>
      have hm := (List.min?_eq_some_iff.mp hmin).2 v hv
      simpa [hmin] using hm

/-- Every member of a list is at most its defaulted maximum. -/
private theorem max_getD_le {xs : List Int} {v : Int} (hv : v ∈ xs) :
    v ≤ xs.max?.getD 0 := by
  cases hmax : xs.max? with
  | none =>
      have hnil := List.max?_eq_none_iff.mp hmax
      simp [hnil] at hv
  | some m =>
      have hm := (List.max?_eq_some_iff.mp hmax).2 v hv
      simpa [hmax] using hm

/-- A prefix path always begins at zero and is therefore nonempty. -/
private theorem path_eq_zero_cons (xs : List Int) :
    ∃ tail, path xs = 0 :: tail := by
  cases xs with
  | nil => exact ⟨[], rfl⟩
  | cons x xs =>
      refine ⟨xs.scanl (· + ·) x, ?_⟩
      simp [path, List.scanl_cons]

/-- Every prefix path is nonempty because it includes the incoming accumulator. -/
private theorem path_ne_nil (xs : List Int) : path xs ≠ [] := by
  obtain ⟨tail, htail⟩ := path_eq_zero_cons xs
  simp [htail]

/-- Shifting a scan's initial accumulator shifts every recorded prefix. -/
private theorem scanl_add_shift (xs : List Int) (s : Int) :
    xs.scanl (· + ·) s = (path xs).map (fun v => s + v) := by
  induction xs generalizing s with
  | nil => simp [path]
  | cons x xs ih =>
      simp only [path, List.scanl_cons, List.map_cons, Int.add_zero]
      rw [ih (s + x), ih x]
      congr 1
      apply List.map_congr_left
      intro v hv
      omega

/-- Prepending one term shifts the tail of the prefix path by that term. -/
private theorem path_cons (x : Int) (xs : List Int) :
    path (x :: xs) = 0 :: (path xs).map (fun v => x + v) := by
  unfold path
  rw [List.scanl_cons]
  rw [show (0 : Int) + x = x by omega, scanl_add_shift xs x]

/-- The total of a chunk is one of its observed prefix accumulators. -/
private theorem sum_mem_path (xs : List Int) : xs.sum ∈ path xs := by
  induction xs with
  | nil => simp [path]
  | cons x xs ih =>
      rw [path_cons]
      exact List.mem_cons_of_mem 0
        (List.mem_map.mpr ⟨xs.sum, ih, by simp [List.sum_cons]⟩)

/-- The low prefix extremum is attained by the path. -/
private theorem low_mem (xs : List Int) : low xs ∈ path xs := by
  exact min_getD_mem (path_ne_nil xs)

/-- The high prefix extremum is attained by the path. -/
private theorem high_mem (xs : List Int) : high xs ∈ path xs := by
  exact max_getD_mem (path_ne_nil xs)

/-- The low prefix extremum bounds every observed accumulator from below. -/
private theorem low_le_of_mem {xs : List Int} {v : Int} (hv : v ∈ path xs) :
    low xs ≤ v := by
  exact min_getD_le hv

/-- The high prefix extremum bounds every observed accumulator from above. -/
private theorem le_high_of_mem {xs : List Int} {v : Int} (hv : v ∈ path xs) :
    v ≤ high xs := by
  exact max_getD_le hv

/-- Appending chunks keeps the left path and shifts the right path past the seam. -/
private theorem path_append (a b : List Int) :
    path (a ++ b) =
      path a ++ (path b).tail.map (fun v => a.sum + v) := by
  unfold path
  rw [List.scanl_append]
  rw [← List.sum_eq_foldl]
  rw [scanl_add_shift b a.sum]
  rw [← List.map_tail]
  rfl

/-- Membership in an appended path is membership in the left path or a shifted right prefix. -/
private theorem mem_path_append {a b : List Int} {v : Int} :
    v ∈ path (a ++ b) ↔
      v ∈ path a ∨ ∃ w ∈ path b, v = a.sum + w := by
  constructor
  · rw [path_append, List.mem_append]
    rintro (ha | hb)
    · exact Or.inl ha
    · rw [List.mem_map] at hb
      obtain ⟨w, hw, rfl⟩ := hb
      exact Or.inr ⟨w, List.mem_of_mem_tail hw, rfl⟩
  · rintro (ha | ⟨w, hw, rfl⟩)
    · rw [path_append]
      exact List.mem_append_left _ ha
    · rw [path_append]
      obtain ⟨tail, htail⟩ := path_eq_zero_cons b
      rw [htail] at hw ⊢
      simp only [List.tail_cons, List.mem_cons] at hw ⊢
      rcases hw with rfl | hw
      · exact List.mem_append_left _ (by simpa using sum_mem_path a)
      · exact List.mem_append_right _ (List.mem_map.mpr ⟨w, hw, rfl⟩)

/-- A defaulted minimum is unchanged by reversing a nonempty list. -/
private theorem min_getD_reverse (xs : List Int) (hne : xs ≠ []) :
    xs.reverse.min?.getD 0 = xs.min?.getD 0 := by
  apply min_getD_eq_of_mem
  · exact List.mem_reverse.mpr (min_getD_mem hne)
  · intro v hv
    exact min_getD_le (List.mem_reverse.mp hv)

/-- A defaulted maximum is unchanged by reversing a list. -/
private theorem max_getD_reverse (xs : List Int) (hne : xs ≠ []) :
    xs.reverse.max?.getD 0 = xs.max?.getD 0 := by
  apply max_getD_eq_of_mem
  · exact List.mem_reverse.mpr (max_getD_mem hne)
  · intro v hv
    exact max_getD_le (List.mem_reverse.mp hv)

/-- Subtracting a constant turns a list minimum into the complementary maximum. -/
private theorem min_getD_map_sub (xs : List Int) (s : Int) (hne : xs ≠ []) :
    (xs.map (fun v => s - v)).min?.getD 0 = s - xs.max?.getD 0 := by
  apply min_getD_eq_of_mem
  · exact List.mem_map.mpr ⟨xs.max?.getD 0, max_getD_mem hne, rfl⟩
  · intro v hv
    obtain ⟨w, hw, rfl⟩ := List.mem_map.mp hv
    have hbound := max_getD_le hw
    omega

/-- Subtracting a constant turns a list maximum into the complementary minimum. -/
private theorem max_getD_map_sub (xs : List Int) (s : Int) (hne : xs ≠ []) :
    (xs.map (fun v => s - v)).max?.getD 0 = s - xs.min?.getD 0 := by
  apply max_getD_eq_of_mem
  · exact List.mem_map.mpr ⟨xs.min?.getD 0, min_getD_mem hne, rfl⟩
  · intro v hv
    obtain ⟨w, hw, rfl⟩ := List.mem_map.mp hv
    have hbound := min_getD_le hw
    omega

/-- Right suffix sums are the complements of the forward prefix sums. -/
private theorem scanr_add_eq_map_sub (xs : List Int) :
    xs.scanr (fun x s => x + s) 0 =
      (path xs).map (fun p => xs.sum - p) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      rw [List.scanr_cons]
      have hfold : (x :: xs).foldr (fun x s => x + s) 0 = (x :: xs).sum :=
        List.sum_eq_foldr.symm
      rw [hfold, ih, path_cons, List.map_cons]
      simp only [List.sum_cons, Int.add_zero, sub_zero]
      apply List.map_congr_left
      intro a ha
      omega

/-- Reversing a chunk complements and reverses its prefix path. -/
private theorem path_reverse (xs : List Int) :
    path xs.reverse = ((path xs).map (fun v => xs.sum - v)).reverse := by
  change xs.reverse.scanl (· + ·) 0 = _
  have hflip : (flip (· + ·) : Int → Int → Int) = (fun x s => x + s) := by
    funext x s
    exact Int.add_comm s x
  rw [List.scanl_reverse, hflip, scanr_add_eq_map_sub]

/-- Concatenation combines low prefix extrema using the left chunk's total. -/
private theorem low_append (a b : List Int) :
    low (a ++ b) = min (low a) (a.sum + low b) := by
  apply min_getD_eq_of_mem
  · rcases Int.le_total (low a) (a.sum + low b) with hle | hle
    · rw [Int.min_eq_left hle]
      exact mem_path_append.mpr (Or.inl (low_mem a))
    · rw [Int.min_eq_right hle]
      exact mem_path_append.mpr (Or.inr ⟨low b, low_mem b, rfl⟩)
  · intro v hv
    rcases mem_path_append.mp hv with ha | ⟨w, hw, rfl⟩
    · exact Int.le_trans (Int.min_le_left _ _) (low_le_of_mem ha)
    · exact Int.le_trans (Int.min_le_right _ _)
        (Int.add_le_add_left (low_le_of_mem hw) a.sum)

/-- Concatenation combines high prefix extrema using the left chunk's total. -/
private theorem high_append (a b : List Int) :
    high (a ++ b) = max (high a) (a.sum + high b) := by
  apply max_getD_eq_of_mem
  · rcases Int.le_total (a.sum + high b) (high a) with hle | hle
    · rw [Int.max_eq_left hle]
      exact mem_path_append.mpr (Or.inl (high_mem a))
    · rw [Int.max_eq_right hle]
      exact mem_path_append.mpr (Or.inr ⟨high b, high_mem b, rfl⟩)
  · intro v hv
    rcases mem_path_append.mp hv with ha | ⟨w, hw, rfl⟩
    · exact Int.le_trans (le_high_of_mem ha) (Int.le_max_left _ _)
    · exact Int.le_trans (Int.add_le_add_left (le_high_of_mem hw) a.sum)
        (Int.le_max_right _ _)

/-- A safe cons cell certifies its incoming state and the shifted remaining chunk. -/
private theorem safe_cons_elim (lo hi s x : Int) (xs : List Int)
    (hs : safe lo hi s (x :: xs) = true) :
    lo ≤ s ∧ s ≤ hi ∧ safe lo hi (s + x) xs := by
  have hall := List.all_eq_true.mp hs
  have hzero : (lo ≤ s + 0 && s + 0 ≤ hi) = true :=
    hall 0 (by simp [path_cons])
  have htail : safe lo hi (s + x) xs = true := by
    apply List.all_eq_true.mpr
    intro v hv
    have hmem : x + v ∈ path (x :: xs) := by
      rw [path_cons]
      exact List.mem_cons_of_mem 0 (List.mem_map.mpr ⟨v, hv, rfl⟩)
    have hrange := hall (x + v) hmem
    simp only [Bool.and_eq_true, decide_eq_true_eq] at hrange ⊢
    omega
  simp only [Bool.and_eq_true, decide_eq_true_eq] at hzero
  exact ⟨by omega, by omega, htail⟩

/-- After a safe incoming state, the first fault of a cons cell is one position later. -/
private theorem faultAt_cons (lo hi s x : Int) (xs : List Int)
    (hzero : lo ≤ s ∧ s ≤ hi) :
    faultAt lo hi s (x :: xs) =
      (faultAt lo hi (s + x) xs).map (· + 1) := by
  have hbadzero : (s + 0 < lo || hi < s + 0) = false := by
    by_cases hlt : s + 0 < lo
    · omega
    · by_cases hgt : hi < s + 0
      · omega
      · simpa [hlt, hgt] using hzero
  unfold faultAt
  rw [path_cons, List.findIdx?_cons, hbadzero]
  simp only [Bool.false_eq_true, ite_false, List.findIdx?_map, Function.comp_def]
  apply congrArg (Option.map (· + 1))
  congr 1
  funext v
  have heq : s + (x + v) = (s + x) + v := by omega
  rw [heq]

/-- Prefix extrema are an exact guard for all additions in an ordered SQL chunk. -/
theorem prefixExcursionSingleGuard : PrefixExcursionSingleGuard.Goal := by
  intro lo hi s xs
  simp only [safe, List.all_eq_true, Bool.and_eq_true, decide_eq_true_eq]
  constructor
  · intro hs
    have hlo := hs (low xs) (low_mem xs)
    have hhi := hs (high xs) (high_mem xs)
    omega
  · rintro ⟨hlo, hhi⟩ v hv
    have hlow := low_le_of_mem hv
    have hhigh := le_high_of_mem hv
    omega

/-- Endpoint-only net-change validation is unsound for intermediate SQL accumulators. -/
theorem netChangeVectorGuard_refuted : ¬ NetChangeVectorGuard.Goal := by
  intro h
  have hbad := h (-5) 10 0 [-10, 10] (by decide) (by omega) (by decide)
  have hcounter : safe (-5) 10 0 [-10, 10] ≠ true := by decide
  exact hcounter hbad

/-- Compiled chunks can be fused from their totals and exact prefix extrema. -/
theorem excursionSummaryConcatenation : ExcursionSummaryConcatenation.Goal := by
  intro a b
  exact Prod.ext (low_append a b) (high_append a b)

/-- Raising an accumulator preserves safety when the upper margin still covers the high prefix. -/
theorem counterOffsetGuardReuse : CounterOffsetGuardReuse.Goal := by
  intro lo hi a b xs hab hs hhi
  simp only [safe, List.all_eq_true, Bool.and_eq_true, decide_eq_true_eq] at hs ⊢
  intro v hv
  have hrange := hs v hv
  have hhigh := le_high_of_mem hv
  omega

/-- A whole-chunk certificate does not license a middle subtotal restarted from zero. -/
theorem horizontalTreeSafety_refuted : ¬ HorizontalTreeSafety.Goal := by
  intro h
  have hbad := h (-150) 150 0 [-100] [200] [-100]
    (by omega) (by omega) (by decide)
  have hcounter : safe (-150) 150 0 [200] ≠ true := by decide
  exact hcounter hbad

/-- Deleting terms can create a new unsafe prefix, so predicate erasure needs a fresh guard. -/
theorem predicateErasureMaintainsRange_refuted :
    ¬ PredicateErasureMaintainsRange.Goal := by
  intro h
  have hbad := h 0 10 0 [10, -10] (fun x => x == -10) (by decide)
  have hcounter :
      safe 0 10 0 ([10, -10].filter (fun x => x == -10)) ≠ true := by decide
  exact hcounter hbad

/-- Periodic unrolling needs only one period's high prefix and the sign of its total drift. -/
theorem excursionPeriodUnroll : ExcursionPeriodUnroll.Goal := by
  intro xs n
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.flatten_cons]
      rw [high_append, ih]
      simp only [Int.natCast_succ]
      by_cases htotal : 0 ≤ xs.sum
      · have hn : 0 ≤ (n : Int) * xs.sum :=
          Int.mul_nonneg (Int.natCast_nonneg n) htotal
        rw [Int.max_eq_right hn]
        have hn1 : 0 ≤ ((n : Int) + 1) * xs.sum := by
          rw [Int.add_mul, Int.one_mul]
          omega
        rw [Int.max_eq_right hn1]
        have hsecond : high xs ≤ xs.sum + (high xs + (n : Int) * xs.sum) := by
          omega
        rw [Int.max_eq_right hsecond]
        rw [Int.add_mul, Int.one_mul]
        omega
      · have hneg : xs.sum ≤ 0 := by omega
        have hn : (n : Int) * xs.sum ≤ 0 :=
          Int.mul_nonpos_of_nonneg_of_nonpos (Int.natCast_nonneg n) hneg
        rw [Int.max_eq_left hn]
        simp only [Int.add_zero]
        have hn1 : ((n : Int) + 1) * xs.sum ≤ 0 := by
          rw [Int.add_mul, Int.one_mul]
          omega
        rw [Int.max_eq_left hn1]
        have hsecond : xs.sum + high xs ≤ high xs := by
          omega
        rw [Int.max_eq_left hsecond]
        simp

/-- Resuming after a certified prefix preserves the exact first failing addition. -/
theorem failIndexAfterCleanPrefix : FailIndexAfterCleanPrefix.Goal := by
  intro lo hi s a b
  induction a generalizing s with
  | nil => simp [faultAt]
  | cons x xs ih =>
      intro hs
      obtain ⟨hlo, hhi, htail⟩ := safe_cons_elim lo hi s x xs hs
      rw [List.cons_append]
      rw [faultAt_cons lo hi s x (xs ++ b) ⟨hlo, hhi⟩]
      rw [ih (s := s + x) htail]
      have hsum : s + x + xs.sum = s + (x + xs.sum) := by omega
      rw [hsum]
      simp only [Option.map_map, Function.comp_def]
      congr 1
      funext i
      simp only [List.length_cons, Int.natCast_add, Int.natCast_one]
      omega

/-- Reverse scans obtain both extrema by complementing the forward extrema. -/
theorem reverseChunkRange : ReverseChunkRange.Goal := by
  intro xs
  have hpath : path xs ≠ [] := path_ne_nil xs
  have hmap : (path xs).map (fun v => xs.sum - v) ≠ [] := by
    intro hnil
    have hnil' : path xs = [] := by simpa using hnil
    exact hpath hnil'
  constructor
  · change (path xs.reverse).min?.getD 0 = _
    rw [path_reverse, min_getD_reverse _ hmap, min_getD_map_sub _ _ hpath]
    rfl
  · change (path xs.reverse).max?.getD 0 = _
    rw [path_reverse, max_getD_reverse _ hmap, max_getD_map_sub _ _ hpath]
    rfl

/-- Endpoint bisection is unsound because later negative terms can hide an earlier overflow. -/
theorem endpointBisectionOfOverflow_refuted : ¬ EndpointBisectionOfOverflow.Goal := by
  intro h
  have hbad := h 0 0 [10, -10] 1 2 (by omega) (by decide) (by decide)
  have hcounter : ¬ 0 < 0 + ([10, -10].take 2).sum := by decide
  exact hcounter hbad

end TinylambOpt.ResearchProofs.JitKernels.ExcursionGuardsAudit

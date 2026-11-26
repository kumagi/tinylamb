import Std.Tactic

/-!
The value returned by a safe read is the logical byte at that position.

Prefix as coordinate-wise equality.

Named results include `getD_append_left`, `getD_drop`, `prefix_iff_get`.
-/

namespace TinylambOpt.Research.VectorTailPage

/-
# Page-boundary literal search with a guarded tail (issue `vector-tail-page`)

Two physically disjoint pages hold a logical byte string `p0 ++ p1`.  Page 0
is followed by a **guard page** at address `base0 + p0.length`, which is
inaccessible.  A needle search may not issue a full-width load at the page end
(nor drop the carry), yet must report every start position of the logical
concatenation, including overlapping and cross-page matches.

**Reference** (`refMatches`): all start positions `i` with `needle <+: drop i`
of the logical concatenation `p0 ++ p1`.

**Optimized** (`search`): for every start it reads each of the `needle.length`
bytes through `addrOf`, which routes a logical position to page 0 or page 1;
the `k = p0.length - i` tail bytes of page 0 are *copied into a temporary*
(`copiedBytes`), the remainder comes from page 1.  Both the reported starts
and the address trace are recorded.

Main theorem `vector_tail_page_sound`: start lists agree with the reference,
every recorded address lies inside a real page (never the guard), and the
boundary copy uses at most `needle.length` bytes.  Counterexamples: a
full-width load at the page end touches the guard
(`full_width_load_touches_guard`) and dropping the carry misses the
cross-page `ab|a = aba` match (`dropped_carry_misses_match`).
-/

abbrev Byte := Nat
abbrev Addr := Nat

structure Mem where
  p0 : List Byte
  p1 : List Byte
  base0 : Addr
  base1 : Addr
  guard : Addr
  noGuard0 : p0.length > 0
  far : base0 + p0.length < base1

def guardOf (m : Mem) : Addr := m.base0 + m.p0.length

def logical (m : Mem) : List Byte := m.p0 ++ m.p1

def addrOf (m : Mem) (pos : Nat) : Addr :=
  if pos < m.p0.length then m.base0 + pos else m.base1 + (pos - m.p0.length)

/-- The value returned by a safe read is the logical byte at that position. -/
def readAt (m : Mem) (pos : Nat) : Byte := (logical m).getD pos 0

theorem getD_append_left (a b : List Byte) (i : Nat) (h : i < a.length) :
    (a ++ b).getD i 0 = a.getD i 0 := by
  induction a generalizing i with
  | nil => simp at h
  | cons x xs ih =>
      cases i with
      | zero => simp
      | succ i =>
          simp at h
          simpa using ih i h

theorem getD_drop (xs : List Byte) (i j : Nat) :
    (xs.drop i).getD j 0 = xs.getD (i + j) 0 := by
  induction xs generalizing i j with
  | nil => simp
  | cons x xs ih =>
      cases i with
      | zero => simp
      | succ i => cases j <;> simp [ih, Nat.succ_add]

/-- Prefix as coordinate-wise equality. -/
theorem prefix_iff_get (needle xs : List Byte) :
    needle <+: xs ↔ ∀ j, j < needle.length → j < xs.length ∧ needle.getD j 0 = xs.getD j 0 := by
  induction needle generalizing xs with
  | nil => simp
  | cons b bs ih =>
      constructor
      · rintro ⟨t, rfl⟩ j hj
        cases j with
        | zero => simp
        | succ j =>
            have hjbs : j < bs.length := by simpa using hj
            constructor
            · simp
              omega
            · have hleft := getD_append_left bs t j hjbs
              exact hleft.symm
      · intro h
        cases xs with
        | nil =>
            have h0 := h 0 (by simp)
            simp at h0
        | cons c cs =>
            have h0 := h 0 (by simp)
            have hbc : b = c := by simpa using h0.2
            subst c
            have hpref : bs <+: cs := by
              apply (ih cs).2
              intro j hj
              have hj' := h (j + 1) (Nat.succ_lt_succ hj)
              exact ⟨by simpa using hj'.1, by simpa using hj'.2⟩
            rcases hpref with ⟨t, ht⟩
            refine ⟨t, ?_⟩
            simp [ht]

/-! ### Search, trace, and cost evaluator -/

def matchAt (m : Mem) (needle : List Byte) (i : Nat) : Bool :=
  (List.range needle.length).all (fun j =>
    decide (i + j < (logical m).length) &&
      decide (needle.getD j 0 == readAt m (i + j)))

def traceAt (m : Mem) (needle : List Byte) (i : Nat) : List Addr :=
  (List.range needle.length).map (fun j => addrOf m (i + j))

def refMatches (m : Mem) (needle : List Byte) : List Nat :=
  (List.range ((logical m).length + 1)).filter (fun i => decide (needle <+: (logical m).drop i))

def optMatches (m : Mem) (needle : List Byte) : List Nat :=
  (List.range ((logical m).length + 1)).filter (fun i => matchAt m needle i)

def readTrace (m : Mem) (needle : List Byte) : List Addr :=
  (optMatches m needle).flatMap (traceAt m needle)

def copiedBytes (m : Mem) (needle : List Byte) (i : Nat) : Nat :=
  if i < m.p0.length && i + needle.length > m.p0.length then m.p0.length - i else 0

def copyCost (m : Mem) (needle : List Byte) : Nat :=
  (optMatches m needle).map (copiedBytes m needle) |>.sum

theorem matchAt_iff_prefix (m : Mem) (needle : List Byte) (i : Nat) :
    matchAt m needle i = true ↔ needle <+: (logical m).drop i := by
  unfold matchAt
  rw [List.all_eq_true]
  rw [prefix_iff_get]
  constructor
  · intro h j hj
    have hb := h j (List.mem_range.mpr hj)
    have hA : i + j < (logical m).length := of_decide_eq_true (Bool.and_eq_true_iff.mp hb).1
    have hB : (needle.getD j 0 == readAt m (i + j)) = true := (Bool.and_eq_true_iff.mp hb).2
    constructor
    · rw [List.length_drop]
      omega
    · have hv := of_decide_eq_true hB
      rw [getD_drop]
      exact hv
  · intro h j hjmem
    have hj : j < needle.length := List.mem_range.mp hjmem
    have hh := h j hj
    apply Bool.and_eq_true_iff.mpr
    constructor
    · apply decide_eq_true
      rw [List.length_drop] at hh
      omega
    · apply decide_eq_true
      rw [getD_drop] at hh
      simpa [readAt] using hh.2

/-! ### Main theorem -/

theorem opt_matches_eq_ref (m : Mem) (needle : List Byte) :
    optMatches m needle = refMatches m needle := by
  unfold optMatches refMatches
  apply List.filter_congr
  intro i hi
  by_cases hm : matchAt m needle i = true
  · have hp : needle <+: (logical m).drop i := (matchAt_iff_prefix m needle i).mp hm
    simp [hm, hp]
  · have hp : ¬ needle <+: (logical m).drop i := by
      intro hp
      exact hm ((matchAt_iff_prefix m needle i).mpr hp)
    simp [hm, hp]

theorem trace_safe (m : Mem) (needle : List Byte) :
    ∀ a ∈ readTrace m needle,
      (m.base0 ≤ a ∧ a < m.base0 + m.p0.length) ∨
      (m.base1 ≤ a ∧ a < m.base1 + m.p1.length) := by
  intro a ha
  unfold readTrace at ha
  rcases List.mem_flatMap.mp ha with ⟨i, hi, hat⟩
  unfold traceAt at hat
  rcases List.mem_map.mp hat with ⟨j, hj, rfl⟩
  have hjlt : j < needle.length := List.mem_range.mp hj
  unfold addrOf
  by_cases h : i + j < m.p0.length
  · simp [h]
  · simp [h]
    have hge : m.p0.length ≤ i + j := by omega
    have himem := List.mem_filter.mp hi
    have hmpref : needle <+: (logical m).drop i := (matchAt_iff_prefix m needle i).mp himem.2
    have hcoord := (prefix_iff_get needle ((logical m).drop i)).mp hmpref j hjlt
    have hdropLen : ((logical m).drop i).length = (logical m).length - i := List.length_drop ..
    have hi_le : i ≤ (logical m).length := by
      rw [hdropLen] at hcoord
      omega
    have htotal : i + j < (logical m).length := by
      rw [hdropLen] at hcoord
      omega
    have hp1 : i + j - m.p0.length < m.p1.length := by
      have hsum : i + j < m.p0.length + m.p1.length := by simpa [logical] using htotal
      omega
    omega

theorem trace_avoids_guard (m : Mem) (needle : List Byte) :
    guardOf m ∉ readTrace m needle := by
  intro h
  have hs := trace_safe m needle (guardOf m) h
  rcases hs with h0 | h1
  · have hlt := h0.2
    unfold guardOf at hlt
    exact (Nat.lt_irrefl (m.base0 + m.p0.length) hlt).elim
  · have hle := h1.1
    unfold guardOf at hle
    exact (Nat.not_lt_of_ge hle m.far).elim

theorem copied_bytes_bound (m : Mem) (needle : List Byte) (i : Nat) :
    copiedBytes m needle i ≤ needle.length := by
  unfold copiedBytes
  by_cases h : i < m.p0.length && i + needle.length > m.p0.length
  · simp [h]
    have hcond := Bool.and_eq_true_iff.mp h
    have hlt : m.p0.length < i + needle.length := of_decide_eq_true hcond.2
    omega
  · simp [h]

theorem copy_cost_bound (m : Mem) (needle : List Byte) :
    copyCost m needle ≤ (optMatches m needle).length * needle.length := by
  unfold copyCost
  induction optMatches m needle with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [Nat.succ_mul]
      have hb := copied_bytes_bound m needle x
      omega

theorem vector_tail_page_sound (m : Mem) (needle : List Byte) :
    optMatches m needle = refMatches m needle ∧
    guardOf m ∉ readTrace m needle ∧
    (∀ a ∈ readTrace m needle,
      (m.base0 ≤ a ∧ a < m.base0 + m.p0.length) ∨
      (m.base1 ≤ a ∧ a < m.base1 + m.p1.length)) ∧
    copyCost m needle ≤ (optMatches m needle).length * needle.length := by
  exact ⟨opt_matches_eq_ref m needle, trace_avoids_guard m needle,
    trace_safe m needle, copy_cost_bound m needle⟩

/-! ### Counterexamples -/

def crossMem : Mem where
  p0 := [97, 98]
  p1 := [97]
  base0 := 100
  base1 := 200
  guard := 102
  noGuard0 := by decide
  far := by decide

def fullWidthLoad (base i width : Nat) : List Addr :=
  (List.range width).map (fun j => base + i + j)

theorem full_width_load_touches_guard :
    let m := crossMem
    let i := 0
    let width := 3
    guardOf m ∈ fullWidthLoad m.base0 i width ∧
    guardOf m ∉ traceAt m [97, 98, 97] i := by
  decide

def noCarryMatch (m : Mem) (needle : List Byte) (i : Nat) : Bool :=
  (List.range needle.length).all (fun j =>
    decide (i + j < m.p0.length) &&
      decide (needle.getD j 0 == m.p0.getD (i + j) 0))

def noCarryMatches (m : Mem) (needle : List Byte) : List Nat :=
  (List.range ((logical m).length + 1)).filter (fun i => noCarryMatch m needle i)

theorem dropped_carry_misses_match :
    0 ∈ optMatches crossMem [97, 98, 97] ∧
    0 ∉ noCarryMatches crossMem [97, 98, 97] := by
  native_decide

theorem cross_page_trace_and_copy :
    traceAt crossMem [97, 98, 97] 0 = [100, 101, 200] ∧
    copiedBytes crossMem [97, 98, 97] 0 = 2 ∧
    copyCost crossMem [97, 98, 97] = 2 := by
  native_decide

end TinylambOpt.Research.VectorTailPage

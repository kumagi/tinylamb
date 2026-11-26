import Std.Tactic

/-! Mid-batch switching between a sparse selection list and a dense bit mask.

    Reference computation: `runSparse` consumes a sorted list of candidate
    physical row numbers, emitting at most `k` of them and returning the
    physical row number where to resume.
    Optimized computation: `runDense` consumes a mask covering physical rows
    `[base, base + mask.length)`, skipping consumed rows (resume row `p`),
    emitting at most `k` rows and returning the same kind of resume row.

    The switch is legal under these hypotheses (see
    `executor/selection_vector.cpp`):
      * candidate rows are unique and strictly increasing (`StrictInc`);
      * every candidate is above the batch start (`gt`);
      * the resume position is always a physical row number (`p`, `lastOr`);
      * at the switch point, the unconsumed candidates equal the dense
        iterator's remaining selection (`inv`): `cands.drop k₁ =
        remainingDense base mask p₁`. This is the shared invariant of the
        two iterators.

    `k₁`/`k₂` model output-buffer capacity and resume: the theorem shows the
    switched run emits the same rows and resumes at the same physical row as
    running the sparse representation for `k₁ + k₂` rows from the start.
    `adaptive_selection_sound_from_dense` gives the corresponding comparison
    against a dense-only run. -/

namespace TinylambOpt.AdaptiveSelection

/-- Unique, strictly increasing physical row numbers. -/
def StrictInc : List Nat → Prop
  | [] | [_] => True
  | a :: b :: rest => a < b ∧ StrictInc (b :: rest)

theorem StrictInc_head (a : Nat) (xs : List Nat) (h : StrictInc (a :: xs)) :
    ∀ x ∈ xs, a < x := by
  induction xs generalizing a with
  | nil => simp
  | cons b ys ih =>
      simp [StrictInc] at h
      intro x hx
      simp at hx
      rcases hx with rfl | hx
      · exact h.1
      · exact Nat.lt_trans h.1 (ih b h.2 x hx)

theorem StrictInc_tail (a : Nat) (xs : List Nat) (h : StrictInc (a :: xs)) : StrictInc xs := by
  cases xs with
  | nil => simp [StrictInc]
  | cons b ys => simpa [StrictInc] using h.2

/-- Last emitted row, or the batch start when nothing was emitted. -/
def lastOr (p : Nat) (xs : List Nat) : Nat := xs.foldl (fun _ x => x) p

theorem lastOr_append (p : Nat) (l₁ l₂ : List Nat) :
    lastOr p (l₁ ++ l₂) = lastOr (lastOr p l₁) l₂ := by
  simp [lastOr, List.foldl_append]

theorem take_add (xs : List Nat) (n m : Nat) :
    xs.take n ++ (xs.drop n).take m = xs.take (n + m) := by
  induction n generalizing xs with
  | zero => simp
  | succ n ih =>
      cases xs with
      | nil => simp
      | cons x xs =>
          have hnm : Nat.succ n + m = Nat.succ (n + m) := by omega
          rw [hnm]
          simp [ih]

/-- Sparse reference iterator: emit at most `k` rows, return output and
    resume row. The cursor advances to the emitted physical row. -/
def runSparse (k p : Nat) : List Nat → List Nat × Nat
  | [] => ([], p)
  | r :: rs =>
      if k = 0 then ([], p)
      else
        let (out, p') := runSparse (k - 1) r rs
        (r :: out, p')

theorem runSparse_take (k p : Nat) (cands : List Nat)
    (sinc : StrictInc cands) (gt : ∀ r ∈ cands, p < r) :
    (runSparse k p cands).1 = cands.take k ∧
    (runSparse k p cands).2 = lastOr p (cands.take k) := by
  induction k generalizing p cands with
  | zero =>
      cases cands <;> simp [runSparse, lastOr]
  | succ k ih =>
      cases cands with
      | nil => simp [runSparse, lastOr]
      | cons r rs =>
          have hrs : ∀ x ∈ rs, r < x := StrictInc_head r rs sinc
          have stail := StrictInc_tail r rs sinc
          have h := ih r rs stail hrs
          rcases hRun : runSparse k r rs with ⟨out, pf⟩
          have hout : out = rs.take k := by simpa [hRun] using h.1
          have hpf : pf = lastOr r (rs.take k) := by simpa [hRun] using h.2
          constructor
          · simp [runSparse, hRun, hout]
          · simp [runSparse, hRun, hout, hpf, lastOr]

/-- Selection still to be emitted by the dense representation: set rows
    strictly after the resume position `p`. -/
def remainingDense (base : Nat) : List Bool → Nat → List Nat
  | [], _ => []
  | b :: bs, p => (if b && p < base then [base] else []) ++
      remainingDense (base + 1) bs p

/-- The next set row after `p`, together with the unscanned mask suffix. -/
def nextDense (base : Nat) : List Bool → Nat → Option (Nat × List Bool)
  | [], _ => none
  | b :: bs, p =>
      if b && p < base then some (base, bs)
      else nextDense (base + 1) bs p

theorem remainingDense_eq_of_lt (base : Nat) (mask : List Bool) (p q : Nat)
    (hpq : p ≤ q) (hq : q < base) :
    remainingDense base mask p = remainingDense base mask q := by
  induction mask generalizing base p q with
  | nil => simp [remainingDense]
  | cons b bs ih =>
      have hp : p < base := by omega
      have hq' : q < base + 1 := by omega
      simp [remainingDense, hp, hq, ih (base + 1) p q (by omega) hq']

theorem nextDense_head (base : Nat) (mask : List Bool) (p : Nat) :
    (match nextDense base mask p with
     | none => remainingDense base mask p = []
     | some (row, rest) =>
         remainingDense base mask p = row :: remainingDense (row + 1) rest row) := by
  induction mask generalizing base with
  | nil => simp [nextDense, remainingDense]
  | cons b bs ih =>
      by_cases h : b && p < base
      · have hpbase : p < base := by
          by_cases hb : b <;> simp [hb] at h <;> omega
        simp [nextDense, remainingDense, h]
        exact remainingDense_eq_of_lt (base + 1) bs p base (by omega) (by omega)
      · simp [nextDense, remainingDense, h]
        exact ih (base + 1)

/-- Dense optimized iterator: emit at most `k` rows and return output,
    resume row, updated base and remaining mask. -/
def runDense (k : Nat) (base : Nat) (mask : List Bool) (p : Nat) :
    List Nat × Nat × Nat × List Bool :=
  if k = 0 then ([], p, base, mask)
  else match nextDense base mask p with
       | none => ([], p, base, mask)
       | some (row, rest) =>
           let (out, pf, bf, mf) := runDense (k - 1) (row + 1) rest row
           (row :: out, pf, bf, mf)

theorem runDense_take (k base : Nat) (mask : List Bool) (p : Nat) :
    (runDense k base mask p).1 = (remainingDense base mask p).take k ∧
    (runDense k base mask p).2.1 = lastOr p ((remainingDense base mask p).take k) := by
  induction k generalizing base mask p with
  | zero => simp [runDense, lastOr]
  | succ k ih =>
      rcases hnd : nextDense base mask p with _ | ⟨row, rest⟩
      · have hd := nextDense_head base mask p
        simp [hnd] at hd
        simp [runDense, hnd, hd, lastOr]
      · have hd := nextDense_head base mask p
        simp [hnd] at hd
        have h := ih (row + 1) rest row
        rcases hRun : runDense k (row + 1) rest row with ⟨out, pf, bf, mf⟩
        have hout : out = (remainingDense (row + 1) rest row).take k := by
          simpa [hRun] using h.1
        have hpf : pf = lastOr row ((remainingDense (row + 1) rest row).take k) := by
          simpa [hRun] using h.2
        constructor
        · simp [runDense, hnd, hRun, hout, hd]
        · simp [runDense, hnd, hRun, hout, hpf, hd, lastOr]

/-- Sparse view of the not-yet-consumed candidates. -/
def remainingSparse (p : Nat) : List Nat → List Nat
  | [] => []
  | r :: rs => (if p < r then [r] else []) ++ remainingSparse p rs

theorem remainingSparse_self (p : Nat) (cands : List Nat)
    (h : ∀ r ∈ cands, p < r) : remainingSparse p cands = cands := by
  induction cands with
  | nil => simp [remainingSparse]
  | cons r rs ih =>
      have hr : p < r := h r (by simp)
      have hrs : ∀ x ∈ rs, p < x := by intro x hx; exact h x (by simp [hx])
      simp [remainingSparse, hr, ih hrs]

theorem adaptive_selection_sound (start k₁ k₂ : Nat) (cands : List Nat)
    (base : Nat) (mask : List Bool)
    (sinc : StrictInc cands)
    (gt : ∀ r ∈ cands, start < r)
    (inv : cands.drop k₁ = remainingDense base mask (lastOr start (cands.take k₁))) :
    let (out₁, p₁) := runSparse k₁ start cands
    let (out₂, p₂, _, _) := runDense k₂ base mask p₁
    (out₁ ++ out₂, p₂) = runSparse (k₁ + k₂) start cands := by
  dsimp only
  have hsparse := runSparse_take k₁ start cands sinc gt
  rcases hRunS : runSparse k₁ start cands with ⟨out₁, p₁⟩
  have ho1 : out₁ = cands.take k₁ := by simpa [hRunS] using hsparse.1
  have hp1 : p₁ = lastOr start (cands.take k₁) := by simpa [hRunS] using hsparse.2
  have hinv : cands.drop k₁ = remainingDense base mask p₁ := by simpa [hp1] using inv
  have hdense := runDense_take k₂ base mask p₁
  rcases hRunD : runDense k₂ base mask p₁ with ⟨out₂, p₂, bf, mf⟩
  have ho2tmp : out₂ = (remainingDense base mask p₁).take k₂ := by
    simpa [hRunD] using hdense.1
  have hp2tmp : p₂ = lastOr p₁ ((remainingDense base mask p₁).take k₂) := by
    simpa [hRunD] using hdense.2
  rw [← hinv] at ho2tmp hp2tmp
  have hall := runSparse_take (k₁ + k₂) start cands sinc gt
  rcases hRunAll : runSparse (k₁ + k₂) start cands with ⟨outall, pall⟩
  have hoall : outall = cands.take (k₁ + k₂) := by simpa [hRunAll] using hall.1
  have hpall : pall = lastOr start (cands.take (k₁ + k₂)) := by simpa [hRunAll] using hall.2
  apply Prod.ext
  · simp [ho1, ho2tmp, hoall, take_add]
  · simp [hp2tmp, hpall, ← take_add, lastOr_append, ← hp1]

/-- Dense-only processing from the start agrees with the sparse reference:
    this is the degenerate switch `k₁ = 0` (no prefix emitted). -/
theorem adaptive_selection_sound_from_dense (start k : Nat) (cands : List Nat)
    (base : Nat) (mask : List Bool)
    (sinc : StrictInc cands) (gt : ∀ r ∈ cands, start < r)
    (inv₀ : cands = remainingDense base mask start) :
    (runDense k base mask start).1 = cands.take k ∧
    (runDense k base mask start).2.1 = lastOr start (cands.take k) ∧
    (runDense k base mask start).1 = (runSparse k start cands).1 ∧
    (runDense k base mask start).2.1 = (runSparse k start cands).2 := by
  have h := runDense_take k base mask start
  have hs := runSparse_take k start cands sinc gt
  have h1 : (runDense k base mask start).1 = cands.take k := by
    simpa [← inv₀] using h.1
  have h2 : (runDense k base mask start).2.1 = lastOr start (cands.take k) := by
    simpa [← inv₀] using h.2
  exact ⟨h1, h2, h1.trans hs.1.symm, h2.trans hs.2.symm⟩

/-- Next physical offset, with zero representing the position before row 0. -/
def consumedLanes : Nat → List Bool → Nat
  | 0, _ | _, [] => 0
  | k+1, b :: bs => 1 + consumedLanes (if b then k else k+1) bs

def selected (base : Nat) : List Bool → List Nat
  | [] => []
  | b :: bs => (if b then [base] else []) ++ selected (base+1) bs

theorem physical_switch (mask : List Bool) (base k : Nat) :
    let n := consumedLanes k mask
    selected (base+n) (mask.drop n) = (selected base mask).drop k ∧
    selected base (mask.take n) = (selected base mask).take k := by
  induction mask generalizing base k with
  | nil => cases k <;> simp [selected, consumedLanes]
  | cons b bs ih =>
    cases k with
    | zero => simp [consumedLanes, selected]
    | succ k =>
      cases b <;> simp [consumedLanes, selected, Nat.add_comm 1, Nat.add_right_comm,
        Nat.add_assoc, List.drop_succ_cons, List.take_succ_cons] at *
      all_goals first | exact ih (base+1) k | exact ih (base+1) (k+1) | grind

theorem cursor_after_switch (bits : List Bool) (first next : Nat) :
    let offset := consumedLanes first bits
    offset + consumedLanes next (bits.drop offset) = consumedLanes (first+next) bits := by
  induction bits generalizing first next with
  | nil => cases first <;> cases next <;> rfl
  | cons b bs ih =>
    cases first with
    | zero => simp [consumedLanes]
    | succ k =>
      cases b <;> simp [consumedLanes, Nat.succ_add, Nat.add_comm 1,
        List.drop_succ_cons, Nat.succ_add, ih]

end TinylambOpt.AdaptiveSelection

-- C++ references to this model or its proof.
-- executor/selection_vector.hpp

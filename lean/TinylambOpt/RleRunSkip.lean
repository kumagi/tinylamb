import Std

/-!
  RLE run / visibility-mask scan skipping (C++: `RleDictRegionCountSum`,
  `RleIdxRegionCountSum` in `table/pax_chunk/pax_chunk_scan.cpp`).

  A run-structured column is a list of runs `(start, len, val)` covering the
  row ordinals `0 .. n-1`.  The reference semantics is *expansion then row
  evaluation*: expand every run into `len` copies of its constant `val`,
  apply the visibility mask row by row, and evaluate a (possibly failing,
  e.g. CAST) predicate on each visible row in ordinal order.  `valueAt` below
  is the lookup into that expansion; the reference `refScan` walks each
  visible ordinal and reads its value with `valueAt`.

  The optimized `runScan` intersects a run's row span `[start, start+len)`
  with the selection interval `[lo, hi)`, intersects that with the visibility
  mask, and — crucially — evaluates the predicate **once**, only when the
  run has at least one visible candidate.  A run whose rows are all invisible
  never evaluates a failing predicate, so a bad CAST on such a run cannot
  fail the query.

  Applicability conditions (modelled explicitly):
    * `runsCover 0 n runs` — every `len > 0`, every `start` equals the
      running offset (start-position check) and `start + len ≤ n`
      (length/fit check); both reference and optimized scan fail closed
      (`none`) when it is violated, matching the C++ `return false` gates.
    * `hpred` — within one run the predicate outcome is identical for every
      ordinal (the value is constant and the error verdict is decided by the
      first visible candidate), so evaluating it once is sound.
-/

namespace TinylambOpt.RleRunSkip

/-- Outcome of a row predicate. `err` models a failing CAST/conversion. -/
inductive PredResult where
  | ok : Bool → PredResult
  | err : PredResult
  deriving DecidableEq, Repr

/-- A run of `len` equal values `val` beginning at absolute ordinal `start`. -/
structure Run (V : Type) where
  start : Nat
  len   : Nat
  val   : V

/-- `runsCover pos n runs`: the runs are contiguous, have positive length,
    and cover exactly `[pos, n)`. This is the length / start-position check. -/
def runsCover (V : Type) : Nat → Nat → List (Run V) → Prop
  | pos, n, [] => pos = n
  | pos, n, r :: rs =>
      r.start = pos ∧ 0 < r.len ∧ r.start + r.len ≤ n ∧
        runsCover V (r.start + r.len) n rs

/-- Lookup of the expanded value at absolute ordinal `i`: the first run whose
    span contains `i` provides the value. -/
def valueAt {V : Type} : List (Run V) → Nat → Option V
  | [], _ => none
  | r :: rs, i =>
      if r.start ≤ i ∧ i < r.start + r.len then some r.val else valueAt rs i

/-- The value of any ordinal inside the head run is that run's value. -/
theorem valueAt_head {V : Type} (r : Run V) (rs : List (Run V)) {i : Nat}
    (hi1 : r.start ≤ i) (hi2 : i < r.start + r.len) :
    valueAt (r :: rs) i = some r.val := by
  simp [valueAt, hi1, hi2]

/-- The ordinal list `[a, b)` in increasing order. -/
def idxList (a b : Nat) : List Nat := (List.range (b - a)).map (fun k => a + k)

theorem mem_idxList {a b i : Nat} (h : i ∈ idxList a b) : a ≤ i ∧ i < b := by
  simp [idxList] at h
  rcases h with ⟨k, hk, rfl⟩
  exact ⟨Nat.le_add_right _ _, by omega⟩

theorem idxList_filter_vis (vis : Nat → Bool) (a b : Nat) :
    (idxList a b).filter vis = (idxList a b).filter fun i => vis i := rfl

/-- One-row step of the reference scan. `step i` is the predicate result at
    ordinal `i`; `err` aborts the whole scan (`none`). -/
def scanList (vis : Nat → Bool) (step : Nat → PredResult) : List Nat → Option (List Nat)
  | [] => some []
  | i :: is =>
      if !vis i then scanList vis step is
      else match step i with
        | .err => none
        | .ok false => scanList vis step is
        | .ok true => (scanList vis step is).map (fun rest => i :: rest)

/-- The run-level verdict for a list of candidates: an error only fires if at
    least one candidate is visible; otherwise the predicate is never called. -/
def grouped (vis : Nat → Bool) (res : PredResult) (l : List Nat) : Option (List Nat) :=
  let vs := l.filter vis
  if vs.isEmpty then some [] else
  match res with
  | .err => none
  | .ok false => some []
  | .ok true => some vs

/-- A constant predicate result on every list element reduces `scanList` to
    a single `grouped` decision. -/
theorem scanList_const (vis : Nat → Bool) (res : PredResult) (l : List Nat) :
    scanList vis (fun _ => res) l = grouped vis res l := by
  induction l with
  | nil => simp [scanList, grouped]
  | cons i is ih =>
      by_cases hvi : vis i
      · cases hvsis : (is.filter vis) with
        | nil =>
            cases res with
            | err => simp [scanList, hvi, grouped, hvsis]
            | ok b => cases b <;> simp [scanList, hvi, ih, grouped, hvsis]
        | cons v vs =>
            cases res with
            | err => simp [scanList, hvi, grouped, hvsis]
            | ok b => cases b <;> simp [scanList, hvi, ih, grouped, hvsis]
      · simp [scanList, ih, grouped, List.filter, hvi]

/-- If two step functions agree on the list, they produce the same scan. -/
theorem scanList_congr_step (vis : Nat → Bool) (s₁ s₂ : Nat → PredResult) (l : List Nat)
    (h : ∀ i ∈ l, s₁ i = s₂ i) : scanList vis s₁ l = scanList vis s₂ l := by
  induction l with
  | nil => rfl
  | cons i is ih =>
      have hi := h i (by simp)
      have his : ∀ j ∈ is, s₁ j = s₂ j := fun j hj => h j (by simp [hj])
      simp [scanList, hi, ih his]

/-- Sequential composition of two scan results (first error wins). -/
def seqConcat : Option (List Nat) → Option (List Nat) → Option (List Nat)
  | none, _ => none
  | some _, none => none
  | some a, some b => some (a ++ b)

/-- Reference segment for one run: expand + visibility + per-row evaluation. -/
def refSeg {V : Type} (runs : List (Run V)) (r : Run V)
    (vis : Nat → Bool) (lo hi : Nat) (pred : Nat → V → PredResult) : Option (List Nat) :=
  let a := max lo r.start
  let b := min hi (r.start + r.len)
  scanList vis (fun i => match valueAt runs i with
    | none => .err
    | some v => pred i v) (idxList a b)

/-- Optimized segment for one run: evaluate once at the first ordinal `a`. -/
def runSeg {V : Type} (r : Run V) (vis : Nat → Bool) (lo hi : Nat)
    (pred : Nat → V → PredResult) : Option (List Nat) :=
  let a := max lo r.start
  let b := min hi (r.start + r.len)
  let vs := (idxList a b).filter vis
  if vs.isEmpty then some [] else
  match pred a r.val with
  | .err => none
  | .ok false => some []
  | .ok true => some vs

/-- Per-run equality under the "predicate constant within a run" hypothesis. -/
theorem refSeg_eq_runSeg {V : Type} (r : Run V) (rs : List (Run V))
    (vis : Nat → Bool) (lo hi : Nat) (pred : Nat → V → PredResult)
    (hpred : ∀ i j, r.start ≤ i → i < r.start + r.len →
        r.start ≤ j → j < r.start + r.len → pred i r.val = pred j r.val) :
    refSeg (r :: rs) r vis lo hi pred = runSeg r vis lo hi pred := by
  unfold refSeg runSeg
  dsimp only
  let a := max lo r.start
  let b := min hi (r.start + r.len)
  have ha_start : r.start ≤ a := Nat.le_max_right _ _
  have hb_end : b ≤ r.start + r.len := Nat.min_le_right _ _
  have hstep : ∀ i ∈ idxList a b,
      (match valueAt (r :: rs) i with
        | none => PredResult.err
        | some v => pred i v) = pred a r.val := by
    intro i hi_mem
    have hi := mem_idxList hi_mem
    have hvi : valueAt (r :: rs) i = some r.val :=
      valueAt_head r rs (Nat.le_trans ha_start hi.1) (Nat.lt_of_lt_of_le hi.2 hb_end)
    rw [hvi]
    have ha_in : r.start ≤ a ∧ a < r.start + r.len := by
      constructor
      · exact ha_start
      · by_cases hab : a < b
        · exact Nat.lt_of_lt_of_le hab hb_end
        · have hba : b ≤ a := Nat.le_of_not_gt hab
          omega
    exact hpred i a (Nat.le_trans ha_start hi.1) (Nat.lt_of_lt_of_le hi.2 hb_end)
      ha_in.1 ha_in.2
  rw [scanList_congr_step vis _ (fun _ => pred a r.val) (idxList a b) hstep]
  rw [scanList_const]
  simp [a, b, grouped]

/-- Reference scan over the whole run list. -/
def refGo {V : Type} : List (Run V) → (Nat → Bool) → Nat → Nat →
    (Nat → V → PredResult) → Option (List Nat)
  | [], _, _, _, _ => some []
  | r :: rs, vis, lo, hi, pred =>
      seqConcat (refSeg (r :: rs) r vis lo hi pred) (refGo rs vis lo hi pred)

/-- Run-level scan over the whole run list. -/
def runGo {V : Type} : List (Run V) → (Nat → Bool) → Nat → Nat →
    (Nat → V → PredResult) → Option (List Nat)
  | [], _, _, _, _ => some []
  | r :: rs, vis, lo, hi, pred =>
      seqConcat (runSeg r vis lo hi pred) (runGo rs vis lo hi pred)

theorem refGo_eq_runGo {V : Type} (runs : List (Run V)) (vis : Nat → Bool)
    (lo hi : Nat) (pred : Nat → V → PredResult)
    (hpred : ∀ r ∈ runs, ∀ i j, r.start ≤ i → i < r.start + r.len →
        r.start ≤ j → j < r.start + r.len → pred i r.val = pred j r.val) :
    refGo runs vis lo hi pred = runGo runs vis lo hi pred := by
  induction runs with
  | nil => rfl
  | cons r rs ih =>
      have hr : ∀ i j, r.start ≤ i → i < r.start + r.len →
          r.start ≤ j → j < r.start + r.len → pred i r.val = pred j r.val :=
        hpred r (by simp)
      have hrs : ∀ q ∈ rs, ∀ i j, q.start ≤ i → i < q.start + q.len →
          q.start ≤ j → j < q.start + q.len → pred i q.val = pred j q.val :=
        fun q hq => hpred q (by simp [hq])
      simp [refGo, runGo, ih hrs, refSeg_eq_runSeg r rs vis lo hi pred hr]

open Classical

/-- The public reference and optimized scans; both fail closed when the
    length / offset checks (the C++ `return false` gates) do not pass. -/
noncomputable def refScan {V : Type} (runs : List (Run V)) (n lo hi : Nat)
    (vis : Nat → Bool) (pred : Nat → V → PredResult) : Option (List Nat) :=
  if runsCover V 0 n runs then refGo runs vis lo hi pred else none

noncomputable def runScan {V : Type} (runs : List (Run V)) (n lo hi : Nat)
    (vis : Nat → Bool) (pred : Nat → V → PredResult) : Option (List Nat) :=
  if runsCover V 0 n runs then runGo runs vis lo hi pred else none

/-- Main soundness theorem: run-level evaluation with a visibility mask and a
    single predicate evaluation per (visible) run equals expansion + row-level
    evaluation with the visibility mask. -/
theorem rle_run_skip_sound {V : Type} (runs : List (Run V)) (n lo hi : Nat)
    (vis : Nat → Bool) (pred : Nat → V → PredResult)
    (hcover : runsCover V 0 n runs)
    (hpred : ∀ r ∈ runs, ∀ i j, r.start ≤ i → i < r.start + r.len →
        r.start ≤ j → j < r.start + r.len → pred i r.val = pred j r.val) :
    refScan runs n lo hi vis pred = runScan runs n lo hi vis pred := by
  simp [refScan, runScan, hcover, refGo_eq_runGo runs vis lo hi pred hpred]

/-- Every ordinal emitted by the optimized scan lies in the selection interval
    and is visible: the mask intersection never manufactures a row. -/
theorem runSeg_output_in_bounds {V : Type} (r : Run V) (vis : Nat → Bool)
    (lo hi : Nat) (pred : Nat → V → PredResult) {out : List Nat}
    (hres : runSeg r vis lo hi pred = some out) :
    ∀ i ∈ out, lo ≤ i ∧ i < hi ∧ vis i = true := by
  let a := max lo r.start
  let b := min hi (r.start + r.len)
  let l := idxList a b
  have hrun : runSeg r vis lo hi pred = grouped vis (pred a r.val) l := by
    simp [runSeg, grouped, a, b, l]
  rw [hrun] at hres
  cases hvl : l.filter vis with
  | nil =>
      simp [grouped, hvl] at hres
      subst out
      intro i hi'
      cases hi'
  | cons v vs =>
      cases hp : pred a r.val with
      | err =>
          simp [grouped, hvl, hp] at hres
      | ok b =>
          cases b with
          | false =>
              simp [grouped, hvl, hp] at hres
              subst out
              intro i hi'
              cases hi'
          | true =>
              simp [grouped, hvl, hp] at hres
              subst out
              intro i hi_mem
              have hfilter : i ∈ l.filter vis := by
                rw [hvl]
                exact hi_mem
              have hf := List.mem_filter.mp hfilter
              have hidx := mem_idxList hf.1
              constructor
              · exact Nat.le_trans (Nat.le_max_left _ _) hidx.1
              · constructor
                · exact Nat.lt_of_lt_of_le hidx.2 (Nat.min_le_left _ _)
                · exact hf.2

end TinylambOpt.RleRunSkip

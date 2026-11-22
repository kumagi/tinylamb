import Std.Tactic

namespace TinylambOpt.DfaBlockSummary

/-- Formal model of DFA block summaries for existence search on one long row.

    Assumptions:
      * Only existence ("does any prefix match") is summarized; captures and
        capture priority are out of scope.  The DFA is total on Nat states and
        consumes one byte per transition (byte semantics, no folding flags).
      * A summary maps a start state to the pair (state after the chunk,
        "accept was seen at some nonempty position while scanning it").  The
        end state is what makes cross-boundary matches visible: OR-ing only
        the per-chunk flags would miss `ab` split into `a|b`.
      * Chunks are composed strictly in input order.  Empty chunks are the
        identity with flag false; empty-string acceptance at the initial state
        is added separately to the final flag, so `a*` still accepts an empty
        row.  Row boundaries reset the start state (summaries are never
        composed across different rows).
      * The state width is finite in C++ but the theorem is over Nat states,
        so it applies to any width including a 64-bit word.

    C++ realization: `expression/jit.cpp` summarizes a block of bytes by a
    compact transition/accept mask, then folds block masks left-to-right in a
    parallel scan.  The theorems below prove the monoid laws (identity and
    associativity), the homomorphism from byte-chunk concatenation to summary
    composition, and that the folded flag (plus initial-state acceptance)
    equals sequential existence search on the whole row. -/

structure DFA where
  step : Nat → UInt8 → Nat
  accept : Nat → Bool

/-- A summary is just a mapping from an incoming state to (outgoing state,
    "accept seen after at least one consumed byte"). -/
abbrev Summary := Nat → Nat × Bool

def idSummary : Summary := fun q => (q, false)

def stepSummary (d : DFA) (b : UInt8) : Summary :=
  fun q => (d.step q b, d.accept (d.step q b))

/-- Compose the summary of the first chunk with the summary of the second.
    The accept flag is the OR of the two chunks, while the end state flows
    from the first chunk into the second. -/
def compose (s₁ s₂ : Summary) : Summary :=
  fun q =>
    let (m, f) := s₁ q
    let (m', f') := s₂ m
    (m', f || f')

def summaryOf (d : DFA) : List UInt8 → Summary
  | [] => idSummary
  | b :: bs => compose (stepSummary d b) (summaryOf d bs)

theorem id_compose_left (s : Summary) : compose idSummary s = s := by
  funext q
  simp [compose, idSummary]

theorem id_compose_right (s : Summary) : compose s idSummary = s := by
  funext q
  dsimp [compose, idSummary]
  rcases s q with ⟨m, f⟩
  simp

theorem compose_assoc (s₁ s₂ s₃ : Summary) :
    compose (compose s₁ s₂) s₃ = compose s₁ (compose s₂ s₃) := by
  funext q
  dsimp [compose]
  rcases s₁ q with ⟨m, f⟩
  rcases s₂ m with ⟨m', f'⟩
  rcases s₃ m' with ⟨m'', f''⟩
  simp [Bool.or_assoc]

/-- Chunk concatenation is a monoid homomorphism into summary composition. -/
theorem summary_append (d : DFA) (xs ys : List UInt8) :
    summaryOf d (xs ++ ys) = compose (summaryOf d xs) (summaryOf d ys) := by
  induction xs with
  | nil =>
      funext q
      simp [summaryOf, compose, idSummary]
  | cons b bs ih =>
      simp [summaryOf, ih]
      rw [compose_assoc]

/-- Sequential reference scan: final state plus whether accept was reached at
    any position after consuming at least one byte. -/
def scan (d : DFA) (q : Nat) : List UInt8 → Nat × Bool
  | [] => (q, false)
  | b :: bs =>
      let r := d.step q b
      let (e, f) := scan d r bs
      (e, d.accept r || f)

def acceptingAfter (d : DFA) (q : Nat) : List UInt8 → Bool
  | [] => false
  | b :: bs => d.accept (d.step q b) || acceptingAfter d (d.step q b) bs

/-- Existence search including acceptance of the empty string at the start
    state (row start / `^` handling is encoded by the caller's start state). -/
def acceptingPrefix (d : DFA) (q : Nat) : List UInt8 → Bool
  | [] => d.accept q
  | b :: bs => d.accept q || acceptingPrefix d (d.step q b) bs

theorem summary_scan (d : DFA) (xs : List UInt8) (q : Nat) :
    summaryOf d xs q = scan d q xs := by
  induction xs generalizing q with
  | nil => rfl
  | cons b bs ih =>
      dsimp [summaryOf, stepSummary, compose, scan]
      rw [ih]

theorem scan_accept (d : DFA) (q : Nat) (xs : List UInt8) :
    (scan d q xs).2 = acceptingAfter d q xs := by
  induction xs generalizing q with
  | nil => rfl
  | cons b bs ih =>
      simp [scan, acceptingAfter, ih]

theorem acceptingPrefix_eq (d : DFA) (q : Nat) (xs : List UInt8) :
    acceptingPrefix d q xs = (d.accept q || acceptingAfter d q xs) := by
  induction xs generalizing q with
  | nil => simp [acceptingPrefix, acceptingAfter]
  | cons b bs ih =>
      simp only [acceptingPrefix, acceptingAfter]
      rw [ih]

/-- Fold of chunk summaries with an arbitrary accumulator. -/
theorem fold_summaries_acc (d : DFA) (chunks : List (List UInt8)) (acc : Summary) :
    (chunks.map (summaryOf d)).foldl compose acc =
      compose acc (summaryOf d chunks.flatten) := by
  induction chunks generalizing acc with
  | nil =>
      funext q
      dsimp [compose, idSummary, summaryOf]
      cases hq : acc q with
      | mk m f => simp [hq]
  | cons c cs ih =>
      simp [List.flatten]
      rw [ih, compose_assoc, summary_append]

theorem dfa_block_summary_sound (d : DFA) (chunks : List (List UInt8)) (start : Nat) :
    let folded := (chunks.map (summaryOf d)).foldl compose idSummary
    (folded start).2 || d.accept start = true ↔
      acceptingPrefix d start chunks.flatten = true := by
  dsimp
  have hfold : (chunks.map (summaryOf d)).foldl compose idSummary =
      summaryOf d chunks.flatten := by
    rw [fold_summaries_acc]
    exact id_compose_left _
  rw [hfold, summary_scan, scan_accept, acceptingPrefix_eq, Bool.or_comm]
  cases d.accept start <;> simp

end TinylambOpt.DfaBlockSummary

import Std.Tactic

/-!
Greedy candidate endpoints in descending order, including p and len.

Split at the first delimiter, returning (captured bytes, delimiter::tail). This is the
same first-delimiter run as `DelimitedCaptureJit.beforeDelimiter`.

Named results include `firstResult_append`, `descendingEnds_mem`,
`descendingEnds_decreasing`.
-/

namespace TinylambOpt.RegexCapturePriority

/-- Formal model of ordered backtracking with captures.

    Assumptions (restricted grammar, byte semantics, no flags):
      * Patterns are built from epsilon, a literal byte, ordered choice,
        sequential composition, a greedy "any run" atom that tries end
        positions from longest to shortest, and a delimited capture atom
        `capture d` which consumes bytes up to (and including) the next `d`.
      * The reference API fixes leftmost-start priority and source order of
        alternatives.  `firstResult` tries candidates in list order and stops
        at the first success, exactly like `DelimitedCaptureJit.firstResult`.
      * A capture is `Option (Nat × Nat)` (start, length): `none` means the
        capture did not participate, `some (p, 0)` means it participated with
        an empty span.  The two are observationally different.
      * When several captures participate, the leftmost one in evaluation
        order wins (`mergeOutcome`).  No look-around or back-references.

    C++ realization: `expression/jit.hpp` / `expression/jit.cpp` compile the
    restricted grammar to a backtracking VM whose split instructions preserve
    source order, and record the first participating capture as a packed
    (start,length) slice.  The theorem proves that the compiled VM and the
    ordered reference evaluator agree on the first match and its capture, for
    every start position, including empty input and a delimiter at the start
    of a capture. -/

structure Outcome where
  pos : Nat
  cap : Option (Nat × Nat)

def firstResult {α β} (f : α → Option β) : List α → Option β
  | [] => none
  | x :: xs => (f x).orElse (fun _ => firstResult f xs)

theorem firstResult_append {α β} (f : α → Option β) (xs ys : List α) :
    firstResult f (xs ++ ys) = (firstResult f xs).orElse (fun _ => firstResult f ys) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      simp [firstResult, ih]
      cases f x <;> rfl

inductive Re where
  | eps : Re
  | byte : UInt8 → Re
  | choose : Re → Re → Re
  | seq : Re → Re → Re
  | greedyAny : Re
  | capture : UInt8 → Re

inductive Code where
  | accept : Code
  | byte : UInt8 → Code
  | alt : Code → Code → Code
  | seq : Code → Code → Code
  | greedyAny : Code
  | capture : UInt8 → Code

def compile : Re → Code
  | .eps => .accept
  | .byte b => .byte b
  | .choose r₁ r₂ => .alt (compile r₁) (compile r₂)
  | .seq r₁ r₂ => .seq (compile r₁) (compile r₂)
  | .greedyAny => .greedyAny
  | .capture d => .capture d

def mergeOutcome (o₁ o₂ : Outcome) : Outcome :=
  { pos := o₂.pos, cap := o₁.cap <|> o₂.cap }

/-- Greedy candidate endpoints in descending order, including p and len. -/
def descendingEnds (len p : Nat) : List Nat :=
  (List.range' p (len + 1 - p)).reverse

theorem descendingEnds_mem (len p e : Nat) :
    e ∈ descendingEnds len p ↔ p ≤ e ∧ e ≤ len := by
  simp only [descendingEnds, List.mem_reverse, List.mem_range', Nat.one_mul]
  constructor
  · rintro ⟨i, hi, rfl⟩
    omega
  · rintro ⟨_, _⟩
    exact ⟨e-p, by omega, by omega⟩

theorem descendingEnds_decreasing (len p : Nat) :
    (descendingEnds len p).Pairwise (· > ·) := by
  simp [descendingEnds, List.pairwise_reverse, List.pairwise_lt_range']

/-- Split at the first delimiter, returning (captured bytes, delimiter::tail).
    This is the same first-delimiter run as `DelimitedCaptureJit.beforeDelimiter`. -/
def splitBefore (d : UInt8) : List UInt8 → List UInt8 × List UInt8
  | [] => ([], [])
  | b :: bs =>
      if b == d then ([], b :: bs)
      else
        let (cap, rest) := splitBefore d bs
        (b :: cap, rest)

def beforeDelimiter (d : UInt8) (bytes : List UInt8) : List UInt8 :=
  (splitBefore d bytes).1

/-- Capture a slice before the next delimiter.  `none` means no delimiter was
    found; `some (len, bytes, tail)` keeps an empty capture distinct from a
    non-participating one. -/
def captureAt (d : UInt8) (bytes : List UInt8) :
    Option (Nat × List UInt8 × List UInt8) :=
  let (cap, rest) := splitBefore d bytes
  if rest.isEmpty then none else some (cap.length, cap, rest)

theorem splitBefore_spec (d : UInt8) :
    ∀ xs : List UInt8, ∀ {cap rest}, splitBefore d xs = (cap, rest) →
      xs.take cap.length = cap ∧ xs.drop cap.length = rest
  | [], cap, rest, h => by
      have h' : cap = [] ∧ rest = [] := by simpa [splitBefore] using h
      rcases h' with ⟨rfl, rfl⟩
      simp
  | b :: bs, cap, rest, h => by
      by_cases hb : b == d
      · have h' : cap = [] ∧ b :: bs = rest := by simpa [splitBefore, hb] using h
        rcases h' with ⟨hcap, hrest⟩
        subst cap
        symm at hrest
        subst rest
        simp
      · cases hs : splitBefore d bs with
        | mk cap' rest' =>
            have h' : b :: cap' = cap ∧ rest' = rest := by
              simpa [splitBefore, hb, hs] using h
            rcases h' with ⟨hcap, hrest⟩
            symm at hcap
            symm at hrest
            subst cap
            subst rest
            have ih := splitBefore_spec d bs hs
            simp [ih.1, ih.2]

theorem drop_ne_nil_length_lt : ∀ (xs : List α) {n : Nat}, xs.drop n ≠ [] → n < xs.length
  | [], n, h => by simp at h
  | x :: xs, 0, h => by simp
  | x :: xs, n + 1, h => by
      simpa using h

/-- The captured span is exactly the bytes before the delimiter; its length is
    strictly smaller than the remaining input, so a delimiter really exists. -/
theorem captureAt_slice (d : UInt8) (bytes : List UInt8)
    {len cap tail} (h : captureAt d bytes = some (len, cap, tail)) :
    len < bytes.length ∧ bytes.take len = cap ∧ cap = beforeDelimiter d bytes := by
  cases hs : splitBefore d bytes with
  | mk cap' rest' =>
      by_cases hempty : rest'.isEmpty
      · simp [captureAt, hs, hempty] at h
      · simp [captureAt, hs, hempty] at h
        rcases h with ⟨hlen, hcap, htail⟩
        have hspec := splitBefore_spec d bytes hs
        have hrest : rest' ≠ [] := by simpa using hempty
        refine ⟨?_, ?_, ?_⟩
        · have hdrop : bytes.drop cap'.length ≠ [] := by
            rw [hspec.2]
            exact hrest
          have hlt := drop_ne_nil_length_lt bytes hdrop
          simpa [hlen] using hlt
        · rw [← hlen, ← hcap]
          exact hspec.1
        · simp [beforeDelimiter, hs, hcap]

/-- Ordered reference evaluator (CPS so that the continuation is shared with
    the compiled code). -/
def searchK {γ} : Re → (Outcome → Option γ) → List UInt8 → Nat → Option γ
  | .eps, k, _, p => k ⟨p, none⟩
  | .byte b, k, s, p =>
      if s[p]? == some b then k ⟨p + 1, none⟩ else none
  | .choose r₁ r₂, k, s, p =>
      searchK r₁ k s p |>.orElse (fun _ => searchK r₂ k s p)
  | .seq r₁ r₂, k, s, p =>
      searchK r₁ (fun o₁ => searchK r₂ (fun o₂ => k (mergeOutcome o₁ o₂)) s o₁.pos) s p
  | .greedyAny, k, s, p =>
      firstResult (fun e => k ⟨e, none⟩) (descendingEnds s.length p)
  | .capture d, k, s, p =>
      match captureAt d (s.drop p) with
      | none => none
      | some (len, _, _) => k ⟨p + len + 1, some (p, len)⟩

/-- Compiled backtracking VM.  It has the same control-flow shape as the
    reference evaluator but is a separate definition over `Code`. -/
def execK {γ} : Code → (Outcome → Option γ) → List UInt8 → Nat → Option γ
  | .accept, k, _, p => k ⟨p, none⟩
  | .byte b, k, s, p =>
      if s[p]? == some b then k ⟨p + 1, none⟩ else none
  | .alt c₁ c₂, k, s, p =>
      execK c₁ k s p |>.orElse (fun _ => execK c₂ k s p)
  | .seq c₁ c₂, k, s, p =>
      execK c₁ (fun o₁ => execK c₂ (fun o₂ => k (mergeOutcome o₁ o₂)) s o₁.pos) s p
  | .greedyAny, k, s, p =>
      firstResult (fun e => k ⟨e, none⟩) (descendingEnds s.length p)
  | .capture d, k, s, p =>
      match captureAt d (s.drop p) with
      | none => none
      | some (len, _, _) => k ⟨p + len + 1, some (p, len)⟩

/-- Compilation preserves the ordered evaluator for every continuation. -/
theorem compiled_matches_reference (r : Re) {γ} (k : Outcome → Option γ)
    (s : List UInt8) (p : Nat) :
    searchK r k s p = execK (compile r) k s p := by
  induction r generalizing k p with
  | eps => rfl
  | byte b => rfl
  | choose r₁ r₂ ih₁ ih₂ =>
      simp [searchK, execK, compile, ih₁ k p, ih₂ k p]
  | seq r₁ r₂ ih₁ ih₂ =>
      simp only [searchK, execK, compile]
      rw [ih₁ (fun o₁ => searchK r₂ (fun o₂ => k (mergeOutcome o₁ o₂)) s o₁.pos) p]
      apply congrArg (fun m => execK (compile r₁) m s p)
      funext o₁
      rw [ih₂ (fun o₂ => k (mergeOutcome o₁ o₂)) o₁.pos]
  | greedyAny => rfl
  | capture d => rfl

/-- Existence search: try candidate start positions left to right. -/
def referenceSearch (r : Re) (s : List UInt8) : Option Outcome :=
  firstResult (fun p => searchK r (fun o => some o) s p) (List.range (s.length + 1))

def compiledSearch (r : Re) (s : List UInt8) : Option Outcome :=
  firstResult (fun p => execK (compile r) (fun o => some o) s p) (List.range (s.length + 1))

theorem regex_capture_priority_sound (r : Re) (s : List UInt8) :
    referenceSearch r s = compiledSearch r s := by
  unfold referenceSearch compiledSearch
  apply congrArg (fun f => firstResult f (List.range (s.length + 1)))
  funext p
  exact compiled_matches_reference r (fun o => some o) s p

/-- Recovering the captured slice from the (start,length) pair: the empty
    input and a delimiter at the capture start are covered by the same lemma,
    and `none` can never be confused with `some (_, 0)`. -/
theorem capture_from_result (d : UInt8) (s : List UInt8) (p : Nat) :
    (match captureAt d (s.drop p) with
     | none => True
     | some (len, cap, _) => (s.drop p).take len = cap) := by
  match h : captureAt d (s.drop p) with
  | none => simp
  | some (len, cap, tail) =>
      simp
      exact (captureAt_slice d (s.drop p) h).2.1

theorem greedy_backtracks_before_final_literal :
    descendingEnds 2 0 = [2, 1, 0] ∧
    ((compiledSearch (.seq .greedyAny (.byte 98)) [97, 98]).map Outcome.pos) = some 2 := by
  decide

/-- Return the starting offset as well as the end and capture. Only the
    first successful starting position is selected. -/
def searchSpan (r : Re) (s : List UInt8) : Option (Nat × Outcome) :=
  let run := fun p => execK (compile r) some s p
  ((List.range (s.length+1)).find? fun i => (run i).isSome).bind
    fun p => (run p).map (fun out => (p,out))

theorem searchSpan_iff (r : Re) (s : List UInt8) (p : Nat) (o : Outcome) :
    searchSpan r s = some (p,o) ↔ p ≤ s.length ∧ searchK r some s p = some o ∧
      ∀ i < p, searchK r some s i = none := by
  unfold searchSpan
  simp only [← compiled_matches_reference]
  change ((List.range (s.length+1)).find? (fun i => (searchK r some s i).isSome)).bind
    (fun q => (searchK r some s q).map (fun out => (q,out))) = some (p,o) ↔ _
  constructor
  · intro h
    cases he : (List.range (s.length+1)).find? (fun i => (searchK r some s i).isSome) with
    | none => simp [he] at h
    | some q =>
      obtain ⟨hq,hbound,hmin⟩ := List.find?_range_eq_some.mp he
      obtain ⟨out,hrun⟩ := Option.isSome_iff_exists.mp hq
      simp [he,hrun] at h
      obtain ⟨rfl,rfl⟩ := h
      exact ⟨by have := List.mem_range.mp hbound; omega, hrun, fun i hi => by simpa using hmin i hi⟩
  · rintro ⟨hp,ho,hm⟩
    have he : (List.range (s.length+1)).find? (fun i => (searchK r some s i).isSome) = some p :=
      List.find?_range_eq_some.mpr ⟨by simp [ho],List.mem_range.mpr (by omega),fun i hi => by simp [hm i hi]⟩
    simp [he,ho]

end TinylambOpt.RegexCapturePriority

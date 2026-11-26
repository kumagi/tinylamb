/-!
Sort direction for one ORDER BY term.

NULL placement for one ORDER BY term.

Named results include `termLess_trans`, `lexLess_trans`, `leKey_trans`.
-/

/-
  TinylambOpt/TopNBlockFenceExplicitTerms.lean

  Lexicographic fence for skipping the remaining Top-N blocks.

  Abstract semantics (stated explicitly):
    * an order key is a list of nullable integers; each order term fixes a
      direction (ASC/DESC) and a NULL placement (NULLS FIRST/LAST);
    * `termLess t a b` is the strict "a sorts before b" relation for one term;
      `lexLess terms a b` is its lexicographic extension (first differing
      term decides);
    * `leKey terms a b := a = b ∨ lexLess terms a b` means "a is at least as
      good as b" (a can still enter the result at or ahead of b);
    * a block advertises a conservative bound `bound` with
      `∀ row ∈ block, leKey terms bound row`.  It is assembled from the best
      possible value of each term independently (`leTermWise` below), so it
      need not correspond to any real row; this is still conservative.

  Soundness hypotheses (sufficient):
    (H1) the bound is at least as good as every row of the block;
    (H2) the fence is checked only against a fixed K-th threshold (issued
          after K rows are settled): without ties, `bound` must not sort
          before the threshold; with WITH TIES, `bound` must not be at or
          before the threshold.

  `topn_block_fence_sound` proves the block contributes no candidate row,
  so `improving (block ++ rest) = improving rest` for the reference filter.
  Boundary theorems cover mixed ASC/DESC, NULLS FIRST/LAST, ties (a block
  equal to the threshold must be retained), and impossible combinations.

  C++ realization: executor/topn.cpp.  Emit the threshold only after K rows
  are fixed; for WITH TIES use the non-strict (`leKey`) fence so that a
  block tied with the K-th row is still read.  Per-term extrema must use the
  term's direction and NULL placement.
-/

namespace TinylambOpt.TopNBlockFenceExplicitTerms

/-- Sort direction for one ORDER BY term. -/
inductive Dir where
  | asc | desc
  deriving DecidableEq, Repr

/-- NULL placement for one ORDER BY term. -/
inductive Nulls where
  | first | last
  deriving DecidableEq, Repr

/-- One ORDER BY term. -/
structure OrderTerm where
  dir : Dir
  nulls : Nulls
  deriving DecidableEq, Repr

/-- A composite order key. -/
abbrev Key := List (Option Int)

/-- Boolean kernel of the strict one-term order, honoring direction and
    NULLS FIRST/LAST. -/
def termLessB (t : OrderTerm) : Option Int → Option Int → Bool
  | none, none => false
  | none, some _ => t.nulls == Nulls.first
  | some _, none => t.nulls == Nulls.last
  | some x, some y => match t.dir with
    | Dir.asc => decide (x < y)
    | Dir.desc => decide (y < x)

/-- Strict "a sorts before b" for one term (the Prop is `termLessB = true`,
    so it is decidable by computation). -/
def termLess (t : OrderTerm) (a b : Option Int) : Prop := termLessB t a b = true

/-- Non-strict "a is at least as good as b" for one term. -/
def leTerm (t : OrderTerm) (a b : Option Int) : Prop := a = b ∨ termLess t a b

/-- Boolean kernel of the lexicographic strict order. -/
def lexLessB : List OrderTerm → Key → Key → Bool
  | [], [], [] => false
  | t :: ts, a :: as, b :: bs => termLessB t a b || (a == b && lexLessB ts as bs)
  | _, _, _ => false

/-- Lexicographic strict order on composite keys (equal lengths assumed). -/
def lexLess (terms : List OrderTerm) (a b : Key) : Prop := lexLessB terms a b = true

/-- Non-strict lexicographic order: better than, or equal to. -/
def leKey (terms : List OrderTerm) (a b : Key) : Prop := a = b ∨ lexLess terms a b

/-- Computable decidability: each Prop is either a Bool equality or a
    combination of decidable Props.  The `show` form lets typeclass search
    see through the user-defined names. -/
instance termLessDec (t : OrderTerm) (a b : Option Int) : Decidable (termLess t a b) :=
  show Decidable (termLessB t a b = true) from inferInstance

instance leTermDec (t : OrderTerm) (a b : Option Int) : Decidable (leTerm t a b) :=
  show Decidable (a = b ∨ termLess t a b) from inferInstance

instance lexLessDec (terms : List OrderTerm) (a b : Key) : Decidable (lexLess terms a b) :=
  show Decidable (lexLessB terms a b = true) from inferInstance

instance leKeyDec (terms : List OrderTerm) (a b : Key) : Decidable (leKey terms a b) :=
  show Decidable (a = b ∨ lexLess terms a b) from inferInstance

/-- One-term strict order is transitive (all NULL/direction combinations). -/
theorem termLess_trans (t : OrderTerm) {a b c : Option Int}
    (hab : termLess t a b) (hbc : termLess t b c) : termLess t a c := by
  cases t with
  | mk d n =>
      cases d <;> cases n <;> cases a <;> cases b <;> cases c <;>
        simp [termLess, termLessB] at hab hbc ⊢ <;> omega

/-- Lexicographic strict order is transitive. -/
theorem lexLess_trans {terms : List OrderTerm} {a b c : Key}
    (hab : lexLess terms a b) (hbc : lexLess terms b c) : lexLess terms a c := by
  induction terms generalizing a b c with
  | nil =>
      cases a <;> cases b <;> cases c <;> simp [lexLess, lexLessB] at hab
  | cons t ts ih =>
      cases a with
      | nil => simp [lexLess, lexLessB] at hab
      | cons a as =>
          cases b with
          | nil => simp [lexLess, lexLessB] at hab
          | cons b bs =>
              cases c with
              | nil => simp [lexLess, lexLessB] at hbc
              | cons c cs =>
                  simp [lexLess, lexLessB] at hab hbc ⊢
                  rcases hab with hltab | ⟨heqab, hltabTail⟩
                  · rcases hbc with hltbc | ⟨heqbc, hltbcTail⟩
                    · exact Or.inl (termLess_trans t hltab hltbc)
                    · subst c
                      exact Or.inl hltab
                  · rcases hbc with hltbc | ⟨heqbc, hltbcTail⟩
                    · subst b
                      exact Or.inl hltbc
                    · subst b
                      subst c
                      exact Or.inr (And.intro rfl (ih hltabTail hltbcTail))

/-- Transitivity of the non-strict order. -/
theorem leKey_trans {terms : List OrderTerm} {a b c : Key}
    (hab : leKey terms a b) (hbc : leKey terms b c) : leKey terms a c := by
  rcases hab with rfl | hltab
  · exact hbc
  · rcases hbc with rfl | hltbc
    · exact Or.inr hltab
    · exact Or.inr (lexLess_trans hltab hltbc)

/-- A non-strict step followed by a strict step is strict. -/
theorem leKey_trans_less {terms : List OrderTerm} {a b c : Key}
    (hab : leKey terms a b) (hbc : lexLess terms b c) : lexLess terms a c := by
  rcases hab with rfl | hltab
  · exact hbc
  · exact lexLess_trans hltab hbc

/-- Boolean kernel of the per-term bounds (best possible value of every
    term, independently). -/
def leTermWiseB : List OrderTerm → Key → Key → Bool
  | [], [], [] => true
  | t :: ts, a :: as, b :: bs => decide (leTerm t a b) && leTermWiseB ts as bs
  | _, _, _ => false

def leTermWise (terms : List OrderTerm) (a b : Key) : Prop :=
  leTermWiseB terms a b = true

instance leTermWiseDec (terms : List OrderTerm) (a b : Key) : Decidable (leTermWise terms a b) :=
  show Decidable (leTermWiseB terms a b = true) from inferInstance

/-- Assembling independent per-term bounds gives a conservative composite
    bound even when the assembled key is not a real row. -/
theorem leKey_of_leTermWise {terms : List OrderTerm} {a b : Key}
    (h : leTermWise terms a b) : leKey terms a b := by
  induction terms generalizing a b with
  | nil =>
      cases a <;> cases b <;> simp [leTermWise, leTermWiseB, leKey, lexLess, lexLessB] at h ⊢
  | cons t ts ih =>
      cases a with
      | nil => simp [leTermWise, leTermWiseB] at h
      | cons a as =>
          cases b with
          | nil => simp [leTermWise, leTermWiseB] at h
          | cons b bs =>
              simp [leTermWise, leTermWiseB] at h
              rcases h with ⟨h0, hrest⟩
              rcases h0 with heq | hlt
              · subst b
                rcases ih (by simpa [leTermWise, leTermWiseB] using hrest) with heqTail | hltTail
                · subst bs
                  unfold leKey
                  left
                  rfl
                · unfold leKey
                  right
                  simp [lexLess, lexLessB]
                  exact Or.inr hltTail
              · unfold leKey
                right
                simp [lexLess, lexLessB]
                exact Or.inl hlt

/-- A row is a Top-N candidate relative to the K-th threshold.  WITH TIES
    keeps rows equal to the threshold. -/
def candidate (terms : List OrderTerm) (threshold : Key) (ties : Bool) (r : Key) : Prop :=
  if ties then leKey terms r threshold else lexLess terms r threshold

instance candidateDec (terms : List OrderTerm) (threshold : Key) (ties : Bool) (r : Key) :
    Decidable (candidate terms threshold ties r) :=
  show Decidable (if ties then leKey terms r threshold else lexLess terms r threshold) from
    inferInstance

def improving (terms : List OrderTerm) (threshold : Key) (ties : Bool)
    (rows : List Key) : List Key :=
  rows.filter (fun r => decide (candidate terms threshold ties r))

/-- MAIN: the fence excludes every candidate in the block, hence skipping
    the block leaves the reference Top-N candidate filter unchanged. -/
theorem topn_block_fence_sound (terms : List OrderTerm) (block rest : List Key)
    (bound threshold : Key) (ties : Bool)
    (hbound : ∀ r ∈ block, leKey terms bound r)
    (hfence : if ties then ¬ leKey terms bound threshold
                      else ¬ lexLess terms bound threshold) :
    improving terms threshold ties block = [] ∧
    improving terms threshold ties (block ++ rest) =
      improving terms threshold ties rest := by
  have hnone : ∀ r ∈ block, ¬ candidate terms threshold ties r := by
    intro r hr
    by_cases ht : ties
    · simp [candidate, ht]
      intro hrt
      have hle : leKey terms bound threshold := leKey_trans (hbound r hr) hrt
      have hf : ¬ leKey terms bound threshold := by simpa [ht] using hfence
      exact hf hle
    · simp [candidate, ht]
      intro hrt
      have hlt : lexLess terms bound threshold := leKey_trans_less (hbound r hr) hrt
      have hf : ¬ lexLess terms bound threshold := by simpa [ht] using hfence
      exact hf hlt
  have hfilter : block.filter (fun r => decide (candidate terms threshold ties r)) = [] := by
    apply List.eq_nil_iff_forall_not_mem.mpr
    intro r hr
    have hm := List.mem_filter.mp hr
    exact hnone r hm.1 (by simpa using hm.2)
  constructor
  · exact hfilter
  · simp [improving, List.filter_append, hfilter]

/-- Common term configurations for the boundary examples. -/
def descTerm : OrderTerm := { dir := Dir.desc, nulls := Nulls.last }
def ascTerm : OrderTerm := { dir := Dir.asc, nulls := Nulls.last }

/-- Mixed ORDER BY a DESC, b ASC: same a, smaller b still improves. -/
theorem mixed_direction_row_improves :
    let terms := [descTerm, ascTerm]
    let threshold : Key := [some 5, some 3]
    let row : Key := [some 5, some 2]
    lexLess terms row threshold := by
  decide

/-- NULLS FIRST vs NULLS LAST are opposite for the same direction. -/
theorem nulls_first_and_last_are_opposite :
    termLess { dir := Dir.asc, nulls := Nulls.first } none (some 0) ∧
    termLess { dir := Dir.asc, nulls := Nulls.last } (some 0) none ∧
    ¬ termLess { dir := Dir.asc, nulls := Nulls.last } none (some 0) := by
  decide

/-- The per-term bound may be a combination that no row realizes; it is
    still at least as good as every row (conservative lexicographic bound). -/
theorem conservative_bound_need_not_be_a_row :
    let terms := [descTerm, ascTerm]
    let bound : Key := [some 5, some 0]
    let rows : List Key := [[some 5, some 2], [some 4, some 9]]
    leTermWise terms bound rows[0]! ∧ leTermWise terms bound rows[1]! ∧ bound ∉ rows := by
  decide

/-- WITH TIES: a block tied with the K-th row must be retained.  The strict
    fence would be true only when no row is at or before the threshold. -/
theorem ties_keep_equal_block :
    let terms := [descTerm, ascTerm]
    let bound : Key := [some 5, some 2]
    let threshold : Key := [some 5, some 2]
    leKey terms bound threshold ∧
    (if true then ¬ leKey terms bound threshold
              else ¬ lexLess terms bound threshold) = false ∧
    improving terms threshold true [bound] = [bound] := by
  decide

end TinylambOpt.TopNBlockFenceExplicitTerms

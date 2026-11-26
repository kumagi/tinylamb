import Std.Tactic

/-!
Reference acceptance derivation. `Matches r s` means the regex r matches the whole byte
string s; the existence-search prefilter is expressed by the candidate filter theorem
below, which looks for substrings in rows.

A literal is mandatory for a regex only if it is present in every derivation;
alternation intersects, repetitions with minimum 0 keep nothing.

Named results include `substring_append_left`, `substring_append_right`,
`mandatory_sound`.
-/

namespace TinylambOpt.RegexMandatoryLiteral

/-- Formal model of the regex mandatory-literal prefilter.

    Assumptions (all enforced by the restricted grammar below):
      * Patterns are byte based (no UTF-8 folding), case sensitive, and have no
        flags, look-around, or back-references.  If the C++ compiler cannot
        prove those conditions it must fall back to the unfiltered path.
      * The AST supports the empty regex, a literal byte, concatenation,
        ordered alternation, `star` and `repeatMin r n` ("r{n,}": at least n
        copies of r).  `repeatMin r 0` may match the empty string, so it has
        no mandatory literal.
      * Extraction is *conservative*: alternation keeps only literals common
        to every branch, star and zero-minimum repetition keep none.  Missing
        a literal is safe; inventing one is not.
      * Existence (not anchored) matching is modelled by `IsSubstring`: a
        literal must occur contiguously somewhere in the row.

    C++ realization: `common/regex_guard.cpp` extracts literal runs from a
    compiled regex and tests each candidate row with a substring search before
    running the full regex matcher.  The theorem below says that no row that
    the reference matcher accepts is discarded by that prefilter, hence a
    "filter by mandatory literals, then match" pipeline is observationally
    identical to matching every row. -/

inductive Regex where
  | empty : Regex
  | char : UInt8 → Regex
  | concat : Regex → Regex → Regex
  | alt : Regex → Regex → Regex
  | star : Regex → Regex
  | repeatMin : Regex → Nat → Regex

/-- Reference acceptance derivation.  `Matches r s` means the regex r matches
    the whole byte string s; the existence-search prefilter is expressed by
    the candidate filter theorem below, which looks for substrings in rows. -/
inductive Matches : Regex → List UInt8 → Prop where
  | empty : Matches .empty []
  | char (b : UInt8) : Matches (.char b) [b]
  | concat {r₁ r₂ s₁ s₂} : Matches r₁ s₁ → Matches r₂ s₂ →
      Matches (.concat r₁ r₂) (s₁ ++ s₂)
  | altL {r₁ r₂ s} : Matches r₁ s → Matches (.alt r₁ r₂) s
  | altR {r₁ r₂ s} : Matches r₂ s → Matches (.alt r₁ r₂) s
  | starNil {r} : Matches (.star r) []
  | starCons {r s t} : Matches r s → Matches (.star r) t →
      Matches (.star r) (s ++ t)
  | repZero {r} : Matches (.repeatMin r 0) []
  | repStar {r s} : Matches (.star r) s → Matches (.repeatMin r 0) s
  | repOne {r s t} : Matches r s → Matches (.star r) t →
      Matches (.repeatMin r 1) (s ++ t)
  | repSucc {r n s t} : Matches r s → Matches (.repeatMin r n) t →
      Matches (.repeatMin r (n + 1)) (s ++ t)

/-- A literal is mandatory for a regex only if it is present in every
    derivation; alternation intersects, repetitions with minimum 0 keep
    nothing. -/
def mandatory : Regex → List (List UInt8)
  | .empty => []
  | .char b => [[b]]
  | .concat r₁ r₂ => mandatory r₁ ++ mandatory r₂
  | .alt r₁ r₂ => (mandatory r₁).filter (fun l => l ∈ mandatory r₂)
  | .star _ => []
  | .repeatMin _ 0 => []
  | .repeatMin r (_ + 1) => mandatory r

/-- Contiguous occurrence (substring) of one byte string in another. -/
def IsSubstring (l s : List UInt8) : Prop :=
  ∃ pre suf, s = pre ++ l ++ suf

theorem substring_append_left {l s t : List UInt8} (h : IsSubstring l s) :
    IsSubstring l (s ++ t) := by
  rcases h with ⟨pre, suf, rfl⟩
  refine ⟨pre, suf ++ t, ?_⟩
  simp [List.append_assoc]

theorem substring_append_right {l s t : List UInt8} (h : IsSubstring l t) :
    IsSubstring l (s ++ t) := by
  rcases h with ⟨pre, suf, rfl⟩
  refine ⟨s ++ pre, suf, ?_⟩
  simp [List.append_assoc]

/-- Central soundness lemma: every mandatory literal occurs in every string
    accepted by the reference derivation. -/
theorem mandatory_sound {r : Regex} {s : List UInt8} (h : Matches r s) :
    ∀ l ∈ mandatory r, IsSubstring l s := by
  induction h with
  | empty => intro l hl; simp [mandatory] at hl
  | char b =>
      intro l hl
      simp [mandatory] at hl
      subst l
      exact ⟨[], [], by simp⟩
  | concat h₁ h₂ ih₁ ih₂ =>
      intro l hl
      simp [mandatory] at hl
      rcases hl with hl | hl
      · exact substring_append_left (ih₁ l hl)
      · exact substring_append_right (ih₂ l hl)
  | altL h ih =>
      intro l hl
      simp [mandatory] at hl
      exact ih l hl.1
  | altR h ih =>
      intro l hl
      simp [mandatory] at hl
      exact ih l hl.2
  | starNil => intro l hl; simp [mandatory] at hl
  | starCons h₁ h₂ ih₁ ih₂ => intro l hl; simp [mandatory] at hl
  | repZero => intro l hl; simp [mandatory] at hl
  | repStar _ _ => intro l hl; simp [mandatory] at hl
  | repOne h₁ h₂ ih₁ ih₂ =>
      intro l hl
      simp [mandatory] at hl
      exact substring_append_left (ih₁ l hl)
  | repSucc h₁ h₂ ih₁ ih₂ =>
      intro l hl
      simp [mandatory] at hl
      exact substring_append_left (ih₁ l hl)

/-- Executable substring test used by the prefilter. -/
def isPrefixBytes : List UInt8 → List UInt8 → Bool
  | [], _ => true
  | _, [] => false
  | b :: bs, c :: cs => (b == c) && isPrefixBytes bs cs

def containsSlice : List UInt8 → List UInt8 → Bool
  | needle, [] => needle.isEmpty
  | needle, hay@(_ :: rest) => isPrefixBytes needle hay || containsSlice needle rest

theorem isPrefixBytes_eq (pre s : List UInt8) :
    isPrefixBytes pre s = true ↔ ∃ t, s = pre ++ t := by
  induction pre generalizing s with
  | nil =>
      constructor
      · intro _; exact ⟨s, by simp⟩
      · intro _; rfl
  | cons b bs ih =>
      cases s with
      | nil => simp [isPrefixBytes]
      | cons c cs =>
          by_cases h : b = c
          · subst c
            simpa [isPrefixBytes] using (ih cs)
          · have hbeq : (b == c) = false := by simp [h]
            constructor
            · intro hp
              exfalso
              simp [isPrefixBytes, hbeq] at hp
            · rintro ⟨t, ht⟩
              simp only [List.cons_append, List.cons.injEq] at ht
              exact False.elim (h ht.1.symm)

theorem containsSlice_eq (needle hay : List UInt8) :
    containsSlice needle hay = true ↔ IsSubstring needle hay := by
  induction hay with
  | nil =>
      simp [containsSlice]
      constructor
      · intro hn
        subst needle
        exact ⟨[], [], by simp⟩
      · rintro ⟨pre, suf, hEq⟩
        have hlen : needle.length = 0 := by
          have hl := congrArg List.length hEq
          simp at hl
          omega
        have hnil : needle = [] := List.eq_nil_of_length_eq_zero hlen
        simp [hnil]
  | cons b bs ih =>
      by_cases hp : isPrefixBytes needle (b :: bs) = true
      · simp [containsSlice, hp]
        rcases (isPrefixBytes_eq needle (b :: bs)).mp hp with ⟨t, ht⟩
        exact ⟨[], t, ht⟩
      · simp [containsSlice, hp]
        constructor
        · intro htail
          rcases ih.mp htail with ⟨pre, suf, hs⟩
          exact ⟨b :: pre, suf, by simp [hs]⟩
        · rintro ⟨pre, suf, hEq⟩
          cases pre with
          | nil =>
              have hpref : isPrefixBytes needle (b :: bs) = true :=
                (isPrefixBytes_eq needle (b :: bs)).mpr ⟨suf, by simpa using hEq⟩
              exact False.elim (hp hpref)
          | cons a pre' =>
              have hbs : bs = pre' ++ needle ++ suf := by
                have ht := hEq
                simp only [List.cons_append, List.cons.injEq] at ht
                exact ht.2
              exact ih.mpr ⟨pre', suf, hbs⟩

def allMandatory : List (List UInt8) → List UInt8 → Bool
  | [], _ => true
  | l :: ls, row => containsSlice l row && allMandatory ls row

theorem allMandatory_eq (ls : List (List UInt8)) (row : List UInt8) :
    allMandatory ls row = true ↔ ∀ l ∈ ls, IsSubstring l row := by
  induction ls with
  | nil => simp [allMandatory]
  | cons l ls ih =>
      simp only [allMandatory, Bool.and_eq_true, ih, containsSlice_eq, List.mem_cons]
      constructor
      · intro h l' hmem
        rcases hmem with hEq | hmem
        · subst l'
          exact h.1
        · exact h.2 l' hmem
      · intro h
        exact ⟨h l (Or.inl rfl), fun l' hmem => h l' (Or.inr hmem)⟩

def keepCandidates (r : Regex) (rows : List (List UInt8)) : List (List UInt8) :=
  rows.filter (fun row => allMandatory (mandatory r) row)

/-- No accepted row is lost by the prefilter. -/
theorem mandatory_filter_keeps_matches (r : Regex) (rows : List (List UInt8))
    {row : List UInt8} (inRows : row ∈ rows) (acc : Matches r row) :
    row ∈ keepCandidates r rows := by
  simp only [keepCandidates, List.mem_filter]
  refine ⟨inRows, ?_⟩
  exact (allMandatory_eq _ _).2 (mandatory_sound acc)

/-- Composition theorem: with any reference decider, evaluating the regex on
    the prefiltered rows is the same as evaluating it on all rows. -/
theorem regex_mandatory_literal_sound (r : Regex) (rows : List (List UInt8))
    [DecidablePred (Matches r)] :
    (keepCandidates r rows).filter (Matches r) = rows.filter (Matches r) := by
  rw [keepCandidates, List.filter_filter]
  apply List.filter_congr
  intro row _
  by_cases hacc : Matches r row
  · have hkeep : allMandatory (mandatory r) row = true :=
      (allMandatory_eq _ _).2 (mandatory_sound hacc)
    simp [hacc, hkeep]
  · simp [hacc]

def SearchMatch (r : Regex) (row : List UInt8) : Prop :=
  ∃ mid, IsSubstring mid row ∧ Matches r mid

theorem mandatory_search_sound (r : Regex) (rows : List (List UInt8))
    [DecidablePred (SearchMatch r)] :
    (keepCandidates r rows).filter (SearchMatch r) = rows.filter (SearchMatch r) := by
  rw [keepCandidates, List.filter_filter]
  apply List.filter_congr
  intro row _
  by_cases h : SearchMatch r row
  · obtain ⟨mid, ⟨pre, post, rfl⟩, hm⟩ := h
    have hk : allMandatory (mandatory r) (pre ++ mid ++ post) = true := by
      apply (allMandatory_eq ..).2
      intro lit hl
      exact substring_append_left (substring_append_right (mandatory_sound hm lit hl))
    simp only [hk, Bool.and_true]
  · simp [h]

end TinylambOpt.RegexMandatoryLiteral

-- C++ references to this model or its proof.
-- common/regex_guard.cpp
-- common/regex_guard.hpp

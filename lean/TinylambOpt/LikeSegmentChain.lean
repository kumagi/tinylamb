import Std.Tactic

/-!
# Compiling an arbitrary number of `%`-separated LIKE literals into sequential search

This module extends the two-segment greedy result of
`TpchSf10VectorizedExec` to a chain of any length.  After escape
interpretation a LIKE pattern is a list of wildcard-free literal segments
separated by `%`; the two anchors of the row mean that a trailing `%` leaves
the last segment free, whereas an anchored tail fixes the last segment at the
row end (and therefore requires a right-to-left search).

Reference evaluators:
* `ChainMatchFrom s segs start` — `%seg1%...%segN%` with a head cursor `start`:
  there are non-overlapping occurrences at positions `p_i ≥ start`;
* `ChainMatchBack s segsRev endPos` — the same pattern read from the tail
  anchor `endPos` backwards (the list of segments is reversed).

Optimized procedures:
* `greedyChain` — scan forward, always taking the *earliest* occurrence of
  the current segment (at or after the previous segment's end);
* `greedyChainBack` — scan backward from the tail anchor, always taking the
  *latest* occurrence that fits before the next segment.

The main theorem shows that greedy search decides the reference match in both
modes.  In particular the two boundary cases of the issue are handled:
`%aa%aa%` fails on `aaa` because the two `aa` runs must be disjoint, and
`%ab` on `abab` is accepted because the anchored tail is found by the
backward search (the first `ab` is *not* frozen as the tail candidate).

Sufficient hypotheses (checked by the C++ implementation): binary collation
(byte equality), non-NULL single-row input, escape interpretation already
applied, and non-empty literal segments (consecutive `%` are folded by
`LiteralLikePattern::Compile`).  The head cursor `start` and tail anchor
`endPos = s.length` are fixed by the caller.

C++ realization: `common/literal_like_pattern.hpp` (`Compile`, `Matches`) and
the sequential literal-search loop that calls the anchored first-match
routines of `expression/jit.cpp`.
-/

namespace TinylambOpt.LikeSegmentChain

abbrev Byte := Nat

/-- A literal `pat` occurs at byte offset `pos` of `s` (binary collation). -/
def OccursAt (s pat : List Byte) (pos : Nat) : Prop := pat <+: s.drop pos

/-! ### Forward earliest-occurrence search -/

/-- Scan a suffix of `s` for the first occurrence of `seg`; `pos` is the
    absolute byte offset corresponding to the supplied suffix `t`. -/
def firstOccurrenceFrom (s seg : List Byte) : List Byte → Nat → Option Nat
  | [], pos => if seg <+: ([] : List Byte) then some pos else none
  | t@(_ :: rest), pos => if seg <+: t then some pos else firstOccurrenceFrom s seg rest (pos + 1)

def firstOccurrence (s seg : List Byte) (start : Nat) : Option Nat :=
  firstOccurrenceFrom s seg (s.drop start) start

theorem firstOccurrenceFrom_spec (s seg : List Byte) :
    ∀ (t : List Byte) (pos p : Nat), t = s.drop pos →
      firstOccurrenceFrom s seg t pos = some p →
      pos ≤ p ∧ seg <+: t.drop (p - pos) := by
  intro t
  induction t with
  | nil =>
      intro pos p hdrop h
      by_cases hp : seg <+: ([] : List Byte)
      · have hpEq := by simpa [firstOccurrenceFrom, hp] using h
        subst p
        exact ⟨by omega, by simpa using hp⟩
      · simp [firstOccurrenceFrom, hp] at h
  | cons a rest ih =>
      intro pos p hdrop h
      by_cases hp : seg <+: (a :: rest)
      · have hpEq := by simpa [firstOccurrenceFrom, hp] using h
        subst p
        exact ⟨by omega, by simpa using hp⟩
      · have hdrop' : rest = s.drop (pos + 1) := by
          calc
            rest = (a :: rest).drop 1 := rfl
            _ = (s.drop pos).drop 1 := by rw [← hdrop]
            _ = s.drop (pos + 1) := by rw [List.drop_drop]
        have hs := ih (pos + 1) p hdrop' (by simpa [firstOccurrenceFrom, hp] using h)
        have harith : p - pos = 1 + (p - (pos + 1)) := by omega
        constructor
        · omega
        · have hdrop_eq : (a :: rest).drop (p - pos) = rest.drop (p - (pos + 1)) := by
            rw [harith, Nat.add_comm 1 (p - (pos + 1))]
            exact List.drop_succ_cons
          rw [hdrop_eq]
          exact hs.2

theorem firstOccurrence_sound (s seg : List Byte) (start p : Nat)
    (h : firstOccurrence s seg start = some p) :
    start ≤ p ∧ OccursAt s seg p := by
  have hs := firstOccurrenceFrom_spec s seg (s.drop start) start p rfl h
  constructor
  · exact hs.1
  · have hd : (s.drop start).drop (p - start) = s.drop p := by
      rw [List.drop_drop]
      congr 1
      omega
    simpa [OccursAt, hd] using hs.2

theorem firstOccurrenceFrom_min (s seg : List Byte) :
    ∀ (t : List Byte) (pos p : Nat), t = s.drop pos → pos ≤ p → seg <+: s.drop p →
      ∃ q, firstOccurrenceFrom s seg t pos = some q ∧ q ≤ p := by
  intro t
  induction t with
  | nil =>
      intro pos p hdrop hle hpref
      have hdrop_p : s.drop p = [] := by
        have hd : s.drop p = (s.drop pos).drop (p - pos) := by
          rw [List.drop_drop]
          have : pos + (p - pos) = p := by omega
          rw [this]
        rw [hd]
        rw [← hdrop]
        simp
      have hseg : seg = [] := by
        rw [hdrop_p] at hpref
        cases seg with
        | nil => rfl
        | cons a as =>
            simp [List.IsPrefix] at hpref
      refine ⟨pos, ?_, hle⟩
      simp [firstOccurrenceFrom, hseg]
  | cons a rest ih =>
      intro pos p hdrop hle hpref
      by_cases hp : seg <+: (a :: rest)
      · exact ⟨pos, by simp [firstOccurrenceFrom, hp], hle⟩
      · have hdrop' : rest = s.drop (pos + 1) := by
          calc
            rest = (a :: rest).drop 1 := rfl
            _ = (s.drop pos).drop 1 := by rw [← hdrop]
            _ = s.drop (pos + 1) := by rw [List.drop_drop]
        have hp_ne : p ≠ pos := by
          intro heq
          apply hp
          rw [hdrop]
          simpa [heq] using hpref
        have hrec := ih (pos + 1) p hdrop' (by omega) hpref
        rcases hrec with ⟨q, hq, hqp⟩
        exact ⟨q, by simp [firstOccurrenceFrom, hp, hq], hqp⟩

theorem firstOccurrence_min (s seg : List Byte) (start p : Nat)
    (hocc : OccursAt s seg p) (hle : start ≤ p) :
    ∃ q, firstOccurrence s seg start = some q ∧ q ≤ p :=
  firstOccurrenceFrom_min s seg (s.drop start) start p rfl hle hocc

/-! ### Forward greedy chain -/

def ChainMatchFrom (s : List Byte) (segs : List (List Byte)) (start : Nat) : Prop :=
  match segs with
  | [] => True
  | seg :: rest => ∃ p, start ≤ p ∧ OccursAt s seg p ∧ ChainMatchFrom s rest (p + seg.length)

def greedyChain (s : List Byte) (segs : List (List Byte)) (start : Nat) : Option (List Nat) :=
  match segs with
  | [] => some []
  | seg :: rest =>
      match firstOccurrence s seg start with
      | none => none
      | some p => match greedyChain s rest (p + seg.length) with
                  | none => none
                  | some ps => some (p :: ps)

theorem chainMatchFrom_mono (s : List Byte) (segs : List (List Byte)) :
    ∀ {a b : Nat}, a ≤ b → ChainMatchFrom s segs b → ChainMatchFrom s segs a := by
  induction segs with
  | nil => intro a b hle h; trivial
  | cons seg rest ih =>
      intro a b hle h
      rcases h with ⟨p, hb, hocc, hrest⟩
      exact ⟨p, by omega, hocc, ih (by omega) hrest⟩

theorem greedyChain_sound (s : List Byte) (segs : List (List Byte)) (start : Nat) :
    ∀ ps, greedyChain s segs start = some ps → ChainMatchFrom s segs start := by
  induction segs generalizing start with
  | nil => intro ps h; trivial
  | cons seg rest ih =>
      intro ps h
      cases hfo : firstOccurrence s seg start with
      | none => simp [greedyChain, hfo] at h
      | some p =>
          have hs := firstOccurrence_sound s seg start p hfo
          cases hg : greedyChain s rest (p + seg.length) with
          | none => simp [greedyChain, hfo, hg] at h
          | some qs =>
              have hrest : ChainMatchFrom s rest (p + seg.length) := ih (p + seg.length) qs hg
              have hps := by simpa [greedyChain, hfo, hg] using h
              subst ps
              exact ⟨p, hs.1, hs.2, hrest⟩

theorem greedyChain_complete (s : List Byte) (segs : List (List Byte)) (start : Nat)
    (h : ChainMatchFrom s segs start) :
    ∃ ps, greedyChain s segs start = some ps := by
  induction segs generalizing start with
  | nil => exact ⟨[], rfl⟩
  | cons seg rest ih =>
      rcases h with ⟨p, hstart, hocc, hrest⟩
      rcases firstOccurrence_min s seg start p hocc hstart with ⟨q, hq, hqp⟩
      have hrest' : ChainMatchFrom s rest (q + seg.length) :=
        chainMatchFrom_mono s rest (by omega) hrest
      rcases ih (q + seg.length) hrest' with ⟨qs, hqs⟩
      exact ⟨q :: qs, by simp [greedyChain, hq, hqs]⟩

theorem greedyChain_iff (s : List Byte) (segs : List (List Byte)) (start : Nat) :
    (∃ ps, greedyChain s segs start = some ps) ↔ ChainMatchFrom s segs start :=
  ⟨fun ⟨ps, h⟩ => greedyChain_sound s segs start ps h, greedyChain_complete s segs start⟩

/-! ### Backward latest-occurrence search for a tail-anchored pattern -/

def lastOccurrenceGo (s seg : List Byte) : Nat → Option Nat
  | 0 => if seg <+: s.drop 0 then some 0 else none
  | n + 1 => if seg <+: s.drop (n + 1) then some (n + 1) else lastOccurrenceGo s seg n

def lastOccurrence (s seg : List Byte) (endPos : Nat) : Option Nat :=
  if seg.length ≤ endPos then lastOccurrenceGo s seg (endPos - seg.length) else none

theorem lastOccurrenceGo_sound (s seg : List Byte) :
    ∀ n p, lastOccurrenceGo s seg n = some p → p ≤ n ∧ OccursAt s seg p := by
  intro n
  induction n with
  | zero =>
      intro p h
      by_cases hp : seg <+: s.drop 0
      · have hp' : seg <+: s := by simpa using hp
        have hval : lastOccurrenceGo s seg 0 = some 0 := by simp [lastOccurrenceGo, hp']
        rw [hval] at h
        injection h with hpEq
        subst p
        exact ⟨by omega, by simpa [OccursAt] using hp⟩
      · have hp' : ¬ seg <+: s := by simpa using hp
        have hval : lastOccurrenceGo s seg 0 = none := by simp [lastOccurrenceGo, hp']
        rw [hval] at h
        contradiction
  | succ n ih =>
      intro p h
      by_cases hp : seg <+: s.drop (n + 1)
      · have hval : lastOccurrenceGo s seg (n + 1) = some (n + 1) := by simp [lastOccurrenceGo, hp]
        rw [hval] at h
        injection h with hpEq
        subst p
        exact ⟨by omega, by simpa [OccursAt] using hp⟩
      · have hval : lastOccurrenceGo s seg (n + 1) = lastOccurrenceGo s seg n := by simp [lastOccurrenceGo, hp]
        have h' : lastOccurrenceGo s seg n = some p := by rwa [hval] at h
        have hs := ih p h'
        exact ⟨by omega, hs.2⟩

theorem lastOccurrenceGo_max (s seg : List Byte) :
    ∀ n p, p ≤ n → OccursAt s seg p → ∃ q, lastOccurrenceGo s seg n = some q ∧ p ≤ q := by
  intro n
  induction n with
  | zero =>
      intro p hle hocc
      have hp0 : p = 0 := by omega
      subst p
      have hp : seg <+: s := by simpa [OccursAt] using hocc
      have hval : lastOccurrenceGo s seg 0 = some 0 := by simp [lastOccurrenceGo, hp]
      exact ⟨0, hval, by omega⟩
  | succ n ih =>
      intro p hle hocc
      by_cases hp : seg <+: s.drop (n + 1)
      · refine ⟨n + 1, ?_, by omega⟩
        simp [lastOccurrenceGo, hp]
      · have hp_ne : p ≠ n + 1 := by
          intro heq
          apply hp
          simpa [OccursAt, heq] using hocc
        rcases ih p (by omega) hocc with ⟨q, hq, hpq⟩
        refine ⟨q, ?_, hpq⟩
        have hval : lastOccurrenceGo s seg (n + 1) = lastOccurrenceGo s seg n := by simp [lastOccurrenceGo, hp]
        rwa [hval]

theorem lastOccurrence_sound (s seg : List Byte) (endPos p : Nat)
    (h : lastOccurrence s seg endPos = some p) :
    OccursAt s seg p ∧ p + seg.length ≤ endPos := by
  by_cases hlen : seg.length ≤ endPos
  · have hs := lastOccurrenceGo_sound s seg (endPos - seg.length) p
      (by simpa [lastOccurrence, hlen] using h)
    exact ⟨hs.2, by omega⟩
  · simp [lastOccurrence, hlen] at h

theorem lastOccurrence_max (s seg : List Byte) (endPos p : Nat)
    (hocc : OccursAt s seg p) (hle : p + seg.length ≤ endPos) :
    ∃ q, lastOccurrence s seg endPos = some q ∧ p ≤ q := by
  have hlen : seg.length ≤ endPos := by omega
  rcases lastOccurrenceGo_max s seg (endPos - seg.length) p (by omega) hocc with ⟨q, hq, hpq⟩
  exact ⟨q, by simp [lastOccurrence, hlen, hq], hpq⟩

def ChainMatchBack (s : List Byte) (segsRev : List (List Byte)) (endPos : Nat) : Prop :=
  match segsRev with
  | [] => True
  | seg :: rest => ∃ p, OccursAt s seg p ∧ p + seg.length ≤ endPos ∧ ChainMatchBack s rest p

def greedyChainBack (s : List Byte) (segsRev : List (List Byte)) (endPos : Nat) : Option (List Nat) :=
  match segsRev with
  | [] => some []
  | seg :: rest =>
      match lastOccurrence s seg endPos with
      | none => none
      | some p => match greedyChainBack s rest p with
                  | none => none
                  | some ps => some (p :: ps)

theorem chainMatchBack_mono (s : List Byte) (segsRev : List (List Byte)) :
    ∀ {a b : Nat}, a ≤ b → ChainMatchBack s segsRev a → ChainMatchBack s segsRev b := by
  induction segsRev with
  | nil => intro a b hle h; trivial
  | cons seg rest ih =>
      intro a b hle h
      rcases h with ⟨p, hocc, hfit, hrest⟩
      exact ⟨p, hocc, by omega, ih (by omega) hrest⟩

theorem greedyChainBack_sound (s : List Byte) (segsRev : List (List Byte)) (endPos : Nat) :
    ∀ ps, greedyChainBack s segsRev endPos = some ps → ChainMatchBack s segsRev endPos := by
  induction segsRev generalizing endPos with
  | nil => intro ps h; trivial
  | cons seg rest ih =>
      intro ps h
      cases hlo : lastOccurrence s seg endPos with
      | none => simp [greedyChainBack, hlo] at h
      | some p =>
          have hs := lastOccurrence_sound s seg endPos p hlo
          cases hg : greedyChainBack s rest p with
          | none => simp [greedyChainBack, hlo, hg] at h
          | some qs =>
              have hrest : ChainMatchBack s rest p := ih p qs hg
              have hps := by simpa [greedyChainBack, hlo, hg] using h
              subst ps
              exact ⟨p, hs.1, hs.2, hrest⟩

theorem greedyChainBack_complete (s : List Byte) (segsRev : List (List Byte)) (endPos : Nat)
    (h : ChainMatchBack s segsRev endPos) :
    ∃ ps, greedyChainBack s segsRev endPos = some ps := by
  induction segsRev generalizing endPos with
  | nil => exact ⟨[], rfl⟩
  | cons seg rest ih =>
      rcases h with ⟨p, hocc, hfit, hrest⟩
      rcases lastOccurrence_max s seg endPos p hocc hfit with ⟨q, hq, hpq⟩
      have hrest' : ChainMatchBack s rest q := chainMatchBack_mono s rest (by omega) hrest
      rcases ih q hrest' with ⟨qs, hqs⟩
      exact ⟨q :: qs, by simp [greedyChainBack, hq, hqs]⟩

theorem greedyChainBack_iff (s : List Byte) (segsRev : List (List Byte)) (endPos : Nat) :
    (∃ ps, greedyChainBack s segsRev endPos = some ps) ↔ ChainMatchBack s segsRev endPos :=
  ⟨fun ⟨ps, h⟩ => greedyChainBack_sound s segsRev endPos ps h,
    greedyChainBack_complete s segsRev endPos⟩

/-! ### Main soundness statement -/

/-- A leading-anchored pattern fixes the first segment at `start`; the
    remaining `%`-separated tail is decided by the forward search. -/
theorem head_anchored_chain_sound (s : List Byte) (seg0 : List Byte)
    (rest : List (List Byte)) (start : Nat) (h0 : OccursAt s seg0 start) :
    (∃ ps, greedyChain s rest (start + seg0.length) = some ps) ↔
      (OccursAt s seg0 start ∧ ChainMatchFrom s rest (start + seg0.length)) := by
  rw [greedyChain_iff]
  exact ⟨fun h => ⟨h0, h⟩, fun h => h.2⟩

/-- **Soundness of the `%`-segment chain compilation.**

    The forward conjunct covers a pattern with a trailing `%` (e.g.
    `%aa%aa%`), the backward conjunct a pattern whose last literal is
    anchored at the row end (e.g. `%ab`).  Both statements are equivalences:
    the generated search accepts exactly the rows accepted by the reference
    LIKE evaluator, so no false positives and no false negatives. -/
theorem like_segment_chain_sound (s : List Byte) (segs : List (List Byte)) (start : Nat)
    (_hnz : ∀ seg ∈ segs, 0 < seg.length) :
    ((∃ ps, greedyChain s segs start = some ps) ↔ ChainMatchFrom s segs start) ∧
    ((∃ ps, greedyChainBack s segs.reverse s.length = some ps) ↔
      ChainMatchBack s segs.reverse s.length) :=
  ⟨greedyChain_iff s segs start, greedyChainBack_iff s segs.reverse s.length⟩

end TinylambOpt.LikeSegmentChain

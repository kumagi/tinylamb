import Std.Tactic

/-!
`seg` occurs as a contiguous substring of `row`.

Ordered disjoint in-order occurrences starting at `start`.

Named results include `prefix_drop_le`, `occurs_iff`, `chainMatchFrom_cand`.
-/

namespace TinylambOpt.Research.LikeMultiPrefilter

/-
# Multi-LIKE prefilter from shared mandatory literals (issue `like-multi-prefilter`)

A predicate is a list of mandatory literal segments (the `%`-separated pieces
of a LIKE pattern) plus a negation flag.  Escaped wildcards have already been
interpreted, so every segment is a plain byte string; an empty segment (from
`%%`) is harmless.  The tail anchor is conservatively modelled by substring
occurrence (a real implementation additionally checks that the final segment
ends at the row end).  NULL input yields UNKNOWN and is never filtered.

**Reference** (`evalRef`): ordered disjoint in-order occurrence search for the
segments (`chainOccurs`), then the three-valued negation.

**Optimized** (`evalOpt`): a shared literal matcher computes candidate bits
from the union of mandatory literals; only candidates run the residual ordered
search.  A false candidate decides the predicate without the residual (flipped
for `NOT`).

**Aho–Corasick abstraction.**  C++ shares all literal search in one automaton;
this model replaces it by the conservative primitive `occurs` (one literal
substring test).  Sharing affects only the cost evaluator `skippedPairs`, never
truth values: a false candidate never rejects a true LIKE match, and the
residual restores exact semantics.

Main theorem `like_multi_prefilter_sound`: for any predicate list the
three-valued truth vectors agree.  Counterexamples: overlapping occurrences
of `%ab%ab%` must not be shared (`overlapping_occurrences_not_shared`), and a
`NOT` predicate inverts the filter direction (`not_predicate_inverts_filter`).
-/

abbrev Byte := Nat

inductive TV where
  | tfalse | ttrue | tnull
  deriving DecidableEq, Repr

structure Pred where
  segs : List (List Byte)
  neg : Bool
  deriving DecidableEq, Repr

/-! ### Literal occurrence and ordered disjoint search -/

theorem prefix_drop_le {seg row : List Byte} {p : Nat} (hne : seg ≠ [])
    (hp : seg <+: row.drop p) : p ≤ row.length := by
  by_cases hle : p ≤ row.length
  · exact hle
  · have hgt : row.length < p := by omega
    have hd : row.drop p = [] := by
      apply List.drop_eq_nil_of_le
      omega
    rw [hd] at hp
    cases seg with
    | nil => exact (hne rfl).elim
    | cons b bs => simp [List.IsPrefix] at hp

/-- `seg` occurs as a contiguous substring of `row`. -/
def occurs (seg row : List Byte) : Bool :=
  if seg.isEmpty then true
  else (List.range (row.length + 1)).any (fun p => decide (seg <+: row.drop p))

theorem occurs_iff (seg row : List Byte) : occurs seg row = true ↔ ∃ p, seg <+: row.drop p := by
  unfold occurs
  by_cases hempty : seg.isEmpty
  · have hseg : seg = [] := by simpa [List.isEmpty_iff] using hempty
    subst seg
    simp [occurs]
  · simp [hempty]
    constructor
    · rintro ⟨p, hp, hpref⟩
      exact ⟨p, hpref⟩
    · rintro ⟨p, hpref⟩
      have hne : seg ≠ [] := by
        intro h
        apply hempty
        simpa [h]
      have hle : p ≤ row.length := prefix_drop_le hne hpref
      exact ⟨p, Nat.lt_succ_of_le hle, hpref⟩

/-- Ordered disjoint in-order occurrences starting at `start`. -/
def chainMatchFrom (segs : List (List Byte)) (row : List Byte) (start : Nat) : Bool :=
  match segs with
  | [] => true
  | seg :: rest =>
      (List.range (row.length + 1)).any (fun p =>
        decide (start ≤ p) && decide (seg <+: row.drop p) &&
          chainMatchFrom rest row (p + seg.length))
  termination_by segs.length

def chainOccurs (segs : List (List Byte)) (row : List Byte) : Bool :=
  chainMatchFrom segs row 0

theorem chainMatchFrom_cand (segs : List (List Byte)) (row : List Byte) :
    ∀ start, chainMatchFrom segs row start = true →
      ∀ seg ∈ segs, occurs seg row = true := by
  induction segs with
  | nil => intro start h seg hs; simp at hs
  | cons s ss ih =>
      intro start h seg hs
      unfold chainMatchFrom at h
      rw [List.any_eq_true] at h
      rcases h with ⟨p, hp, hval⟩
      have hparts := Bool.and_eq_true_iff.mp hval
      have hparts2 := Bool.and_eq_true_iff.mp hparts.1
      have hseg : decide (s <+: row.drop p) = true := hparts2.2
      have htail : chainMatchFrom ss row (p + s.length) = true := hparts.2
      rcases List.mem_cons.mp hs with hsegEq | hs
      · subst seg
        exact (occurs_iff s row).mpr ⟨p, of_decide_eq_true hseg⟩
      · exact ih (p + s.length) htail seg hs

theorem chainOccurs_cand {segs : List (List Byte)} {row : List Byte} (h : chainOccurs segs row = true) :
    ∀ seg ∈ segs, occurs seg row = true :=
  chainMatchFrom_cand segs row 0 h

def tvNot : TV → TV
  | .tfalse => .ttrue
  | .ttrue => .tfalse
  | .tnull => .tnull

def tvOfBool (b : Bool) : TV := if b then .ttrue else .tfalse

def applyNeg (neg : Bool) (b : Bool) : TV := tvOfBool (if neg then !b else b)

/-- Reference three-valued truth of one predicate on one row. -/
def evalRef (p : Pred) (row : Option (List Byte)) : TV :=
  match row with
  | none => .tnull
  | some s => applyNeg p.neg (chainOccurs p.segs s)

/-! ### Prefilter + residual -/

/-- Shared candidate check: every mandatory literal occurs somewhere.  This
    is the abstracted Aho–Corasick candidate bit. -/
def candidate (p : Pred) (row : List Byte) : Bool :=
  p.segs.all (fun seg => occurs seg row)

/-- Optimized evaluation: false candidates skip the residual. -/
def evalOpt (p : Pred) (row : Option (List Byte)) : TV :=
  match row with
  | none => .tnull
  | some s =>
      if candidate p s then
        applyNeg p.neg (chainOccurs p.segs s)
      else
        tvOfBool p.neg

/-- Number of (row, predicate) pairs for which the residual is skipped. -/
def skippedPairs (ps : List Pred) (rows : List (Option (List Byte))) : Nat :=
  (rows.flatMap (fun r => ps.map (fun p =>
    match r with | some s => ! candidate p s | none => false))).count true

def residualPairs (ps : List Pred) (rows : List (Option (List Byte))) : Nat :=
  rows.length * ps.length - skippedPairs ps rows

theorem skipped_all_length (ps : List Pred) (rows : List (Option (List Byte))) :
    (rows.flatMap (fun r => ps.map (fun p =>
      match r with | some s => ! candidate p s | none => false))).length =
      ps.length * rows.length := by
  rw [List.length_flatMap]
  simp only [List.length_map]
  induction rows with
  | nil => simp
  | cons r rs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih, Nat.mul_succ]
      rw [Nat.add_comm]

theorem skippedPairs_le (ps : List Pred) (rows : List (Option (List Byte))) :
    skippedPairs ps rows ≤ rows.length * ps.length := by
  unfold skippedPairs
  rw [List.count_eq_length_filter]
  have hs : (List.filter (fun b => b == true)
      (rows.flatMap (fun r => ps.map (fun p =>
        match r with | some s => ! candidate p s | none => false)))).Sublist
      (rows.flatMap (fun r => ps.map (fun p =>
        match r with | some s => ! candidate p s | none => false))) := List.filter_sublist
  have hlen1 := hs.length_le
  have hlen2 := skipped_all_length ps rows
  rw [Nat.mul_comm] at hlen2
  omega

/-! ### Soundness -/

theorem evalOpt_eq_evalRef (p : Pred) (row : Option (List Byte)) :
    evalOpt p row = evalRef p row := by
  unfold evalOpt evalRef
  cases row with
  | none => rfl
  | some s =>
      cases hc : candidate p s
      · simp [hc]
        have hnot : chainOccurs p.segs s = false := by
          cases h : chainOccurs p.segs s
          · rfl
          · have hcand : candidate p s = true := by
              unfold candidate
              apply List.all_eq_true.mpr
              intro seg hseg
              exact chainOccurs_cand h seg hseg
            rw [hcand] at hc
            contradiction
        rw [hnot]
        unfold applyNeg
        cases p.neg <;> rfl
      · simp [hc]

theorem truth_vector_eq (ps : List Pred) (r : Option (List Byte)) :
    ps.map (fun p => evalOpt p r) = ps.map (fun p => evalRef p r) := by
  induction ps with
  | nil => rfl
  | cons p ps ih =>
      simp only [List.map_cons]
      rw [evalOpt_eq_evalRef, ih]

theorem like_multi_prefilter_sound (ps : List Pred) (rows : List (Option (List Byte))) :
    rows.map (fun r => ps.map (fun p => evalOpt p r)) =
      rows.map (fun r => ps.map (fun p => evalRef p r)) := by
  induction rows with
  | nil => rfl
  | cons r rs ih =>
      simp only [List.map_cons]
      rw [truth_vector_eq, ih]

/-! ### Counterexamples -/

/-- `%ab%ab%` needs two disjoint occurrences of `ab`.  On `ab` the shared
    literal matcher sees `ab` (candidate TRUE), but the ordered residual
    rejects: sharing the single overlapping occurrence would be wrong. -/
theorem overlapping_occurrences_not_shared :
    let p : Pred := { segs := [[97, 98], [97, 98]], neg := false }
    let row := [97, 98]
    candidate p row = true ∧
    chainOccurs p.segs row = false ∧
    evalRef p (some row) = .tfalse ∧
    evalOpt p (some row) = .tfalse := by
  native_decide

/-- A `NOT` predicate: when the candidate is false, the optimized evaluator
    must report TRUE.  A lowering that drops the inversion reports FALSE. -/
def naiveNotOpt (p : Pred) (row : Option (List Byte)) : TV :=
  match row with
  | none => .tnull
  | some s => if candidate p s then
      applyNeg p.neg (chainOccurs p.segs s) else .tfalse

theorem not_predicate_inverts_filter :
    let p : Pred := { segs := [[97, 98]], neg := true }
    let row := [97]
    candidate p row = false ∧
    evalOpt p (some row) = .ttrue ∧
    naiveNotOpt p (some row) = .tfalse ∧
    evalRef p (some row) = .ttrue := by
  native_decide

/-- NULL is UNKNOWN and is never filtered, even for a NOT predicate. -/
theorem null_is_unknown (p : Pred) :
    evalOpt p none = .tnull ∧ evalRef p none = .tnull ∧
    skippedPairs [p] [none] = 0 := by
  simp [evalOpt, evalRef, skippedPairs]

/-- Escape interpretation: an escaped `%` becomes a plain byte in a segment,
    not a wildcard.  The model receives already-interpreted segments. -/
theorem escaped_literal_is_a_segment :
    let p : Pred := { segs := [[37]], neg := false }
    evalRef p (some [37]) = .ttrue ∧ evalRef p (some [97, 37]) = .ttrue := by
  native_decide

/-- An empty segment (from a doubled `%`) is harmless. -/
theorem empty_segment_is_harmless :
    let p : Pred := { segs := [[], [97, 98]], neg := false }
    evalRef p (some [97, 98]) = .ttrue := by
  native_decide

end TinylambOpt.Research.LikeMultiPrefilter

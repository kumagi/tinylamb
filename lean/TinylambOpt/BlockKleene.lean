/-!
The four observable outcomes of predicate evaluation.

Kleene NOT; ERROR is absorbing.

Named results include `aeval_contains_ceval`, `block_kleene_sound`,
`block_all_pass_sound`.
-/

/-
  TinylambOpt/BlockKleene.lean

  Block-level predicate pruning for a four-valued Kleene logic that also
  carries a runtime ERROR outcome (T / F / UNKNOWN / ERROR).

  Abstract semantics (stated explicitly, no external library):
    * a concrete row stores a Scalar per column: either a nullable Int
      (ok (some v) / ok none) or an evaluation error (err);
    * concrete evaluation uses strict (left-to-right, error-propagating)
      Kleene operators, so an ERROR produced by an earlier subexpression
      can never be cancelled by a later FALSE/TRUE;
    * the abstract value of a column summary is the *list of all Scalar
      values it admits*.  Abstract evaluation is the pointwise image of
      the concrete operators over those lists (flatMap for binary nodes).
      Duplicates are harmless; the list is a finite set of possible results.

  Soundness hypotheses (sufficient, made explicit in the theorem):
    (H1) every concrete row value of a column belongs to that column's
         summary list (the summary over-approximates the block);
    (H2) abstract operators are exactly the image of the concrete ones,
         hence `aeval_contains_ceval` holds by structural induction.

  `block_kleene_sound` then says: if the abstract result can be neither
  TRUE nor ERROR, every concrete row is rejected with no observable error,
  so the whole block may be skipped.  `block_all_pass_sound` is the dual:
  if the abstract result can only be TRUE, no row needs to be checked at
  all.  Two boundary theorems pin down the traps from the issue: an
  all-NULL block never makes NOT(x = 1) TRUE, and a leading CAST error
  survives AND/OR/NOT even when no TRUE is possible.

  C++ realization: executor/zone_map.cpp (per-column min/max/null/error
  flags and the composite-predicate zone check).  The implementation must
  use the same strict evaluation order as expression/; if it short-circuits
  AND/OR differently, the ERROR part of the abstract result is no longer
  an over-approximation and the skip is invalid.
-/

namespace TinylambOpt.BlockKleene

/-- The four observable outcomes of predicate evaluation. -/
inductive K4 where
  | t | f | u | e
  deriving DecidableEq, Repr

/-- Kleene NOT; ERROR is absorbing. -/
def kNot : K4 → K4
  | .t => .f
  | .f => .t
  | .u => .u
  | .e => .e

/-- Kleene AND with strict error propagation: ERROR dominates everything. -/
def kAnd : K4 → K4 → K4
  | .e, _ => .e
  | _, .e => .e
  | .f, _ => .f
  | _, .f => .f
  | .t, .t => .t
  | _, _ => .u

/-- Kleene OR with strict error propagation: ERROR dominates everything. -/
def kOr : K4 → K4 → K4
  | .e, _ => .e
  | _, .e => .e
  | .t, _ => .t
  | _, .t => .t
  | .f, .f => .f
  | _, _ => .u

/-- A concrete scalar: a nullable integer, or an evaluation error (e.g. a
    leading CAST that fails). -/
inductive Scalar where
  | ok : Option Int → Scalar
  | err : Scalar
  deriving DecidableEq, Repr

/-- SQL `=` with NULL-as-UNKNOWN and error propagation. -/
def cmpEq (c : Int) : Scalar → K4
  | .err => .e
  | .ok none => .u
  | .ok (some v) => if v = c then .t else .f

/-- SQL `IS NULL` with error propagation. -/
def isNull : Scalar → K4
  | .err => .e
  | .ok none => .t
  | .ok (some _) => .f

/-- The restricted predicate language: comparison, IS NULL, an always-failing
    leading CAST, NOT, AND, OR.  `col` is a column index. -/
inductive Expr where
  | cmp : Nat → Int → Expr
  | compareValue : Nat → (Int → Bool) → Expr
  | isNullExpr : Nat → Expr
  | castErr : Expr
  | notExpr : Expr → Expr
  | andExpr : Expr → Expr → Expr
  | orExpr : Expr → Expr → Expr

def compareScalar (p : Int → Bool) : Scalar → K4
  | .err => .e
  | .ok none => .u
  | .ok (some v) => if p v then .t else .f

/-- Concrete evaluator, following the evaluator's (strict) order. -/
def ceval (row : Nat → Scalar) : Expr → K4
  | .cmp col c => cmpEq c (row col)
  | .compareValue col p => compareScalar p (row col)
  | .isNullExpr col => isNull (row col)
  | .castErr => .e
  | .notExpr e => kNot (ceval row e)
  | .andExpr a b => kAnd (ceval row a) (ceval row b)
  | .orExpr a b => kOr (ceval row a) (ceval row b)

/-- A column summary admits a finite list of possible Scalar values. -/
abbrev Summary := Nat → List Scalar

/-- Abstract evaluator: the image of the concrete evaluator over every
    combination admitted by the summaries. -/
def aeval (s : Summary) : Expr → List K4
  | .cmp col c => (s col).map (cmpEq c)
  | .compareValue col p => (s col).map (compareScalar p)
  | .isNullExpr col => (s col).map isNull
  | .castErr => [.e]
  | .notExpr e => (aeval s e).map kNot
  | .andExpr a b => (aeval s a).flatMap (fun x => (aeval s b).map (kAnd x))
  | .orExpr a b => (aeval s a).flatMap (fun x => (aeval s b).map (kOr x))

/-- Core abstraction lemma: each concrete result is a member of the abstract
    result set, PROVIDED every row value is covered by its summary.  Proof by
    structural induction over the five expression forms. -/
theorem aeval_contains_ceval (s : Summary) (row : Nat → Scalar)
    (hrow : ∀ col, row col ∈ s col) (e : Expr) :
    ceval row e ∈ aeval s e := by
  induction e with
  | compareValue col p => exact List.mem_map_of_mem (hrow col)
  | cmp col c =>
      simpa [ceval, aeval] using List.mem_map.mpr ⟨row col, hrow col, rfl⟩
  | isNullExpr col =>
      simpa [ceval, aeval] using List.mem_map.mpr ⟨row col, hrow col, rfl⟩
  | castErr =>
      simp [ceval, aeval]
  | notExpr e ih =>
      simpa [ceval, aeval] using List.mem_map.mpr ⟨ceval row e, ih, rfl⟩
  | andExpr a b iha ihb =>
      simp only [ceval, aeval]
      apply List.mem_flatMap.mpr
      refine ⟨ceval row a, iha, ?_⟩
      exact List.mem_map.mpr ⟨ceval row b, ihb, rfl⟩
  | orExpr a b iha ihb =>
      simp only [ceval, aeval]
      apply List.mem_flatMap.mpr
      refine ⟨ceval row a, iha, ?_⟩
      exact List.mem_map.mpr ⟨ceval row b, ihb, rfl⟩

/-- MAIN: a block whose abstract result contains neither TRUE nor ERROR can
    be skipped: every consistent concrete row evaluates to F or U, i.e. is
    rejected by WHERE, and no observable error is lost. -/
theorem block_kleene_sound (s : Summary) (e : Expr)
    (hno : ∀ v ∈ aeval s e, v ≠ .t ∧ v ≠ .e) :
    ∀ row, (∀ col, row col ∈ s col) →
      ceval row e ≠ .t ∧ ceval row e ≠ .e := by
  intro row hrow
  exact hno _ (aeval_contains_ceval s row hrow e)

/-- Dual condition: when the abstract result can only be TRUE, every
    consistent row passes, so the row-level check may be omitted entirely. -/
theorem block_all_pass_sound (s : Summary) (e : Expr)
    (hall : ∀ v ∈ aeval s e, v = .t) :
    ∀ row, (∀ col, row col ∈ s col) → ceval row e = .t := by
  intro row hrow
  exact hall _ (aeval_contains_ceval s row hrow e)

/-- Boundary example 1: an all-NULL block makes `x = 1` UNKNOWN, so
    `NOT (x = 1)` stays UNKNOWN and never becomes TRUE. -/
theorem not_of_all_null_block_never_true :
    K4.t ∉ aeval (fun _ => [Scalar.ok none]) (Expr.notExpr (Expr.cmp 0 1)) := by
  decide

/-- Boundary example 2: a leading CAST error cannot be eliminated by a
    later FALSE/TRUE (AND, OR) or by NOT. -/
theorem leading_cast_error_survives_and :
    K4.e ∈ aeval (fun _ => [Scalar.ok (some 5)])
      (Expr.andExpr Expr.castErr (Expr.cmp 0 1)) := by
  decide

theorem leading_cast_error_survives_or :
    K4.e ∈ aeval (fun _ => [Scalar.ok (some 5)])
      (Expr.orExpr Expr.castErr (Expr.cmp 0 1)) := by
  decide

theorem leading_cast_error_survives_not :
    K4.e ∈ aeval (fun _ => [Scalar.ok (some 5)])
      (Expr.notExpr Expr.castErr) := by
  decide

/-- The ERROR side of the guard is essential: there is a block with no
    possible TRUE that still has an observable ERROR. -/
theorem no_true_but_error_is_observable :
    let a := aeval (fun _ => [Scalar.ok (some 5)])
      (Expr.andExpr Expr.castErr (Expr.cmp 0 1))
    K4.t ∉ a ∧ K4.e ∈ a := by
  decide

/-- The dual condition is achievable: an all-ones block gives only TRUE for
    `x = 1`. -/
theorem all_ones_block_all_pass :
    ∀ v ∈ aeval (fun _ => [Scalar.ok (some 1)]) (Expr.cmp 0 1), v = K4.t := by
  decide

/-- Construct the finite column summary from encoded dictionary values.
    Decoding (including a failed cast) happens before abstract comparison. -/
theorem dictionary_block_skip (rows : List (Nat → Nat)) (decode : Nat → Nat → Scalar)
    (e : Expr)
    (skip : ∀ v ∈ aeval (fun col => (rows.map (fun row => decode col (row col))).eraseDups) e,
      v ≠ .t ∧ v ≠ .e) :
    ∀ row ∈ rows, let result := ceval (fun col => decode col (row col)) e
                  result ≠ .t ∧ result ≠ .e := by
  intro row hr
  apply block_kleene_sound _ e skip
  intro col
  simpa using List.mem_map_of_mem (f := fun row => decode col (row col)) hr

end TinylambOpt.BlockKleene

-- C++ references to this model or its proof.
-- executor/parallel_scan.cpp
-- executor/zone_map.cpp
-- executor/zone_map.hpp

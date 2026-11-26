import TinylambOpt.ResearchGoals.BinaryCollationStringEquiJoinElision

/-!
Proved

- `Goal` — Binary collation residual-predicate elision for string equi-joins (STATS-CEB
  C4 / DictionaryJoinsAndLateLabels B2): under `Collation.binary`, eliding residual
  string equality into exact byte-equality inside the hash join matches `refJoin` for
  all hash functions (including total hash collisions) on both INNER and LEFT JOIN,
  whereas `asciiCaseInsensitive` and `padSpace` collations must keep residual checks.

The reference model is defined in
`TinylambOpt.ResearchGoals.BinaryCollationStringEquiJoinElision`.
-/

open TinylambOpt.ResearchGoals.BinaryCollationStringEquiJoinElision

namespace TinylambOpt.ResearchProofs.BinaryCollationStringEquiJoinElision

/-- Congruence for the emit / null-extend block around a candidate list. -/
private theorem blockEq {α : Type} (t : List α) (f : RRow → α) (A B : List RRow)
    (hAB : A = B) :
    (if A.isEmpty then t else List.map f A) = (if B.isEmpty then t else List.map f B) := by
  subst hAB
  rfl

private theorem flatMap_congr {α β : Type} (f g : α → List β) (hfg : ∀ x, f x = g x) :
    ∀ ls : List α, ls.flatMap f = ls.flatMap g := by
  intro ls
  induction ls with
  | nil => rfl
  | cons x xs ih => rw [List.flatMap_cons, List.flatMap_cons, hfg x, ih]

/-- Eliding the residual predicate preserves the join under `Collation.binary`,
    for both INNER and LEFT shape and any (possibly fully colliding) hash. -/
private theorem opt_eq_ref (isLeft : Bool) (h : List UInt8 → Nat) :
    ∀ (ls : List LRow) (rs : List RRow),
      optElidedJoin isLeft h ls rs = refJoin Collation.binary isLeft h ls rs := by
  intro ls rs
  unfold optElidedJoin refJoin
  apply flatMap_congr
  intro l
  cases hk : l.k with
  | none => simp
  | some lk =>
      -- Fuse the reference join's bucket-filter then residual-filter into one
      -- filter, then match it pointwise against the fused byte-equality test:
      -- under binary collation `collEq` is exactly `lk == rk` on `some` keys.
      simp only []
      rw [List.filter_filter]
      refine blockEq ?_ ?_ _ _ ?_
      apply List.filter_congr
      intro a _
      cases a.k with
      | none => rfl
      | some rk => exact Bool.and_comm _ _

private theorem exists_ci :
    ∃ (isLeft : Bool) (h : List UInt8 → Nat) (ls : List LRow) (rs : List RRow),
      optElidedJoin isLeft h ls rs ≠ refJoin Collation.asciiCaseInsensitive isLeft h ls rs :=
  ⟨false, fun _ => 1,
   [{ id := 1, k := some ([83, 81, 76] : List UInt8) }],
   [{ id := 2, k := some ([115, 113, 108] : List UInt8) }],
   by decide⟩

private theorem exists_pad :
    ∃ (isLeft : Bool) (h : List UInt8 → Nat) (ls : List LRow) (rs : List RRow),
      optElidedJoin isLeft h ls rs ≠ refJoin Collation.padSpace isLeft h ls rs :=
  ⟨true, fun _ => 1,
   [{ id := 1, k := some ([97] : List UInt8) }],
   [{ id := 2, k := some ([97, 32] : List UInt8) }],
   by decide⟩

theorem sound : TinylambOpt.ResearchGoals.BinaryCollationStringEquiJoinElision.Goal :=
  ⟨opt_eq_ref, exists_ci, exists_pad⟩

end TinylambOpt.ResearchProofs.BinaryCollationStringEquiJoinElision

-- C++ references to this model or its proof.
-- executor/detail/planning_heuristics.cpp
-- executor/detail/planning_heuristics.hpp

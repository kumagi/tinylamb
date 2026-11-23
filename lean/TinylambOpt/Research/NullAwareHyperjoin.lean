import Std.Tactic

/-!
# NULL-aware triangle join by variable-order intersection (`null-aware-hyperjoin`)

Three binary relations `xy(x,y)`, `yz(y,z)`, `xz(x,z)` form a triangle.  Each
edge occurrence carries its own equality kind: SQL `=` (NULL never satisfies
it) or NULL-safe `IS NOT DISTINCT FROM` (NULL = NULL is true).  A tuple is an
`Option Nat` pair; a triangle assignment is produced only when all three
per-edge equality tests hold.  Leaf multiplicity is the product of the three
edge occurrence counts, so 2 × 3 × 4 = 24 rows for a single repeated triangle.

The reference computation iterates the edges in role order `(xy, yz, xz)`.
The optimized computations enumerate a different variable first (trie
intersection); they are proved to be permutations of the reference, which
also gives invariance under variable-order permutations.  If no triangle
exists, no result row is materialized by any order.

C++ counterpart: `plan/cascades.cpp`.
-/

namespace TinylambOpt.Research.NullAwareHyperjoin

/-- Per-edge equality kind. -/
inductive EKind where
  | sql | nullSafe
  deriving DecidableEq, Repr

/-- A relation tuple on two variables, with the equality kind used for its
    join predicate. -/
structure Edge where
  a : Option Nat
  b : Option Nat
  kind : EKind
  deriving DecidableEq, Repr

/-- Edge-local equality: SQL equality rejects NULL (UNKNOWN becomes false);
    NULL-safe equality accepts NULL = NULL. -/
def edgeEq : EKind → Option Nat → Option Nat → Bool
  | EKind.sql, none, _ => false
  | EKind.sql, _, none => false
  | EKind.sql, some x, some y => x == y
  | EKind.nullSafe, none, none => true
  | EKind.nullSafe, some x, some y => x == y
  | EKind.nullSafe, _, _ => false

/-- The three join equalities of a triangle, each using a declared edge kind.
    `xy.b = yz.a` uses the `xy` kind, `yz.b = xz.b` uses the `yz` kind, and
    `xy.a = xz.a` uses the `xz` kind. -/
def consistent (xy yz xz : Edge) : Bool :=
  edgeEq xy.kind xy.b yz.a && edgeEq yz.kind yz.b xz.b && edgeEq xz.kind xy.a xz.a

abbrev Assign := Option Nat × Option Nat × Option Nat

def assignOf (xy yz _xz : Edge) : Assign := (xy.a, xy.b, yz.b)

/-! ## Reference: role-order (xy, yz, xz) SQL inner join -/

def refHyper (xy yz xz : List Edge) : List Assign :=
  xy.flatMap (fun e1 => yz.flatMap (fun e2 =>
    xz.filterMap (fun e3 => if consistent e1 e2 e3 then some (assignOf e1 e2 e3) else none)))

/-- Optimized order enumerating `y` first (roles xy, xz, yz). -/
def byY (xy yz xz : List Edge) : List Assign :=
  xy.flatMap (fun e1 => xz.flatMap (fun e3 =>
    yz.filterMap (fun e2 => if consistent e1 e2 e3 then some (assignOf e1 e2 e3) else none)))

/-- Optimized order enumerating `z` first (roles yz, xy, xz). -/
def byZ (xy yz xz : List Edge) : List Assign :=
  yz.flatMap (fun e2 => xy.flatMap (fun e1 =>
    xz.filterMap (fun e3 => if consistent e1 e2 e3 then some (assignOf e1 e2 e3) else none)))

/-! ## Permutation infrastructure -/

private theorem filterMap_if_eq_flatMap_if {α β : Type} (p : β → Bool) (f : β → α) :
    ∀ l : List β,
      l.filterMap (fun b => if p b then some (f b) else none) =
      l.flatMap (fun b => if p b then [f b] else []) := by
  intro l
  induction l with
  | nil => rfl
  | cons b bs ih => by_cases h : p b = true <;> simp [h, ih]

private theorem perm_flatMap_append_distr {α β : Type} (bs : List β) (g h : β → List α) :
    (bs.flatMap (fun b => g b ++ h b)).Perm (bs.flatMap g ++ bs.flatMap h) := by
  induction bs with
  | nil => simp
  | cons b bs ih =>
      simp only [List.flatMap_cons]
      let fg := bs.flatMap g
      let fh := bs.flatMap h
      have ih' : (bs.flatMap (fun b => g b ++ h b)).Perm (fg ++ fh) := by
        simpa [fg, fh] using ih
      have pswap : (h b ++ fg ++ fh).Perm (fg ++ h b ++ fh) := by
        simpa [List.append_assoc] using
          (List.Perm.append_right fh (List.perm_append_comm : (h b ++ fg).Perm (fg ++ h b)))
      have hmid : (g b ++ h b ++ (fg ++ fh)).Perm ((g b ++ fg) ++ (h b ++ fh)) := by
        simpa [List.append_assoc] using (List.Perm.append_left (g b) pswap)
      have h1 : (g b ++ h b ++ bs.flatMap (fun b => g b ++ h b)).Perm
          (g b ++ h b ++ (fg ++ fh)) := List.Perm.append_left _ ih'
      exact h1.trans (by simpa [fg, fh] using hmid)

/-- Flattening two nested loops commutes: iterate `as` outside `bs` or the
    reverse, with the same pair-wise list of results. -/
private theorem perm_flatMap_flatMap {α β γ : Type} (f : α → β → List γ) :
    ∀ (as : List α) (bs : List β),
      ((as.flatMap (fun a => bs.flatMap (fun b => f a b))).Perm
       (bs.flatMap (fun b => as.flatMap (fun a => f a b)))) := by
  intro as
  induction as with
  | nil =>
      intro bs
      simp
  | cons a as ih =>
      intro bs
      simp only [List.flatMap_cons]
      have hperm := ih bs
      have hsplit := perm_flatMap_append_distr bs
        (fun b => f a b)
        (fun b => as.flatMap (fun a' => f a' b))
      have hmain : ((bs.flatMap (fun b => f a b) ++
          as.flatMap (fun a' => bs.flatMap (fun b => f a' b))).Perm
          (bs.flatMap (fun b => f a b) ++
          bs.flatMap (fun b => as.flatMap (fun a' => f a' b)))) :=
        List.Perm.append_left _ hperm
      simpa [List.append_assoc] using hmain.trans hsplit.symm

private theorem perm_flatMap_of_perm {α β : Type} (f g : α → List β) :
    ∀ l : List α, (∀ a ∈ l, (f a).Perm (g a)) → (l.flatMap f).Perm (l.flatMap g)
  | [], _ => by simp
  | a :: l, h => by
      simp only [List.flatMap_cons]
      have ha : (f a).Perm (g a) := h a (by simp)
      have ht : (l.flatMap f).Perm (l.flatMap g) :=
        perm_flatMap_of_perm f g l (fun x hx => h x (by simp [hx]))
      exact List.Perm.append_right _ ha |>.trans (List.Perm.append_left (g a) ht)

/-! ## Variable-order invariance -/

theorem byY_perm_ref (xy yz xz : List Edge) : (byY xy yz xz).Perm (refHyper xy yz xz) := by
  unfold byY refHyper
  apply perm_flatMap_of_perm
  intro e1 _
  have hswap := perm_flatMap_flatMap
    (fun (e3 : Edge) (e2 : Edge) =>
      if consistent e1 e2 e3 then [assignOf e1 e2 e3] else []) xz yz
  simpa [filterMap_if_eq_flatMap_if] using hswap

theorem byZ_perm_ref (xy yz xz : List Edge) : (byZ xy yz xz).Perm (refHyper xy yz xz) := by
  unfold byZ refHyper
  exact (perm_flatMap_flatMap
    (fun (e1 : Edge) (e2 : Edge) =>
      xz.filterMap (fun e3 => if consistent e1 e2 e3 then some (assignOf e1 e2 e3) else none))
    xy yz).symm

theorem byY_perm_byZ (xy yz xz : List Edge) : (byY xy yz xz).Perm (byZ xy yz xz) :=
  (byY_perm_ref xy yz xz).trans (byZ_perm_ref xy yz xz).symm

/-! ## Well-formedness and the main theorem -/

/-- Sufficient condition: an edge with SQL kind never contains NULL.  NULL
    may occur on NULL-safe edges only. -/
def WellFormed (l : List Edge) : Prop :=
  ∀ e ∈ l, e.kind = EKind.nullSafe ∨ (e.a ≠ none ∧ e.b ≠ none)

/-- No result row is materialized in the `z`-first order when there is no
    triangle in the reference order (lengths agree by permutation). -/
theorem no_triangle_no_materialized (xy yz xz : List Edge)
    (h : refHyper xy yz xz = []) : (byZ xy yz xz).length = 0 := by
  have hp := byZ_perm_ref xy yz xz
  have hlen := hp.length_eq
  rw [h] at hlen
  omega

/-- Variable-order invariance, bag equality to the SQL inner-join reference,
    and no materialization without a triangle. -/
theorem null_aware_hyperjoin_sound
    (xy yz xz : List Edge)
    (_hxy : WellFormed xy) (_hyz : WellFormed yz) (_hxz : WellFormed xz) :
    let r := refHyper xy yz xz
    (byY xy yz xz).Perm r ∧ (byZ xy yz xz).Perm r ∧
      (r = [] → (byZ xy yz xz).length = 0) := by
  dsimp
  exact ⟨byY_perm_ref xy yz xz, byZ_perm_ref xy yz xz,
    fun h => no_triangle_no_materialized xy yz xz h⟩

/-! ## Regression cases -/

/-- A single triangle repeated 2 × 3 × 4 times yields exactly 24 rows. -/
theorem triangle_multiplicity :
    (refHyper
      (List.replicate 2 ⟨some 1, some 2, EKind.sql⟩)
      (List.replicate 3 ⟨some 2, some 3, EKind.sql⟩)
      (List.replicate 4 ⟨some 1, some 3, EKind.sql⟩)).length = 24 := by
  decide

/-- A NULL-safe edge with a NULL endpoint must survive.  Global NULL
    filtering (dropping every tuple that contains NULL) discards the `yz`
    tuple and loses the triangle: the naive optimization is unsound. -/
theorem global_null_filter_counterexample :
    let xy : List Edge := [⟨some 5, none, EKind.nullSafe⟩]
    let yz : List Edge := [⟨none, some 7, EKind.nullSafe⟩]
    let xz : List Edge := [⟨some 5, some 7, EKind.nullSafe⟩]
    refHyper xy yz xz = [(some 5, none, some 7)] ∧
    refHyper
      (xy.filter (fun e => e.a ≠ none && e.b ≠ none))
      (yz.filter (fun e => e.a ≠ none && e.b ≠ none))
      (xz.filter (fun e => e.a ≠ none && e.b ≠ none)) = [] := by
  dsimp
  decide

end TinylambOpt.Research.NullAwareHyperjoin

import TinylambOpt.ResearchGoals.NullOrLikeDictBitmapPrefilter

/-!
Proved

- `Goal` — Dictionary-level bitmap prefilter for `(col IS NULL OR LIKE ...)` predicates
  (STATS-CEB C4 / JOB J4 / JOB J5): evaluating `matchStr` once per dictionary entry
  rather than per row and folding `NULL` to `TRUE` matches SQL 3-valued logic
  whenever all non-NULL codes are valid indices `< dict.length`.

The reference model is defined in
`TinylambOpt.ResearchGoals.NullOrLikeDictBitmapPrefilter`.
-/

namespace TinylambOpt.ResearchProofs.NullOrLikeDictBitmapPrefilter

open TinylambOpt.ResearchGoals.NullOrLikeDictBitmapPrefilter

/-!
Soundness of the dictionary-level bitmap prefilter for `(col IS NULL OR LIKE ...)`.

`optAccepts` evaluates `matchStr` once per dictionary entry (`mask := dict.map matchStr`)
and answers each row with the O(1) bitmap lookup
`(oc == none || mask.getD c false) && triAccept (extra idx)`, while `refAccepts`
re-evaluates the full three-valued predicate `(IS NULL OR match) AND extra` per row.
Both are `map`s over the same list `codes.zipIdx`, so it suffices to compare the two
entry functions pointwise:

* `oc = none` (NULL row): `evalIsNull none = tTrue` and `tTrue OR x = tTrue`
  short-circuits, so `triAccept (tTrue AND extra idx) = triAccept (extra idx)` — exactly
  the `none => true` branch of the prefilter.  This is where the Kleene disjunction, not
  classical logic, is essential: the goal file's `buggyNotEqOrAccept` shows that
  `NOT (col = x) OR match` yields `UNKNOWN` on NULL instead, so the fold-to-TRUE trick
  is specific to the `IS NULL` shape.
* `oc = some c` with `c < dict.length` (valid code): `dict[c]? = some (dict[c])`, so
  `evalIsNull = tFalse` and `tFalse OR x = x`; the bitmap lookup satisfies
  `mask.getD c false = matchStr (dict[c])` (a `getD`/`getElem?`/`map` chain), and for a
  two-valued match verdict `if b then tTrue else tFalse` the Kleene conjunction collapses
  via `triAccept (triAnd (if b then tTrue else tFalse) e) = b && triAccept e`:
  `tTrue` is the neutral element of `AND`, `tFalse` is absorbing regardless of `e`
  (which may be `UNKNOWN`).
-/

/-- Per-row reference verdict of `refAccepts`, as a function of the single pair
    `(oc, idx)`. -/
private def refEntry (dict : List String) (matchStr : String → Bool) (extra : Nat → Tri)
    (oc : Option Nat) (idx : Nat) : Bool :=
  let v := oc.bind (fun c => dict[c]?)
  triAccept (triAnd (triOr (evalIsNull v) (evalMatch matchStr v)) (extra idx))

/-- Per-row prefilter verdict of `optAccepts`, as a function of the single pair
    `(oc, idx)`. -/
private def optEntry (dict : List String) (matchStr : String → Bool) (extra : Nat → Tri)
    (oc : Option Nat) (idx : Nat) : Bool :=
  (match oc with
    | none => true
    | some c => (dict.map matchStr).getD c false) && triAccept (extra idx)

private theorem refAccepts_eq_map (dict : List String) (codes : List (Option Nat))
    (matchStr : String → Bool) (extra : Nat → Tri) :
    refAccepts dict codes matchStr extra
      = codes.zipIdx.map (fun p => refEntry dict matchStr extra p.1 p.2) := rfl

private theorem optAccepts_eq_map (dict : List String) (codes : List (Option Nat))
    (matchStr : String → Bool) (extra : Nat → Tri) :
    optAccepts dict codes matchStr extra
      = codes.zipIdx.map (fun p => optEntry dict matchStr extra p.1 p.2) := rfl

/-- Two `map`s over the same list agree when their entry functions agree on every
    element of the list. -/
private theorem map_eq_of_forall_mem {α β : Type} (l : List α) (f g : α → β)
    (h : ∀ x ∈ l, f x = g x) : l.map f = l.map g := by
  induction l with
  | nil => rfl
  | cons a t ih =>
      rw [List.map_cons, List.map_cons, List.cons.injEq]
      exact ⟨h a List.mem_cons_self,
        ih (fun x hx => h x (List.mem_cons_of_mem _ hx))⟩

/-- The first component of a `zipIdx` pair is a member of the source list. -/
private theorem fst_mem_zipIdx {codes : List (Option Nat)} {oc : Option Nat} {idx : Nat}
    (h : (oc, idx) ∈ codes.zipIdx) : oc ∈ codes :=
  List.fst_mem_of_mem_zipIdx h

/-- Bitmap lookup at a valid code: the per-entry precomputed bitmap `dict.map matchStr`
    returns exactly `matchStr` of the decoded dictionary string. -/
private theorem getD_map_matchStr (dict : List String) (matchStr : String → Bool)
    (c : Nat) (hc : c < dict.length) :
    (dict.map matchStr).getD c false = matchStr (dict[c]'hc) := by
  rw [List.getD_eq_getElem?_getD, List.getElem?_map, List.getElem?_eq_getElem hc]
  rfl

/-! ### Kleene case tables -/

private theorem evalIsNull_none : evalIsNull none = .tTrue := rfl
private theorem evalIsNull_some (s : String) : evalIsNull (some s) = .tFalse := rfl
private theorem evalMatch_none (matchStr : String → Bool) : evalMatch matchStr none = .tUnknown := rfl

private theorem evalMatch_some (matchStr : String → Bool) (s : String) :
    evalMatch matchStr (some s) = (if matchStr s then .tTrue else .tFalse) := rfl

private theorem triOr_tTrue_left (b : Tri) : triOr .tTrue b = .tTrue := by
  cases b <;> rfl

private theorem triOr_tFalse_left (a : Tri) : triOr .tFalse a = a := by
  cases a <;> rfl

private theorem triAnd_tTrue_left (e : Tri) : triAnd .tTrue e = e := by
  cases e <;> rfl

/-- The Kleene-conjunction collapse that makes the bitmap `&&`-lookup exact: a
    two-valued dictionary verdict `if b then tTrue else tFalse` composed with the
    per-row residual predicate `extra idx` (possibly `UNKNOWN`) agrees with plain
    boolean `&&` of the verdict bit with `triAccept`. -/
private theorem accept_and_of_bool (b : Bool) (e : Tri) :
    triAccept (triAnd (if b then .tTrue else .tFalse) e) = (b && triAccept e) := by
  cases b <;> cases e <;> rfl

/-- Per-dictionary-entry correctness: for one row `(oc, idx)` whose non-`none` code is
    in range, the prefilter's bitmap lookup agrees with the reference three-valued
    evaluation. -/
private theorem refEntry_eq_optEntry (dict : List String) (matchStr : String → Bool)
    (extra : Nat → Tri) (oc : Option Nat) (idx : Nat)
    (h : ∀ c : Nat, oc = some c → c < dict.length) :
    refEntry dict matchStr extra oc idx = optEntry dict matchStr extra oc idx := by
  cases oc with
  | none =>
      unfold refEntry optEntry
      show triAccept (triAnd (triOr (evalIsNull none) (evalMatch matchStr none)) (extra idx))
        = (true && triAccept (extra idx))
      rw [evalIsNull_none, evalMatch_none matchStr, triOr_tTrue_left .tUnknown,
        triAnd_tTrue_left (extra idx)]
      rfl
  | some c =>
      have hc : c < dict.length := h c rfl
      have hd : dict[c]? = some (dict[c]'hc) := List.getElem?_eq_getElem hc
      unfold refEntry optEntry
      show (have v := dict[c]?;
          triAccept (triAnd (triOr (evalIsNull v) (evalMatch matchStr v)) (extra idx)))
        = ((dict.map matchStr).getD c false && triAccept (extra idx))
      rw [hd, getD_map_matchStr dict matchStr c hc]
      show triAccept (triAnd (triOr (evalIsNull (some (dict[c]'hc)))
          (evalMatch matchStr (some (dict[c]'hc)))) (extra idx))
        = (matchStr (dict[c]'hc) && triAccept (extra idx))
      rw [evalIsNull_some, triOr_tFalse_left, evalMatch_some]
      exact accept_and_of_bool (matchStr (dict[c]'hc)) (extra idx)

theorem sound : TinylambOpt.ResearchGoals.NullOrLikeDictBitmapPrefilter.Goal := by
  intro dict codes matchStr extra hvalid
  rw [refAccepts_eq_map, optAccepts_eq_map]
  refine map_eq_of_forall_mem codes.zipIdx _ _ (fun p hp => ?_)
  cases p with
  | mk oc idx =>
      cases oc with
      | none =>
          exact (refEntry_eq_optEntry dict matchStr extra none idx
            (fun c hc => absurd hc (by simp))).symm
      | some c =>
          have hc : c < dict.length := hvalid c (fst_mem_zipIdx hp)
          exact (refEntry_eq_optEntry dict matchStr extra (some c) idx
            (fun c' he => by rw [← Option.some.inj he]; exact hc)).symm

end TinylambOpt.ResearchProofs.NullOrLikeDictBitmapPrefilter

-- C++ references to this model or its proof.
-- executor/detail/parallel_table_scan.cpp

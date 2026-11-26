import TinylambOpt.ResearchGoals.JitKernels.CodeReachability

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# IdentityPreservingRelocation (proved)

Claim: relabelling every object key and every embedded call target through an
injective relocation `f` commutes with tracing:
`f k ∈ mark (relabel f heap) (roots.map f) n ↔ k ∈ mark heap roots n`, i.e.
relinking all direct calls under an injective code-id map preserves exactly the
reachable set (up to the relocation).

Proof sketch.  Injectivity gives `(f a == f b) = (a == b)` (`beq_inj`), hence
`eraseDups` commutes with `map f` (`eraseDups_map`, via a filter-transfer
lemma `filter_map_eq`).  Lookup commutes too:
`(heap.map (relabel f)).find? (·.key == f k) = (heap.find? (·.key == k)).map
(relabel f)` (`find?_map`, by induction with `find?_cons_of_pos/neg`), which
yields `edges (relabel f heap) (f x) = (edges heap x).map f` (`edges_map`) and,
with a flatMap transfer lemma (`flatMap_map`), the commuting of `mark` with the
relocation for every fuel (`mark_map`, induction on fuel unfolding `grow`).
The conclusion is then `f k ∈ l.map f ↔ k ∈ l` (`List.mem_map` plus
injectivity). -/

namespace TinylambOpt.ResearchProofs.JitKernels.CodeReachability_IdentityPreservingRelocation
open TinylambOpt.ResearchGoals.JitKernels.CodeReachability

private theorem not_true_eq_false {b : Bool} (h : ¬(b = true)) : b = false := by
  cases b with
  | false => rfl
  | true => exact absurd rfl h

private def relabel (f : Key → Key) (o : Obj) : Obj :=
  { o with key := f o.key, calls := o.calls.map f }

private theorem beq_inj (f : Key → Key) (inj : ∀ a b, f a = f b → a = b) (a b : Key) :
    (f a == f b) = (a == b) := by
  have h : ((f a == f b) = true) ↔ ((a == b) = true) := by
    rw [beq_iff_eq, beq_iff_eq]
    exact ⟨inj a b, fun hab => congrArg f hab⟩
  cases hab : (a == b) with
  | true =>
      cases hfab : (f a == f b) with
      | true => rfl
      | false => exact absurd (h.mpr hab) (by rw [hfab]; simp)
  | false =>
      cases hfab : (f a == f b) with
      | true => exact absurd (h.mp hfab) (by rw [hab]; simp)
      | false => rfl

private theorem filter_map_eq (f : Key → Key) (p : Key → Bool) (r : Key → Bool)
    (h : (x : Key) → r x = p (f x)) :
    (l : List Key) → (l.filter r).map f = (l.map f).filter p
  | [] => rfl
  | a :: as => by
      cases hra : (r a) with
      | true =>
          rw [List.filter_cons_of_pos hra, List.map_cons, List.map_cons,
            List.filter_cons_of_pos (show p (f a) = true from by rw [← h a, hra]),
            filter_map_eq f p r h as]
      | false =>
          have hn : ¬ r a := by
            intro he
            rw [hra] at he
            cases he
          have hn2 : ¬ p (f a) := by
            intro he
            rw [← h a] at he
            exact hn he
          rw [List.filter_cons_of_neg hn, List.map_cons,
            List.filter_cons_of_neg hn2, filter_map_eq f p r h as]

private theorem eraseDups_map (f : Key → Key) (inj : ∀ a b, f a = f b → a = b) :
    (l : List Key) → (l.map f).eraseDups = (l.eraseDups).map f
  | [] => rfl
  | a :: as => by
      rw [List.map_cons, List.eraseDups_cons, List.eraseDups_cons, List.map_cons,
        ← filter_map_eq f (fun b => !(b == f a)) (fun b => !(b == a))
          (fun b => congrArg Bool.not ((beq_inj f inj b a).symm)),
        eraseDups_map f inj (List.filter (fun b => !(b == a)) as)]
  termination_by l => l.length
  decreasing_by
    show (List.filter (fun b => !(b == a)) as).length < (a :: as).length
    have hle := List.length_filter_le (fun b => !(b == a)) as
    rw [List.length_cons]
    omega

private theorem find?_map (f : Key → Key) (inj : ∀ a b, f a = f b → a = b) (k : Key) :
    (heap : List Obj) →
      (heap.map (relabel f)).find? (fun o => o.key == f k) =
        (heap.find? (fun o => o.key == k)).map (relabel f)
  | [] => rfl
  | o :: os => by
      rw [List.map_cons]
      have hk : ((relabel f o).key == f k) = (o.key == k) := beq_inj f inj o.key k
      by_cases h : (o.key == k) = true
      · rw [List.find?_cons_of_pos (p := fun o => o.key == f k) (a := relabel f o)
            (show ((relabel f o).key == f k) = true from by rw [hk]; exact h),
          List.find?_cons_of_pos (p := fun o => o.key == k) (a := o) h,
          Option.map_some]
      · rw [List.find?_cons_of_neg (p := fun o => o.key == f k) (a := relabel f o)
            (show ¬((relabel f o).key == f k) = true from by
              intro hc; rw [hk] at hc; exact h hc),
          List.find?_cons_of_neg (p := fun o => o.key == k) (a := o) h,
          find?_map f inj k os]

private theorem edges_map (f : Key → Key) (inj : ∀ a b, f a = f b → a = b)
    (heap : List Obj) (x : Key) :
    edges (heap.map (relabel f)) (f x) = (edges heap x).map f := by
  unfold edges
  rw [find?_map f inj x heap]
  cases heap.find? (fun o => o.key == x) with
  | none => rfl
  | some o => rfl

private theorem flatMap_map (g : Key → Key) (h : Key → List Key) (h2 : Key → List Key)
    (heq : (x : Key) → h (g x) = (h2 x).map g) :
    (l : List Key) → (l.map g).flatMap h = (l.flatMap h2).map g
  | [] => rfl
  | a :: as => by
      rw [List.map_cons, List.flatMap_cons, List.flatMap_cons, heq a,
        flatMap_map g h h2 heq as, List.map_append]

private theorem mark_succ (heap : List Obj) (roots : List Key) (n : Nat) :
    mark heap roots (n+1) = grow heap (mark heap roots n) := rfl

private theorem mark_map (f : Key → Key) (inj : ∀ a b, f a = f b → a = b)
    (heap : List Obj) (roots : List Key) :
    (n : Nat) → mark (heap.map (relabel f)) (roots.map f) n =
      (mark heap roots n).map f
  | 0 => eraseDups_map f inj roots
  | n+1 => by
      rw [mark_succ, mark_succ, mark_map f inj heap roots n]
      show grow (heap.map (relabel f)) ((mark heap roots n).map f) =
        (grow heap (mark heap roots n)).map f
      unfold grow
      rw [flatMap_map f (edges (heap.map (relabel f)))
        (edges heap) (fun x => edges_map f inj heap x),
        ← List.map_append,
        ← eraseDups_map f inj (mark heap roots n ++
          (mark heap roots n).flatMap (edges heap))]

theorem sound : IdentityPreservingRelocation.Goal := by
  intro heap roots f inj n k
  change f k ∈ mark (heap.map (relabel f)) (roots.map f) n ↔ k ∈ mark heap roots n
  rw [mark_map f inj heap roots n]
  constructor
  · intro h
    obtain ⟨x, hx, heq⟩ := List.mem_map.mp h
    exact (inj x k heq) ▸ hx
  · intro h
    exact List.mem_map.mpr ⟨k, h, rfl⟩

end TinylambOpt.ResearchProofs.JitKernels.CodeReachability_IdentityPreservingRelocation

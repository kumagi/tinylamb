import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

/-!
This module audits the exact guarded-version dispatch goals. It proves the
sound guard, canonicalization, coalescing, pruning, uniform-row, and matching-set
properties, and gives concrete refutations for reordering, arbitrary common-bit
coalescing, and unchecked last-hit reuse. The positive results identify when SQL
execution may change guard representation or dispatch-chain layout without
changing the modeled selected code; the negative results show which additional
conditions such changes require.
-/

namespace TinylambOpt.ResearchProofs.JitKernels.VersionDispatchProof

open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

/-- Mask containment and agreement on common required bits exactly characterize
implication between guards. Thus a dominated guard may be removed without
admitting any new feature word. -/
theorem bitGuardImpliesWeaker : BitGuardImpliesWeaker.Goal := by
  intro a b
  cases a with
  | mk am av ac =>
    cases b with
    | mk bm bv bc =>
      constructor
      · intro h w hb
        simp only [weaker, clash, Bool.and_eq_true, accepts] at h hb
        have hmask := h.1
        have hclash : ((av ^^^ bv) &&& am &&& bm) = 0 := by
          simpa using h.2
        simp only [accepts]
        bv_decide
      · intro h
        simp only [accepts] at h
        let w : Word := (bv &&& bm) ||| (~~~av &&& ~~~bm)
        have hbw : ((w &&& bm) == (bv &&& bm)) = true := by
          simp only [w]
          bv_decide
        have haw := h w hbw
        simp only [beq_iff_eq] at haw
        simp only [weaker, clash, Bool.and_eq_true]
        clear h hbw
        constructor
        · bv_decide
        · simp only [w] at haw
          bv_decide

/-- Two guards clash exactly when no feature word can satisfy both, so clash
detection is a complete static disjointness test for compiled versions. -/
theorem commonBitsDetectDisjointness : CommonBitsDetectDisjointness.Goal := by
  intro a b
  cases a with
  | mk am av _ =>
    cases b with
    | mk bm bv _ =>
      simp only [clash, bne_iff_ne]
      constructor
      · intro hc ⟨w, ha, hb⟩
        simp only [accepts] at ha hb
        bv_decide
      · intro hn
        refine fun hz => hn ⟨(av &&& am) ||| (bv &&& bm), ?_, ?_⟩ <;>
          simp only [accepts] <;> bv_decide

/-- Guard implication does not require equal code bodies, so two mutually
compatible guards with different bodies remain order-sensitive. This refutes
the claim that guard weakness alone permits specificity sorting. -/
theorem specificitySortDispatch_refuted : ¬ SpecificitySortDispatch.Goal := by
  intro h
  let a : Entry := { mask := 0, value := 0, code := 1 }
  let b : Entry := { mask := 0, value := 0, code := 2 }
  have hw : weaker a b := by decide
  have hbad := h a b 0 hw
  have hne : dispatch [a, b] 0 ≠ dispatch [b, a] 0 := by decide
  exact hne hbad

/-- Masking a guard's stored value by its own mask does not change which
feature words it accepts, so irrelevant bits may be dropped from cache keys. -/
theorem canonicalGuardCodeKey : CanonicalGuardCodeKey.Goal := by
  intro e w
  cases e with
  | mk m v c => simp [accepts, BitVec.and_assoc]

/-- Equal bodies do not make arbitrary common-bit coalescing sound: a word can
satisfy every agreed bit while mixing the guards' conflicting requirements and
matching neither original guard. -/
theorem commonRequirementsCoverUnion_refuted :
    ¬ CommonRequirementsCoverUnion.Goal := by
  intro h
  let a : Entry := { mask := 3, value := 0, code := 7 }
  let b : Entry := { mask := 3, value := 3, code := 7 }
  have hbad := h a b 1 (by decide) (by decide)
  have hne : dispatch [a, b] 1 ≠
      dispatch [{ a with mask := a.mask &&& ~~~(a.value ^^^ b.value) }] 1 := by
    decide
  exact hne hbad

/-- Removing a rejected entry from an appended suffix preserves first-match
dispatch; this is the list-level operation used by shadow-chain pruning. -/
private theorem find?_remove_rejected_append {α : Type} (p : α → Bool)
    (xs tail : List α) (b : α) (hb : p b = false) :
    (xs ++ b :: tail).find? p = (xs ++ tail).find? p := by
  induction xs with
  | nil => simp [hb]
  | cons x xs ih =>
    simp only [List.cons_append, List.find?_cons]
    cases hx : p x <;> simp [hx, ih]

/-- If an earlier guard implies a later one, the later entry is shadowed
wherever it occurs after the earlier guard. SQL execution may therefore prune
that entry without changing the selected code or generic miss. -/
theorem shadowChainPruning : ShadowChainPruning.Goal := by
  intro a b rest tail w hweak
  have himp := (bitGuardImpliesWeaker a b).mp hweak
  cases ha : accepts a w
  · have hb : accepts b w = false := by
      cases hbval : accepts b w
      · rfl
      · have hcontra := himp w hbval
        simp [ha] at hcontra
    simp [dispatch, ha, find?_remove_rejected_append, hb]
  · simp [dispatch, ha]

/-- Rechecking only the cached last match is insufficient: an earlier guard
that rejected the old word may accept the fresh word and outrank the cached
entry under ordered dispatch. -/
theorem reuseLastPassingEntry_refuted : ¬ ReuseLastPassingEntry.Goal := by
  intro h
  let hi : Entry := { mask := 2, value := 0, code := 1 }
  let e : Entry := { mask := 0, value := 0, code := 2 }
  let es : List Entry := [hi, e]
  have hfind : es.find? (accepts · 2) = some e := by decide
  have hfresh : accepts e 0 := by decide
  have hbad := h es 2 0 e hfind hfresh
  have hne : dispatch es 0 ≠ some e.code := by decide
  exact hne hbad

/-- Every word belonging to a list is contained in the OR accumulated over the
list. This is the mask-containment fact behind uniform-row guard hoisting. -/
private theorem mem_le_foldl_or (xs : List Word) (acc : Word) {x : Word}
    (hx : x ∈ xs) : x &&& xs.foldl (· ||| ·) acc = x := by
  induction xs generalizing acc with
  | nil => simp at hx
  | cons y ys ih =>
    simp only [List.foldl_cons]
    rcases List.mem_cons.mp hx with rfl | hx
    · bv_decide
    · have h := ih acc hx
      bv_decide

/-- Pointwise equality of Boolean guard tests makes `find?` return the same
entry, which in turn makes `dispatch` return the same optional code. -/
private theorem find?_eq_of_pointwise {α : Type} (p q : α → Bool)
    (xs : List α) (h : ∀ e ∈ xs, p e = q e) :
    xs.find? p = xs.find? q := by
  induction xs with
  | nil => rfl
  | cons e es ih =>
    have he := h e (by simp)
    have htail : ∀ f ∈ es, p f = q f := by
      intro f hf
      exact h f (by simp [hf])
    simp only [List.find?_cons, he]
    exact ih htail

/-- Words agreeing on the union of all guard masks agree on every individual
guard. A batch with that uniform projection can therefore reuse one dispatch
result without changing any selected code identity. -/
theorem precomputeGuardOnUniformRows : PrecomputeGuardOnUniformRows.Goal := by
  intro es a b
  dsimp only
  intro hab
  unfold dispatch
  congr 1
  apply find?_eq_of_pointwise
  intro e he
  cases e with
  | mk m v c =>
    have hm := mem_le_foldl_or (({ mask := m, value := v, code := c } :: es).map
      Entry.mask) 0 (by simp)
    simp only [Entry.mask, List.map_cons, List.foldl_cons] at hm
    simp only [accepts]
    bv_decide

end TinylambOpt.ResearchProofs.JitKernels.VersionDispatchProof
